import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
applications_table = dynamodb.Table(os.environ["APPLICATIONS_TABLE"])
tasks_table = dynamodb.Table(os.environ["TASKS_TABLE"])

ALLOWED_STATUSES = {
    "Saved", "Applied", "Assessment", "Interview",
    "Offer", "Rejected", "Withdrawn"
}
EDITABLE_FIELDS = {
    "company", "role", "status", "jobUrl", "location",
    "stipend", "deadline", "dateApplied", "notes", "nextAction"
}

TASK_STATUSES = {"Pending", "In Progress", "Completed", "Cancelled"}
TASK_TYPES = {"Assessment", "Interview", "Follow-up", "Deadline", "Other"}
EDITABLE_TASK_FIELDS = {
    "title", "description", "dueDate", "type", "status", "applicationId"
}


def json_default(value):
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization,X-User-Id",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS"
        },
        "body": json.dumps(body, default=json_default)
    }


def parse_body(event):
    raw_body = event.get("body") or {}
    if isinstance(raw_body, str):
        return json.loads(raw_body)
    return raw_body


def get_method(event):
    return (
        event.get("requestContext", {}).get("http", {}).get("method")
        or event.get("httpMethod", "GET")
    ).upper()


def get_path(event):
    return (event.get("rawPath") or event.get("path") or "/").rstrip("/") or "/"


def parse_resource_path(path):
    parts = [part for part in path.split("/") if part]

    for i, part in enumerate(parts):
        if part in {"applications", "tasks", "resumes"}:
            tail = parts[i + 1:]

            if len(tail) > 1:
                return part, None, False

            return part, tail[0] if tail else None, True

    return None, None, False


def get_user_id(event):
    headers = {str(k).lower(): v for k, v in (event.get("headers") or {}).items()}
    # Development only: caller-controlled identity. Replace with verified auth before public deployment.
    return headers.get("x-user-id", "demo-user")


def handle_applications(method, application_id, user_id, event):
    if method == "GET" and application_id is None:
        result = applications_table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
        return response(200, {"applications": result.get("Items", [])})

    if method == "POST" and application_id is None:
        body = parse_body(event)
        if not isinstance(body, dict):
            return response(400, {"error": "Body must be a JSON object"})

        company = body.get("company")
        role = body.get("role")
        if not isinstance(company, str) or not company.strip():
            return response(400, {"error": "company is required"})
        if not isinstance(role, str) or not role.strip():
            return response(400, {"error": "role is required"})

        status = body.get("status", "Saved")
        if status not in ALLOWED_STATUSES:
            return response(400, {"error": "Invalid application status"})

        now = datetime.now(timezone.utc).isoformat()
        item = {
            "userId": user_id,
            "applicationId": str(uuid.uuid4()),
            "company": company.strip(),
            "role": role.strip(),
            "status": status,
            "createdAt": now,
            "updatedAt": now
        }
        for field in EDITABLE_FIELDS - {"company", "role", "status"}:
            if body.get(field) is not None:
                item[field] = body[field]

        applications_table.put_item(Item=item)
        logger.info("Created application %s", item["applicationId"])
        return response(201, {"application": item})

    if method == "PUT" and application_id:
        body = parse_body(event)
        if not isinstance(body, dict):
            return response(400, {"error": "Body must be a JSON object"})

        updates = {key: value for key, value in body.items() if key in EDITABLE_FIELDS}
        if not updates:
            return response(400, {"error": "No editable fields provided"})

        for field in ("company", "role"):
            if field in updates:
                if not isinstance(updates[field], str) or not updates[field].strip():
                    return response(400, {"error": f"{field} cannot be empty"})
                updates[field] = updates[field].strip()

        if "status" in updates and updates["status"] not in ALLOWED_STATUSES:
            return response(400, {"error": "Invalid application status"})

        updates["updatedAt"] = datetime.now(timezone.utc).isoformat()
        expression_names = {f"#f{i}": field for i, field in enumerate(updates)}
        expression_values = {f":v{i}": value for i, value in enumerate(updates.values())}
        update_expression = "SET " + ", ".join(
            f"#f{i} = :v{i}" for i in range(len(updates))
        )

        result = applications_table.update_item(
            Key={"userId": user_id, "applicationId": application_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values,
            ConditionExpression="attribute_exists(applicationId)",
            ReturnValues="ALL_NEW"
        )
        logger.info("Updated application %s", application_id)
        return response(200, {"application": result["Attributes"]})

    if method == "DELETE" and application_id:
        applications_table.delete_item(
            Key={"userId": user_id, "applicationId": application_id},
            ConditionExpression="attribute_exists(applicationId)"
        )
        logger.info("Deleted application %s", application_id)
        return response(200, {"message": "Application deleted"})

    return response(404, {"error": "Route not found"})


def handle_tasks(method, task_id, user_id, event):
    if method == "GET" and task_id is None:
        result = tasks_table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
        items = result.get("Items", [])
        # Query can paginate; fetch remaining pages.
        while result.get("LastEvaluatedKey"):
            result = tasks_table.query(
                KeyConditionExpression=Key("userId").eq(user_id),
                ExclusiveStartKey=result["LastEvaluatedKey"]
            )
            items.extend(result.get("Items", []))
        return response(200, {"tasks": items})

    if method == "POST" and task_id is None:
        body = parse_body(event)
        if not isinstance(body, dict):
            return response(400, {"error": "Body must be a JSON object"})

        title = body.get("title")
        due_date = body.get("dueDate")
        if not isinstance(title, str) or not title.strip():
            return response(400, {"error": "title is required"})
        if not isinstance(due_date, str) or not due_date.strip():
            return response(400, {"error": "dueDate is required (use YYYY-MM-DD or an ISO date-time)"})

        task_type = body.get("type", "Other")
        status = body.get("status", "Pending")
        if task_type not in TASK_TYPES:
            return response(400, {"error": "Invalid task type"})
        if status not in TASK_STATUSES:
            return response(400, {"error": "Invalid task status"})

        now = datetime.now(timezone.utc).isoformat()
        generated_id = str(uuid.uuid4())
        item = {
            "userId": user_id,
            "taskKey": f"{due_date.strip()}#{generated_id}",
            "taskId": generated_id,
            "title": title.strip(),
            "dueDate": due_date.strip(),
            "type": task_type,
            "status": status,
            "createdAt": now,
            "updatedAt": now
        }

        for field in ("description", "applicationId"):
            if body.get(field) is not None:
                if not isinstance(body[field], str):
                    return response(400, {"error": f"{field} must be a string"})
                item[field] = body[field].strip()

        tasks_table.put_item(Item=item)
        logger.info("Created task %s", generated_id)
        return response(201, {"task": item})

    if method == "PUT" and task_id:
        body = parse_body(event)
        if not isinstance(body, dict):
            return response(400, {"error": "Body must be a JSON object"})

        result = tasks_table.query(
            KeyConditionExpression=Key("userId").eq(user_id),
            FilterExpression=boto3.dynamodb.conditions.Attr("taskId").eq(task_id)
        )
        items = result.get("Items", [])
        while not items and result.get("LastEvaluatedKey"):
            result = tasks_table.query(
                KeyConditionExpression=Key("userId").eq(user_id),
                FilterExpression=boto3.dynamodb.conditions.Attr("taskId").eq(task_id),
                ExclusiveStartKey=result["LastEvaluatedKey"]
            )
            items = result.get("Items", [])
        if not items:
            return response(404, {"error": "Task not found"})

        existing = items[0]
        updates = {k: v for k, v in body.items() if k in EDITABLE_TASK_FIELDS}
        if not updates:
            return response(400, {"error": "No editable task fields provided"})
        if "title" in updates and (not isinstance(updates["title"], str) or not updates["title"].strip()):
            return response(400, {"error": "title cannot be empty"})
        if "type" in updates and updates["type"] not in TASK_TYPES:
            return response(400, {"error": "Invalid task type"})
        if "status" in updates and updates["status"] not in TASK_STATUSES:
            return response(400, {"error": "Invalid task status"})
        if "dueDate" in updates and (not isinstance(updates["dueDate"], str) or not updates["dueDate"].strip()):
            return response(400, {"error": "dueDate cannot be empty"})

        # Changing dueDate changes the sort key, so delete old key and put updated item.
        updated_item = dict(existing)
        for key, value in updates.items():
            if key in {"title", "description", "dueDate", "type", "status", "applicationId"}:
                updated_item[key] = value.strip() if isinstance(value, str) else value
        updated_item["updatedAt"] = datetime.now(timezone.utc).isoformat()
        updated_item["taskKey"] = f"{updated_item['dueDate']}#{task_id}"

        if updated_item["taskKey"] != existing["taskKey"]:
            tasks_table.put_item(Item=updated_item)
            tasks_table.delete_item(Key={"userId": user_id, "taskKey": existing["taskKey"]})
        else:
            tasks_table.put_item(Item=updated_item)

        logger.info("Updated task %s", task_id)
        return response(200, {"task": updated_item})

    if method == "DELETE" and task_id:
        result = tasks_table.query(
            KeyConditionExpression=Key("userId").eq(user_id),
            FilterExpression=boto3.dynamodb.conditions.Attr("taskId").eq(task_id)
        )
        items = result.get("Items", [])
        while not items and result.get("LastEvaluatedKey"):
            result = tasks_table.query(
                KeyConditionExpression=Key("userId").eq(user_id),
                FilterExpression=boto3.dynamodb.conditions.Attr("taskId").eq(task_id),
                ExclusiveStartKey=result["LastEvaluatedKey"]
            )
            items = result.get("Items", [])
        if not items:
            return response(404, {"error": "Task not found"})
        tasks_table.delete_item(
            Key={"userId": user_id, "taskKey": items[0]["taskKey"]},
            ConditionExpression="attribute_exists(taskKey)"
        )
        logger.info("Deleted task %s", task_id)
        return response(200, {"message": "Task deleted"})

    return response(404, {"error": "Route not found"})


def lambda_handler(event, context):
    method = get_method(event)
    path = get_path(event)

    if method == "OPTIONS":
        return response(200, {"message": "OK"})

    resource, resource_id, valid_path = parse_resource_path(path)
    if not valid_path or resource is None:
        return response(404, {"error": "Route not found"})

    user_id = get_user_id(event)

    try:
        if resource == "applications":
            return handle_applications(method, resource_id, user_id, event)
        if resource == "tasks":
            return handle_tasks(method, resource_id, user_id, event)
        if resource == "resumes":
            return response(501, {"error": "Resume functionality is not implemented yet"})
        return response(404, {"error": "Route not found"})

    except json.JSONDecodeError:
        return response(400, {"error": "Request body must be valid JSON"})
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "ConditionalCheckFailedException":
            return response(404, {"error": "Record not found"})
        logger.exception("DynamoDB operation failed")
        return response(500, {"error": "Database operation failed"})
    except Exception:
        logger.exception("Unexpected API error")
        return response(500, {"error": "Internal server error"})
