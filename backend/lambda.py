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
resumes_table = dynamodb.Table(os.environ["RESUMES_TABLE"])
s3_client = boto3.client(
    "s3",
    region_name="ap-southeast-2",
    endpoint_url="https://s3.ap-southeast-2.amazonaws.com"
)
resume_bucket = os.environ["RESUME_BUCKET"]
PRESIGNED_URL_EXPIRY = 300
RESUME_MAX_SIZE_BYTES = int(os.environ.get("RESUME_MAX_SIZE_BYTES", 5 * 1024 * 1024))
RESUME_PENDING_TTL_SECONDS = 7 * 24 * 60 * 60
RESUME_CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
}

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


def handle_resumes(method, resume_id, user_id, event):
    if method == "GET" and resume_id is None:
        result = resumes_table.query(
            KeyConditionExpression=Key("userId").eq(user_id)
        )
        items = result.get("Items", [])
        while result.get("LastEvaluatedKey"):
            result = resumes_table.query(
                KeyConditionExpression=Key("userId").eq(user_id),
                ExclusiveStartKey=result["LastEvaluatedKey"]
            )
            items.extend(result.get("Items", []))
        return response(200, {"resumes": items})

    if method == "POST" and resume_id is None:
        body = parse_body(event)
        if not isinstance(body, dict):
            return response(400, {"error": "Body must be a JSON object"})

        role_category = body.get("roleCategory")
        file_name = body.get("fileName")
        content_type = body.get("contentType")
        file_size = body.get("fileSize")
        if not isinstance(role_category, str) or not role_category.strip():
            return response(400, {"error": "roleCategory is required"})
        if not isinstance(file_name, str) or not file_name.strip():
            return response(400, {"error": "fileName is required"})
        expected_content_type = RESUME_CONTENT_TYPES.get(
            os.path.splitext(file_name.strip())[1].lower()
        )
        if not expected_content_type or content_type != expected_content_type:
            return response(400, {"error": "Only PDF and DOCX resumes are supported"})
        if (not isinstance(file_size, int) or isinstance(file_size, bool)
                or file_size <= 0 or file_size > RESUME_MAX_SIZE_BYTES):
            return response(400, {
                "error": f"fileSize must be between 1 and {RESUME_MAX_SIZE_BYTES} bytes"
            })

        now = datetime.now(timezone.utc).isoformat()
        generated_id = str(uuid.uuid4())
        item = {
            "userId": user_id,
            "resumeId": generated_id,
            "roleCategory": role_category.strip(),
            "fileName": file_name.strip(),
            "contentType": content_type,
            "fileSize": file_size,
            "s3Key": f"resumes/pending/{generated_id}",
            "createdAt": now,
            "updatedAt": now,
            "uploadStatus": "pending",
            "expiresAt": int(datetime.now(timezone.utc).timestamp())
            + RESUME_PENDING_TTL_SECONDS
        }
        try:
            upload_url = s3_client.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": resume_bucket,
                    "Key": item["s3Key"],
                    "ContentType": content_type
                },
                ExpiresIn=PRESIGNED_URL_EXPIRY
            )
        except ClientError:
            logger.exception("Could not create resume upload URL")
            return response(500, {"error": "Resume storage operation failed"})

        resumes_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(resumeId)"
        )
        logger.info("Created resume metadata %s", generated_id)
        return response(201, {
            "resume": item,
            "uploadUrl": upload_url,
            "uploadUrlExpiresIn": PRESIGNED_URL_EXPIRY
        })

    if method == "GET" and resume_id:
        result = resumes_table.get_item(
            Key={"userId": user_id, "resumeId": resume_id}
        )
        item = result.get("Item")
        if not item:
            return response(404, {"error": "Resume not found"})
        if item.get("uploadStatus") != "uploaded":
            return response(409, {"error": "Resume upload has not been verified"})
        try:
            download_url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": resume_bucket, "Key": item["s3Key"]},
                ExpiresIn=PRESIGNED_URL_EXPIRY
            )
        except ClientError:
            logger.exception("Could not create resume download URL")
            return response(500, {"error": "Resume storage operation failed"})
        return response(200, {
            "resume": item,
            "downloadUrl": download_url,
            "downloadUrlExpiresIn": PRESIGNED_URL_EXPIRY
        })

    if method == "POST" and resume_id:
        body = parse_body(event)
        if not isinstance(body, dict) or body.get("action") != "complete":
            return response(400, {"error": "action must be 'complete'"})

        result = resumes_table.get_item(
            Key={"userId": user_id, "resumeId": resume_id}
        )
        item = result.get("Item")
        if not item:
            return response(404, {"error": "Resume not found"})
        if item.get("uploadStatus") == "uploaded":
            return response(200, {"resume": item})
        if item.get("uploadStatus") != "pending":
            return response(409, {"error": "Resume upload is not pending"})

        try:
            uploaded = s3_client.head_object(
                Bucket=resume_bucket,
                Key=item["s3Key"]
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return response(409, {"error": "Resume upload was not found in storage"})
            logger.exception("Could not verify resume upload")
            return response(500, {"error": "Resume storage operation failed"})

        if (uploaded.get("ContentLength") != item.get("fileSize")
                or uploaded.get("ContentLength", 0) > RESUME_MAX_SIZE_BYTES
                or uploaded.get("ContentType") != item.get("contentType")):
            return response(400, {
                "error": "Uploaded file type or size does not match the declared resume"
            })

        completed_key = f"resumes/{resume_id}"
        try:
            s3_client.copy_object(
                Bucket=resume_bucket,
                Key=completed_key,
                CopySource={"Bucket": resume_bucket, "Key": item["s3Key"]}
            )
        except ClientError:
            logger.exception("Could not finalize resume upload")
            return response(500, {"error": "Resume storage operation failed"})

        uploaded_at = datetime.now(timezone.utc).isoformat()
        try:
            result = resumes_table.update_item(
                Key={"userId": user_id, "resumeId": resume_id},
                UpdateExpression=(
                    "SET uploadStatus = :uploaded, s3Key = :s3_key, "
                    "uploadedAt = :uploaded_at, updatedAt = :updated_at "
                    "REMOVE expiresAt"
                ),
                ExpressionAttributeValues={
                    ":uploaded": "uploaded",
                    ":s3_key": completed_key,
                    ":uploaded_at": uploaded_at,
                    ":updated_at": uploaded_at,
                    ":pending": "pending"
                },
                ConditionExpression="uploadStatus = :pending",
                ReturnValues="ALL_NEW"
            )
        except ClientError:
            try:
                s3_client.delete_object(Bucket=resume_bucket, Key=completed_key)
            except ClientError:
                logger.exception("Could not clean up uncommitted resume object")
            raise

        try:
            s3_client.delete_object(Bucket=resume_bucket, Key=item["s3Key"])
        except ClientError:
            logger.exception("Could not clean up temporary resume object")
            return response(500, {
                "error": "Resume was verified but temporary upload cleanup failed",
                "resume": result["Attributes"]
            })

        logger.info("Verified resume upload %s", resume_id)
        return response(200, {"resume": result["Attributes"]})

    if method == "PUT" and resume_id:
        body = parse_body(event)
        if not isinstance(body, dict):
            return response(400, {"error": "Body must be a JSON object"})

        updates = {
            key: body[key].strip()
            for key in ("roleCategory", "fileName")
            if key in body and isinstance(body[key], str) and body[key].strip()
        }
        for field in ("roleCategory", "fileName"):
            if field in body and field not in updates:
                return response(400, {"error": f"{field} cannot be empty"})
        if not updates:
            return response(400, {"error": "No editable resume fields provided"})

        existing_result = resumes_table.get_item(
            Key={"userId": user_id, "resumeId": resume_id}
        )
        existing = existing_result.get("Item")
        if not existing:
            return response(404, {"error": "Resume not found"})
        if "fileName" in updates:
            expected_content_type = RESUME_CONTENT_TYPES.get(
                os.path.splitext(updates["fileName"])[1].lower()
            )
            if expected_content_type != existing.get("contentType"):
                return response(400, {
                    "error": "Resume file extension cannot be changed to another file type"
                })

        updates["updatedAt"] = datetime.now(timezone.utc).isoformat()
        expression_names = {f"#f{i}": field for i, field in enumerate(updates)}
        expression_values = {f":v{i}": value for i, value in enumerate(updates.values())}
        update_expression = "SET " + ", ".join(
            f"#f{i} = :v{i}" for i in range(len(updates))
        )
        result = resumes_table.update_item(
            Key={"userId": user_id, "resumeId": resume_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values,
            ConditionExpression="attribute_exists(resumeId)",
            ReturnValues="ALL_NEW"
        )
        item = result["Attributes"]
        if item.get("uploadStatus") == "pending":
            try:
                upload_url = s3_client.generate_presigned_url(
                    "put_object",
                    Params={
                        "Bucket": resume_bucket,
                        "Key": item["s3Key"],
                        "ContentType": item["contentType"]
                    },
                    ExpiresIn=PRESIGNED_URL_EXPIRY
                )
            except ClientError:
                logger.exception("Could not create resume upload URL")
                return response(500, {"error": "Resume storage operation failed"})
            return response(200, {
                "resume": item,
                "uploadUrl": upload_url,
                "uploadUrlExpiresIn": PRESIGNED_URL_EXPIRY
            })
        logger.info("Updated resume metadata %s", resume_id)
        return response(200, {"resume": item})

    if method == "DELETE" and resume_id:
        result = resumes_table.get_item(
            Key={"userId": user_id, "resumeId": resume_id}
        )
        item = result.get("Item")
        if not item:
            return response(404, {"error": "Resume not found"})

        resumes_table.delete_item(
            Key={"userId": user_id, "resumeId": resume_id},
            ConditionExpression="attribute_exists(resumeId)"
        )
        try:
            s3_client.delete_object(Bucket=resume_bucket, Key=item["s3Key"])
        except ClientError:
            logger.exception("Could not delete resume from object storage")
            return response(500, {"error": "Resume storage operation failed"})

        logger.info("Deleted resume %s", resume_id)
        return response(200, {"message": "Resume deleted"})

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
            return handle_resumes(method, resource_id, user_id, event)
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
