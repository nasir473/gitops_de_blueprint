"""Manifest Validator Lambda Function.

Triggered by AWS S3 Event Notifications when a manifest file is uploaded to the
Bronze landing bucket.

Workflow:
1. Extracts bucket and object key from S3 event.
2. Reads and parses the manifest JSON.
3. Atomically updates the batch status in Amazon DynamoDB for the given batch_date.
4. Checks if all required sources ('api', 'file_share', 'oracle') are READY.
5. If all sources are ready and not yet notified, dispatches a 'BATCH_READY' event
   to the SQS Batch Ready Queue to trigger the Airflow pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "gitops-batch-coordination-dev")
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL", "")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
REQUIRED_SOURCES_STR = os.environ.get("REQUIRED_SOURCES", "api,file_share,oracle")
REQUIRED_SOURCES = [s.strip().lower() for s in REQUIRED_SOURCES_STR.split(",") if s.strip()]

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DYNAMODB_TABLE)
sqs_client = boto3.client("sqs")
sns_client = boto3.client("sns")


def normalize_source_name(source: str) -> str:
    """Normalize source names to canonical keys: 'api', 'file_share', 'oracle'."""
    src = source.lower().replace("-", "_")
    if "api" in src:
        return "api"
    if "file" in src or "share" in src:
        return "file_share"
    if "oracle" in src:
        return "oracle"
    return src


def parse_manifest_from_s3(bucket: str, key: str) -> Dict[str, Any]:
    """Read manifest content from S3 and extract metadata."""
    logger.info("Reading manifest from s3://%s/%s", bucket, key)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    
    try:
        data = json.loads(body)
    except Exception as e:
        logger.error("Failed to parse manifest JSON from %s: %s", key, e)
        raise ValueError(f"Invalid manifest JSON: {e}")

    # Fallback to current UTC date if not present in manifest
    batch_date = data.get("batch_date")
    if not batch_date:
        batch_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    raw_source = data.get("source", "unknown")
    canonical_source = normalize_source_name(raw_source)

    return {
        "source": canonical_source,
        "batch_date": str(batch_date),
        "record_count": data.get("record_count", 0),
        "files": data.get("files", []),
        "status": data.get("status", "COMPLETED"),
        "raw_payload": data,
        "s3_key": key,
    }


def update_state_and_check_completion(manifest_info: Dict[str, Any]) -> bool:
    """Update DynamoDB batch item atomically and determine if all sources are ready.
    
    Returns True if this invocation completed the batch and should trigger SQS.
    """
    batch_date = manifest_info["batch_date"]
    source = manifest_info["source"]
    now_iso = datetime.now(timezone.utc).isoformat()
    ttl_epoch = int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())

    status_attr = f"{source}_status"
    manifest_attr = f"{source}_manifest"
    received_attr = f"{source}_received_at"

    # Step 1: Update the specific source status in DynamoDB
    update_expr = (
        f"SET #status = :ready, #manifest = :manifest, #received = :received, "
        f"#ttl = :ttl, #updated_at = :updated_at"
    )
    expr_names = {
        "#status": status_attr,
        "#manifest": manifest_attr,
        "#received": received_attr,
        "#ttl": "ttl_timestamp",
        "#updated_at": "updated_at",
    }
    expr_values = {
        ":ready": "READY",
        ":manifest": manifest_info["s3_key"],
        ":received": now_iso,
        ":ttl": ttl_epoch,
        ":updated_at": now_iso,
    }

    logger.info("Updating DynamoDB batch state for date=%s, source=%s...", batch_date, source)
    table.update_item(
        Key={"batch_date": batch_date},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )

    # Step 2: Fetch current state to evaluate all required sources
    item_response = table.get_item(Key={"batch_date": batch_date})
    item = item_response.get("Item", {})

    all_ready = True
    sources_summary = {}
    for req_source in REQUIRED_SOURCES:
        curr_status = item.get(f"{req_source}_status")
        sources_summary[req_source] = curr_status
        if curr_status != "READY":
            all_ready = False

    logger.info("Current batch state for %s: %s (All ready: %s)", batch_date, sources_summary, all_ready)

    if not all_ready:
        return False

    # Step 3: Atomic check-and-set to guarantee exactly ONE SQS notification per batch_date
    if item.get("batch_notified") is True:
        logger.info("Batch %s already notified. Skipping duplicate SQS message.", batch_date)
        return False

    try:
        table.update_item(
            Key={"batch_date": batch_date},
            UpdateExpression="SET #notified = :true, #notified_at = :now",
            ConditionExpression="attribute_not_exists(#notified) OR #notified = :false",
            ExpressionAttributeNames={"#notified": "batch_notified", "#notified_at": "batch_notified_at"},
            ExpressionAttributeValues={":true": True, ":false": False, ":now": now_iso},
        )
        logger.info("Successfully acquired notification lock for batch_date=%s", batch_date)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("Notification lock was already claimed by concurrent invocation for batch=%s", batch_date)
            return False
        raise


def send_sqs_batch_ready_message(manifest_info: Dict[str, Any]):
    """Publish the 'BATCH_READY' event message to SQS."""
    batch_date = manifest_info["batch_date"]
    
    # Retrieve complete batch item
    item_response = table.get_item(Key={"batch_date": batch_date})
    item = item_response.get("Item", {})

    message_body = {
        "event_type": "BATCH_READY",
        "batch_id": f"batch_{batch_date.replace('-', '')}",
        "batch_date": batch_date,
        "required_sources": REQUIRED_SOURCES,
        "ready_at": datetime.now(timezone.utc).isoformat(),
        "sources_detail": {
            s: {
                "status": item.get(f"{s}_status"),
                "manifest": item.get(f"{s}_manifest"),
                "received_at": item.get(f"{s}_received_at"),
            }
            for s in REQUIRED_SOURCES
        },
    }

    # 1. Publish to SNS Fan-out Topic if configured
    if SNS_TOPIC_ARN:
        logger.info("🚀 Publishing BATCH_READY event to SNS Fan-out Topic (%s)...", SNS_TOPIC_ARN)
        try:
            sns_resp = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=json.dumps(message_body),
                Subject=f"BATCH_READY: {batch_date}",
                MessageAttributes={
                    "EventType": {
                        "DataType": "String",
                        "StringValue": "BATCH_READY",
                    },
                    "BatchDate": {
                        "DataType": "String",
                        "StringValue": batch_date,
                    },
                },
            )
            logger.info("✅ SNS event published successfully. MessageId=%s", sns_resp.get("MessageId"))
        except Exception as e:
            logger.error("❌ Failed to publish to SNS: %s", str(e), exc_info=True)
            if not SQS_QUEUE_URL:
                raise

    # 2. Direct SQS fallback if SNS is not configured
    if SQS_QUEUE_URL and not SNS_TOPIC_ARN:
        logger.info("🚀 Publishing BATCH_READY event directly to SQS (%s)...", SQS_QUEUE_URL)
        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body),
            MessageAttributes={
                "EventType": {
                    "DataType": "String",
                    "StringValue": "BATCH_READY",
                },
                "BatchDate": {
                    "DataType": "String",
                    "StringValue": batch_date,
                },
            },
        )
        logger.info("✅ SQS message published successfully. MessageId=%s", response.get("MessageId"))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point triggered by S3 Event Notifications."""
    logger.info("Received event: %s", json.dumps(event))

    records = event.get("Records", [])
    if not records:
        logger.warning("No records found in S3 event notification.")
        return {"statusCode": 200, "body": "No records to process"}

    processed_count = 0
    for record in records:
        s3_data = record.get("s3", {})
        bucket = s3_data.get("bucket", {}).get("name")
        raw_key = s3_data.get("object", {}).get("key")

        if not bucket or not raw_key:
            continue

        key = urllib.parse.unquote_plus(raw_key)
        logger.info("Processing S3 event for bucket=%s, key=%s", bucket, key)

        try:
            manifest_info = parse_manifest_from_s3(bucket, key)
            should_notify = update_state_and_check_completion(manifest_info)

            if should_notify:
                send_sqs_batch_ready_message(manifest_info)

            processed_count += 1
        except Exception as e:
            logger.error("Error processing record for %s/%s: %s", bucket, key, e, exc_info=True)
            raise e

    return {
        "statusCode": 200,
        "body": json.dumps({"processed_count": processed_count, "status": "SUCCESS"}),
    }
