#!/usr/bin/env python3
"""Multi-Source Ingestion Simulator (API, File Share, Oracle).

Simulates external daily data ingestion into S3 Bronze bucket:
1. Generates synthetic employee CSV data for the selected source ('api', 'file_share', 'oracle').
2. Writes data payload to S3 Bronze:
   raw/{source}/batch_date=YYYY-MM-DD/{source}_data_{timestamp}.csv
3. Writes manifest JSON to S3 Bronze:
   raw/manifests/batch_date=YYYY-MM-DD/{source}.manifest

Upon uploading the *.manifest file, S3 Event Notifications trigger the Manifest
Validator Lambda function to synchronize multi-source batch readiness in DynamoDB
and publish a BATCH_READY event to SQS.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mock_ingestion")

S3_BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "gitops-bronze-bkt-dev-790347819012")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

DEPARTMENTS = ["IT", "HR", "Finance", "Sales"]
CITIES = ["Bangalore", "Mumbai", "Delhi", "Hyderabad", "Pune", "Chennai"]


def generate_mock_employees(source_name: str, count: int = 10, start_id: int = 50000) -> List[Dict[str, Any]]:
    """Generate sample employee rows."""
    records = []
    for i in range(count):
        records.append({
            "emp_id": start_id + i,
            "name": f"{source_name[:3].upper()}_{random.randint(1000, 9999)}",
            "department": random.choice(DEPARTMENTS),
            "salary": random.randint(55000, 140000),
            "city": random.choice(CITIES),
            "date_of_joining": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        })
    return records


def upload_source_data_and_manifest(
    source_name: str,
    batch_date: str,
    record_count: int = 10,
    bucket: str = S3_BRONZE_BUCKET,
    region: str = AWS_REGION,
):
    """Upload data file followed by manifest file to Bronze S3."""
    s3_client = boto3.client("s3", region_name=region)
    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")

    # 1. Generate & Upload Data CSV
    records = generate_mock_employees(source_name, count=record_count, start_id=random.randint(20000, 90000))
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["emp_id", "name", "department", "salary", "city", "date_of_joining"], lineterminator="\n")
    writer.writeheader()
    writer.writerows(records)
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    data_key = f"raw/employees/year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/{source_name}_data_{timestamp_str}.csv"
    logger.info("📤 Uploading %d records to s3://%s/%s...", record_count, bucket, data_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=data_key,
        Body=csv_bytes,
        ContentType="text/csv",
        ServerSideEncryption="AES256",
    )

    # 2. Generate Manifest Payload
    manifest_payload = {
        "source": source_name,
        "batch_date": batch_date,
        "record_count": record_count,
        "files": [data_key],
        "uploaded_at": now.isoformat(),
        "status": "COMPLETED",
    }
    manifest_bytes = json.dumps(manifest_payload, indent=2).encode("utf-8")

    # Notice the suffix is .manifest to match the S3 Event Notification filter!
    manifest_key = f"raw/manifests/batch_date={batch_date}/{source_name}.manifest"
    logger.info("📄 Uploading manifest to s3://%s/%s...", bucket, manifest_key)
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    logger.info("✅ SUCCESS: Source '%s' data + manifest uploaded successfully.", source_name)


def main():
    parser = argparse.ArgumentParser(description="Multi-Source Ingestion Simulator")
    parser.add_argument(
        "--source",
        choices=["api", "file_share", "oracle", "all"],
        default="all",
        help="Source to simulate ('api', 'file_share', 'oracle', or 'all')",
    )
    parser.add_argument(
        "--batch-date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Batch date string in YYYY-MM-DD format",
    )
    parser.add_argument("--count", type=int, default=15, help="Number of records per source")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay in seconds between source uploads")
    parser.add_argument("--bucket", default=S3_BRONZE_BUCKET, help="Target S3 Bronze bucket")
    args = parser.parse_args()

    sources = ["api", "file_share", "oracle"] if args.source == "all" else [args.source]

    logger.info("=" * 65)
    logger.info("🚀 Starting Multi-Source Ingestion Simulation")
    logger.info("   Batch Date : %s", args.batch_date)
    logger.info("   Sources    : %s", ", ".join(sources))
    logger.info("   Target S3  : s3://%s", args.bucket)
    logger.info("=" * 65)

    for i, src in enumerate(sources):
        upload_source_data_and_manifest(
            source_name=src,
            batch_date=args.batch_date,
            record_count=args.count,
            bucket=args.bucket,
        )
        if i < len(sources) - 1:
            logger.info("⏳ Waiting %.1fs before triggering next source...", args.delay)
            time.sleep(args.delay)

    logger.info("=" * 65)
    logger.info("🎉 All requested source uploads finished!")
    logger.info("   S3 Event Notifications will now trigger Lambda to coordinate state.")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
