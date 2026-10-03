"""First Workflow DAG: Ingest local CSV data into S3 Bronze Landing Zone.

Reads ./Data/csv/sample_data.csv from the local workspace and uploads it
to the S3 Bronze bucket (gitops-bronze-bkt-dev-790347819012) under a partitioned raw prefix.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

import boto3
from botocore.exceptions import ClientError

# Configuration defaults
DEFAULT_BUCKET_NAME = os.getenv("S3_BRONZE_BUCKET", "gitops-bronze-bkt-dev-790347819012")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

# Resolve local CSV path within container or host
POTENTIAL_PATHS = [
    Path("/opt/airflow/Data/csv/sample_data.csv"),
    Path("./Data/csv/sample_data.csv"),
    Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "Data/csv/sample_data.csv",
]


def resolve_source_file() -> Path:
    """Locate the sample CSV file across standard mount points."""
    for p in POTENTIAL_PATHS:
        if p.exists() and p.is_file():
            return p
    raise FileNotFoundError(
        f"sample_data.csv not found in any expected location: {[str(p) for p in POTENTIAL_PATHS]}"
    )


def validate_local_csv(**context) -> str:
    """Validate that the local CSV file exists and has data."""
    file_path = resolve_source_file()
    file_size = file_path.stat().st_size

    with open(file_path, "r", encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1  # Exclude header

    print(f"Source file verified: {file_path}")
    print(f"File size: {file_size} bytes")
    print(f"Total data rows: {row_count}")

    if row_count <= 0:
        raise ValueError("Source CSV file contains zero data rows!")

    # Push resolved path to XCom for downstream tasks
    return str(file_path)


def upload_to_bronze_s3(**context) -> dict:
    """Upload local CSV to Bronze S3 bucket."""
    ti = context["ti"]
    source_path = ti.xcom_pull(task_ids="validate_local_csv") or str(resolve_source_file())

    bucket_name = DEFAULT_BUCKET_NAME
    # Format partitioned key: raw/employees/year=YYYY/month=MM/day=DD/sample_data.csv
    now = datetime.utcnow()
    s3_key = (
        f"raw/employees/year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/sample_data.csv"
    )

    print(f"Connecting to AWS S3 in region {AWS_REGION}...")
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    print(f"Uploading {source_path} to s3://{bucket_name}/{s3_key}...")
    try:
        s3_client.upload_file(
            Filename=source_path,
            Bucket=bucket_name,
            Key=s3_key,
            ExtraArgs={"ServerSideEncryption": "AES256"},
        )
        print("Upload completed successfully!")
    except ClientError as e:
        print(f"Failed to upload to S3: {e}")
        raise

    result = {
        "bucket": bucket_name,
        "key": s3_key,
        "s3_uri": f"s3://{bucket_name}/{s3_key}",
    }
    return result


def verify_s3_object(**context) -> None:
    """Verify that the object exists in S3 and log metadata."""
    ti = context["ti"]
    upload_result = ti.xcom_pull(task_ids="upload_to_bronze_s3")

    bucket = upload_result["bucket"]
    key = upload_result["key"]

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    response = s3_client.head_object(Bucket=bucket, Key=key)

    content_length = response.get("ContentLength", 0)
    etag = response.get("ETag", "").strip('"')
    encryption = response.get("ServerSideEncryption", "None")

    print("=" * 60)
    print("S3 BRONZE UPLOAD VERIFICATION SUCCESSFUL")
    print("=" * 60)
    print(f"S3 URI: s3://{bucket}/{key}")
    print(f"Object Size: {content_length} bytes")
    print(f"ETag: {etag}")
    print(f"Server-Side Encryption: {encryption}")
    print("=" * 60)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="first_workflow",
    default_args=default_args,
    description="Ingest local sample CSV data into S3 Bronze Landing Zone",
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze", "s3", "raw"],
) as dag:
    start_task = BashOperator(
        task_id="start_ingestion",
        bash_command='echo "Starting Bronze S3 Ingestion pipeline at $(date)"',
    )

    validate_task = PythonOperator(
        task_id="validate_local_csv",
        python_callable=validate_local_csv,
    )

    upload_task = PythonOperator(
        task_id="upload_to_bronze_s3",
        python_callable=upload_to_bronze_s3,
    )

    verify_task = PythonOperator(
        task_id="verify_s3_object",
        python_callable=verify_s3_object,
    )

    end_task = BashOperator(
        task_id="end_ingestion",
        bash_command='echo "Bronze S3 Ingestion pipeline completed successfully at $(date)"',
    )

    # Define DAG task dependencies
    start_task >> validate_task >> upload_task >> verify_task >> end_task
