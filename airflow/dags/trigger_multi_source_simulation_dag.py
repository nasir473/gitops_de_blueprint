"""DAG to Simulate Multi-Source Upstream Ingestion.

When triggered, this DAG simulates the 3 external source systems:
1. Task 1: Uploads API data (Data/event_driven/api_data.csv) + api.manifest to Bronze S3.
2. Task 2: Uploads File Share data (Data/event_driven/file_share_data.csv) + file_share.manifest to Bronze S3.
3. Task 3: Uploads Oracle data (Data/event_driven/oracle_data.csv) + oracle.manifest to Bronze S3.

The arrival of the 3rd manifest triggers the AWS S3 -> Lambda -> DynamoDB -> SQS barrier,
which in turn automatically wakes up and executes the 'event_driven_batch_workflow' DAG!
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

import boto3

logger = logging.getLogger("airflow.task")

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "gitops-bronze-bkt-dev-790347819012")

# Resolving local sample file paths inside container or host
DATA_DIR_CANDIDATES = [
    Path("/opt/airflow/Data/event_driven"),
    Path("./Data/event_driven"),
    Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / "Data/event_driven",
]


def _get_data_dir() -> Path:
    for candidate in DATA_DIR_CANDIDATES:
        if candidate.exists() and candidate.is_dir():
            return candidate
    raise FileNotFoundError(f"Data/event_driven directory not found in {[str(p) for p in DATA_DIR_CANDIDATES]}")


def _upload_source(source_name: str, csv_filename: str, **context) -> dict:
    """Upload data file and corresponding .manifest to Bronze S3."""
    ti = context["ti"]
    dag_run = context.get("dag_run")

    # Generate or retrieve unique batch_date across tasks in this run
    batch_date = ti.xcom_pull(key="batch_date", task_ids="upload_api_source")
    if not batch_date:
        # Check if user provided manual conf, else create timestamped batch
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        batch_date = conf.get("batch_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S")
        ti.xcom_push(key="batch_date", value=batch_date)

    data_dir = _get_data_dir()
    local_file = data_dir / csv_filename
    if not local_file.exists():
        raise FileNotFoundError(f"Source file {local_file} does not exist!")

    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # 1. Read & Upload data CSV
    with open(local_file, "r", encoding="utf-8") as f:
        csv_content = f.read()
        row_count = len([line for line in csv_content.splitlines() if line.strip()]) - 1

    data_key = f"raw/employees/year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/{source_name}_data_{timestamp_str}.csv"
    print(f"📤 Uploading {row_count} records from {local_file} to s3://{BRONZE_BUCKET}/{data_key}...")
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=data_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
        ServerSideEncryption="AES256",
    )

    # 2. Build & Upload manifest JSON
    manifest_payload = {
        "source": source_name,
        "batch_date": batch_date,
        "record_count": row_count,
        "files": [data_key],
        "uploaded_at": now.isoformat(),
        "status": "COMPLETED",
    }
    manifest_key = f"raw/manifests/batch_date={batch_date}/{source_name}.manifest"
    print(f"📄 Uploading manifest to s3://{BRONZE_BUCKET}/{manifest_key}...")
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest_payload, indent=2).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    print(f"✅ Source '{source_name}' uploaded successfully. Manifest: s3://{BRONZE_BUCKET}/{manifest_key}")
    return {"source": source_name, "batch_date": batch_date, "data_key": data_key, "manifest_key": manifest_key}


def upload_api_source(**context):
    return _upload_source("api", "api_data.csv", **context)


def upload_file_share_source(**context):
    time.sleep(2)  # Short visual delay
    return _upload_source("file_share", "file_share_data.csv", **context)


def upload_oracle_source(**context):
    time.sleep(2)  # Short visual delay
    return _upload_source("oracle", "oracle_data.csv", **context)


def summarize_simulation(**context):
    ti = context["ti"]
    batch_date = ti.xcom_pull(key="batch_date", task_ids="upload_api_source")
    print("=" * 70)
    print("🎉 MULTI-SOURCE INGESTION SIMULATION COMPLETE")
    print(f"   Batch Identifier : {batch_date}")
    print(f"   Target Bucket    : s3://{BRONZE_BUCKET}")
    print("=" * 70)
    print("All 3 sources (API, File Share, Oracle) uploaded their data + manifests.")
    print("S3 Event Notification has invoked the Manifest Validator Lambda.")
    print("Lambda has verified DynamoDB and published BATCH_READY to SQS.")
    print("Now watch 'event_driven_batch_workflow' automatically wake up and run!")
    print("=" * 70)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="trigger_multi_source_simulation",
    default_args=default_args,
    description="Simulates upstream ingestion from API, File Share, and Oracle into S3 Bronze",
    schedule=None,  # Manual trigger via UI button
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["simulation", "multi-source", "event-driven", "bronze"],
) as dag:

    task_api = PythonOperator(
        task_id="upload_api_source",
        python_callable=upload_api_source,
    )

    task_file_share = PythonOperator(
        task_id="upload_file_share_source",
        python_callable=upload_file_share_source,
    )

    task_oracle = PythonOperator(
        task_id="upload_oracle_source",
        python_callable=upload_oracle_source,
    )

    task_summary = PythonOperator(
        task_id="simulation_summary",
        python_callable=summarize_simulation,
    )

    # Sequential execution: API -> File Share -> Oracle -> Summary
    task_api >> task_file_share >> task_oracle >> task_summary
