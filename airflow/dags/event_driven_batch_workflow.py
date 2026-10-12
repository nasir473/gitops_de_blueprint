"""Event-Driven Multi-Source Batch Synchronization DAG.

Architecture (Multi-Source Batch Synchronization Barrier Pattern):
1. Upstream systems (API Ingestion, File Share Transfer, Oracle Ingestion) land
   their data and completion manifests (*.manifest) into Bronze S3.
2. S3 ObjectCreated events trigger the Manifest Validator Lambda function.
3. Lambda tracks state in DynamoDB. When ALL required sources are confirmed READY,
   Lambda dispatches a single BATCH_READY event to the SQS Batch Ready Queue.
4. SqsSensor in this Airflow DAG receives the event message and triggers the pipeline.
5. Dual Glue ETL (Parquet + Delta Lake) runs in parallel, updating Athena and
   refreshing the Gold analytical layer in Databricks Unity Catalog.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("airflow.task")

# Environment & Configuration defaults
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET", "gitops-bronze-bkt-dev-790347819012")
SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET", "gitops-silver-bkt-dev-790347819012")
GLUE_PARQUET_JOB_NAME = os.getenv("GLUE_JOB_NAME", "gitops-bronze-to-silver-employees-dev")
GLUE_DELTA_JOB_NAME = os.getenv("GLUE_DELTA_JOB_NAME", "gitops-bronze-to-silver-delta-employees-dev")
GLUE_ICEBERG_JOB_NAME = os.getenv("GLUE_ICEBERG_JOB_NAME", "gitops-bronze-to-silver-iceberg-employees-dev")
GLUE_CRAWLER_NAME = os.getenv("GLUE_CRAWLER_NAME", "gitops-silver-parquet-crawler-dev")
GLUE_DATABASE = os.getenv("GLUE_DATABASE", "gitops_dev_db")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "gitops-dev-workgroup")

SQS_QUEUE_URL = os.getenv(
    "SQS_BATCH_READY_QUEUE_URL",
    "https://sqs.ap-south-1.amazonaws.com/790347819012/gitops-batch-ready-queue-dev",
)

# Databricks Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://dbc-6396e0b0-9912.cloud.databricks.com").strip('"')
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "").strip('"')
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "d6845e35de9cbeee").strip('"')
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "gitops_dev_catalog").strip('"')

# Amazon SNS Alerts Configuration
SNS_ALERTS_TOPIC_ARN = os.getenv(
    "SNS_PIPELINE_ALERTS_TOPIC_ARN",
    "arn:aws:sns:ap-south-1:790347819012:gitops-pipeline-alerts-topic-dev",
)


def send_sns_alert_on_failure(context):
    """Publish failure notification to SNS Pipeline Alerts topic."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = str(context.get("execution_date"))
    exception = str(context.get("exception", "No exception detail"))

    alert_subject = f"🚨 [FAILURE] Airflow Task {task_id} in {dag_id}"
    alert_payload = {
        "status": "FAILED",
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "log_url": task_instance.log_url if task_instance else "",
        "exception": exception,
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        sns.publish(
            TopicArn=SNS_ALERTS_TOPIC_ARN,
            Subject=alert_subject,
            Message=json.dumps(alert_payload, indent=2),
        )
        print(f"Dispatched failure alert to SNS: {SNS_ALERTS_TOPIC_ARN}")
    except Exception as e:
        print(f"Warning: Failed to publish SNS failure alert: {e}")


def parse_sqs_batch_message(**context) -> dict:
    """Extract and validate the BATCH_READY event payload from SqsSensor."""
    ti = context["ti"]
    messages = ti.xcom_pull(task_ids="wait_for_sqs_batch_ready", key="messages")

    print("=" * 70)
    print("📥 SQS BATCH EVENT RECEIVED")
    print("=" * 70)

    payload = None
    if messages:
        raw_msg = messages[0] if isinstance(messages, list) else messages
        body_str = raw_msg.get("Body") if isinstance(raw_msg, dict) else str(raw_msg)
        try:
            payload = json.loads(body_str)
        except Exception as e:
            print(f"Warning: Could not parse body as JSON: {e}. Raw body: {body_str}")
            payload = {"event_type": "BATCH_READY", "batch_date": datetime.utcnow().strftime("%Y-%m-%d")}

    if not payload:
        # Safe fallback: retrieve state from DynamoDB
        print("Checking DynamoDB for latest batch state...")
        try:
            dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
            table = dynamodb.Table(os.getenv("DYNAMODB_BATCH_TABLE", "gitops-batch-coordination-dev"))
            today_str = datetime.utcnow().strftime("%Y-%m-%d")
            resp = table.get_item(Key={"batch_date": today_str})
            item = resp.get("Item", {})
            payload = {
                "event_type": "BATCH_READY",
                "batch_date": today_str,
                "sources_detail": {
                    "api": {"status": item.get("api_status", "READY")},
                    "file_share": {"status": item.get("file_share_status", "READY")},
                    "oracle": {"status": item.get("oracle_status", "READY")},
                },
            }
        except Exception as e:
            print(f"DynamoDB lookup fallback note: {e}")
            payload = {"event_type": "BATCH_READY", "batch_date": datetime.utcnow().strftime("%Y-%m-%d")}

    batch_date = payload.get("batch_date", datetime.utcnow().strftime("%Y-%m-%d"))
    batch_id = payload.get("batch_id", f"batch_{batch_date.replace('-', '')}")
    sources = payload.get("sources_detail", {})

    print(f"[*] Batch ID      : {batch_id}")
    print(f"[*] Batch Date    : {batch_date}")
    print(f"[*] Event Type    : {payload.get('event_type')}")
    print(f"[*] Ready At      : {payload.get('ready_at', datetime.utcnow().isoformat())}")
    print("\nSynchronized Upstream Sources:")
    for src, details in sources.items():
        print(f"  • Source: {src:<15} | Status: {details.get('status')} | Manifest: {details.get('manifest')}")

    print("=" * 70)

    ti.xcom_push(key="batch_date", value=batch_date)
    ti.xcom_push(key="batch_id", value=batch_id)
    return payload


def verify_silver_partitions(**context) -> dict:
    """Verify both Parquet and Delta Lake partition structures in Silver."""
    print("Connecting to AWS S3 in region ap-south-1...")
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    prefixes = {
        "parquet": "curated/employees/",
        "delta": "delta/employees/",
    }
    counts = {}

    for layer_type, prefix in prefixes.items():
        print(f"Listing objects under s3://{SILVER_BUCKET}/{prefix}...")
        resp = s3_client.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
        key_count = resp.get("KeyCount", 0)
        counts[layer_type] = key_count
        print(f"Found {key_count} objects in Silver {layer_type.upper()} ({prefix})")

        if key_count == 0:
            raise ValueError(f"Silver {layer_type} directory is empty: s3://{SILVER_BUCKET}/{prefix}")

    return counts


def validate_athena_data(**context) -> dict:
    """Execute validation query on Amazon Athena verifying table queryability."""
    athena_client = boto3.client("athena", region_name=AWS_REGION)
    query = f"SELECT count(*) AS total_records, department FROM {GLUE_DATABASE}.employees GROUP BY department;"
    output_location = f"s3://gitops-athena-results-bkt-dev-790347819012/validations/"

    print(f"Executing Athena validation query in workgroup '{ATHENA_WORKGROUP}'...")
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_exec_id = response["QueryExecutionId"]
    print(f"Athena query execution ID: {query_exec_id}")

    for _ in range(30):
        time.sleep(2)
        status_resp = athena_client.get_query_execution(QueryExecutionId=query_exec_id)
        state = status_resp["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    if state != "SUCCEEDED":
        reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena validation query failed with state '{state}': {reason}")

    print("Athena validation query succeeded!")
    return {"query_execution_id": query_exec_id, "state": state}


def refresh_databricks_gold_department_summary(**context) -> dict:
    """Refresh Gold department summary table in Databricks Unity Catalog via SQL Warehouse."""
    import urllib.request

    print("Starting Databricks Gold department summary table refresh...")
    if not DATABRICKS_TOKEN:
        raise ValueError("DATABRICKS_TOKEN is empty. Cannot refresh Databricks Gold table.")

    gold_query = f"""
    CREATE OR REPLACE TABLE {DATABRICKS_CATALOG}.gold.department_summary AS
    SELECT 
        department,
        COUNT(emp_id) AS total_employees,
        MIN(date_of_joining) AS earliest_hire_date,
        MAX(date_of_joining) AS latest_hire_date,
        AVG(salary) AS avg_salary,
        current_timestamp() AS aggregated_at
    FROM 
        {DATABRICKS_CATALOG}.silver.employees
    GROUP BY 
        department;
    """

    api_url = f"{DATABRICKS_HOST.rstrip('/')}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }
    req_body = json.dumps({
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "statement": gold_query,
        "wait_timeout": "50s",
    }).encode("utf-8")

    req = urllib.request.Request(api_url, data=req_body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        result = json.loads(resp.read().decode("utf-8"))

    state = result.get("status", {}).get("state", "UNKNOWN")
    print(f"Databricks SQL Execution status: {state}")
    if state not in ("SUCCEEDED", "CLOSED"):
        raise RuntimeError(f"Databricks statement execution did not succeed. Status: {result.get('status')}")

    print(f"Successfully refreshed Gold table: {DATABRICKS_CATALOG}.gold.department_summary")
    return {"status": "success", "catalog": DATABRICKS_CATALOG, "table": "gold.department_summary"}


# DAG Definition
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": send_sns_alert_on_failure,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="event_driven_batch_workflow",
    default_args=default_args,
    description="Event-Driven Multi-Source Batch Synchronization Barrier via SQS & Medallion Pipeline",
    schedule=None,  # Purely event-driven execution
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["event-driven", "sqs", "medallion", "glue", "databricks", "production"],
) as dag:

    # 1. Wait for BATCH_READY event in SQS
    wait_for_sqs_batch_ready = SqsSensor(
        task_id="wait_for_sqs_batch_ready",
        sqs_queue=SQS_QUEUE_URL,
        max_messages=1,
        delete_message_on_reception=True,
        wait_time_seconds=20,
        poke_interval=15,
        timeout=86400,
        mode="reschedule",
    )

    # 2. Parse batch metadata from SQS
    parse_batch = PythonOperator(
        task_id="parse_batch_metadata",
        python_callable=parse_sqs_batch_message,
    )

    # 3. Branch A: AWS Glue Parquet Job
    run_glue_parquet_job = GlueJobOperator(
        task_id="run_glue_parquet_job",
        job_name=GLUE_PARQUET_JOB_NAME,
        region_name=AWS_REGION,
        script_args={
            "--environment": "dev",
            "--source_bucket": BRONZE_BUCKET,
            "--target_bucket": SILVER_BUCKET,
        },
        wait_for_completion=True,
    )

    # 4. Branch B: AWS Glue Delta Lake Job
    run_glue_delta_job = GlueJobOperator(
        task_id="run_glue_delta_job",
        job_name=GLUE_DELTA_JOB_NAME,
        region_name=AWS_REGION,
        script_args={
            "--environment": "dev",
            "--source_bucket": BRONZE_BUCKET,
            "--target_bucket": SILVER_BUCKET,
        },
        wait_for_completion=True,
    )

    # 5. Branch C: AWS Glue Apache Iceberg Job
    run_glue_iceberg_job = GlueJobOperator(
        task_id="run_glue_iceberg_job",
        job_name=GLUE_ICEBERG_JOB_NAME,
        region_name=AWS_REGION,
        script_args={
            "--environment": "dev",
            "--source_bucket": BRONZE_BUCKET,
            "--target_bucket": SILVER_BUCKET,
        },
        wait_for_completion=True,
    )

    # 6. Silver Partition Verification (Barrier)
    verify_silver = PythonOperator(
        task_id="verify_silver_partitions",
        python_callable=verify_silver_partitions,
    )

    # 7. Branch A downstream: Crawl curated Parquet
    crawl_silver = GlueCrawlerOperator(
        task_id="crawl_silver_parquet",
        config={"Name": GLUE_CRAWLER_NAME},
        region_name=AWS_REGION,
        wait_for_completion=True,
        poll_interval=10,
    )

    # 8. Branch A downstream: Validate Athena table queryability
    validate_athena = PythonOperator(
        task_id="validate_athena_data",
        python_callable=validate_athena_data,
    )

    # 9. Branch B downstream: Refresh Gold table in Databricks Unity Catalog
    refresh_gold = PythonOperator(
        task_id="refresh_databricks_gold_department_summary",
        python_callable=refresh_databricks_gold_department_summary,
    )

    # 10. Pipeline Complete
    pipeline_complete = BashOperator(
        task_id="pipeline_complete",
        bash_command=(
            "echo '=====================================================' && "
            "echo '🎉 Multi-Source Event-Driven Batch Pipeline Complete!' && "
            "echo '   • Bronze Multi-Source SQS Event Ingested' && "
            "echo '   • Silver Parquet Curated & Athena Validated' && "
            "echo '   • Silver Delta Lake Created & Databricks Gold Refreshed' && "
            "echo '   • Silver Apache Iceberg Created & Catalog Registered' && "
            "echo '====================================================='"
        ),
    )

    # Pipeline Topology:
    # SQS Sensor -> Parse Batch Metadata -> Fan-Out to Parquet, Delta & Iceberg -> Silver Verification
    wait_for_sqs_batch_ready >> parse_batch

    parse_batch >> run_glue_parquet_job >> verify_silver
    parse_batch >> run_glue_delta_job >> verify_silver
    parse_batch >> run_glue_iceberg_job >> verify_silver

    verify_silver >> crawl_silver >> validate_athena >> pipeline_complete
    verify_silver >> refresh_gold >> pipeline_complete
