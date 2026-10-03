"""First Workflow DAG: Medallion Ingestion, Dual Glue ETL (Parquet & Delta), and Catalog Crawl.

End-to-End Flow:
1. Validates local CSV file (./Data/csv/sample_data.csv).
2. Ingests raw CSV to Bronze S3 bucket (gitops-bronze-bkt-dev-790347819012) partitioned by date.
3. Verifies Bronze S3 object metadata and encryption.
4. Fans out to dual transformations in parallel:
   - Branch A (CSV -> Parquet): AWS Glue job gitops-bronze-to-silver-employees-dev
     writes curated Snappy Parquet to s3://gitops-silver-bkt-dev-790347819012/curated/employees/
   - Branch B (CSV -> Delta): AWS Glue job gitops-bronze-to-silver-delta-employees-dev
     writes ACID Delta Lake table to s3://gitops-silver-bkt-dev-790347819012/delta/employees/
5. Verifies both Parquet and Delta Lake partition structures in the Silver S3 bucket.
6. Crawls the curated Parquet directory (STRICTLY restricted to curated/employees/)
   via AWS Glue Crawler (gitops-silver-parquet-crawler-dev) to register/update the
   Data Catalog table (gitops_dev_db.employees).
7. Executes an automated Amazon Athena validation query verifying catalog availability.
8. Completes pipeline when both data layers and catalog updates are verified.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

import boto3
from botocore.exceptions import ClientError


# Configuration defaults with defensive sanitization
def _resolve_bucket(env_var: str, default: str) -> str:
    val = os.getenv(env_var, "").strip()
    if not val or "my-org" in val:
        return default
    return val


BRONZE_BUCKET = _resolve_bucket("S3_BRONZE_BUCKET", "gitops-bronze-bkt-dev-790347819012")
SILVER_BUCKET = _resolve_bucket("S3_SILVER_BUCKET", "gitops-silver-bkt-dev-790347819012")
GLUE_PARQUET_JOB_NAME = os.getenv("GLUE_JOB_NAME", "gitops-bronze-to-silver-employees-dev")
GLUE_DELTA_JOB_NAME = os.getenv(
    "GLUE_DELTA_JOB_NAME", "gitops-bronze-to-silver-delta-employees-dev"
)
GLUE_CRAWLER_NAME = os.getenv(
    "GLUE_CRAWLER_NAME", "gitops-silver-parquet-crawler-dev"
)
GLUE_DATABASE = os.getenv("GLUE_DATABASE", "gitops_dev_db")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "gitops-dev-workgroup")
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

    return str(file_path)


def upload_to_bronze_s3(**context) -> dict:
    """Upload local CSV to Bronze S3 bucket."""
    ti = context["ti"]
    source_path = ti.xcom_pull(task_ids="validate_local_csv") or str(resolve_source_file())

    bucket_name = BRONZE_BUCKET
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


def verify_bronze_object(**context) -> None:
    """Verify that the raw object exists in Bronze S3 and log metadata."""
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


def verify_silver_parquet_data(**context) -> None:
    """Verify that curated Parquet files were created in the Silver S3 bucket."""
    print(f"Connecting to AWS S3 in region {AWS_REGION}...")
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    prefix = "curated/employees/"
    print(f"Checking for curated Parquet files in s3://{SILVER_BUCKET}/{prefix}...")

    response = s3_client.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    contents = response.get("Contents", [])

    parquet_files = [obj for obj in contents if obj["Key"].endswith(".parquet")]

    print("=" * 60)
    print("S3 SILVER PARQUET VERIFICATION")
    print("=" * 60)
    print(f"Silver Bucket: s3://{SILVER_BUCKET}")
    print(f"Target Prefix: {prefix}")
    print(f"Total Objects Found: {len(contents)}")
    print(f"Parquet Partitions Found: {len(parquet_files)}")

    for obj in parquet_files:
        print(f"  - {obj['Key']} ({obj['Size']} bytes, Last Modified: {obj['LastModified']})")

    print("=" * 60)

    if not parquet_files:
        raise ValueError(
            f"No Parquet files found in s3://{SILVER_BUCKET}/{prefix} after Glue job run!"
        )

    print("Silver Parquet data successfully verified!")


def verify_silver_delta_data(**context) -> None:
    """Verify that curated Delta Lake table was created in the Silver S3 bucket."""
    print(f"Connecting to AWS S3 in region {AWS_REGION}...")
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    prefix = "delta/employees/"
    print(f"Checking for Delta Lake table in s3://{SILVER_BUCKET}/{prefix}...")

    response = s3_client.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    contents = response.get("Contents", [])

    delta_logs = [obj for obj in contents if "_delta_log" in obj["Key"]]
    data_files = [obj for obj in contents if obj["Key"].endswith(".parquet")]

    print("=" * 60)
    print("S3 SILVER DELTA LAKE VERIFICATION")
    print("=" * 60)
    print(f"Silver Bucket: s3://{SILVER_BUCKET}")
    print(f"Delta Table Prefix: {prefix}")
    print(f"Total Objects Found: {len(contents)}")
    print(f"Delta Log Commits: {len(delta_logs)}")
    print(f"Delta Data Partitions: {len(data_files)}")

    for obj in contents[:10]:
        print(f"  - {obj['Key']} ({obj['Size']} bytes)")
    if len(contents) > 10:
        print(f"  ... and {len(contents) - 10} more files")

    print("=" * 60)

    if not delta_logs or not data_files:
        raise ValueError(
            f"Delta Lake table incomplete in s3://{SILVER_BUCKET}/{prefix}! Logs: {len(delta_logs)}, Files: {len(data_files)}"
        )

    print("Silver Delta Lake table successfully verified!")


def query_athena_curated_data(**context) -> None:
    """Execute validation query in Athena confirming catalog discovery and table queryability."""
    print(f"Connecting to AWS Athena in region {AWS_REGION}...")
    athena_client = boto3.client("athena", region_name=AWS_REGION)

    query_str = (
        f'SELECT department, count(*) as emp_count, round(avg(salary), 2) as avg_sal '
        f'FROM "{GLUE_DATABASE}"."employees" '
        f'GROUP BY department ORDER BY emp_count DESC'
    )

    print(f"Executing Athena verification query: {query_str}")
    response = athena_client.start_query_execution(
        QueryString=query_str,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_id = response["QueryExecutionId"]
    print(f"Athena QueryExecutionId: {query_id}")

    # Poll for query completion
    max_attempts = 30
    for _ in range(max_attempts):
        status_resp = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status_resp["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena query failed with state {state}: {reason}")

    # Fetch and log results
    results_resp = athena_client.get_query_results(QueryExecutionId=query_id)
    rows = results_resp["ResultSet"]["Rows"]

    print("=" * 60)
    print("ATHENA VALIDATION QUERY SUCCEEDED")
    print("=" * 60)
    header = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
    print(f"{header[0]:<15} | {header[1]:<10} | {header[2]:<10}")
    print("-" * 40)
    total_records = 0
    for r in rows[1:]:
        vals = [c.get("VarCharValue", "") for c in r["Data"]]
        print(f"{vals[0]:<15} | {vals[1]:<10} | {vals[2]:<10}")
        total_records += int(vals[1])
    print("=" * 60)
    print(f"Total Discovered Records Verified in Athena: {total_records}")

    if total_records <= 0:
        raise ValueError("Athena query returned zero records from cataloged table!")


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
    description="End-to-end Medallion pipeline: Bronze Ingestion, Dual Glue ETL (Parquet & Delta), Crawler, and Athena Validation",
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "bronze", "silver", "glue", "parquet", "delta", "crawler", "athena"],
) as dag:
    start_task = BashOperator(
        task_id="start_pipeline",
        bash_command='echo "Starting Medallion Ingestion & Dual ETL pipeline at $(date)"',
    )

    validate_task = PythonOperator(
        task_id="validate_local_csv",
        python_callable=validate_local_csv,
    )

    upload_task = PythonOperator(
        task_id="upload_to_bronze_s3",
        python_callable=upload_to_bronze_s3,
    )

    verify_bronze_task = PythonOperator(
        task_id="verify_bronze_upload",
        python_callable=verify_bronze_object,
    )

    # Transformation 1: CSV -> Parquet
    glue_parquet_task = GlueJobOperator(
        task_id="transform_bronze_to_silver_parquet",
        job_name=GLUE_PARQUET_JOB_NAME,
        region_name=AWS_REGION,
        script_args={
            "--source_bucket": BRONZE_BUCKET,
            "--target_bucket": SILVER_BUCKET,
            "--source_prefix": "raw/employees/",
            "--target_prefix": "curated/employees/",
            "--environment": "dev",
        },
        wait_for_completion=True,
        job_poll_interval=10,
        verbose=True,
    )

    # Transformation 2: CSV -> Delta Lake
    glue_delta_task = GlueJobOperator(
        task_id="transform_bronze_to_silver_delta",
        job_name=GLUE_DELTA_JOB_NAME,
        region_name=AWS_REGION,
        script_args={
            "--source_bucket": BRONZE_BUCKET,
            "--target_bucket": SILVER_BUCKET,
            "--source_prefix": "raw/employees/",
            "--target_prefix": "delta/employees/",
            "--environment": "dev",
        },
        wait_for_completion=True,
        job_poll_interval=10,
        verbose=True,
    )

    verify_parquet_task = PythonOperator(
        task_id="verify_silver_parquet",
        python_callable=verify_silver_parquet_data,
    )

    verify_delta_task = PythonOperator(
        task_id="verify_silver_delta",
        python_callable=verify_silver_delta_data,
    )

    # Catalog & Schema Discovery: Strictly crawls ONLY curated/employees/
    crawl_parquet_task = GlueCrawlerOperator(
        task_id="crawl_silver_parquet",
        config={"Name": GLUE_CRAWLER_NAME},
        region_name=AWS_REGION,
        wait_for_completion=True,
        poll_interval=10,
    )

    # Analytics Validation: Query cataloged table via Amazon Athena
    athena_validation_task = PythonOperator(
        task_id="query_athena_curated",
        python_callable=query_athena_curated_data,
    )

    end_task = BashOperator(
        task_id="end_pipeline",
        bash_command='echo "Medallion Pipeline (Bronze Ingestion -> Silver Parquet & Delta -> Glue Crawler -> Athena) completed successfully at $(date)"',
    )

    # Define DAG task dependencies:
    # 1. Ingestion & Bronze Verification
    start_task >> validate_task >> upload_task >> verify_bronze_task

    # 2. Branch A: Parquet Ingestion -> S3 Verify -> Glue Crawler -> Athena Query -> End
    (
        verify_bronze_task
        >> glue_parquet_task
        >> verify_parquet_task
        >> crawl_parquet_task
        >> athena_validation_task
        >> end_task
    )

    # 3. Branch B: Delta Lake Ingestion -> S3 Verify -> End
    (
        verify_bronze_task
        >> glue_delta_task
        >> verify_delta_task
        >> end_task
    )
