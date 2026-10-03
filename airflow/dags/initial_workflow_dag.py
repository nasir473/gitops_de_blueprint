"""Initial workflow DAG for verifying local Apache Airflow deployment.

Provides a baseline pipeline to validate task scheduling, execution, and
environment variables prior to deploying production data pipelines.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def print_platform_environment(**context) -> None:
    """Print runtime execution environment details and verify dependencies."""
    print("=" * 60)
    print("AWS MODERN DATA PLATFORM - AIRFLOW LOCAL RUNNER")
    print("=" * 60)
    print(f"Python Version: {sys.version}")
    print(f"Airflow Home: {os.getenv('AIRFLOW_HOME', '/opt/airflow')}")
    print(f"Current Working Dir: {os.getcwd()}")
    print(f"AWS Default Region: {os.getenv('AWS_DEFAULT_REGION', 'not-set')}")
    print(f"Logical Date: {context.get('ds', 'N/A')}")
    print(f"Run ID: {context.get('run_id', 'N/A')}")
    print("=" * 60)


with DAG(
    dag_id="initial_workflow_dag",
    default_args=default_args,
    description="Initial smoke test workflow for local Airflow validation",
    schedule=None,  # Manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["foundational", "smoke-test", "dev"],
) as dag:
    start_task = BashOperator(
        task_id="start_pipeline",
        bash_command='echo "Starting initial data platform workflow at $(date)"',
    )

    env_check_task = PythonOperator(
        task_id="verify_runtime_environment",
        python_callable=print_platform_environment,
    )

    structure_check_task = BashOperator(
        task_id="verify_directory_mounts",
        bash_command="""
        echo "Checking mounted directories:"
        ls -la /opt/airflow/dags
        ls -la /opt/airflow/plugins
        echo "Verification complete."
        """,
    )

    end_task = BashOperator(
        task_id="end_pipeline",
        bash_command='echo "Initial data platform workflow completed successfully at $(date)"',
    )

    # Define task dependencies
    start_task >> env_check_task >> structure_check_task >> end_task
