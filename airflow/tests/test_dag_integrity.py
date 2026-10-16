"""Airflow DAG Integrity and Topology Tests.

Validates that:
1. All DAG files parse without import errors or syntax violations.
2. The event-driven multi-source batch DAG contains all expected tasks and branches.
3. The Snowflake Iceberg refresh task is properly wired downstream of verify_silver.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def mock_dag_env():
    """Mock required environment variables for DAG loading."""
    env_vars = {
        "AWS_DEFAULT_REGION": "ap-south-1",
        "S3_BRONZE_BUCKET": "gitops-bronze-bkt-dev-790347819012",
        "S3_SILVER_BUCKET": "gitops-silver-bkt-dev-790347819012",
        "GLUE_JOB_NAME": "gitops-bronze-to-silver-employees-dev",
        "GLUE_DELTA_JOB_NAME": "gitops-bronze-to-silver-delta-employees-dev",
        "GLUE_ICEBERG_JOB_NAME": "gitops-bronze-to-silver-iceberg-employees-dev",
        "GLUE_CRAWLER_NAME": "gitops-silver-parquet-crawler-dev",
        "GLUE_DATABASE": "gitops_dev_db",
        "ATHENA_WORKGROUP": "gitops-dev-workgroup",
        "SQS_BATCH_READY_QUEUE_URL": "https://sqs.ap-south-1.amazonaws.com/790347819012/gitops-batch-ready-queue-dev",
        "SNOWFLAKE_DATABASE": "ANALYTICS_DEV",
        "SNOWFLAKE_SCHEMA": "SILVER_ICEBERG",
        "SNOWFLAKE_TABLE": "employees_iceberg",
    }
    with patch.dict(os.environ, env_vars):
        yield


def test_event_driven_batch_dag_structure():
    """Test that event_driven_batch_workflow loads correctly and has all required tasks."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("Airflow module not installed in current python environment.")

    dags_folder = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag = DagBag(dag_folder=dags_folder, include_examples=False)

    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

    dag = dag_bag.get_dag("event_driven_batch_workflow")
    assert dag is not None, "event_driven_batch_workflow DAG not found"

    # Verify task existence
    expected_tasks = {
        "wait_for_sqs_batch_ready",
        "parse_batch_metadata",
        "run_glue_parquet_job",
        "run_glue_delta_job",
        "run_glue_iceberg_job",
        "verify_silver_partitions",
        "crawl_silver_parquet",
        "validate_athena_data",
        "refresh_databricks_gold_department_summary",
        "refresh_snowflake_iceberg_table",
        "pipeline_complete",
    }
    actual_tasks = {task.task_id for task in dag.tasks}
    assert expected_tasks.issubset(actual_tasks), f"Missing tasks: {expected_tasks - actual_tasks}"

    # Verify Snowflake task dependencies
    refresh_snowflake_task = dag.get_task("refresh_snowflake_iceberg_table")
    verify_silver_task = dag.get_task("verify_silver_partitions")
    pipeline_complete_task = dag.get_task("pipeline_complete")

    assert verify_silver_task in refresh_snowflake_task.upstream_list, (
        "verify_silver_partitions must be upstream of refresh_snowflake_iceberg_table"
    )
    assert pipeline_complete_task in refresh_snowflake_task.downstream_list, (
        "pipeline_complete must be downstream of refresh_snowflake_iceberg_table"
    )


def test_snowflake_refresh_callable_simulation():
    """Verify that refresh_snowflake_iceberg_table executes simulation cleanly without errors."""
    from airflow.dags.event_driven_batch_workflow import refresh_snowflake_iceberg_table

    result = refresh_snowflake_iceberg_table()
    assert result["status"] == "simulated"
    assert "ALTER ICEBERG TABLE ANALYTICS_DEV.SILVER_ICEBERG.employees_iceberg REFRESH;" in result["statement"]
