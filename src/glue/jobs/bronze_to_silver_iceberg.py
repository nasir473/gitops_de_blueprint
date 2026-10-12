"""AWS Glue PySpark ETL Job: Bronze to Silver Apache Iceberg Transformation.

This job performs batch ingestion and curation into Apache Iceberg format:
1. Reads raw CSV payloads from the Bronze S3 landing zone.
2. Applies strict schema enforcement and type casting.
3. Cleanses and enriches records (whitespace trimming, metadata audit columns).
4. Deduplicates records based on primary business key (emp_id).
5. Writes an ACID-compliant, Snappy-compressed Apache Iceberg table into a dedicated
   folder within the Silver S3 bucket (iceberg/employees/), partitioned by department,
   and registers it in the AWS Glue Data Catalog.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, Dict

# Configure standard logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("glue_bronze_to_silver_iceberg")


def get_job_arguments() -> Dict[str, Any]:
    """Parse runtime arguments passed by AWS Glue or command line.

    Supports both AWS Glue `getResolvedOptions` and fallback to standard sys.argv
    for local or CI testing.
    """
    default_args = {
        "JOB_NAME": "gitops-bronze-to-silver-iceberg-employees-dev",
        "source_bucket": "gitops-bronze-bkt-dev-790347819012",
        "target_bucket": "gitops-silver-bkt-dev-790347819012",
        "source_prefix": "raw/employees/",
        "target_prefix": "iceberg/employees/",
        "database_name": "gitops_dev_db",
        "table_name": "employees_iceberg",
        "catalog_name": "glue_catalog",
        "environment": "dev",
    }

    try:
        from awsglue.utils import getResolvedOptions

        expected_keys = [
            "JOB_NAME",
            "source_bucket",
            "target_bucket",
        ]
        keys_to_resolve = [k for k in expected_keys if f"--{k}" in sys.argv]
        optional_keys = [
            "source_prefix",
            "target_prefix",
            "database_name",
            "table_name",
            "catalog_name",
            "environment",
        ]
        for opt in optional_keys:
            if f"--{opt}" in sys.argv:
                keys_to_resolve.append(opt)

        if keys_to_resolve:
            resolved = getResolvedOptions(sys.argv, keys_to_resolve)
            default_args.update(resolved)
    except ImportError:
        logger.warning(
            "awsglue module not detected; running in standalone PySpark mode with default parameters."
        )

    return default_args


def init_spark_and_glue(catalog_name: str = "glue_catalog", target_warehouse: str = ""):
    """Initialize SparkSession with Apache Iceberg extensions and GlueContext."""
    try:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from pyspark.context import SparkContext

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        is_glue = True
    except ImportError:
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.appName("bronze_to_silver_iceberg_employees")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(
                f"spark.sql.catalog.{catalog_name}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
            )
            .config(
                f"spark.sql.catalog.{catalog_name}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
        )
        if target_warehouse:
            builder = builder.config(
                f"spark.sql.catalog.{catalog_name}.warehouse", target_warehouse
            )

        spark = builder.getOrCreate()
        glue_context = None
        Job = None
        is_glue = False

    return spark, glue_context, Job, is_glue


def get_employee_raw_schema():
    """Define the explicit schema for incoming employee CSV data."""
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("emp_id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("department", StringType(), nullable=True),
            StructField("salary", DoubleType(), nullable=True),
            StructField("city", StringType(), nullable=True),
            StructField("date_of_joining", StringType(), nullable=True),
        ]
    )


def read_bronze_csv(spark, source_s3_path: str):
    """Read partitioned Bronze CSV files using explicit schema."""
    logger.info("Reading raw employee CSV data from Bronze S3 path: %s", source_s3_path)
    schema = get_employee_raw_schema()

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(source_s3_path)
    )

    logger.info("Successfully loaded Bronze DataFrame. Schema discovered:")
    df.printSchema()
    return df


def transform_bronze_to_silver(df):
    """Cleanse, enrich, and deduplicate employee records.

    Transformations:
    - Trim whitespaces on string columns
    - Cast and format date_of_joining as DATE
    - Ensure partition columns year, month, day are integers
    - Add auditing metadata: _ingested_at and _source_file
    - Deduplicate on emp_id, selecting latest date_of_joining and ingestion timestamp
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    logger.info("Starting Bronze-to-Silver transformations for Iceberg target...")

    # Clean strings and standardize values
    transformed_df = (
        df.withColumn("name", F.trim(F.col("name")))
        .withColumn("department", F.upper(F.trim(F.col("department"))))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn(
            "date_of_joining",
            F.coalesce(
                F.to_date(F.col("date_of_joining"), "yyyy-MM-dd"),
                F.to_date(F.col("date_of_joining"), "dd-MM-yyyy"),
            ),
        )
    )

    # Cast or infer partition date columns
    if "year" in transformed_df.columns:
        transformed_df = transformed_df.withColumn("year", F.col("year").cast("int"))
    else:
        transformed_df = transformed_df.withColumn("year", F.year(F.col("date_of_joining")))

    if "month" in transformed_df.columns:
        transformed_df = transformed_df.withColumn("month", F.col("month").cast("int"))
    else:
        transformed_df = transformed_df.withColumn("month", F.month(F.col("date_of_joining")))

    if "day" in transformed_df.columns:
        transformed_df = transformed_df.withColumn("day", F.col("day").cast("int"))
    else:
        transformed_df = transformed_df.withColumn("day", F.dayofmonth(F.col("date_of_joining")))

    # Ingestion audit columns
    transformed_df = (
        transformed_df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_source_file",
            F.coalesce(F.input_file_name(), F.lit("unknown")),
        )
    )

    # Filter out records where primary key emp_id or department is null
    valid_df = transformed_df.filter(
        F.col("emp_id").isNotNull() & F.col("department").isNotNull()
    )

    # Deduplicate on business key: emp_id
    window_spec = Window.partitionBy("emp_id").orderBy(
        F.col("date_of_joining").desc(),
        F.col("_ingested_at").desc(),
    )

    deduped_df = (
        valid_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Reorder columns explicitly
    final_columns = [
        "emp_id",
        "name",
        "department",
        "salary",
        "city",
        "date_of_joining",
        "year",
        "month",
        "day",
        "_ingested_at",
        "_source_file",
    ]
    return deduped_df.select(*final_columns)


def write_silver_iceberg(
    spark,
    df,
    target_s3_path: str,
    catalog_name: str,
    database_name: str,
    table_name: str,
    partition_cols=None,
):
    """Write curated DataFrame to Silver S3 layer in Apache Iceberg format.

    Features enabled:
    - ACID transaction management with Iceberg metadata snapshots
    - Partitioned by department for query optimization
    - Format version 2 with Snappy Parquet data files
    - Registered in AWS Glue Data Catalog via Iceberg GlueCatalog
    """
    if partition_cols is None:
        partition_cols = ["department"]

    full_table_identifier = f"{catalog_name}.{database_name}.{table_name}"
    partition_clause = ", ".join(partition_cols)

    logger.info(
        "Writing curated Apache Iceberg table to: %s (Catalog table: %s, Partitioned by: %s)",
        target_s3_path,
        full_table_identifier,
        partition_clause,
    )

    # Repartition and sort by partition columns for Iceberg clustered writer compliance
    clustered_df = df.repartition(*partition_cols).sortWithinPartitions(*partition_cols)
    temp_view_name = "temp_silver_employees_iceberg"
    clustered_df.createOrReplaceTempView(temp_view_name)

    # Ensure database exists in catalog
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{database_name}")

    # Create or replace table atomically using Spark SQL with fanout enabled
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {full_table_identifier}
    USING iceberg
    PARTITIONED BY ({partition_clause})
    LOCATION '{target_s3_path}'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.spark.fanout.enabled' = 'true'
    )
    AS SELECT * FROM {temp_view_name} ORDER BY {partition_clause}
    """

    logger.info("Executing Iceberg Table Creation SQL:\n%s", create_table_sql)
    spark.sql(create_table_sql)

    logger.info(
        "Successfully wrote and registered curated Apache Iceberg table: %s",
        full_table_identifier,
    )


def main():
    """Main execution flow for AWS Glue Apache Iceberg ETL job."""
    logger.info("Starting Bronze to Silver Apache Iceberg Glue ETL job...")

    # 1. Resolve arguments
    args = get_job_arguments()
    logger.info("Job Parameters: %s", args)

    source_bucket = args["source_bucket"]
    target_bucket = args["target_bucket"]
    source_prefix = args["source_prefix"].rstrip("/")
    target_prefix = args["target_prefix"].rstrip("/")
    database_name = args["database_name"]
    table_name = args["table_name"]
    catalog_name = args["catalog_name"]

    source_s3_path = f"s3://{source_bucket}/{source_prefix}/"
    target_s3_path = f"s3://{target_bucket}/{target_prefix}/"
    target_warehouse = f"s3://{target_bucket}/iceberg/"

    # 2. Initialize Spark / Glue contexts
    spark, glue_context, Job, is_glue = init_spark_and_glue(
        catalog_name=catalog_name, target_warehouse=target_warehouse
    )

    job = None
    if is_glue and Job:
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)

    try:
        # 3. Read raw CSV from Bronze
        raw_df = read_bronze_csv(spark, source_s3_path)
        raw_count = raw_df.count()
        logger.info("Raw record count ingested from Bronze: %d", raw_count)

        if raw_count == 0:
            logger.warning("No records found at source path %s. Skipping.", source_s3_path)
            return

        # 4. Transform into Silver curated dataset
        silver_df = transform_bronze_to_silver(raw_df)
        silver_count = silver_df.count()
        logger.info("Curated record count for Silver Iceberg zone: %d", silver_count)

        # 5. Write to Silver in Apache Iceberg format partitioned by department
        write_silver_iceberg(
            spark=spark,
            df=silver_df,
            target_s3_path=target_s3_path,
            catalog_name=catalog_name,
            database_name=database_name,
            table_name=table_name,
            partition_cols=["department"],
        )

        # 6. Commit job if in AWS Glue
        if job:
            job.commit()

        logger.info("Bronze to Silver Apache Iceberg ETL completed successfully.")

    except Exception as e:
        logger.error("Job execution failed with error: %s", str(e), exc_info=True)
        raise
    finally:
        if not is_glue:
            spark.stop()


if __name__ == "__main__":
    main()
