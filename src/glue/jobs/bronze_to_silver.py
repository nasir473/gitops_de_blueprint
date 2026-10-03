"""AWS Glue PySpark ETL Job: Bronze to Silver Layer Transformation.

This job performs batch ingestion and curation:
1. Reads raw CSV payloads from the Bronze S3 landing zone.
2. Applies strict schema enforcement and type casting.
3. Cleanses and enriches records (whitespace trimming, metadata audit columns).
4. Deduplicates records based on primary business key (emp_id).
5. Writes optimized Snappy-compressed Parquet files to the Silver S3 bucket,
   partitioned for high-performance Athena querying.
"""

import sys
import logging
from typing import Dict, Any

# Configure standard logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("glue_bronze_to_silver")


def get_job_arguments() -> Dict[str, Any]:
    """Parse runtime arguments passed by AWS Glue or command line.

    Supports both AWS Glue `getResolvedOptions` and fallback to standard sys.argv
    for local or CI testing.
    """
    default_args = {
        "JOB_NAME": "bronze_to_silver_employees",
        "source_bucket": "gitops-bronze-bkt-dev-790347819012",
        "target_bucket": "gitops-silver-bkt-dev-790347819012",
        "source_prefix": "raw/employees/",
        "target_prefix": "curated/employees/",
        "environment": "dev",
    }

    try:
        from awsglue.utils import getResolvedOptions

        # Standard Glue argument parser
        expected_keys = [
            "JOB_NAME",
            "source_bucket",
            "target_bucket",
        ]
        # Check which expected keys exist in sys.argv
        keys_to_resolve = [k for k in expected_keys if f"--{k}" in sys.argv]
        optional_keys = ["source_prefix", "target_prefix", "environment"]
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


def init_spark_and_glue():
    """Initialize SparkSession and optional GlueContext."""
    try:
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job

        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        is_glue = True
    except ImportError:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("bronze_to_silver_employees")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )
        glue_context = None
        Job = None
        is_glue = False

    return spark, glue_context, Job, is_glue


def get_employee_raw_schema():
    """Define the explicit schema for the incoming employee CSV data."""
    from pyspark.sql.types import (
        StructType,
        StructField,
        IntegerType,
        StringType,
        DoubleType,
    )

    return StructType(
        [
            StructField("emp_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("date_of_joining", StringType(), True),
        ]
    )


def read_bronze_csv(spark, source_s3_path: str):
    """Read raw CSV dataset from Bronze S3 path with schema enforcement."""
    logger.info("Reading raw Bronze CSV files from: %s", source_s3_path)

    schema = get_employee_raw_schema()

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(source_s3_path)
    )

    return df


def transform_bronze_to_silver(df):
    """Transform, validate, and enrich raw DataFrame into curated Silver standard.

    Transformations:
    - Cast date_of_joining string into proper DateType.
    - Trim whitespaces on all string attributes.
    - Filter invalid null/negative employee IDs.
    - Deduplicate records keeping unique emp_id.
    - Add data lineage and audit metadata columns (_ingested_at, _source_file).
    """
    from pyspark.sql.functions import (
        col,
        to_date,
        trim,
        current_timestamp,
        input_file_name,
    )
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    logger.info("Applying Silver data curation and transformation rules...")

    # 1. Cleaning, casting, and audit columns
    curated_df = (
        df.filter(col("emp_id").isNotNull() & (col("emp_id") > 0))
        .withColumn("name", trim(col("name")))
        .withColumn("department", trim(col("department")))
        .withColumn("city", trim(col("city")))
        .withColumn("date_of_joining", to_date(col("date_of_joining"), "yyyy-MM-dd"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    # 2. Business key deduplication (emp_id)
    # In case of duplicate entries across daily landing files, retain the latest
    window_spec = Window.partitionBy("emp_id").orderBy(col("_ingested_at").desc())
    deduped_df = (
        curated_df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return deduped_df


def write_silver_parquet(df, target_s3_path: str, partition_cols=None):
    """Write curated DataFrame to Silver S3 layer as Snappy Parquet.

    Uses partitionBy for query pruning in Amazon Athena.
    """
    if partition_cols is None:
        partition_cols = ["department"]

    logger.info(
        "Writing curated Parquet files to: %s (Partitioned by: %s)",
        target_s3_path,
        partition_cols,
    )

    (
        df.write.mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy(*partition_cols)
        .save(target_s3_path)
    )

    logger.info("Successfully wrote curated Parquet dataset to Silver layer.")


def main():
    """Main execution flow for AWS Glue job."""
    logger.info("Starting Bronze to Silver Glue ETL job...")

    # 1. Resolve arguments
    args = get_job_arguments()
    logger.info("Job Parameters: %s", args)

    source_bucket = args["source_bucket"]
    target_bucket = args["target_bucket"]
    source_prefix = args["source_prefix"].rstrip("/")
    target_prefix = args["target_prefix"].rstrip("/")

    source_s3_path = f"s3://{source_bucket}/{source_prefix}/"
    target_s3_path = f"s3://{target_bucket}/{target_prefix}/"

    # 2. Initialize Spark / Glue contexts
    spark, glue_context, Job, is_glue = init_spark_and_glue()

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
        logger.info("Curated record count for Silver zone: %d", silver_count)

        # 5. Write Parquet to Silver partitioned by department
        write_silver_parquet(silver_df, target_s3_path, partition_cols=["department"])

        # 6. Commit job if in AWS Glue
        if job:
            job.commit()

        logger.info("Bronze to Silver ETL completed successfully.")

    except Exception as e:
        logger.error("Job execution failed with error: %s", str(e), exc_info=True)
        raise
    finally:
        if not is_glue:
            spark.stop()


if __name__ == "__main__":
    main()
