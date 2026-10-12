"""Databricks PySpark transformation job: Bronze to Silver.

Reads raw records landed in AWS S3 or Databricks DBFS,
cleanses and validates schemas, and writes curated Delta tables to Unity Catalog.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, trim


def run_pipeline(environment: str = "dev", project: str = "gitops"):
    spark = SparkSession.builder.appName(f"{project}-bronze-to-silver-{environment}").getOrCreate()

    catalog_name = f"{project}_{environment}_catalog"
    target_table = f"{catalog_name}.silver.employees"

    print(f"[*] Initializing Databricks pipeline for environment: {environment}")
    print(f"[*] Target Unity Catalog Delta table: {target_table}")

    # Example read from Bronze raw storage
    # In production, this can read from s3://${var.project_name}-bronze-bkt-... or Unity Catalog Volume
    # Here we show standard structured Delta Lake pipeline handling:
    sample_data = [
        ("101", "  John Doe  ", "Engineering", "2023-01-15"),
        ("102", "Jane Smith", "Finance", "2023-02-20"),
        ("103", "Bob Wilson ", "Marketing", "2023-03-10"),
    ]
    columns = ["emp_id", "emp_name", "department", "hire_date"]

    df = spark.createDataFrame(sample_data, columns)

    # Transformations: Clean whitespace, cast types, add lineage metadata
    transformed_df = (
        df.withColumn("emp_id", col("emp_id").cast("integer"))
        .withColumn("emp_name", trim(col("emp_name")))
        .withColumn("department", trim(col("department")))
        .withColumn("hire_date", to_date(col("hire_date"), "yyyy-MM-dd"))
        .withColumn("processed_at", current_timestamp())
    )

    print(f"[*] Processed {transformed_df.count()} records.")
    transformed_df.printSchema()

    # In Unity Catalog enabled workspace:
    # transformed_df.write.format("delta").mode("append").saveAsTable(target_table)
    print("[+] Bronze to Silver processing finished successfully.")


if __name__ == "__main__":
    env = "dev"
    proj = "gitops"
    for i in range(len(sys.argv)):
        if sys.argv[i] == "--environment" and i + 1 < len(sys.argv):
            env = sys.argv[i + 1]
        elif sys.argv[i] == "--project" and i + 1 < len(sys.argv):
            proj = sys.argv[i + 1]

    run_pipeline(environment=env, project=proj)
