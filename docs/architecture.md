# Cloud Data Platform Architecture

## 1. Overview & Architectural Philosophy

This blueprint defines an enterprise-grade, GitOps-driven modern data platform built natively on AWS with future-ready integration for Snowflake. The platform adheres to the **Medallion (Bronze/Silver/Gold) architecture**, event-driven processing patterns, zero-trust IAM principles, and strict Infrastructure-as-Code (Terraform) governance.

### Core Tenets
- **GitOps First**: All infrastructure, schemas, ETL jobs, and orchestration definitions reside in version control. Deployments are declarative, auditable, and automated.
- **Decoupled Compute and Storage**: Persistent data lives in Amazon S3, allowing compute layers (AWS Glue, Amazon Athena, Apache Airflow, Snowflake) to scale independently.
- **Fail-Safe Ingestion & Quarantine**: Malformed or non-compliant records are segregated into a dedicated quarantine layer rather than silently dropped or halting pipelines.
- **Event-Driven & Scheduled Processing**: Real-time arrival notifications feed SQS/Lambda triggers, while complex batch workflows are coordinated by Apache Airflow.

---

## 2. High-Level Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion ["1. Multi-Source Ingestion & Manifests"]
        SRC_FS["File Share Transfer<br/>(file_share_data.csv)"]
        SRC_API["REST API Ingestion<br/>(api_data.csv)"]
        SRC_ORA["Oracle DB Extraction<br/>(oracle_data.csv)"]
        S3_BRONZE[("S3 Bronze Bucket<br/>(Raw CSV + .manifest)")]
        SRC_FS & SRC_API & SRC_ORA -->|"Raw CSV + Manifest"| S3_BRONZE
    end

    subgraph Barrier ["2. Serverless Barrier & Synchronization"]
        LAMBDA["AWS Lambda Ingestion Trigger<br/>(Manifest Validator)"]
        DDB[("Amazon DynamoDB<br/>(Atomic Barrier State)")]
        SQS["Amazon SQS Queue<br/>(gitops-batch-ready-queue-dev)"]
        SNS["Amazon SNS Topic<br/>(Batch Ready Broadcast)"]
        SQS_DLQ["Dead-Letter Queue (DLQ)"]

        S3_BRONZE -->|"ObjectCreated (*.manifest)"| LAMBDA
        LAMBDA <-->|"Conditional Check Expression"| DDB
        LAMBDA -->|"All Sources Complete"| SQS
        LAMBDA -->|"Broadcast Alert"| SNS
        SQS -.->|"Failure"| SQS_DLQ
    end

    subgraph Orchestration ["3. Airflow Orchestration & Sensors"]
        AIRFLOW_SENSOR["Airflow SqsSensor<br/>(wait_for_sqs_batch_ready)"]
        AIRFLOW_PARSE["Parse Batch Metadata"]
        SQS -->|"Dequeue"| AIRFLOW_SENSOR
        AIRFLOW_SENSOR --> AIRFLOW_PARSE
    end

    subgraph Processing ["4. Parallel PySpark Processing (AWS Glue 4.0)"]
        GLUE_PQ["AWS Glue Job: Parquet<br/>(bronze_to_silver)"]
        GLUE_DELTA["AWS Glue Job: Delta Lake<br/>(bronze_to_silver_delta)"]
        GLUE_ICE["AWS Glue Job: Iceberg<br/>(bronze_to_silver_iceberg)"]

        AIRFLOW_PARSE --> GLUE_PQ
        AIRFLOW_PARSE --> GLUE_DELTA
        AIRFLOW_PARSE --> GLUE_ICE

        S3_BRONZE -.-> GLUE_PQ & GLUE_DELTA & GLUE_ICE

        S3_SILVER_PQ[("S3 Silver Parquet")]
        S3_QUARANTINE[("S3 Quarantine Bucket")]
        S3_SILVER_DELTA[("S3 Silver Delta Lake")]
        S3_SILVER_ICE[("S3 Silver Apache Iceberg")]

        GLUE_PQ -->|"Valid Snappy Parquet"| S3_SILVER_PQ
        GLUE_PQ -->|"Corrupt / Invalid Schema"| S3_QUARANTINE
        GLUE_DELTA -->|"ACID Delta Log & Data"| S3_SILVER_DELTA
        GLUE_ICE -->|"Table Format v2"| S3_SILVER_ICE
    end

    subgraph Discovery ["5. Verification & Metadata Discovery"]
        CRAWLER["AWS Glue Crawler<br/>(silver-parquet-crawler)"]
        GLUE_CATALOG[("AWS Glue Data Catalog<br/>(gitops_dev_db)")]
        S3_SILVER_PQ --> CRAWLER
        CRAWLER -->|"Discover Partitions & Schema"| GLUE_CATALOG
        GLUE_ICE -->|"Native Catalog Sync"| GLUE_CATALOG
    end

    subgraph Analytics ["6. Analytics, Governance & Gold Aggregations"]
        ATHENA["Amazon Athena (Engine v3)<br/>Ad-hoc SQL Analytics"]
        GLUE_CATALOG --- ATHENA
        S3_SILVER_PQ --- ATHENA
        S3_ATHENA[("S3 Athena Results Bucket")]
        ATHENA -->|"Query Spooling"| S3_ATHENA

        DBX_SQL["Databricks SQL Warehouse<br/>(Serverless API 2.0)"]
        DBX_UC[("Databricks Unity Catalog<br/>(External Storage Location)")]
        DBX_GOLD[("Databricks Gold Layer<br/>gold_department_summary")]
        S3_SILVER_DELTA <--> DBX_UC
        DBX_UC <--> DBX_SQL
        DBX_SQL -->|"Refresh Gold Aggregations"| DBX_GOLD
    end

    subgraph Governance ["7. CI/CD & Security"]
        IAM["AWS IAM Least Privilege Roles & OIDC"]
        GHA["GitHub Actions CI/CD Pipeline"]
        GHA -->|"Automated Lint, Test & Terraform"| IAM
    end
```

---

## 3. Storage Tier Definitions

| Layer | Bucket Role | Format | Retention / Lifecycle | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | Raw Ingestion | Original (JSON/CSV/Parquet) | 90 days Transition to Glacier | Immutable raw data landing zone partitioned by ingestion date (`year=YYYY/month=MM/day=DD`). |
| **Silver** | Curated & Validated | Snappy Parquet | Standard | Cleaned, deduplicated, schema-enforced datasets registered in the Glue Data Catalog. |
| **Quarantine** | Error Isolation | JSON with error metadata | 30 days Expiration | Malformed records rejected by validation logic for triage and debugging. |
| **Athena** | Query Spillover | CSV / Parquet | 7 days Auto-delete | Query execution spools and intermediate result sets generated by Athena workgroups. |

---

## 4. Key Platform Components

1. **Terraform**: Manages AWS resources across isolated environments (`dev`, `qa`, `prod`) using modular patterns and S3/DynamoDB remote state backends.
2. **Amazon SQS & AWS Lambda**: Decouples S3 ingestion notifications from downstream compute, absorbing traffic spikes with dead-letter queue (DLQ) support.
3. **AWS Glue (PySpark)**: Serverless distributed data processing engine executing Bronze-to-Silver transformations, type casting, schema validation, and partition writes.
4. **AWS Glue Data Catalog**: Centralized metadata repository storing table schemas, partitions, and column statistics.
5. **Amazon Athena**: Interactive SQL query engine for ad-hoc exploration, data quality validations, and business reporting directly against Silver Parquet files.
6. **Apache Airflow**: Workflow orchestrator managing dependency graphs, scheduling, sensor triggers, retries, and operational alerting.
7. **Snowflake (Future Scaffolding)**: Analytical data warehouse connecting to S3 Silver via secure IAM roles and external stages.
8. **GitHub Actions**: Automated CI/CD pipeline enforcing static code analysis (Ruff), unit testing (Pytest), Terraform validation, and OIDC-authenticated AWS deployments.
