# AWS Modern Data Platform: Master Architectural Roadmap

> **Platform Status**: Phase 1 Complete | **Current Stage**: Ready for Phase 2  
> **Repository**: `gitops_de_blueprint`  
> **Author**: Principal Cloud Data Architect & DevOps Engineer  

---

## 1. Executive Summary

This roadmap outlines the phased, incremental implementation plan for building an enterprise-grade, GitOps-managed AWS Modern Data Platform. Each phase is strictly bounded, independently testable, and triggered on-demand to guarantee architectural precision, modularity, and zero premature complexity.

---

## 2. Phase Execution Matrix

| Phase | Title | Status | Primary Deliverables | Trigger Command / Prompt |
| :---: | :--- | :---: | :--- | :--- |
| **1** | **Project Scaffolding & Tooling** | **Complete** | Monorepo layout, baseline tooling (Makefile, Ruff, Pytest), `.editorconfig`, `.gitignore`, `docs/` | *Completed in current session* |
| **2** | **Terraform State Backend & Provider Setup** | **Pending User Trigger** | S3 remote backend, DynamoDB lock table configuration, multi-env provider setup | `"Proceed with Phase 2: Configure Terraform state backend and provider setup"` |
| **3** | **S3 Storage Architecture** | **In Progress** | Reusable S3 module, Bronze, Silver, & Scripts buckets provisioned, SSE-S3 encryption, lifecycle tiering, TLS enforcement | `gitops-bronze-bkt`, `gitops-silver-bkt`, `gitops-scripts-bkt` live in dev |
| **4** | **IAM Roles & GitHub Actions OIDC Trust** | **Pending User Trigger** | Terraform IAM module, least-privilege service roles (Glue, Lambda, Airflow), GitHub Actions OIDC federated trust | `"Proceed with Phase 4: Implement IAM Roles and GitHub Actions OIDC Trust"` |
| **5** | **Event Ingestion (EventBridge / SQS / Lambda Trigger)** | **Pending User Trigger** | S3 event notifications, SQS dead-letter queue, validation Lambda handler, unit tests | `"Proceed with Phase 5: Implement Event Ingestion with SQS and Lambda"` |
| **6** | **Glue Catalog & PySpark Bronze-to-Silver Job** | **In Progress** | Reusable Glue module, IAM role, PySpark ETL script uploaded to S3, Glue job provisioned | `gitops-bronze-to-silver-employees-dev` provisioned in dev |
| **7** | **Airflow Orchestration & DAG Setup** | **Pending User Trigger** | Production Airflow DAGs, Glue job operators, dataset sensors, DAG integrity tests | `"Proceed with Phase 7: Implement Airflow Orchestration and DAG Setup"` |
| **8** | **Athena Ad-Hoc Analytics & Data Quality Rules** | **Pending User Trigger** | Athena workgroup, result bucket settings, DDL partition scripts, SQL data quality assertion suite | `"Proceed with Phase 8: Configure Athena Ad-Hoc Analytics and Data Quality Rules"` |
| **9** | **Future Snowflake Scaffolding** | **Pending User Trigger** | Snowflake external storage integration, stages pointing to S3 Silver, RBAC roles, target analytical tables | `"Proceed with Phase 9: Implement Snowflake Storage Integration and RBAC Scaffolding"` |
| **10** | **CI/CD Pipelines & Automation** | **Pending User Trigger** | GitHub Actions workflows for PR linting, pytest matrix, Terraform plan on PR, automated apply on merge | `"Proceed with Phase 10: Implement CI/CD Pipelines and GitHub Actions Automation"` |

---

## 3. Phase Deep-Dive & Acceptance Criteria

### Phase 1: Project Scaffolding & Tooling (Current Phase)
- **Status**: **Complete**
- **Scope**:
  - Full directory hierarchy for `.github`, `airflow`, `src`, `terraform`, `snowflake`, `sql`, `tests`, `scripts`, `docs`.
  - Developer productivity tooling: `Makefile`, `pyproject.toml` (Ruff & Pytest), `.pre-commit-config.yaml`.
  - Comprehensive `.gitignore`, `.editorconfig`, and template `.env.example`.
  - Architecture documentation (`docs/architecture.md`) with Mermaid flowcharts and operational runbook (`docs/runbook.md`).
- **Validation Gate**:
  - `make lint` passes cleanly.
  - No premature business logic or AWS resource definitions committed.

---

### Phase 2: Terraform State Backend & Provider Setup
- **Status**: **Pending User Trigger**
- **Objective**: Establish remote state isolation and locking across `dev`, `qa`, and `prod` environments.
- **Key Deliverables**:
  - S3 backend bucket configuration with server-side encryption and versioning.
  - DynamoDB state-locking table.
  - `backend.tf` configurations in each environment directory.
  - Clear environment separation without code duplication.
- **Validation Gate**:
  - `terraform init` succeeds in `terraform/environments/dev` using remote backend.
  - State locks verified via DynamoDB.

---

### Phase 3: S3 Storage Architecture (Medallion Layers)
- **Status**: **In Progress (Bronze, Silver, & Scripts Complete)**
- **Objective**: Provision production-grade, secure object storage for all data lake tiers.
- **Key Deliverables**:
  - Reusable `terraform/modules/s3` module (versioning, AES256 encryption, Bucket Keys, TLS-only policy, Block Public Access).
  - **Bronze Bucket**: `gitops-bronze-bkt-dev-790347819012` (**Active** - Raw immutable ingestion zone, 30d IA, 90d Glacier transition).
  - **Silver Bucket**: `gitops-silver-bkt-dev-790347819012` (**Active** - Curated Parquet storage, 60d IA, 180d Glacier transition).
  - **Scripts / Artifacts Bucket**: `gitops-scripts-bkt-dev-790347819012` (**Active** - Code artifacts for Glue jobs, Lambda packages, and utilities).
  - **Quarantine Bucket**: Isolated storage for malformed/rejected payloads with 30-day auto-purge (Pending).
  - **Athena Results Bucket**: Query spooling storage with 7-day expiration (Pending).
- **Validation Gate**:
  - `terraform validate` passes; bucket policies enforce TLS and deny unencrypted uploads.
  - S3 buckets verified via AWS CLI and tagged with `Layer`, `Environment`, and `ManagedBy=Terraform`.

---

### Phase 4: IAM Roles & GitHub Actions OIDC Trust
- **Status**: **Pending User Trigger**
- **Objective**: Establish zero-trust IAM security with keyless CI/CD authentication via OpenID Connect (OIDC).
- **Key Deliverables**:
  - Reusable `terraform/modules/iam` module.
  - AWS OIDC Identity Provider for `token.actions.githubusercontent.com`.
  - IAM Role for GitHub Actions with branch-scoped subject (`sub`) condition claims.
  - Least-privilege IAM service roles for AWS Glue (S3 read/write, Catalog access, CloudWatch Logs).
  - Lambda execution role with SQS read and CloudWatch write policies.
- **Validation Gate**:
  - Zero permanent IAM user credentials or long-lived secret access keys.

---

### Phase 5: Event Ingestion (EventBridge / SQS / Lambda Trigger)
- **Status**: **Pending User Trigger**
- **Objective**: Build resilient, asynchronous event-driven ingestion buffering incoming S3 objects.
- **Key Deliverables**:
  - Reusable `terraform/modules/sqs` module with Dead Letter Queue (DLQ) and redrive policy.
  - S3 ObjectCreated event notification routed to SQS.
  - Reusable `terraform/modules/lambda` module.
  - `src/lambda/handler.py`: Light event parser/validator that reads from SQS, checks payload metadata, and records audit trail.
  - Pytest unit tests for Lambda handler with mocked SQS event fixtures.
- **Validation Gate**:
  - Unit tests in `tests/unit/test_lambda_handler.py` pass with >90% coverage.

---

### Phase 6: Glue Catalog & PySpark Bronze-to-Silver Job (Parquet & Delta)
- **Status**: **Complete (Dual Parquet & Delta Glue Jobs Provisioned & Verified)**
- **Objective**: Implement enterprise distributed ETL processing raw Bronze payloads into clean Silver Parquet and Delta Lake formats.
- **Key Deliverables**:
  - Reusable `terraform/modules/glue` module for AWS Glue 4.0 Jobs and IAM execution roles.
  - `src/glue/jobs/bronze_to_silver.py`: PySpark script implementing schema casting, null handling, deduplication, and Snappy Parquet writes to `curated/employees/`.
  - `src/glue/jobs/bronze_to_silver_delta.py`: PySpark script writing ACID Delta Lake table to `delta/employees/` with transaction logs.
  - S3 Script Deployments in `s3://gitops-scripts-bkt-dev-790347819012/glue/jobs/`.
  - Glue Job Resources:
    - `gitops-bronze-to-silver-employees-dev` (**Active / Verified**)
    - `gitops-bronze-to-silver-delta-employees-dev` (**Active / Provisioned**)
  - Dual-branch Airflow orchestration in `airflow/dags/first_workflow.py`.
- **Validation Gate**:
  - `aws glue get-job` confirms both jobs active in `ap-south-1`.
  - Silver S3 bucket stores Parquet and Delta tables in separate prefixes.

---

### Phase 7: Airflow Orchestration & DAG Setup
- **Status**: **Pending User Trigger**
- **Objective**: Orchestrate data pipelines with dependency management, automated retries, and alerting.
- **Key Deliverables**:
  - `airflow/dags/bronze_to_silver_pipeline.py`: Production DAG triggering Glue jobs upon data arrival or on schedule.
  - S3 / SQS dataset sensors for arrival detection.
  - Custom failure callbacks (Slack/Email notification hooks).
  - `airflow/tests/test_dag_integrity.py`: Validates syntax, cycle-free DAGs, and task dependencies.
- **Validation Gate**:
  - Airflow DAG passes integrity tests and loads cleanly without import errors.

---

### Phase 8: Athena Ad-Hoc Analytics & Data Catalog (Silver Parquet)
- **Status**: **Complete (Glue Catalog Database, Crawler, & Athena Workgroup Active)**
- **Objective**: Enable interactive SQL exploration and automated data quality assertions.
- **Key Deliverables**:
  - `aws_glue_catalog_database`: `gitops_dev_db` (**Active**).
  - `aws_glue_crawler`: `gitops-silver-parquet-crawler-dev` (**Active / Crawl Succeeded**).
    - Scope: Strictly limited to `s3://gitops-silver-bkt-dev-790347819012/curated/employees/`.
    - Cataloged Table: `gitops_dev_db.employees` (46 rows, 4 department partitions).
  - `aws_athena_workgroup`: `gitops-dev-workgroup` (**Active**).
  - `module.s3_athena_results`: `gitops-athena-results-bkt-dev-790347819012` (**Active**).
- **Validation Gate**:
  - Athena SQL queries execute successfully against `gitops_dev_db.employees` and return aggregated and granular records.

---

### Phase 9: Future Snowflake Scaffolding
- **Status**: **Pending User Trigger**
- **Objective**: Establish Snowflake cloud data warehouse integration ready for analytical serving.
- **Key Deliverables**:
  - `snowflake/rbac/`: Role-based access control scripts (`SYSADMIN`, `DATA_ENGINEER`, `ANALYST`).
  - `snowflake/stages/`: External stage definitions referencing AWS S3 Silver bucket via AWS Storage Integration.
  - `snowflake/tables/`: DDL scripts for dimensional tables, staging tables, and views.
- **Validation Gate**:
  - SQL scripts validated against Snowflake dialect syntax; IAM trust policy ready for external ID binding.

---

### Phase 10: CI/CD Pipelines & Automation
- **Status**: **Pending User Trigger**
- **Objective**: End-to-end GitOps automation enforcing code quality and zero-touch deployments.
- **Key Deliverables**:
  - `.github/workflows/ci.yml`: Pull request pipeline running Ruff, Pytest, Terraform fmt check, and `terraform plan`.
  - `.github/workflows/cd.yml`: Merge pipeline executing `terraform apply` across target environments using OIDC.
  - Script synchronization pipeline syncing Glue scripts to S3 script bucket.
- **Validation Gate**:
  - GitHub Actions workflows pass synthetic dry-run; PR status checks block non-compliant code.

---

## 4. Operational Guardrails & Rules of Engagement

1. **Strict Modularity**: Each Terraform component must be an isolated module under `terraform/modules/` with its own `main.tf`, `variables.tf`, and `outputs.tf`.
2. **Never Commit Secrets**: All credentials must be passed via IAM OIDC or environment variables; no `.pem`, `.key`, or credentials committed.
3. **Idempotency**: All ETL jobs and Terraform configurations must be safe to re-run multiple times without side effects or corrupted state.
4. **Step-by-Step Evolution**: Advance exactly one phase at a time to maintain clear auditability and verification.
