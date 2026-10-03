# Operational Runbook: Incremental Platform Build

## 1. Incremental Execution Philosophy

This project is constructed in strictly isolated, testable phases. To maintain total architectural integrity:
- **Never implement multiple phases simultaneously**.
- **Validate each phase** before requesting the next.
- Follow the prompt trigger templates below when prompting the assistant.

---

## 2. Phase Trigger Guide

| Phase | Phase Name | Primary Prompt Trigger Template | Acceptance & Validation Criteria |
| :---: | :--- | :--- | :--- |
| **1** | **Scaffolding & Tooling** | `Act as Principal Cloud Architect. Initialize Phase 1: Project Scaffolding & Tooling.` | Repo structure exists, Makefile/linting passes, `PLATFORM_ROADMAP.md` created. |
| **2** | **Terraform State Backend** | `Proceed with Phase 2: Configure Terraform S3 backend and DynamoDB state locking.` | Remote backend configuration in `environments/dev/backend.tf`, state lock testing. |
| **3** | **S3 Storage Architecture** | `Proceed with Phase 3: Implement the S3 storage module (Bronze, Silver, Quarantine, Athena).` | `terraform/modules/s3` written with encryption, versioning, lifecycles, and environment wiring. |
| **4** | **IAM Roles & OIDC Trust** | `Proceed with Phase 4: Implement IAM module and GitHub Actions OIDC federation.` | `terraform/modules/iam` creates least-privilege roles for Glue, Lambda, and GitHub OIDC. |
| **5** | **Event Ingestion (SQS/Lambda)**| `Proceed with Phase 5: Implement SQS queue, S3 event notifications, and Lambda trigger.` | `terraform/modules/sqs`, `terraform/modules/lambda`, and `src/lambda/handler.py` unit tested. |
| **6** | **Glue Catalog & PySpark ETL** | `Proceed with Phase 6: Implement Glue Data Catalog and Bronze-to-Silver PySpark job.` | `terraform/modules/glue`, `src/glue/jobs/bronze_to_silver.py`, and PySpark unit tests pass. |
| **7** | **Airflow Orchestration** | `Proceed with Phase 7: Implement Airflow DAGs and Glue job operators.` | `airflow/dags/` DAG definition with sensors, Glue trigger, and DAG validation tests passing. |
| **8** | **Athena & Data Quality** | `Proceed with Phase 8: Configure Athena workgroup and SQL data quality validation queries.` | `terraform/modules/athena`, `sql/athena/` DDL/DQL queries, and quality assertions. |
| **9** | **Snowflake Scaffolding** | `Proceed with Phase 9: Implement Snowflake storage integration, RBAC, and stages.` | `snowflake/` SQL scripts for storage integration with S3 Silver, external stage, and RBAC roles. |
| **10** | **CI/CD Pipelines** | `Proceed with Phase 10: Create GitHub Actions workflows for PR checks and Terraform deploy.` | `.github/workflows/ci.yml` and `cd.yml` with lint, test, terraform fmt/plan/apply pipelines. |

---

## 3. Standard Local Development Workflow

Before submitting a pull request or prompting for the next phase, run the standard validation sequence:

```bash
# 1. Check coding standards and syntax
make lint

# 2. Format Python and Terraform code
make format

# 3. Run Pytest suite
make test

# 4. Validate Terraform across all environments
make tf-validate
```

---

## 4. Environment Promotion Strategy

1. **Development (`dev`)**: Feature branches test against dev resources. Fast iteration.
2. **Quality Assurance (`qa`)**: Pull requests targeting `main` execute plan/apply against QA.
3. **Production (`prod`)**: Tagged releases or approvals trigger promotion to production.

---

## 5. Local Airflow Deployment (Docker Compose)

To run Apache Airflow locally and execute DAGs:

```bash
# 1. Initialize Airflow metadata database and admin user (first time only)
make airflow-init

# 2. Launch Airflow services in background
make airflow-up

# 3. Access Airflow Web UI
# URL: http://localhost:8080
# Username: airflow
# Password: airflow

# 4. View real-time container logs
make airflow-logs

# 5. Stop Airflow services
make airflow-down

# 6. Stop and wipe all persistent database volumes
make airflow-clean
```

