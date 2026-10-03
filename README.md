# AWS Modern Data Platform (GitOps Monorepo)

A production-ready, GitOps-driven blueprint for a Modern Data Platform on AWS, featuring Medallion architecture (Bronze/Silver/Quarantine), event-driven ingestion, PySpark batch transformations on AWS Glue, Athena ad-hoc querying, Apache Airflow orchestration, and Snowflake readiness.

---

## Repository Map

```text
.
├── .github/
│   └── workflows/             # CI/CD pipelines (Lint, Test, Terraform Plan/Apply) [Phase 10]
├── airflow/
│   ├── dags/                  # Airflow DAGs for pipeline orchestration [Phase 7]
│   ├── plugins/               # Custom Airflow plugins and operators [Phase 7]
│   ├── tests/                 # Airflow DAG integrity and unit tests [Phase 7]
│   └── requirements.txt       # Airflow package and provider dependencies
├── src/
│   ├── lambda/                # Serverless event handlers & ingestion triggers [Phase 5]
│   └── glue/
│       ├── common/            # Shared PySpark schemas, utilities, and validation logic [Phase 6]
│       └── jobs/              # AWS Glue PySpark transformation scripts [Phase 6]
├── terraform/
│   ├── modules/               # Reusable Terraform modules
│   │   ├── s3/                # Bronze, Silver, Quarantine, Athena buckets [Phase 3]
│   │   ├── iam/               # Least-privilege execution roles & OIDC trust [Phase 4]
│   │   ├── lambda/            # SQS-triggered validation Lambda [Phase 5]
│   │   ├── glue/              # Glue Data Catalog & PySpark job definitions [Phase 6]
│   │   ├── sqs/               # S3 ingestion notification queue & DLQ [Phase 5]
│   │   └── athena/            # Athena workgroup and query results setup [Phase 8]
│   └── environments/
│       ├── dev/               # Development environment Terraform configuration
│       ├── qa/                # QA / Staging environment Terraform configuration
│       └── prod/              # Production environment Terraform configuration
├── snowflake/
│   ├── rbac/                  # Role-based access control DDL [Phase 9]
│   ├── stages/                # External S3 stages and storage integration [Phase 9]
│   └── tables/                # Target analytical tables and views [Phase 9]
├── sql/
│   └── athena/                # Athena DDL, partition repair, and data quality queries [Phase 8]
├── tests/
│   ├── unit/                  # Python unit tests for Lambda and Glue scripts
│   └── conftest.py            # Global Pytest fixtures and mocked AWS environment
├── scripts/                   # Local automation and environment bootstrapping scripts
├── docs/
│   ├── architecture.md        # Architecture overview and Mermaid data-flow diagram
│   └── runbook.md             # Operational runbook and phase invocation guide
├── .editorconfig              # Code formatting rules
├── .env.example               # Template environment variables
├── .gitignore                 # Version control exclusions
├── .pre-commit-config.yaml    # Pre-commit quality hooks (Ruff, Terraform fmt)
├── Makefile                   # Developer productivity commands (lint, test, tf-validate)
├── PLATFORM_ROADMAP.md        # Master architectural roadmap and phase progress tracker
├── pyproject.toml             # Ruff and Pytest tool configurations
└── README.md                  # Project overview and documentation
```

---

## Quick Start

### 1. Prerequisites
- Python 3.11+
- Terraform 1.5+
- AWS CLI v2 configured with appropriate credentials
- Make

### 2. Environment Setup
```bash
# Clone repository and enter directory
cd gitops_de_blueprint

# Copy environment variables template
cp .env.example .env

# Install dependencies and pre-commit hooks
make install
```

### 3. Developer Tooling Commands
```bash
make help          # View available targets
make lint          # Run Ruff and Terraform fmt checks
make format        # Automatically fix formatting issues
make test          # Execute pytest suite
make tf-validate   # Validate Terraform configurations across environments
```

---

## Incremental Build Roadmap

The platform is designed to be built incrementally across 10 sequential phases. See [PLATFORM_ROADMAP.md](file:///Users/mohammednasirudeen/Documents/Project/Data%20Engineering/gitops_de_blueprint/PLATFORM_ROADMAP.md) for full phase details and [docs/runbook.md](file:///Users/mohammednasirudeen/Documents/Project/Data%20Engineering/gitops_de_blueprint/docs/runbook.md) for trigger instructions.

| Phase | Description | Status |
| :---: | :--- | :---: |
| **Phase 1** | Project Scaffolding & Tooling | **Complete** |
| **Phase 2** | Terraform State Backend & Provider Setup | Pending User Trigger |
| **Phase 3** | S3 Storage Architecture (Bronze / Silver / Quarantine / Athena) | Pending User Trigger |
| **Phase 4** | IAM Roles & GitHub Actions OIDC Trust | Pending User Trigger |
| **Phase 5** | Event Ingestion (EventBridge / SQS / Lambda Trigger) | Pending User Trigger |
| **Phase 6** | Glue Catalog & PySpark Bronze-to-Silver Job | Pending User Trigger |
| **Phase 7** | Airflow Orchestration & DAG Setup | Pending User Trigger |
| **Phase 8** | Athena Ad-Hoc Analytics & Data Quality Rules | Pending User Trigger |
| **Phase 9** | Future Snowflake Scaffolding | Pending User Trigger |
| **Phase 10** | CI/CD Pipelines & Automation | Pending User Trigger |

---

## Architecture

For an in-depth breakdown of the data architecture, storage tiers, and a Mermaid component diagram, refer to [docs/architecture.md](file:///Users/mohammednasirudeen/Documents/Project/Data%20Engineering/gitops_de_blueprint/docs/architecture.md).
