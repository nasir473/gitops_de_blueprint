.DEFAULT_GOAL := help
SHELL := /bin/bash

.PHONY: help
help: ## Display this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: install
install: ## Install development dependencies
	pip install --upgrade pip
	pip install -e ".[dev]"
	pre-commit install

.PHONY: lint
lint: ## Run linting checks across Python and Terraform code
	@echo "==> Running Ruff check..."
	ruff check .
	@echo "==> Checking Terraform formatting..."
	terraform fmt -check -recursive terraform/

.PHONY: format
format: ## Auto-format Python and Terraform code
	@echo "==> Running Ruff format..."
	ruff format .
	ruff check --fix .
	@echo "==> Formatting Terraform files..."
	terraform fmt -recursive terraform/

.PHONY: test
test: ## Run unit tests with pytest
	@echo "==> Running Pytest..."
	pytest -v tests/

.PHONY: tf-validate
tf-validate: ## Validate Terraform configurations across environments
	@for env in dev qa prod; do \
		if [ -d "terraform/environments/$$env" ]; then \
			echo "==> Validating terraform/environments/$$env..."; \
			(cd terraform/environments/$$env && terraform init -backend=false && terraform validate) || exit 1; \
		fi \
	done

.PHONY: tf-plan
tf-plan: ## Run terraform plan for target environment (default: dev, usage: make tf-plan ENV=dev)
	@ENV=$${ENV:-dev}; \
	echo "==> Running Terraform plan for terraform/environments/$$ENV..."; \
	terraform -chdir=terraform/environments/$$ENV plan

.PHONY: tf-apply
tf-apply: ## Run terraform apply for target environment (default: dev, usage: make tf-apply ENV=dev)
	@ENV=$${ENV:-dev}; \
	echo "==> Running Terraform apply for terraform/environments/$$ENV..."; \
	terraform -chdir=terraform/environments/$$ENV apply

.PHONY: databricks-plan
databricks-plan: ## Run terraform plan for Databricks resources (usage: make databricks-plan ENV=dev)
	@ENV=$${ENV:-dev}; \
	echo "==> Running Terraform plan with Databricks enabled for terraform/environments/$$ENV..."; \
	terraform -chdir=terraform/environments/$$ENV plan -var="enable_databricks=true"

.PHONY: databricks-apply
databricks-apply: ## Run terraform apply for Databricks resources (usage: make databricks-apply ENV=dev)
	@ENV=$${ENV:-dev}; \
	echo "==> Running Terraform apply with Databricks enabled for terraform/environments/$$ENV..."; \
	terraform -chdir=terraform/environments/$$ENV apply -var="enable_databricks=true"


.PHONY: airflow-init
airflow-init: ## Initialize Airflow metadata database and admin user
	@echo "==> Initializing Airflow database..."
	docker compose run --rm airflow-init

.PHONY: airflow-up
airflow-up: ## Start Airflow containers in detached mode (UI at http://localhost:8080)
	@echo "==> Starting Airflow services..."
	docker compose up -d
	@echo "==> Airflow UI accessible at http://localhost:8080 (User: airflow / Pass: airflow)"

.PHONY: airflow-down
airflow-down: ## Stop Airflow containers
	@echo "==> Stopping Airflow services..."
	docker compose down

.PHONY: airflow-logs
airflow-logs: ## Follow Airflow container logs
	docker compose logs -f

.PHONY: airflow-clean
airflow-clean: ## Stop Airflow and remove persistent volumes
	docker compose down -v --remove-orphans

.PHONY: clean
clean: ## Clean up temporary files, caches, and build artifacts
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".terraform" -exec rm -rf {} +
	find . -type f -name "*.tfstate*" -delete

.PHONY: stream-to-bronze
stream-to-bronze: ## Run Kafka to S3 Bronze streaming consumer (usage: make stream-to-bronze [BATCH_SIZE=50] [FLUSH_INTERVAL=15])
	@PYTHON_BIN=$$(test -f "/Users/mohammednasirudeen/Downloads/Kafka-check/venv/bin/python" && echo "/Users/mohammednasirudeen/Downloads/Kafka-check/venv/bin/python" || echo "python3"); \
	BATCH_SIZE=$${BATCH_SIZE:-50}; \
	FLUSH_INTERVAL=$${FLUSH_INTERVAL:-15}; \
	$$PYTHON_BIN src/streaming/kafka_to_bronze_consumer.py --batch-size $$BATCH_SIZE --flush-interval $$FLUSH_INTERVAL

.PHONY: simulate-sources
simulate-sources: ## Simulate multi-source ingestion (API, File Share, Oracle) uploading data + manifests (usage: make simulate-sources [SOURCE=all|api|file_share|oracle])
	@PYTHON_BIN=$$(test -f "/Users/mohammednasirudeen/Downloads/Kafka-check/venv/bin/python" && echo "/Users/mohammednasirudeen/Downloads/Kafka-check/venv/bin/python" || echo "python3"); \
	SOURCE=$${SOURCE:-all}; \
	$$PYTHON_BIN src/ingestion/mock_multi_source_ingestion.py --source $$SOURCE



