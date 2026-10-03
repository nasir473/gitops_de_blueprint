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

