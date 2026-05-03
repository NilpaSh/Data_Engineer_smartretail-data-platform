.PHONY: help up down generate ingest transform dbt-run dbt-test dashboard test clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up:  ## Start PostgreSQL + Dashboard
	docker-compose up -d postgres dashboard
	@echo "✓ Postgres: localhost:5432"
	@echo "✓ Dashboard: http://localhost:8501"

up-airflow:  ## Start all services including Airflow
	docker-compose --profile airflow up -d
	@echo "✓ Airflow: http://localhost:8080 (admin/admin)"

down:  ## Stop all services
	docker-compose down

generate:  ## Generate synthetic e-commerce data
	python data_generation/generate_ecommerce_data.py
	@echo "✓ Data written to data/raw/"

ingest:  ## Load raw data into Bronze layer
	python ingestion/bronze_loader.py
	@echo "✓ Bronze layer loaded"

transform:  ## Run Silver layer transformations
	python transformation/silver_transformer.py
	@echo "✓ Silver layer transformed"

dbt-run:  ## Run all dbt models (Gold layer)
	cd dbt_project && dbt run
	@echo "✓ Gold layer built"

dbt-test:  ## Run dbt data quality tests
	cd dbt_project && dbt test

dbt-docs:  ## Generate & serve dbt documentation
	cd dbt_project && dbt docs generate && dbt docs serve --port 8080

pipeline:  ## Run full pipeline: generate → ingest → transform → dbt
	$(MAKE) generate
	$(MAKE) ingest
	$(MAKE) transform
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	@echo "✓ Full pipeline complete!"

dashboard:  ## Run Streamlit dashboard locally
	streamlit run dashboard/app.py

test:  ## Run all unit tests
	pytest tests/ -v --cov=ingestion --cov=transformation --cov-report=term-missing

lint:  ## Lint Python code
	black --check .
	flake8 . --max-line-length=100 --exclude=.env,venv
	isort --check-only .

format:  ## Auto-format Python code
	black .
	isort .

clean:  ## Remove generated data files
	rm -rf data/raw/*.csv
	rm -rf data/raw/*.json
	@echo "✓ Cleaned generated data"

reset-db:  ## Drop and recreate all schemas (WARNING: destroys data)
	@echo "This will delete all data. Press Ctrl+C to cancel..."
	@sleep 3
	psql "postgresql://$${POSTGRES_USER}:$${POSTGRES_PASSWORD}@$${POSTGRES_HOST}:$${POSTGRES_PORT}/$${POSTGRES_DB}" \
	  -c "DROP SCHEMA IF EXISTS bronze CASCADE; DROP SCHEMA IF EXISTS silver CASCADE; DROP SCHEMA IF EXISTS gold CASCADE;"
	psql "postgresql://$${POSTGRES_USER}:$${POSTGRES_PASSWORD}@$${POSTGRES_HOST}:$${POSTGRES_PORT}/$${POSTGRES_DB}" \
	  -f sql/init.sql
	@echo "✓ Database reset"
