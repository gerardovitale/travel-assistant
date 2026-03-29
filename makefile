# Makefile

ENV := $(PWD)/.env
DASHBOARD_CREDENTIALS_PATH ?= $(PWD)/fuel-dashboard/gcs-fuel-dashboard-credentials.json

include $(ENV)
export

setup:
	cd fuel-ingestor && uv sync --dev
	cd fuel-dashboard && uv sync --dev

test: fuel-ingestor.test fuel-dashboard.test
test-local: fuel-ingestor.test-local fuel-dashboard.test-local

notebook:
	docker run -it --rm -p 8888:8888 \
		-v "${PWD}":/home/jovyan/work \
		quay.io/jupyter/scipy-notebook:latest


# LOCAL DEV (uv)
fuel-ingestor.sync:
	cd fuel-ingestor && uv sync --no-install-project

fuel-dashboard.sync:
	cd fuel-dashboard && uv sync --no-install-project

sync: fuel-ingestor.sync fuel-dashboard.sync

fuel-ingestor.test-local:
	cd fuel-ingestor && uv run pytest --durations=5 -vv tests/

fuel-dashboard.test-local:
	cd fuel-dashboard && uv run pytest --durations=5 -vv tests/

test-local: fuel-ingestor.test-local fuel-dashboard.test-local


# CLOUD RUN JOB
fuel-ingestor.test:
	./scripts/run-docker-test.sh fuel-ingestor

fuel-ingestor.run:
	cd fuel-ingestor && docker buildx build -t fuel-ingestor . && \
	mkdir -p output && \
	docker run --rm \
		-v $(PWD)/fuel-ingestor/output:/output \
		--entrypoint python3 \
		fuel-ingestor local_run.py


# FUEL DASHBOARD
fuel-dashboard.test:
	./scripts/run-docker-test.sh fuel-dashboard

fuel-dashboard.run:
	cd fuel-dashboard && docker buildx build -t fuel-dashboard . && \
	docker run --rm -p 8080:8080 \
		-v $(DASHBOARD_CREDENTIALS_PATH):/app/credentials.json:ro \
		-e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
		fuel-dashboard


# TF BACKEND
backend.init:
	cd infra/backend_support/ && terraform init

backend.plan:
	cd infra/backend_support/ && terraform plan

backend.apply:
	cd infra/backend_support/ && terraform apply -auto-approve

backend.destroy:
	cd infra/backend_support/ && terraform destroy -auto-approve

backend.run: backend.init backend.plan backend.apply


# DATA
data.download-fuel-daily-prices:
	cd fuel-dashboard && uv run python ../scripts/download_fuel_data.py

data.download-aggregates:
	mkdir -p ./data/aggregates
	gsutil -m -o "GSUtil:parallel_process_count=1" cp \
	"gs://travel-assistant-spain-fuel-prices/aggregates/daily_ingestion_stats.parquet" \
	"gs://travel-assistant-spain-fuel-prices/aggregates/day_of_week_stats.parquet" \
	"gs://travel-assistant-spain-fuel-prices/aggregates/province_daily_stats.parquet" \
	./data/aggregates
