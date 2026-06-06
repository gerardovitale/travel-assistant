# Makefile

ENV := $(PWD)/.env

# Trivy version must match aquasecurity/trivy-action in .github/workflows/deploy.yaml
TRIVY_VERSION := 0.69.3
DASHBOARD_CREDENTIALS_PATH ?= $(PWD)/fuel-dashboard/gcs-fuel-dashboard-credentials.json

include $(ENV)
export

setup:
	uv sync --all-packages --dev

test: spain-fuel-fetcher.test fuel-ingestor.test fuel-dashboard.test fuel-dashboard.ui-test
test-local: setup spain-fuel-fetcher.test-local fuel-ingestor.test-local fuel-dashboard.test-local fuel-dashboard.ui-test-local
scan: fuel-ingestor.scan fuel-dashboard.scan
done: setup test scan


# DATA
data.download-fuel-daily-prices:
	cd fuel-dashboard && uv run python ../scripts/download_fuel_data.py

data.download-aggregates:
	mkdir -p ./data/aggregates
	gsutil -m -o "GSUtil:parallel_process_count=1" cp \
		"gs://travel-assistant-spain-fuel-prices/aggregates/**/*.parquet" \
		./data/aggregates

data.download-reports:
	mkdir -p ./data/reports
	gsutil -m -o "GSUtil:parallel_process_count=1" cp \
		"gs://travel-assistant-spain-fuel-prices/aggregates/reports/**/*.parquet" \
		./data/reports

notebook:
	docker run -it --rm -p 8888:8888 \
		-v "${PWD}":/home/jovyan/work \
		quay.io/jupyter/scipy-notebook:latest


# LOCAL DEV (uv) — single workspace at repo root, one shared .venv
fuel-ingestor.sync:
	uv sync --package fuel-ingestor --no-install-project

fuel-dashboard.sync:
	uv sync --package fuel-dashboard --no-install-project

spain-fuel-fetcher.test-local:
	uv run --package spain-fuel-fetcher pytest --durations=5 -vv spain-fuel-fetcher/tests/

fuel-ingestor.test-local:
	cd fuel-ingestor && uv run pytest --durations=5 -vv tests/

fuel-dashboard.test-local:
	cd fuel-dashboard && uv run pytest --durations=5 -vv tests/

fuel-dashboard.ui-test-local:
	cd fuel-dashboard && npx playwright install chromium && npm run ui:test


# IMAGE SCANNING
define scan-service
	docker buildx build -f $(1)/Dockerfile -t travass-$(1):local . && \
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $(PWD)/.trivyignore:/root/.trivyignore \
		aquasec/trivy:$(TRIVY_VERSION) image \
		--exit-code 1 \
		--severity CRITICAL,HIGH \
		--ignorefile /root/.trivyignore \
		travass-$(1):local
endef

fuel-ingestor.scan:
	$(call scan-service,fuel-ingestor)

fuel-dashboard.scan:
	$(call scan-service,fuel-dashboard)


# SPAIN FUEL FETCHER (shared package)
spain-fuel-fetcher.test:
	./scripts/run-docker-test.sh spain-fuel-fetcher


# FUEL INGESTOR
fuel-ingestor.test:
	./scripts/run-docker-test.sh fuel-ingestor

fuel-ingestor.run:
	docker buildx build -f fuel-ingestor/Dockerfile -t fuel-ingestor . && \
	mkdir -p output && \
	docker run --rm \
		-v $(PWD)/fuel-ingestor/output:/output \
		--entrypoint python3 \
		fuel-ingestor ingestor/local_run.py

fuel-aggregator.run:
	uv sync --frozen --no-dev --no-install-project --package fuel-ingestor && \
	cd fuel-ingestor && \
	PYTHONPATH=app uv run --frozen --no-sync python app/aggregator/main.py


# FUEL DASHBOARD
fuel-dashboard.test:
	./scripts/run-docker-test.sh fuel-dashboard

fuel-dashboard.run:
	docker buildx build -f fuel-dashboard/Dockerfile -t fuel-dashboard . && \
	docker run --rm -p 8080:8080 \
		-v $(DASHBOARD_CREDENTIALS_PATH):/app/credentials.json:ro \
		-e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json \
		fuel-dashboard

fuel-dashboard.ui-test:
	docker buildx build -f fuel-dashboard/Dockerfile.e2e -t fuel-dashboard-e2e . && \
	docker run --rm fuel-dashboard-e2e


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
