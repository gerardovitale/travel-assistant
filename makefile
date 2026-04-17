# Makefile

ENV := $(PWD)/.env

# Trivy version must match aquasecurity/trivy-action in .github/workflows/deploy.yaml
TRIVY_VERSION := 0.69.3
DASHBOARD_CREDENTIALS_PATH ?= $(PWD)/fuel-dashboard/gcs-fuel-dashboard-credentials.json

include $(ENV)
export

setup:
	cd fuel-ingestor && uv sync --dev
	cd fuel-dashboard && uv sync --dev

test: fuel-ingestor.test fuel-dashboard.test
test-local: setup fuel-ingestor.test-local fuel-dashboard.test-local
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

notebook:
	docker run -it --rm -p 8888:8888 \
		-v "${PWD}":/home/jovyan/work \
		quay.io/jupyter/scipy-notebook:latest


# LOCAL DEV (uv)
fuel-ingestor.sync:
	cd fuel-ingestor && uv sync --no-install-project

fuel-dashboard.sync:
	cd fuel-dashboard && uv sync --no-install-project

fuel-ingestor.test-local:
	cd fuel-ingestor && uv run pytest --durations=5 -vv tests/

fuel-dashboard.test-local:
	cd fuel-dashboard && uv run pytest --durations=5 -vv tests/


# IMAGE SCANNING
fuel-ingestor.scan:
	docker buildx build -t travass-fuel-ingestor:local fuel-ingestor/ && \
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $(PWD)/.trivyignore:/root/.trivyignore \
		aquasec/trivy:$(TRIVY_VERSION) image \
		--exit-code 1 \
		--severity CRITICAL,HIGH \
		--ignorefile /root/.trivyignore \
		travass-fuel-ingestor:local

fuel-dashboard.scan:
	docker buildx build -t travass-fuel-dashboard:local fuel-dashboard/ && \
	docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $(PWD)/.trivyignore:/root/.trivyignore \
		aquasec/trivy:$(TRIVY_VERSION) image \
		--exit-code 1 \
		--severity CRITICAL,HIGH \
		--ignorefile /root/.trivyignore \
		travass-fuel-dashboard:local


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
