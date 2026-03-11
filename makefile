# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

test: fuel-ingestor.test fuel-dashboard.test

notebook:
	docker run -it --rm -p 8888:8888 \
		-v "${PWD}":/home/jovyan/work \
		quay.io/jupyter/scipy-notebook:latest


# CLOUD RUN JOB
fuel-ingestor.test:
	./scripts/run-docker-test.sh fuel-ingestor

fuel-ingestor.local:
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
		-v $(HOME)/.config/gcloud:/root/.config/gcloud:ro \
		-e GOOGLE_APPLICATION_CREDENTIALS="" \
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
data.download:
	fuel-dashboard/venv/bin/python3 scripts/download_fuel_data.py
