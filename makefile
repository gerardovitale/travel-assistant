# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

test: fuel-ingestor.test fuel-dashboard.test


# CLOUD RUN JOB
fuel-ingestor.test:
	./run-docker-test.sh fuel-ingestor


# FUEL DASHBOARD
fuel-dashboard.test:
	./run-docker-test.sh fuel-dashboard

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
