# Makefile

ENV := $(PWD)/.env

include $(ENV)
export


# CLOUD RUN JOB
fuel-ingestor.test:
	./run-docker-test.sh fuel-ingestor


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
