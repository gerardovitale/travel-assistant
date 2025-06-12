# Makefile

ENV := $(PWD)/.env

include $(ENV)
export


# CLOUD RUN JOB
fuel-ingestor.test:
	./run-docker-test.sh fuel-ingestor


# CLOUD FUNCT
ingest-fuel-prices.test:
	cd ingest-fuel-prices && time pytest -vv --durations=0 .

ingest-fuel-prices.run:
	cd ingest-fuel-prices && functions-framework-python --target ingest_fuel_prices

ingest-fuel-prices.deploy:
	gcloud functions deploy "$(G_CLOUD_FUNCT_NAME)" \
    --gen2 \
    --region="$(G_CLOUD_REGION)" \
    --service-account="$(G_CLOUD_FUNCT_SERVICE_ACCOUNT)" \
    --runtime="$(G_CLOUD_FUNCT_RUNTIME)" \
    --source="$(G_CLOUD_FUNCT_SOURCE)" \
    --entry-point="$(G_CLOUD_FUNCT_ENTRYPOINT)" \
    --timeout="$(G_CLOUD_FUNCT_TIMEOUT)" \
    --max-instances=1 \
    --trigger-http
	#--no-allow-unauthenticated


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
