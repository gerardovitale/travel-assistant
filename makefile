# Makefile

ENV := $(PWD)/.env

include $(ENV)
export


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
    --trigger-http \
    --timeout=120s
	#--no-allow-unauthenticated


# TF BACKEND
backend.init:
	cd tf_infra/backend_support/ && terraform init

backend.plan:
	cd tf_infra/backend_support/ && terraform plan

backend.apply:
	cd tf_infra/backend_support/ && terraform apply -auto-approve

backend.destroy:
	cd tf_infra/backend_support/ && terraform destroy -auto-approve

backend.run: backend.init backend.plan backend.apply
