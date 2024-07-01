# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

notebook:
	poetry run jupyter-notebook

travel-assistant.unittest:
	docker buildx build -f Dockerfile.test -t unit-test-image-$(DOCKER_LOCAL_IMAGE_NAME) .
	docker run --rm unit-test-image-$(DOCKER_LOCAL_IMAGE_NAME)

travel-assistant.build:
	docker buildx build -f Dockerfile -t $(DOCKER_LOCAL_IMAGE_NAME) .

travel-assistant.run: travel-assistant.build
	docker run --rm \
	--name travel-assistant \
	-v $(PWD)/data/spain-fuel-price:/app/data/spain-fuel-price \
	-v $(PWD)/$(G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH):/app/$(G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH) \
	-e PROD=$(PROD) \
	-e G_CLOUD_PROJECT_ID=$(G_CLOUD_PROJECT_ID) \
	-e G_CLOUD_COMPUTE_INSTANCE_NAME=$(G_CLOUD_COMPUTE_INSTANCE_NAME) \
	-e G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH=$(G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH) \
	-e DATA_SOURCE_URL=$(DATA_SOURCE_URL) \
	-e DATA_DESTINATION_BUCKET=$(DATA_DESTINATION_BUCKET) \
	$(DOCKER_LOCAL_IMAGE_NAME)

travel-assistant.run-interactively: travel-assistant.build
	docker run --rm \
    -it --entrypoint=/bin/bash \
	--name travel-assistant \
	-v $(PWD)/data/spain-fuel-price:/app/data/spain-fuel-price \
	-v $(PWD)/$(G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH):/app/$(G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH) \
	-e PROD=$(PROD) \
	-e G_CLOUD_PROJECT_ID=$(G_CLOUD_PROJECT_ID) \
	-e G_CLOUD_COMPUTE_INSTANCE_NAME=$(G_CLOUD_COMPUTE_INSTANCE_NAME) \
	-e G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH=$(G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH) \
	-e DATA_SOURCE_URL=$(DATA_SOURCE_URL) \
	-e DATA_DESTINATION_BUCKET=$(DATA_DESTINATION_BUCKET) \
	$(DOCKER_LOCAL_IMAGE_NAME)

travel-assistant.publish: travel-assistant.build
	./scripts/docker-publish-update-fuel-prices.sh

clean-docker:
	yes | docker container prune
	yes | docker image prune
	yes | docker volume prune

deploy-function:
	gcloud functions deploy "$(G_CLOUD_FUNCT_NAME)" \
    --gen2 \
    --service-account="$(G_CLOUD_FUNCT_SERVICE_ACCOUNT)" \
    --region="$(G_CLOUD_REGION)" \
    --runtime="$(G_CLOUD_FUNCT_RUNTIME)" \
    --source="$(G_CLOUD_FUNCT_SOURCE)" \
    --entry-point="$(G_CLOUD_FUNCT_ENTRYPOINT)" \
    --trigger-http \
    --set-env-vars PROD="$(PROD)" \
    --set-env-vars G_CLOUD_PROJECT_ID="$(G_CLOUD_PROJECT_ID)" \
    --set-env-vars G_CLOUD_REGION="$(G_CLOUD_REGION)" \
    --set-env-vars G_CLOUD_COMPUTE_INSTANCE_NAME="$(G_CLOUD_COMPUTE_INSTANCE_NAME)" \
    --set-env-vars G_CLOUD_COMPUTE_ZONE="$(G_CLOUD_COMPUTE_ZONE)" \
    --set-env-vars G_CLOUD_COMPUTE_MACHINE_TYPE="$(G_CLOUD_COMPUTE_MACHINE_TYPE)" \
    --set-env-vars G_CLOUD_COMPUTE_DOCKER_IMAGE_TO_DEPLOY="docker.io/$(DOCKER_HUB_USERNAME)/$(PROJECT_NAME)-$(DOCKER_LOCAL_IMAGE_NAME):latest" \
    --set-env-vars G_CLOUD_COMPUTE_SECRET_NAME="$(G_CLOUD_COMPUTE_SECRET_NAME)" \
    --set-env-vars DATA_SOURCE_URL="$(DATA_SOURCE_URL)" \
    --set-env-vars DATA_DESTINATION_BUCKET="$(DATA_DESTINATION_BUCKET)"

test-function:
	./scripts/test-cloud-function.sh


# TF BACKEND
backend.init:
	cd deploy/backend_support/ && terraform init

backend.plan:
	cd deploy/backend_support/ && terraform plan

backend.apply:
	cd deploy/backend_support/ && terraform apply -auto-approve

backend.destroy:
	cd deploy/backend_support/ && terraform destroy -auto-approve

backend.run: backend.init backend.plan backend.apply


# CLOUD FUNCTIONS
ingest-function.unittest:
	cd functions/ingest-fuel-prices/ && time pytest -vv --durations=0 .

travel-assistant.deploy:
	gcloud run deploy ingest-fuel-prices \
	--service-account="$(G_CLOUD_FUNCT_SERVICE_ACCOUNT)" \
	--region="$(G_CLOUD_REGION)" \
	--platform=managed \
	--image="docker.io/$(DOCKER_HUB_USERNAME)/$(PROJECT_NAME)-$(DOCKER_LOCAL_IMAGE_NAME):latest" \
	--set-env-vars PROD="$(PROD)" \
	--set-env-vars DATA_SOURCE_URL="$(DATA_SOURCE_URL)" \
	--set-env-vars DATA_DESTINATION_BUCKET="$(DATA_DESTINATION_BUCKET)" \
	--set-env-vars G_CLOUD_PROJECT_ID="$(G_CLOUD_PROJECT_ID)" \
	--set-env-vars G_CLOUD_COMPUTE_INSTANCE_NAME="$(G_CLOUD_COMPUTE_INSTANCE_NAME)" \
	--set-env-vars G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH="$(G_CLOUD_COMPUTE_APPLICATION_CREDENTIALS_PATH)" \
	--set-env-vars G_CLOUD_COMPUTE_SECRET_NAME="$(G_CLOUD_COMPUTE_SECRET_NAME)" \
	--tag environment=production \
	--tag project=travel-assistant \
	--tag managed-by=gve \
	--no-allow-unauthenticated
