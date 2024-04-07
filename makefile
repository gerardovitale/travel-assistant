# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

notebook:
	poetry run jupyter-notebook

unittest:
	time poetry run pytest -vv --durations=0 .

build-docker:
	docker build -f Dockerfile -t $(DOCKER_LOCAL_IMAGE_NAME) .

run-docker: build-docker
	docker run --rm \
	--name travel-assistant \
	-v $(PWD)/data/spain-fuel-price:/app/data/spain-fuel-price \
	-v $(PWD)/$(GOOGLE_APPLICATION_CREDENTIALS):/app/$(GOOGLE_APPLICATION_CREDENTIALS) \
	-e PROD=$(PROD) \
	-e PROJECT_ID=$(PROJECT_ID) \
	-e INSTANCE_NAME=$(INSTANCE_NAME) \
	-e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS) \
	-e DATA_SOURCE_URL=$(DATA_SOURCE_URL) \
	-e DATA_DESTINATION_BUCKET=$(DATA_DESTINATION_BUCKET) \
	$(DOCKER_LOCAL_IMAGE_NAME)

run-interactive: build-docker
	docker run --rm \
    -it --entrypoint=/bin/bash \
	--name travel-assistant \
	-v $(PWD)/data/spain-fuel-price:/app/data/spain-fuel-price \
	-v $(PWD)/$(GOOGLE_APPLICATION_CREDENTIALS):/app/$(GOOGLE_APPLICATION_CREDENTIALS) \
	-e PROD=$(PROD) \
	-e PROJECT_ID=$(PROJECT_ID) \
	-e INSTANCE_NAME=$(INSTANCE_NAME) \
	-e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS) \
	-e DATA_SOURCE_URL=$(DATA_SOURCE_URL) \
	-e DATA_DESTINATION_BUCKET=$(DATA_DESTINATION_BUCKET) \
	$(DOCKER_LOCAL_IMAGE_NAME)

clean-docker:
	yes | docker container prune
	yes | docker image prune
	yes | docker volume prune

run-locally:
	cp $(GOOGLE_CLOUD_STORAGE_CONNECTOR) \
	   $(shell poetry env info | grep "Virtualenv" -A 4 | awk '/Path:/ {print $$2}')/lib/python3.9/site-packages/pyspark/jars
	poetry run python3 travel_assistant/main.py

publish: build
	./scripts/docker-publish-update-fuel-prices.sh

deploy-function:
	gcloud functions deploy "$(FUNCT_NAME)" \
    --gen2 \
    --service-account="$(SERVICE_ACCOUNT)" \
    --region="$(REGION)" \
    --runtime="$(RUNTIME)" \
    --source="$(SOURCE)" \
    --entry-point="$(ENTRYPOINT)" \
    --trigger-http \
    --set-env-vars PROJECT_ID="$(PROJECT_ID)" \
    --set-env-vars ZONE="$(ZONE)" \
    --set-env-vars REGION="$(REGION)" \
    --set-env-vars INSTANCE_NAME="$(INSTANCE_NAME)" \
    --set-env-vars MACHINE_TYPE="$(MACHINE_TYPE)" \
    --set-env-vars DOCKER_IMAGE_TO_DEPLOY="$(DOCKER_IMAGE_TO_DEPLOY)" \
    --set-env-vars PROD="$(PROD)" \
    --set-env-vars GCS_SECRET_NAME="$(GCS_SECRET_NAME)" \
    --set-env-vars DATA_SOURCE_URL="$(DATA_SOURCE_URL)" \
    --set-env-vars DATA_DESTINATION_BUCKET="$(DATA_DESTINATION_BUCKET)"

test-function:
	./scripts/test-cloud-function.sh
