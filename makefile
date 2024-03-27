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
	-v $(PWD)/cloud_storage_connector/keyfile-credential.json:/app/cloud_storage_connector/keyfile-credential.json \
	-e PROD=$(PROD) \
	-e PROJECT_ID=$(PROJECT_ID) \
	-e INSTANCE_NAME=$(INSTANCE_NAME) \
	-e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS) \
	-e DATA_SOURCE_URL=$(DATA_SOURCE_URL) \
	-e DATA_DESTINATION_BUCKET=$(DATA_DESTINATION_BUCKET) \
	$(DOCKER_LOCAL_IMAGE_NAME)

run-interactive:
    docker run --rm \
    -it --entrypoint="/bin/bash" \
	--name travel-assistant \
	-v $(PWD)/data/spain-fuel-price:/app/data/spain-fuel-price \
	-v $(PWD)/cloud_storage_connector/keyfile-credential.json:/app/cloud_storage_connector/keyfile-credential.json \
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
	cp $(GOOGLE_CLOUD_STORAGE_CONNECTOR) /Users/gerardovitaleerrico/Library/Caches/pypoetry/virtualenvs/travel-assistant-iA21hhyf-py3.9/lib/python3.9/site-packages/pyspark/jars
	poetry run python3 travel-assistant/main.py

publish: build
	./scripts/docker-publish-update-fuel-prices.sh

deploy-function:
	./scripts/deploy-cloud-function.sh

test-function:
	./scripts/test-cloud-function.sh
