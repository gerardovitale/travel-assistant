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
	-v ./data/spain-fuel-price:/app/data/spain-fuel-price \
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
