# Makefile

ENV := $(PWD)/.env

include $(ENV)
export

notebook:
	poetry run jupyter-notebook

test:
	time poetry run pytest -vv --durations=0 .

build:
	docker build -t update_fuel_prices .

run: build
	docker run --rm \
	--name travel-assistant \
	-v ./data/spain-fuel-price:/app/data/spain-fuel-price \
	travass

publish: build
	./scripts/docker-publish-update-fuel-prices.sh

deploy-function:
	./scripts/deploy-cloud-function.sh

test-function:
	./scripts/test-cloud-function.sh
