notebook:
	poetry run jupyter-notebook

test:
	time poetry run pytest -vv --durations=0 .

build:
	docker build -t travass .

run: build
	docker run --rm \
	--name travel-assistant \
	-v ./data/spain-fuel-price:/app/data/spain-fuel-price \
	travass
