notebook:
	poetry run jupyter-notebook

test:
	time poetry run pytest -vv --durations=0 .

update_spain_fuel_prices:
	poetry run python3 -u travel_assistant/main.py
