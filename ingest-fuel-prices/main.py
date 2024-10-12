import logging
import time

import functions_framework

from src.spain_fuel_price import create_spain_fuel_dataframe
from src.spain_fuel_price import extract_fuel_prices_raw_data
from src.spain_fuel_price import write_spain_fuel_prices_data_as_csv

LOGGING_FORMAT = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]"


@functions_framework.http
def ingest_fuel_prices(request):
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    logging.info("Starting Spain fuel data ingestion job")
    start_time = time.time()

    raw_data = extract_fuel_prices_raw_data()
    spain_fuel_price_df = create_spain_fuel_dataframe(raw_data)
    write_spain_fuel_prices_data_as_csv(spain_fuel_price_df)

    end_time = time.time()
    logging.info("Job successfully finished!")
    logging.info(f"Total processing time: {end_time - start_time} seconds")
    return "OK"
