import logging
import os
import time
from datetime import date
from datetime import timedelta

from commodity_api import fetch_commodity_prices
from ingestor.commodity_price import validate_commodity_dataframe
from ingestor.commodity_price import write_commodity_prices_data_as_parquet

LOGGING_FORMAT = "%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

ROLLING_WINDOW_DAYS = 90


def main():
    logging.info("Starting commodity price ingestion job")
    start_time = time.time()

    api_key = os.environ["FRED_API_KEY"]
    today = date.today()
    commodity_df = fetch_commodity_prices(
        api_key=api_key,
        observation_start=(today - timedelta(days=ROLLING_WINDOW_DAYS)).isoformat(),
        observation_end=today.isoformat(),
    )
    validate_commodity_dataframe(commodity_df)
    write_commodity_prices_data_as_parquet(commodity_df)

    end_time = time.time()
    logging.info("Job successfully finished!")
    logging.info(f"Total processing time: {end_time - start_time} seconds, {(end_time - start_time) / 60} minutes")


if __name__ == "__main__":
    main()
