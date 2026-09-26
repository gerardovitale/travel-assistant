import logging
import os
import time
from datetime import date
from datetime import datetime
from datetime import timedelta

from commodity_api import fetch_commodity_prices
from ingestor.commodity_main import ROLLING_WINDOW_DAYS

LOGGING_FORMAT = "%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)


def main():
    logging.info("Starting LOCAL commodity price ingestion")
    start_time = time.time()

    api_key = os.environ["FRED_API_KEY"]
    today = date.today()
    commodity_df = fetch_commodity_prices(
        api_key=api_key,
        observation_start=(today - timedelta(days=ROLLING_WINDOW_DAYS)).isoformat(),
        observation_end=today.isoformat(),
    )

    timestamp = datetime.now().isoformat(timespec="seconds")
    output_path = f"output/commodity_prices_{timestamp}.csv"
    commodity_df.to_csv(output_path, index=False)
    logging.info(f"Wrote {len(commodity_df)} rows to {output_path}")

    end_time = time.time()
    logging.info(f"Total processing time: {end_time - start_time:.1f}s")


if __name__ == "__main__":
    main()
