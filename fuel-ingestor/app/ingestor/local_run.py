import logging
import time
from datetime import datetime

from spain_fuel_api import fetch_fuel_stations

LOGGING_FORMAT = "%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)


def main():
    logging.info("Starting LOCAL Spain fuel data ingestion")
    start_time = time.time()

    spain_fuel_price_df = fetch_fuel_stations()

    timestamp = datetime.now().isoformat(timespec="seconds")
    output_path = f"output/spain_fuel_prices_{timestamp}.csv"
    spain_fuel_price_df.to_csv(output_path, index=False)
    logging.info(f"Wrote {len(spain_fuel_price_df)} rows to {output_path}")

    end_time = time.time()
    logging.info(f"Total processing time: {end_time - start_time:.1f}s")


if __name__ == "__main__":
    main()
