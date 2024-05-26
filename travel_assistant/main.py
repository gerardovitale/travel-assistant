import logging

from dotenv import load_dotenv

from travel_assistant.config import Config
from travel_assistant.fuel_price import update_spain_fuel_price_table

LOGGING_FORMAT = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]"
LOGGING_LEVEL = logging.INFO


def main():
    logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)

    logging.info("Stating job")
    load_dotenv()
    update_spain_fuel_price_table(Config())
    logging.info("Job finished")


if __name__ == "__main__":
    main()
