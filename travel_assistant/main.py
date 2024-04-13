import logging

from config import Config
from dotenv import load_dotenv
from fuel_price import update_spain_fuel_price_table

LOGGING_FORMAT = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s %(filename)s:%(lineno)d"
LOGGING_LEVEL = logging.DEBUG


def main():
    logging.basicConfig(format=LOGGING_FORMAT, level=LOGGING_LEVEL)

    logging.info("Stating job")
    load_dotenv()
    update_spain_fuel_price_table(Config())
    logging.info("Job finished")


if __name__ == "__main__":
    main()
