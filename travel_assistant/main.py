import logging

from config import Config
from dotenv import load_dotenv
from fuel_price import update_spain_fuel_price_table
from py4j.protocol import Py4JJavaError


def main():
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

    logging.info("Stating job")
    load_dotenv()
    update_spain_fuel_price_table(Config())
    logging.info("Job finished")


if __name__ == "__main__":
    main()
