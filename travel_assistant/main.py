import logging

from dotenv import load_dotenv

from travel_assistant.config import Config
from travel_assistant.fuel_price import update_spain_fuel_price_table


def main():
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

    logging.info("Stating job")
    load_dotenv()
    update_spain_fuel_price_table(Config())
    logging.info("Job finished")


if __name__ == "__main__":
    main()
