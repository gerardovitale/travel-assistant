import os

# Supported environment variables (all optional — defaults shown below):
#
#   AGGREGATOR_REPORT_BRANDS                  ballenoil,repsol,costco
#   AGGREGATOR_REPORT_FUEL_COLS               gasoline_95_e5_price,diesel_a_price
#   AGGREGATOR_REPORT_GEO_COLS                zip_code,locality,municipality
#   AGGREGATOR_REPORT_DIRECTIONS              cheapest,priciest
#   AGGREGATOR_REPORT_MIN_APPEARANCES_WIN_RATE        30
#   AGGREGATOR_REPORT_MIN_APPEARANCES_COMPARISON      10


def _csv_list(env_var: str, default: str) -> list[str]:
    return [s.strip() for s in os.environ.get(env_var, default).split(",") if s.strip()]


REPORT_BRANDS = _csv_list("AGGREGATOR_REPORT_BRANDS", "ballenoil,repsol,costco")
REPORT_FUEL_COLS = _csv_list("AGGREGATOR_REPORT_FUEL_COLS", "gasoline_95_e5_price,diesel_a_price")
REPORT_GEO_COLS = _csv_list("AGGREGATOR_REPORT_GEO_COLS", "zip_code,locality,municipality")
REPORT_DIRECTIONS = _csv_list("AGGREGATOR_REPORT_DIRECTIONS", "cheapest,priciest")

REPORT_MIN_APPEARANCES_WIN_RATE = int(os.environ.get("AGGREGATOR_REPORT_MIN_APPEARANCES_WIN_RATE", "30"))
REPORT_MIN_APPEARANCES_COMPARISON = int(os.environ.get("AGGREGATOR_REPORT_MIN_APPEARANCES_COMPARISON", "10"))
