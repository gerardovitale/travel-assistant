from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

SPAIN_FUEL_PRICES_SCHEMA = StructType(
    [
        StructField("date", StringType()),
        StructField("zip_code", IntegerType()),
        StructField("municipality_id", IntegerType()),
        StructField("province_id", IntegerType()),
        StructField("sale_type", StringType()),
        StructField("label", StringType()),
        StructField("address", StringType()),
        StructField("municipality", StringType()),
        StructField("province", StringType()),
        StructField("locality", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("biodiesel_price", DoubleType()),
        StructField("bioethanol_price", DoubleType()),
        StructField("compressed_natural_gas_price", DoubleType()),
        StructField("liquefied_natural_gas_price", DoubleType()),
        StructField("liquefied_petroleum_gases_price", DoubleType()),
        StructField("diesel_a_price", DoubleType()),
        StructField("diesel_b_price", DoubleType()),
        StructField("diesel_premium_price", DoubleType()),
        StructField("gasoline_95_e10_price", DoubleType()),
        StructField("gasoline_95_e5_price", DoubleType()),
        StructField("gasoline_95_e5_premium_price", DoubleType()),
        StructField("gasoline_98_e10_price", DoubleType()),
        StructField("gasoline_98_e5_price", DoubleType()),
        StructField("hydrogen_price", DoubleType()),
    ]
)
