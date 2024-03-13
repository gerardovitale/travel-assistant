from datetime import datetime
from datetime import timezone
from unittest import TestCase
from unittest.mock import patch

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession

from travel_assistant.entity import SpainFuelPrice
from travel_assistant.fuel_price import create_spain_fuel_dataframe
from travel_assistant.fuel_price import get_spain_fuel_price_raw_data
from travel_assistant.fuel_price import map_spain_fuel_data
from travel_assistant.fuel_price import map_spain_fuel_price
from travel_assistant.schema import SPAIN_FUEL_PRICES_SCHEMA


class TestFuelPrice(TestCase):
    @staticmethod
    def setup_test_spark_session():
        builder = (
            SparkSession.builder.master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        return (
            configure_spark_with_delta_pip(builder)
            .appName("unit-tests")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.maxResultSize", "500m")
            .getOrCreate()
        )

    @staticmethod
    def assert_spark_dataframes_equal(expected_df: DataFrame, actual_df: DataFrame):
        assert actual_df is not None, "The actual_df is None"
        assert expected_df.schema == actual_df.schema, "Schema mismatch"
        expected_rows = expected_df.orderBy(expected_df.columns).collect()
        actual_rows = actual_df.orderBy(actual_df.columns).collect()
        assert expected_rows == actual_rows, "Data mismatch"

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.setup_test_spark_session()
        cls.maxDiff = None

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.spark = None

    def setUp(self):
        requests_patch = patch("travel_assistant.fuel_price.requests")
        self.addCleanup(requests_patch.stop)
        self.mock_requests = requests_patch.start()

        logger_patch = patch("travel_assistant.fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    def test_get_spain_gas_price_raw_data(self):
        _ = get_spain_fuel_price_raw_data()
        self.mock_requests.get.assert_called_once()
        self.mock_requests.get().json.assert_called_once()

    def test_map_spain_fuel_price(self):
        test_date = datetime(2024, 3, 12, 18, 48, 4, tzinfo=timezone.utc)
        test_data = {
            "C.P.": "23400",
            "IDMunicipio": "3583",
            "IDProvincia": "23",
            "Tipo Venta": "P",
            "Rótulo": "REPSOL",
            "Dirección": "CALLE CARRETERA DE VILCHES, S/N",
            "Municipio": "Úbeda",
            "Provincia": "JAÉN",
            "Localidad": "UBEDA",
            "Latitud": "38,026167",
            "Longitud (WGS84)": "-3,369528",
            "Precio Biodiesel": "",
            "Precio Bioetanol": "",
            "Precio Gas Natural Comprimido": "",
            "Precio Gas Natural Licuado": "",
            "Precio Gases licuados del petróleo": "",
            "Precio Gasoleo A": "1,617",
            "Precio Gasoleo B": "1,247",
            "Precio Gasoleo Premium": "1,667",
            "Precio Gasolina 95 E10": "",
            "Precio Gasolina 95 E5": "1,647",
            "Precio Gasolina 95 E5 Premium": "",
            "Precio Gasolina 98 E10": "",
            "Precio Gasolina 98 E5": "1,757",
            "Precio Hidrogeno": "",
        }
        expected_datetime_obj = datetime(2024, 3, 12, 18, 48, 4, tzinfo=timezone.utc)
        expected_fuel_price = SpainFuelPrice(
            dt=expected_datetime_obj,
            date=expected_datetime_obj.date(),
            hour=expected_datetime_obj.hour,
            zip_code=23400,
            municipality_id="3583",
            province_id="23",
            sale_type="p",
            label="repsol",
            address="calle carretera de vilches, s/n",
            municipality="úbeda",
            province="jaén",
            locality="ubeda",
            latitude=38.026167,
            longitude=-3.369528,
            biodiesel_price=None,
            bioethanol_price=None,
            compressed_natural_gas_price=None,
            liquefied_natural_gas_price=None,
            liquefied_petroleum_gases_price=None,
            diesel_a_price=1.617,
            diesel_b_price=1.247,
            diesel_premium_price=1.667,
            gasoline_95_e10_price=None,
            gasoline_95_e5_price=1.647,
            gasoline_95_e5_premium_price=None,
            gasoline_98_e10_price=None,
            gasoline_98_e5_price=1.757,
            hydrogen_price=None,
        )
        actual_fuel_price = map_spain_fuel_price(test_data, test_date)
        assert actual_fuel_price.model_dump() == expected_fuel_price.model_dump()

    def test_map_spain_fuel_data(self):
        test_data = {
            "Fecha": "12/03/2024 19:48:04",
            "ListaEESSPrecio": [
                {
                    "C.P.": "48170",
                    "Direcci\u00f3n": "POLIGONO UGALDEGUREN, 25",
                    "Horario": "L-D: 24H",
                    "Latitud": "43,286000",
                    "Localidad": "ZAMUDIO",
                    "Longitud (WGS84)": "-2,872639",
                    "Margen": "I",
                    "Municipio": "Zamudio",
                    "Precio Biodiesel": "",
                    "Precio Bioetanol": "",
                    "Precio Gas Natural Comprimido": "",
                    "Precio Gas Natural Licuado": "",
                    "Precio Gases licuados del petr\u00f3leo": "",
                    "Precio Gasoleo A": "1,389",
                    "Precio Gasoleo B": "",
                    "Precio Gasoleo Premium": "",
                    "Precio Gasolina 95 E10": "",
                    "Precio Gasolina 95 E5": "1,539",
                    "Precio Gasolina 95 E5 Premium": "",
                    "Precio Gasolina 98 E10": "",
                    "Precio Gasolina 98 E5": "",
                    "Precio Hidrogeno": "",
                    "Provincia": "BIZKAIA",
                    "Remisi\u00f3n": "dm",
                    "R\u00f3tulo": "PETROPRIX",
                    "Tipo Venta": "P",
                    "% BioEtanol": "0,0",
                    "% \u00c9ster met\u00edlico": "0,0",
                    "IDEESS": "14197",
                    "IDMunicipio": "7563",
                    "IDProvincia": "48",
                    "IDCCAA": "16",
                },
                {
                    "C.P.": "15570",
                    "Direcci\u00f3n": "AVENIDA DO MAR, 94",
                    "Horario": "L-D: 24H",
                    "Latitud": "43,488583",
                    "Localidad": "GANDARA",
                    "Longitud (WGS84)": "-8,200111",
                    "Margen": "D",
                    "Municipio": "Nar\u00f3n",
                    "Precio Biodiesel": "",
                    "Precio Bioetanol": "",
                    "Precio Gas Natural Comprimido": "",
                    "Precio Gas Natural Licuado": "",
                    "Precio Gases licuados del petr\u00f3leo": "",
                    "Precio Gasoleo A": "1,399",
                    "Precio Gasoleo B": "",
                    "Precio Gasoleo Premium": "",
                    "Precio Gasolina 95 E10": "",
                    "Precio Gasolina 95 E5": "1,499",
                    "Precio Gasolina 95 E5 Premium": "",
                    "Precio Gasolina 98 E10": "",
                    "Precio Gasolina 98 E5": "",
                    "Precio Hidrogeno": "",
                    "Provincia": "CORU\u00d1A (A)",
                    "Remisi\u00f3n": "dm",
                    "R\u00f3tulo": "SBC ",
                    "Tipo Venta": "P",
                    "% BioEtanol": "0,0",
                    "% \u00c9ster met\u00edlico": "0,0",
                    "IDEESS": "13769",
                    "IDMunicipio": "2172",
                    "IDProvincia": "15",
                    "IDCCAA": "12",
                },
            ],
        }
        expected_datetime_obj = datetime(2024, 3, 12, 18, 48, 4, tzinfo=timezone.utc)
        expected_data = [
            SpainFuelPrice(
                dt=expected_datetime_obj,
                date=expected_datetime_obj.date(),
                hour=expected_datetime_obj.hour,
                zip_code=48170,
                municipality_id=7563,
                province_id=48,
                sale_type="p",
                label="petroprix",
                address="poligono ugaldeguren, 25",
                municipality="zamudio",
                province="bizkaia",
                locality="zamudio",
                latitude=43.286000,
                longitude=-2.872639,
                biodiesel_price=None,
                bioethanol_price=None,
                compressed_natural_gas_price=None,
                liquefied_natural_gas_price=None,
                liquefied_petroleum_gases_price=None,
                diesel_a_price=1.389,
                diesel_b_price=None,
                diesel_premium_price=None,
                gasoline_95_e10_price=None,
                gasoline_95_e5_price=1.539,
                gasoline_95_e5_premium_price=None,
                gasoline_98_e10_price=None,
                gasoline_98_e5_price=None,
                hydrogen_price=None,
            ),
            SpainFuelPrice(
                dt=expected_datetime_obj,
                date=expected_datetime_obj.date(),
                hour=expected_datetime_obj.hour,
                zip_code=15570,
                municipality_id=2172,
                province_id=15,
                sale_type="p",
                label="sbc",
                address="avenida do mar, 94",
                municipality="narón",
                province="coruña (a)",
                locality="gandara",
                latitude=43.488583,
                longitude=-8.200111,
                biodiesel_price=None,
                bioethanol_price=None,
                compressed_natural_gas_price=None,
                liquefied_natural_gas_price=None,
                liquefied_petroleum_gases_price=None,
                diesel_a_price=1.399,
                diesel_b_price=None,
                diesel_premium_price=None,
                gasoline_95_e10_price=None,
                gasoline_95_e5_price=1.499,
                gasoline_95_e5_premium_price=None,
                gasoline_98_e10_price=None,
                gasoline_98_e5_price=None,
                hydrogen_price=None,
            ),
        ]
        actual_data = map_spain_fuel_data(test_data)
        assert list(actual_data) == expected_data

    def test_create_spain_fuel_dataframe(self):
        test_datetime_obj = datetime(2024, 3, 12, 18, 48, 4, tzinfo=timezone.utc)
        test_data = [
            SpainFuelPrice(
                dt=test_datetime_obj,
                date=test_datetime_obj.date(),
                hour=test_datetime_obj.hour,
                zip_code=48170,
                municipality_id=7563,
                province_id=48,
                sale_type="p",
                label="petroprix",
                address="poligono ugaldeguren, 25",
                municipality="zamudio",
                province="bizkaia",
                locality="zamudio",
                latitude=43.286000,
                longitude=-2.872639,
                biodiesel_price=None,
                bioethanol_price=None,
                compressed_natural_gas_price=None,
                liquefied_natural_gas_price=None,
                liquefied_petroleum_gases_price=None,
                diesel_a_price=1.389,
                diesel_b_price=None,
                diesel_premium_price=None,
                gasoline_95_e10_price=None,
                gasoline_95_e5_price=1.539,
                gasoline_95_e5_premium_price=None,
                gasoline_98_e10_price=None,
                gasoline_98_e5_price=None,
                hydrogen_price=None,
            ),
            SpainFuelPrice(
                dt=test_datetime_obj,
                date=test_datetime_obj.date(),
                hour=test_datetime_obj.hour,
                zip_code=15570,
                municipality_id=2172,
                province_id=15,
                sale_type="p",
                label="sbc",
                address="avenida do mar, 94",
                municipality="narón",
                province="coruña (a)",
                locality="gandara",
                latitude=43.488583,
                longitude=-8.200111,
                biodiesel_price=None,
                bioethanol_price=None,
                compressed_natural_gas_price=None,
                liquefied_natural_gas_price=None,
                liquefied_petroleum_gases_price=None,
                diesel_a_price=1.399,
                diesel_b_price=None,
                diesel_premium_price=None,
                gasoline_95_e10_price=None,
                gasoline_95_e5_price=1.499,
                gasoline_95_e5_premium_price=None,
                gasoline_98_e10_price=None,
                gasoline_98_e5_price=None,
                hydrogen_price=None,
            ),
        ]
        expected_schema = SPAIN_FUEL_PRICES_SCHEMA
        expected_data = [
            Row(
                dt="2024-03-12T18:48:04+00:00",
                date="2024-03-12",
                hour=18,
                zip_code=48170,
                municipality_id=7563,
                province_id=48,
                sale_type="p",
                label="petroprix",
                address="poligono ugaldeguren, 25",
                municipality="zamudio",
                province="bizkaia",
                locality="zamudio",
                latitude=43.286000,
                longitude=-2.872639,
                biodiesel_price=None,
                bioethanol_price=None,
                compressed_natural_gas_price=None,
                liquefied_natural_gas_price=None,
                liquefied_petroleum_gases_price=None,
                diesel_a_price=1.389,
                diesel_b_price=None,
                diesel_premium_price=None,
                gasoline_95_e10_price=None,
                gasoline_95_e5_price=1.539,
                gasoline_95_e5_premium_price=None,
                gasoline_98_e10_price=None,
                gasoline_98_e5_price=None,
                hydrogen_price=None,
            ),
            Row(
                dt="2024-03-12T18:48:04+00:00",
                date="2024-03-12",
                hour=18,
                zip_code=15570,
                municipality_id=2172,
                province_id=15,
                sale_type="p",
                label="sbc",
                address="avenida do mar, 94",
                municipality="narón",
                province="coruña (a)",
                locality="gandara",
                latitude=43.488583,
                longitude=-8.200111,
                biodiesel_price=None,
                bioethanol_price=None,
                compressed_natural_gas_price=None,
                liquefied_natural_gas_price=None,
                liquefied_petroleum_gases_price=None,
                diesel_a_price=1.399,
                diesel_b_price=None,
                diesel_premium_price=None,
                gasoline_95_e10_price=None,
                gasoline_95_e5_price=1.499,
                gasoline_95_e5_premium_price=None,
                gasoline_98_e10_price=None,
                gasoline_98_e5_price=None,
                hydrogen_price=None,
            ),
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)
        actual_df = create_spain_fuel_dataframe(test_data)
        self.assert_spark_dataframes_equal(expected_df, actual_df)
