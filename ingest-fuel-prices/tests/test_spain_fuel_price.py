from datetime import datetime
from datetime import timezone
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from src.entity import SpainFuelPrice
from src.spain_fuel_price import create_spain_fuel_dataframe
from src.spain_fuel_price import extract_fuel_prices_raw_data
from src.spain_fuel_price import map_raw_data_into_spain_fuel_price
from src.spain_fuel_price import map_spain_fuel_price
from tests.fixture import get_mapped_spain_fuel_price_data


class TestFuelPrice(TestCase):

    def setUp(self):
        requests_patch = patch("src.spain_fuel_price.requests")
        self.addCleanup(requests_patch.stop)
        self.mock_requests = requests_patch.start()

        logger_patch = patch("src.spain_fuel_price.logger")
        self.addCleanup(logger_patch.stop)
        self.mock_logger = logger_patch.start()

    def test_extract_fuel_prices_raw_data(self):
        self.mock_requests.get.return_value.status_code = 200
        _ = extract_fuel_prices_raw_data()
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
            timestamp=expected_datetime_obj,
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

        self.assertEqual(actual_fuel_price.model_dump(), expected_fuel_price.model_dump())

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
        expected_data = get_mapped_spain_fuel_price_data()

        actual_data = map_raw_data_into_spain_fuel_price(test_data)

        self.assertEqual(list(actual_data), expected_data)

    def test_create_spain_fuel_dataframe(self):
        test_data = get_mapped_spain_fuel_price_data()
        expected_data = [
            ("2024-03-12T18:48:04+00:00", "2024-03-12", 18, 48170, 7563, 48, "p", "petroprix",
             "poligono ugaldeguren, 25", "zamudio", "bizkaia", "zamudio", 43.286000, -2.872639, None, None, None, None,
             None, 1.389, None, None, None, 1.539, None, None, None, None,
             ),
            ("2024-03-12T18:48:04+00:00", "2024-03-12", 18, 15570, 2172, 15, "p", "sbc", "avenida do mar, 94", "narón",
             "coruña (a)", "gandara", 43.488583, -8.200111, None, None, None, None, None, 1.399, None, None, None,
             1.499, None, None, None, None,
             ),
        ]
        expected_df = pd.DataFrame(expected_data)

        actual_df = create_spain_fuel_dataframe(test_data)

        self.assertIsInstance(actual_df, pd.DataFrame)
        self.assertEqual(list(actual_df.columns), list(expected_df.columns))
