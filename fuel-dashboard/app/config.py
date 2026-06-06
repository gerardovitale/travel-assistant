from typing import Any

from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings
from pydantic_settings import EnvSettingsSource
from pydantic_settings import SettingsConfigDict

# Fields where a plain comma-separated env var is accepted instead of JSON.
# Add the Python field name here whenever a new list[str] field needs CSV support.
_CSV_LIST_FIELDS = {"report_brands"}


class _CsvListEnvSource(EnvSettingsSource):
    """Custom env source that accepts CSV strings for designated list[str] fields.

    pydantic-settings normally requires JSON arrays for list fields. This source
    intercepts CSV values (anything that doesn't look like a JSON array) before the
    JSON decoder sees them, so DASHBOARD_REPORT_BRANDS=ballenoil,repsol,costco works
    without wrapping in brackets. JSON format still works too.
    """

    def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
        if field_name in _CSV_LIST_FIELDS and isinstance(value, str):
            stripped = value.strip()
            if not stripped.startswith(("[", "{")):
                return [s.strip() for s in stripped.split(",") if s.strip()]
        return super().prepare_field_value(field_name, field, value, value_is_complex)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DASHBOARD_")

    gcp_project_id: str = "travel-assistant-417315"
    gcs_bucket_name: str = "travel-assistant-spain-fuel-prices"

    cache_ttl_seconds: int = 86400
    parquet_cache_dir: str = "/tmp/parquet_cache"
    parquet_cache_max_age_hours: int = 2

    duckdb_threads: int = 2
    duckdb_memory_limit: str = "1GB"

    port: int = 8080
    host: str = "0.0.0.0"

    geocoding_user_agent: str = "spain-fuel-dashboard"

    default_radius_km: float = 5.0
    default_limit: int = 5

    default_consumption_lper100km: float = 7.0
    default_tank_liters: float = 40.0
    default_refill_liters: float = 30.0
    default_fuel_level_pct: float = 25.0
    default_max_detour_minutes: float = 5.0
    default_min_fuel_at_destination_pct: float = 40.0

    osrm_base_url: str = "https://router.project-osrm.org"
    osrm_timeout: float = 5.0
    osrm_enabled: bool = True

    rate_limit: str = "60/minute"

    realtime_enabled: bool = True
    realtime_refresh_seconds: int = 600
    realtime_curl_timeout: int = 120
    realtime_connect_timeout: int = 10
    realtime_max_retries: int = 3
    realtime_retry_base_delay: int = 10

    ui_test_mode: bool = False
    ui_fixture_set: str = "happy_path"
    disable_external_assets: bool = False

    insights_zones_enabled: bool = False
    insights_historical_enabled: bool = False
    insights_reportes_enabled: bool = True
    # Override: DASHBOARD_REPORT_BRANDS=ballenoil,repsol,costco,cepsa  (empty = all brands)
    report_brands: list[str] = ["ballenoil", "repsol", "costco"]

    public_url: str = ""
    analytics_enabled: bool = False
    analytics_domain: str = ""

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, **kwargs):
        return (init_settings, _CsvListEnvSource(settings_cls), dotenv_settings) + tuple(kwargs.values())


settings = Settings()
