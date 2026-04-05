from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


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

    osrm_base_url: str = "https://router.project-osrm.org"
    osrm_timeout: float = 5.0
    osrm_enabled: bool = True

    rate_limit: str = "60/minute"

    realtime_enabled: bool = True
    realtime_refresh_seconds: int = 600
    realtime_curl_timeout: int = 120


settings = Settings()
