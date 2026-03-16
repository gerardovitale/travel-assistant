from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DASHBOARD_")

    gcp_project_id: str = "travel-assistant-417315"
    gcs_bucket_name: str = "travel-assistant-spain-fuel-prices"

    cache_ttl_seconds: int = 86400

    port: int = 8080
    host: str = "0.0.0.0"

    geocoding_user_agent: str = "spain-fuel-dashboard"

    default_radius_km: float = 5.0
    default_limit: int = 5

    default_consumption_lper100km: float = 7.0
    default_tank_liters: float = 40.0

    osrm_base_url: str = "https://router.project-osrm.org"
    osrm_timeout: float = 5.0
    osrm_enabled: bool = True


settings = Settings()
