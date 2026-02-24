from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DASHBOARD_")

    gcp_project_id: str = "travel-assistant-417315"
    gcs_bucket_name: str = "travel-assistant-spain-fuel-prices"
    cache_ttl_seconds: int = 3600
    port: int = 8080
    host: str = "0.0.0.0"
    geocoding_user_agent: str = "spain-fuel-dashboard"
    default_radius_km: float = 5.0
    default_limit: int = 3
    price_weight: float = 0.6
    distance_weight: float = 0.4


settings = Settings()
