# fuel-dashboard

FastAPI + NiceGUI web dashboard for exploring Spanish fuel station prices, with station search, trip planning, and interactive maps.

## Architecture

| Layer | Directory | Description |
|-------|-----------|-------------|
| **API** | `app/api/` | FastAPI router, Pydantic request/response schemas, rate limiting (slowapi) |
| **Services** | `app/services/` | Station queries, trip planner, OSRM routing, geocoding |
| **Data** | `app/data/` | DuckDB engine over Parquet from GCS, caching, GeoJSON loader |
| **UI** | `app/ui/` | NiceGUI pages, Plotly charts, view models, reusable components |

## Project Structure

```
app/
  main.py              FastAPI app with lifespan hooks, health endpoints, UI init
  config.py            Pydantic-settings configuration (env prefix DASHBOARD_*)
  api/
    router.py          API routes
    schemas.py         Pydantic request/response models
  services/
    station_service.py Station queries and filtering
    trip_planner.py    Fuel stop optimization
    routing.py         OSRM integration
    geocoding.py       Address lookup via geopy
    geo_utils.py       Distance and geometry helpers
  data/
    duckdb_engine.py   DuckDB query engine and data loading
    gcs_client.py      GCS parquet download and caching
    cache.py           Background cache refresh
    geojson_loader.py  Province/municipality boundary loader
  ui/
    pages.py           NiceGUI page definitions
    charts.py          Plotly chart builders
    components.py      Reusable UI components
    view_models.py     UI state management
tests/
  fixture.py           Shared test data
  test_*.py            Unit and integration tests
```

## Setup

```bash
cd fuel-dashboard && uv sync --dev
```

Or from the project root: `make setup`

## Configuration

All settings use the `DASHBOARD_` env prefix. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DASHBOARD_GCP_PROJECT_ID` | `travel-assistant-417315` | GCP project |
| `DASHBOARD_GCS_BUCKET_NAME` | `travel-assistant-spain-fuel-prices` | Source bucket |
| `DASHBOARD_PORT` | `8080` | Server port |
| `DASHBOARD_CACHE_TTL_SECONDS` | `86400` | Cache TTL |
| `DASHBOARD_OSRM_ENABLED` | `true` | Enable OSRM routing |
| `DASHBOARD_RATE_LIMIT` | `60/minute` | API rate limit |

See `app/config.py` for the full list and defaults.

## Usage

Run locally via Docker:

```bash
make fuel-dashboard.run
# Dashboard available at http://localhost:8080
```

Endpoints:
- `GET /health` -- health check
- `GET /health/data` -- data layer health
- `GET /api/v1/...` -- API routes

## Testing

Docker (CI parity):

```bash
make fuel-dashboard.test
```

Local (faster iteration):

```bash
make fuel-dashboard.test-local
```

## Dependencies

| Package | Version |
|---------|---------|
| fastapi | >=0.115.12 |
| uvicorn | 0.34.0 |
| nicegui | >=3.7.0 |
| duckdb | 1.1.3 |
| google-cloud-storage | 2.18.2 |
| plotly | 5.24.1 |
| pandas | 2.2.3 |
| geopy | 2.4.1 |
| pydantic-settings | >=2.7.1 |
| slowapi | >=0.1.9 |
