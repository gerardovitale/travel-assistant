# CLAUDE.md

## CONTEXT

- Stack: Python 3.13, FastAPI + Jinja2 (dashboard), pandas + DuckDB, GCP (Cloud Run + GCS), Terraform
- Purpose: fetch Spain fuel station prices from gov API, store Parquet in GCS, serve via FastAPI dashboard
- Key constraints: Docker tests CI source of truth; pre-commit enforces black (120) + flake8 (120) + import order
- `fuel-ingestor/` active; `ingest-fuel-prices/` legacy — don't extend
- GCP project: `travel-assistant-417315`, region: `europe-southwest1`, resource prefix: `travass`

## Architecture

- **fuel-ingestor/** — Cloud Run Job, daily cron via GitHub Actions (`trigger-ingestor.yaml`). Entry: `app/main.py`
- **fuel-dashboard/** — FastAPI + Jinja2 templates. Entry: `app/main.py`
- **ingest-fuel-prices/** — Cloud Function (legacy, HTTP-triggered)
- **infra/** — Terraform: Cloud Run job, GCS bucket, service accounts, IAM
- Pipeline: fetch JSON (Spain fuel API) → transform with pandas → upload Parquet to GCS
- Deployment: push to `main` → Docker test → build & push image (`deploy.yaml`) → Terraform apply

## Commands

```bash
make sync                       # install deps for all services (uv)
make test-local                 # fast local tests (no Docker) + Playwright UI tests
make fuel-ingestor.test         # Docker-based CI tests
make fuel-dashboard.test        # Docker-based CI tests
make backend.run                # terraform init + plan + apply
cd <service> && uv add <pkg>    # add dependency
```

## Code Style

- black (120), flake8 (120), reorder-python-imports — enforced by pre-commit
- Native Python 3.13 generics: `list[X]`, `dict[K, V]`, `X | None` — no `from typing import List/Dict/Optional`
- Run `pre-commit run --all-files` before committing

## Service Details

### fuel-dashboard

- API routes: `app/api/router.py` (FastAPI + slowapi rate limiting, Pydantic schemas in `app/api/schemas.py`)
- Business logic: `app/services/` (station queries, trip planner, forecast, geocoding, routing)
- UI: `app/web/templates/` (Jinja2 — `search.html`, `trip.html`, `insights.html`, `loading.html`) + `app/web/static/`
- Data: `app/data/` — `duckdb_engine.py` (in-memory DuckDB queries), `gcs_client.py` (Parquet cache),
  `geojson_loader.py`, `cache.py` (background refresh threads), `realtime_client.py`
- Config: `app/config.py` (pydantic-settings, env prefix `DASHBOARD_*`)
- UI test mode: `app/ui_test_support.py` — fixture responses when `DASHBOARD_UI_TEST_MODE=true`
- Playwright E2E tests: `tests/ui/` — run via `make fuel-dashboard.ui-test`

### fuel-ingestor

- Pipeline: `app/ingestor/spain_fuel_price.py` (fetch API) → `app/ingestor/entity.py` (transform/map columns) → GCS
  upload
- Aggregation: `app/aggregator/` — pipelines in `pipelines/` (`zip_code_stats.py`, `province_stats.py`,
  `brand_stats.py`, `day_of_week_stats.py`, `ingestion_stats.py`), reports in `reports/` (`brand_win_rate.py`,
  `brand_comparison.py`)
- Aggregation triggered via `run-aggregation.yaml` GitHub Actions workflow
- Backfill: `app/aggregator/backfill.py`

## Adding Features

1. Add tests in `tests/` — use `tests/fixture.py` for shared unit test data
2. Dashboard endpoints: route in `app/api/router.py`, logic in `app/services/`, test in `tests/`
3. Dashboard UI: Jinja2 templates in `app/web/templates/`, static in `app/web/static/`
4. Ingestor changes: extend at right stage (`ingestor/` for fetch/transform, `aggregator/pipelines/` for stats)
5. Run `pre-commit run --all-files` before committing
6. Docker-based tests (`make <service>.test`) CI source of truth

## Troubleshooting

- Pre-commit formatting fails: `black --line-length 120 <file>` + `reorder-python-imports --py313-plus <file>`
- Docker test fails but local passes: check `Dockerfile.test`
- Terraform state issues: `make backend.init`
