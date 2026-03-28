# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spanish Fuel Price Finder — a data ingestion pipeline that fetches fuel station prices from Spain's government API, transforms the data with pandas, and stores CSV files in Google Cloud Storage. Deployed on GCP using Terraform, with CI/CD via GitHub Actions.

## Architecture

Two parallel ingestion implementations exist:

- **fuel-ingestor/** — Cloud Run Job (batch). Runs on a daily cron schedule triggered by GitHub Actions. Entry point: `app/main.py`.
- **ingest-fuel-prices/** — Google Cloud Function (HTTP-triggered via `@functions_framework.http`). Entry point: `ingest_fuel_prices` in `main.py`.

Both follow the same pipeline: fetch JSON from Spain's fuel prices REST API → transform with pandas (Spanish→English column mapping in `entity.py`, price normalization, timestamping) → upload CSV to GCS bucket.

**Infrastructure (infra/):** Terraform configs for Cloud Run job, GCS bucket, service accounts, and IAM. `infra/backend_support/` handles CI/CD service account and Terraform state bucket.

**Data flow:** GitHub Actions daily cron (`trigger-ingestor.yaml` at 5AM UTC) → Cloud Function HTTP call → GCS bucket (`travel-assistant-417315-spain-fuel-prices`).

**Deployment pipeline:** Push to `main` → test (Docker) → build & push image to Docker Hub → Terraform apply to GCP.

## Common Commands

```bash
# Install deps locally (uv)
make sync

# Fast local tests without Docker
make test-local

# Add a dependency
cd <service> && uv add <package>
cd <service> && uv add --group dev <package>

# Run fuel-ingestor tests (Docker-based)
make fuel-ingestor.test

# Run ingest-fuel-prices tests
make ingest-fuel-prices.test
# Or directly: cd ingest-fuel-prices && pytest -vv --durations=0 .

# Run Cloud Function locally
make ingest-fuel-prices.run

# Terraform (backend support)
make backend.init
make backend.plan
make backend.apply
make backend.run    # init + plan + apply
```

## Code Style

Enforced via pre-commit hooks (`.pre-commit-config.yaml`):

- **black** formatter with line-length=120
- **flake8** linter with max-line-length=120
- **reorder-python-imports** for import sorting
- **gitleaks** for secret detection
- **terraform_fmt** and **terraform_validate** for Terraform files

Python version: 3.13. Run `pre-commit run --all-files` to check all hooks.

## Key Configuration

| Item | Value |
|------|-------|
| GCP Project | travel-assistant-417315 |
| GCP Region | europe-southwest1 |
| Resource prefix | travass |
| Terraform version | 1.10.4 |
| Data source API | https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/ |

Environment variables are in `.env` (loaded by Makefile). Terraform variables defined in `infra/varibles.tf` and `infra/backend_support/variables.tf`.

## Service Details

### fuel-dashboard (FastAPI + NiceGUI)
- Entry point: `app/main.py` (FastAPI with lifespan hooks)
- API routes: `app/api/` (FastAPI routers with Pydantic schemas)
- Business logic: `app/services/` (station queries, trip planner, geocoding)
- UI components: `app/ui/` (NiceGUI pages, charts, view models)
- Data layer: `app/data/` (GCS client, DuckDB engine, GeoJSON loader)
- Config: `app/config.py` (pydantic-settings, env prefix `DASHBOARD_*`)
- Test fixtures: `tests/fixture.py`

### fuel-ingestor (Cloud Run Job)
- Entry point: `app/main.py` (simple orchestration)
- Pipeline: `spain_fuel_price.py` (fetch API) → `entity.py` (transform/map columns) → GCS upload
- Aggregation: `aggregator.py` (post-ingestion stats, runs separately via GitHub Actions)
- Backfill: `backfill.py` (historical data recovery)
- Test fixtures: `tests/fixture.py`

### ingest-fuel-prices (Cloud Function — legacy)
- Older implementation, not under active development. Prefer `fuel-ingestor/` for new work.

## When Adding Features

1. Always add tests in the corresponding `tests/` directory
2. Use `tests/fixture.py` for shared test data — both services have one
3. For dashboard endpoints: add route in `app/api/`, logic in `app/services/`, test in `tests/`
4. For ingestor changes: the pipeline is linear (fetch → transform → upload), extend at the right stage
5. Run `pre-commit run --all-files` before committing
6. Docker-based tests (`make <service>.test`) are the CI source of truth

## Troubleshooting

- **Pre-commit fails on formatting**: Run `black --line-length 120 <file>` and `reorder-python-imports --py313-plus <file>`
- **Docker test fails but local passes**: Check `Dockerfile.test` — it runs in an isolated container with its own dependency resolution
- **Terraform state issues**: Run `make backend.init` to reinitialize the backend connection
