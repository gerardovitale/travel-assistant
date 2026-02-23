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

Python version: 3.9. Run `pre-commit run --all-files` to check all hooks.

## Key Configuration

| Item | Value |
|------|-------|
| GCP Project | travel-assistant-417315 |
| GCP Region | europe-southwest1 |
| Resource prefix | travass |
| Terraform version | 1.10.4 |
| Data source API | https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/ |

Environment variables are in `.env` (loaded by Makefile). Terraform variables defined in `infra/varibles.tf` and `infra/backend_support/variables.tf`.
