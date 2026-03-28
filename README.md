[![Deploy](https://github.com/gerardovitale/travel-assistant/actions/workflows/deploy.yaml/badge.svg)](https://github.com/gerardovitale/travel-assistant/actions/workflows/deploy.yaml)
[![Trigger Ingestor](https://github.com/gerardovitale/travel-assistant/actions/workflows/trigger-ingestor.yaml/badge.svg)](https://github.com/gerardovitale/travel-assistant/actions/workflows/trigger-ingestor.yaml)

# Spanish Fuel Price Finder

A data pipeline and dashboard for Spanish fuel station prices. Fetches daily pricing data from Spain's government API,
stores it in Google Cloud Storage, and serves it through an interactive web dashboard with station search, trip
planning, and price visualizations.

## Architecture

```
                          Daily cron (05:00 UTC)
                          GitHub Actions
                                |
                                v
  Spain Gov API ──> fuel-ingestor (Cloud Run Job) ──> GCS Bucket (Parquet)
                                                           |
                                                           v
                                                     fuel-dashboard (Cloud Run Service)
                                                     FastAPI + NiceGUI + DuckDB
                                                           |
                                                           v
                                                       Browser (:8080)
```

**Deployment:** Push to `main` triggers test, build, Trivy security scan, and Terraform apply via GitHub Actions.

## Repository Structure

```
fuel-ingestor/     Batch ingestion pipeline (Cloud Run Job)
fuel-dashboard/    Web dashboard (FastAPI + NiceGUI)
infra/             Terraform IaC for GCP
scripts/           Utility and migration scripts
data/              Local parquet data cache
maps/              GeoJSON boundary files for Spain
```

## Prerequisites

- Python 3.13
- [uv](https://docs.astral.sh/uv/) (package manager)
- Docker
- Terraform 1.10.4 (for infrastructure changes)
- GCP credentials (for cloud operations)

## Quick Start

```bash
make setup                # install all dependencies via uv
make test-local           # run all tests locally (no Docker)
make test                 # run all tests in Docker (CI parity)
make fuel-dashboard.run   # run dashboard at http://localhost:8080
make fuel-ingestor.run    # run ingestor locally (writes to output/)
```

## Modules

- [fuel-ingestor](./fuel-ingestor/README.md) -- batch ingestion pipeline
- [fuel-dashboard](./fuel-dashboard/README.md) -- web dashboard and API
- [infra](./infra/README.md) -- Terraform infrastructure
- [scripts](./scripts/README.md) -- utility scripts

## CI/CD

| Workflow                | Trigger                | Description                                    |
| ----------------------- | ---------------------- | ---------------------------------------------- |
| `deploy.yaml`           | Push to `main`         | Test, build, Trivy scan, Terraform apply       |
| `trigger-ingestor.yaml` | Daily cron (05:00 UTC) | Triggers Cloud Run ingestion job + aggregation |
| `destroy.yaml`          | Manual                 | Tears down GCP infrastructure                  |

## Code Style

Enforced via pre-commit hooks: **black** (line-length 120), **flake8** (max-line-length 120),
**reorder-python-imports**, **gitleaks**, **terraform_fmt**, and **terraform_validate**.

```bash
pre-commit run --all-files
```
