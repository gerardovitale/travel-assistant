# fuel-ingestor

Cloud Run Job that ingests daily fuel station prices from Spain's government REST API and stores them as Parquet files in Google Cloud Storage.

## How It Works

1. **Fetch** -- downloads JSON from Spain's [MinETUR REST API](https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/) (`spain_fuel_price.py`)
2. **Transform** -- maps Spanish column names to English, normalizes prices, adds timestamps (`entity.py`)
3. **Validate** -- checks the resulting DataFrame for consistency
4. **Store** -- writes Parquet to GCS bucket

## Project Structure

```
app/
  main.py              Entry point, orchestrates the pipeline
  spain_fuel_price.py  API fetch, DataFrame creation, validation, GCS upload
  entity.py            Column mapping and transformation logic
  aggregator.py        Post-ingestion statistics (run separately via CI)
  backfill.py          Historical data recovery utility
  local_run.py         Local execution wrapper (writes to output/)
tests/
  fixture.py           Shared test data
  test_*.py            Unit tests
```

## Setup

```bash
cd fuel-ingestor && uv sync --dev
```

Or from the project root: `make setup`

## Usage

Run locally via Docker (writes output to `output/`):

```bash
make fuel-ingestor.run
```

In production, the ingestor runs as a Cloud Run Job triggered daily at 05:00 UTC by GitHub Actions (`trigger-ingestor.yaml`).

## Testing

Docker (CI parity):

```bash
make fuel-ingestor.test
```

Local (faster iteration):

```bash
make fuel-ingestor.test-local
```

## Dependencies

| Package | Version |
|---------|---------|
| pandas | 2.2.3 |
| google-cloud-storage | 2.18.2 |
| requests | 2.32.3 |
| pytz | 2025.2 |
| pyarrow | 18.1.0 |
