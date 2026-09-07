# scripts

Utility scripts for testing, data download, and migration.

## Scripts

| Script                                  | Purpose                                                                                                           | Usage                                                                                              |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `run-docker-test.sh`                    | Runs Docker-based tests for a service                                                                             | `./scripts/run-docker-test.sh <service>` (called by `make <service>.test`)                         |
| `download_fuel_data.py`                 | Downloads fuel price data from GCS to local `data/` directory                                                     | `make data.download`                                                                               |
| `migrate_csv_to_parquet.py`             | Converts CSV files to Parquet format                                                                              | `cd fuel-dashboard && uv run python ../scripts/migrate_csv_to_parquet.py`                          |
| `migrate_parquet_to_new_bucket.py`      | Copies Parquet files between GCS buckets                                                                          | Manual, one-time migration                                                                         |
| `logger.sh`                             | Shared bash logging utility                                                                                       | Sourced by `run-docker-test.sh`                                                                    |
| `idae_common.py`                        | Shared constants/GCS helpers for the IDAE ingest pipeline below                                                   | Imported by the three `idae_*` scripts, not run directly                                           |
| `idae_ingest_raw.py`                    | Stage 1: fetches IDAE's raw vehicle-consumption CSV as-is, uploads to `vehicles/raw/` in GCS                      | `make data.ingest-vehicle-consumption-raw` (or `make data.check-idae-update` to only check)        |
| `idae_transform_vehicle_consumption.py` | Stage 2: filters to passenger cars + WLTP-only, normalizes brand names, uploads to `vehicles/transformed/` in GCS | `make data.transform-vehicle-consumption`                                                          |
| `idae_vehicle_pairs.py`                 | Hand-curated gasoline/diesel model pairs (not a runnable script)                                                  | Imported by `idae_build_vehicle_catalog.py`                                                        |
| `idae_build_vehicle_catalog.py`         | Stage 3: resolves the curated pairs and regenerates `fuel-dashboard/app/data/vehicle_catalog.json`                | `make data.build-vehicle-catalog` (or `make data.refresh-vehicle-catalog` to run all three stages) |
