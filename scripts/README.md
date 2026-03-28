# scripts

Utility scripts for testing, data download, and migration.

## Scripts

| Script                             | Purpose                                                       | Usage                                                                      |
| ---------------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `run-docker-test.sh`               | Runs Docker-based tests for a service                         | `./scripts/run-docker-test.sh <service>` (called by `make <service>.test`) |
| `download_fuel_data.py`            | Downloads fuel price data from GCS to local `data/` directory | `make data.download`                                                       |
| `migrate_csv_to_parquet.py`        | Converts CSV files to Parquet format                          | `cd fuel-dashboard && uv run python ../scripts/migrate_csv_to_parquet.py`  |
| `migrate_parquet_to_new_bucket.py` | Copies Parquet files between GCS buckets                      | Manual, one-time migration                                                 |
| `logger.sh`                        | Shared bash logging utility                                   | Sourced by `run-docker-test.sh`                                            |
