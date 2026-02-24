import argparse

from google.cloud import storage

SOURCE_BUCKET = "spain-fuel-prices"
DESTINATION_BUCKET = "travel-assistant-spain-fuel-prices"


def migrate(source_bucket: str, destination_bucket: str, delete_source: bool = False, dry_run: bool = False) -> None:
    client = storage.Client()
    src_bucket = client.bucket(source_bucket)
    dst_bucket = client.bucket(destination_bucket)
    blobs = list(client.list_blobs(source_bucket))

    parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]
    if not parquet_blobs:
        print("No Parquet files found. Nothing to migrate.")
        return

    print(f"Found {len(parquet_blobs)} Parquet file(s) to migrate.")
    if dry_run:
        print("Dry run mode — no files will be copied or deleted.")
        for blob in parquet_blobs:
            print(f"  Would copy: gs://{source_bucket}/{blob.name} -> gs://{destination_bucket}/{blob.name}")
        return

    copied = 0
    for blob in parquet_blobs:
        print(f"  Copying: gs://{source_bucket}/{blob.name} -> gs://{destination_bucket}/{blob.name}")
        src_bucket.copy_blob(blob, dst_bucket, blob.name)
        copied += 1

        if delete_source:
            blob.delete()
            print(f"    Deleted source: gs://{source_bucket}/{blob.name}")

    print(f"Migration complete. {copied} file(s) copied.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate Parquet files between GCS buckets")
    parser.add_argument(
        "--source-bucket",
        default=SOURCE_BUCKET,
        help=f"Source GCS bucket (default: {SOURCE_BUCKET})",
    )
    parser.add_argument(
        "--destination-bucket",
        default=DESTINATION_BUCKET,
        help=f"Destination GCS bucket (default: {DESTINATION_BUCKET})",
    )
    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete source files after successful copy",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files without copying",
    )
    args = parser.parse_args()
    migrate(
        source_bucket=args.source_bucket,
        destination_bucket=args.destination_bucket,
        delete_source=args.delete_source,
        dry_run=args.dry_run,
    )
