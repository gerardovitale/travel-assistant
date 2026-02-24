import argparse
from io import BytesIO
from io import StringIO

import pandas as pd
from google.cloud import storage

BUCKET_NAME = "spain-fuel-prices"


def migrate(delete_csv: bool = False) -> None:
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = list(client.list_blobs(BUCKET_NAME))

    csv_blobs = [b for b in blobs if b.name.endswith(".csv")]
    if not csv_blobs:
        print("No CSV files found. Nothing to migrate.")
        return

    print(f"Found {len(csv_blobs)} CSV file(s) to migrate.")

    for blob in csv_blobs:
        parquet_name = blob.name.rsplit(".csv", 1)[0] + ".parquet"
        print(f"  Converting: {blob.name} -> {parquet_name}")

        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, compression="snappy")
        parquet_buffer.seek(0)

        parquet_blob = bucket.blob(parquet_name)
        parquet_blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        if delete_csv:
            blob.delete()
            print(f"    Deleted original: {blob.name}")

    print("Migration complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate CSV files to Parquet in GCS")
    parser.add_argument(
        "--delete-csv",
        action="store_true",
        help="Delete original CSV files after successful conversion",
    )
    args = parser.parse_args()
    migrate(delete_csv=args.delete_csv)
