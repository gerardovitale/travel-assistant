import os

from google.cloud import storage

BUCKET_NAME = "travel-assistant-spain-fuel-prices"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
BLOB_PREFIX = "spain_fuel_prices_"


def download():
    os.makedirs(DATA_DIR, exist_ok=True)

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = [b for b in bucket.list_blobs(prefix=BLOB_PREFIX) if b.name.endswith(".parquet")]

    print(f"Found {len(blobs)} parquet file(s) in GCS.")

    local_files = set(os.listdir(DATA_DIR))
    to_download = [b for b in blobs if b.name not in local_files]

    if not to_download:
        print("All files already exist locally. Nothing to download.")
        return

    print(f"Skipping {len(blobs) - len(to_download)} already present. Downloading {len(to_download)} new file(s)...")

    for i, blob in enumerate(to_download, 1):
        dest = os.path.join(DATA_DIR, blob.name)
        print(f"  [{i}/{len(to_download)}] {blob.name}")
        blob.download_to_filename(dest)

    print("Download complete.")


if __name__ == "__main__":
    download()
