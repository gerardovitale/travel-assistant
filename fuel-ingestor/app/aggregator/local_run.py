import logging
import os
import shutil
import time

from aggregator.main import run_aggregation

LOGGING_FORMAT = "%(name)s - [%(levelname)s] - %(message)s [%(filename)s:%(lineno)d]"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)


class _LocalBlob:
    """Filesystem-backed mimic of google.cloud.storage.Blob."""

    def __init__(self, root, name):
        self.name = name
        self._path = os.path.join(root, name)

    def exists(self):
        return os.path.isfile(self._path)

    def download_as_bytes(self):
        with open(self._path, "rb") as f:
            return f.read()

    def download_to_filename(self, path):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        shutil.copy(self._path, path)

    def upload_from_string(self, data, content_type=None):
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        payload = data if isinstance(data, (bytes, bytearray)) else data.encode("utf-8")
        with open(self._path, "wb") as f:
            f.write(payload)


class _LocalBucket:
    """Filesystem-backed mimic of google.cloud.storage.Bucket — recursive walk on list_blobs."""

    def __init__(self, root):
        self._root = root

    def blob(self, name):
        return _LocalBlob(self._root, name)

    def list_blobs(self, prefix=""):
        if not os.path.isdir(self._root):
            return []
        results = []
        for dirpath, _, filenames in os.walk(self._root):
            for fname in filenames:
                rel = os.path.relpath(os.path.join(dirpath, fname), self._root).replace(os.sep, "/")
                if rel.startswith(prefix):
                    results.append(_LocalBlob(self._root, rel))
        return results


def main():
    output_dir = os.environ.get("LOCAL_BUCKET_DIR", "/output/aggregator-local")
    os.makedirs(output_dir, exist_ok=True)

    raw_files = [f for f in os.listdir(output_dir) if f.startswith("spain_fuel_prices_") and f.endswith(".parquet")]
    logger.info(f"Starting LOCAL aggregator run against {output_dir!r} ({len(raw_files)} raw snapshots)")
    if not raw_files:
        logger.warning("No raw snapshots found — seed the directory with spain_fuel_prices_*.parquet files first.")

    start_time = time.time()
    run_aggregation(_LocalBucket(output_dir))
    logger.info(f"Aggregation complete in {time.time() - start_time:.1f}s. Output under {output_dir!r}")


if __name__ == "__main__":
    main()
