# Build context = repo root (workspace). See deploy.yaml / makefile.
FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY spain-fuel-fetcher/ ./spain-fuel-fetcher/
COPY fuel-dashboard/pyproject.toml ./fuel-dashboard/
COPY fuel-ingestor/pyproject.toml ./fuel-ingestor/
RUN uv sync --frozen --no-dev --no-install-project --no-editable --package fuel-dashboard

FROM python:3.13-slim AS asset-builder
WORKDIR /app
COPY ./fuel-dashboard/app/ ./app/
RUN python - <<'PY'
import gzip
import json
from pathlib import Path

geojson_dir = Path("/app/app/data/geojson")
src = geojson_dir / "spain-postal-codes.geojson"
dst = geojson_dir / "spain-postal-codes.geojson.gz"
with src.open(encoding="utf-8") as f:
    payload = json.load(f)
with gzip.open(dst, mode="wt", encoding="utf-8", compresslevel=9) as f:
    json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
src.unlink()
PY

FROM python:3.13-slim
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*
# Runtime never invokes pip (deps are already synced into the venv copied below); strip the
# base image's ensurepip install to drop its vendored msgpack/setuptools CVEs (unused attack surface).
RUN python3 -m pip uninstall --yes --break-system-packages pip setuptools 2>/dev/null; \
    rm -rf /usr/local/lib/python3.13/ensurepip
WORKDIR /app
RUN useradd -r -u 1000 -s /usr/sbin/nologin appuser
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=asset-builder --chown=appuser:appuser /app/app/ ./
ENV PATH="/app/.venv/bin:$PATH"
USER appuser
EXPOSE 8080
ENTRYPOINT ["python3", "main.py"]
