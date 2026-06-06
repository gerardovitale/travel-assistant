# Build context = repo root (workspace). See deploy.yaml / makefile.
FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY spain-fuel-fetcher/ ./spain-fuel-fetcher/
COPY fuel-ingestor/pyproject.toml ./fuel-ingestor/
COPY fuel-dashboard/pyproject.toml ./fuel-dashboard/
RUN uv sync --frozen --no-dev --no-install-project --no-editable --package fuel-ingestor

FROM python:3.13-slim
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY ./fuel-ingestor/app/ ./
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
RUN useradd -r -u 1000 -s /usr/sbin/nologin appuser && chown -R appuser:appuser /app
USER appuser
ENTRYPOINT ["python3", "ingestor/main.py"]
