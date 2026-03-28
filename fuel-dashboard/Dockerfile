FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.6 /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.13-slim
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY ./app/ ./
ENV PATH="/app/.venv/bin:$PATH"
RUN useradd -r -u 1000 -s /usr/sbin/nologin appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8080
ENTRYPOINT ["python3", "main.py"]
