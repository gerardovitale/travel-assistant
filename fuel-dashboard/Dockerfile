FROM python:3.13-slim AS python-build
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


FROM python:3.13-slim AS final
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
COPY --from=python-build /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=python-build /usr/local/bin /usr/local/bin
COPY ./app/ ./
EXPOSE 8080
ENTRYPOINT ["python3", "main.py"]
