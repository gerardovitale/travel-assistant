ARG PYTHON_VERSION=3.9
ARG IMAGE_TYPE=slim


FROM python:${PYTHON_VERSION}-${IMAGE_TYPE} as python-base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.6.1 \
    POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH=/opt/pysetup \
    VENV_PATH=/opt/pysetup/.venv \
    JAVA_HOME=/opt/java/openjdk
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"


FROM eclipse-temurin:11-centos7 AS JAVA11


FROM python-base as builder-base
RUN apt-get update && apt-get install --no-install-recommends -y curl build-essential
RUN curl -sSL https://install.python-poetry.org | python3 -
WORKDIR $PYSETUP_PATH


FROM python-base as development
ARG SOURCE_PATH=travel_assistant/
WORKDIR $PYSETUP_PATH
COPY --from=JAVA11 $JAVA_HOME $JAVA_HOME
COPY --from=builder-base $POETRY_HOME $POETRY_HOME
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH
WORKDIR /app
COPY poetry.lock pyproject.toml ./
COPY ${SOURCE_PATH} ./$SOURCE_PATH
RUN poetry install --only main
ENTRYPOINT [ "poetry", "run", "python3", "travel_assistant/main.py" ]
