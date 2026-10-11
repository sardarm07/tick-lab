# Image for the Python services: the producers and the init job.
# Dependencies are installed exactly as locked in uv.lock; uv itself stays in the build stage.

FROM python:3.12.15-slim-trixie AS deps
COPY --from=ghcr.io/astral-sh/uv:0.12.24 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev

FROM python:3.12.15-slim-trixie
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
COPY --from=deps /opt/venv /opt/venv
WORKDIR /app
COPY schemas ./schemas
COPY config ./config
COPY infra/clickhouse/sql ./infra/clickhouse/sql
COPY ticklab ./ticklab
USER 1000:1000
