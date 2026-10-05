FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DAGSTER_HOME=/dagster_home \
    RUNNING_IN_DOCKER=1

WORKDIR /app

COPY requirements.txt requirements-dashboard.txt ./
RUN pip install -r requirements.txt -r requirements-dashboard.txt

COPY . .

# Build dbt's manifest now, so containers start without a parse step.
RUN mkdir -p /dagster_home /app/data \
    && POSTGRES_HOST=postgres POSTGRES_PORT=5432 python -c "import orchestration.assets"

EXPOSE 3000 8501
CMD ["python", "-m", "orchestration.start"]
