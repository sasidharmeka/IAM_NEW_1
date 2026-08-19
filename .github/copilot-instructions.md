# IAM pipeline contributor guide

## Architecture

The deployed pipeline is intentionally serverless and bounded:

1. producer.py publishes every configured CSV source to one environment-scoped
   Pub/Sub topic with source, environment, and run attributes.
2. consumer.py pulls one bounded batch, groups by source/run, writes Parquet to
   gs://<bucket>/source/<source>/, and acknowledges only after all uploads.
3. one Managed Service for Apache Spark batch reads all sources and computes all
   detections.
4. Spark replaces the current partitioned BigQuery result tables.

Cloud Composer and Airflow are not part of this repository or deployment.

## Important contracts

- Source names are defined in pipeline_config.py and spark_job/schemas.py.
- app_usage is the canonical application source name; do not shorten it to app.
- Pub/Sub payloads stay source-shaped. Pipeline metadata columns begin with an
  underscore.
- Source record IDs are deduplicated before Spark analytics.
- The Spark job is consolidated: never reintroduce one batch per source.
- BigQuery outputs use overwrite semantics because every run recomputes the
  current state from historical source files.
- Add every new detector to build_detections and add a Spark test that triggers
  an action on its output.

## Workflow

- run-pipeline.yaml is the only workflow and is manually triggered.
- validate-only is the default and must remain cloud-free.
- ingestion-only must not enable or submit Dataproc.
- full-pipeline may submit exactly one batch and must retain a short TTL.
- Authentication supports Workload Identity Federation or the GCP_SA_KEY
  fallback. Never print credential content or commit credential files.
- Do not add schedules, push-triggered cloud changes, Composer, Cloud Run,
  Workflows, or persistent compute.

## Local checks

Run:

    python -m pip install -r requirements-dev.txt
    python -m compileall -q .
    ruff check .
    pytest -q

Use the exact Managed Service for Apache Spark runtime-compatible PySpark
version from requirements-dev.txt for local Spark tests.
