"""Run all IAM detections once and replace the current BigQuery result tables."""

from __future__ import annotations

import argparse
import logging
import os
import re
from collections.abc import Iterable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from spark_job.detections import build_detections
from spark_job.schemas import SOURCE_ID_COLUMNS, SOURCE_SCHEMAS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("iam-transform")


def validate_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError(f"{label} contains unsupported characters: {value!r}")
    return value


def validate_bigquery_dataset(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(
            f"bq_dataset must contain only letters, numbers, and underscores: {value!r}"
        )
    return value


def source_path(input_root: str, source: str) -> str:
    return f"{input_root.rstrip('/')}/{source}/*.parquet"


def load_source(spark: SparkSession, input_root: str, source: str) -> DataFrame:
    path = source_path(input_root, source)
    schema = SOURCE_SCHEMAS[source]
    try:
        frame = spark.read.schema(schema).parquet(path)
    except AnalysisException as exc:
        if "PATH_NOT_FOUND" not in str(exc) and "Path does not exist" not in str(exc):
            raise
        LOGGER.warning("No files for %s at %s; using an empty source", source, path)
        frame = spark.createDataFrame([], schema)

    fields = {field.name: field.dataType for field in schema.fields}
    missing = set(fields) - set(frame.columns)
    for column in sorted(missing):
        frame = frame.withColumn(column, F.lit(None).cast(fields[column]))

    ids = list(SOURCE_ID_COLUMNS[source])
    return frame.dropDuplicates(ids)


def write_bigquery(
    frame: DataFrame,
    *,
    project_id: str,
    dataset: str,
    table: str,
    write_method: str,
    temporary_gcs_bucket: str | None,
) -> None:
    writer = (
        frame.write.format("bigquery")
        .option("table", f"{project_id}:{dataset}.{table}")
        .option("writeMethod", write_method)
    )
    if write_method == "indirect":
        writer = writer.option("writeDisposition", "WRITE_TRUNCATE")
    if "event_date" in frame.columns and write_method == "indirect":
        writer = writer.option("partitionField", "event_date").option("partitionType", "DAY")
    if write_method == "indirect":
        if not temporary_gcs_bucket:
            raise ValueError("temporary_gcs_bucket is required for indirect writes")
        writer = writer.option("temporaryGcsBucket", temporary_gcs_bucket)

    writer.mode("overwrite").save()
    LOGGER.info("Replaced %s:%s.%s", project_id, dataset, table)


def selected_tables(
    detections: dict[str, DataFrame], requested_tables: str | None
) -> Iterable[tuple[str, DataFrame]]:
    if not requested_tables:
        return detections.items()

    requested = {item.strip() for item in requested_tables.split(",") if item.strip()}
    unknown = requested - set(detections)
    if unknown:
        raise ValueError(f"Unknown output tables: {sorted(unknown)}")
    return [(table, detections[table]) for table in sorted(requested)]


def run(
    *,
    env: str,
    input_root: str,
    bq_project: str,
    bq_dataset: str,
    run_id: str,
    write_method: str = "indirect",
    temporary_gcs_bucket: str | None = None,
    requested_tables: str | None = None,
    dry_run: bool = False,
    spark: SparkSession | None = None,
) -> dict[str, int]:
    validate_identifier(env, "env")
    validate_bigquery_dataset(bq_dataset)
    validate_identifier(run_id, "run_id")
    spark = spark or SparkSession.builder.appName(f"IAM-{env}-consolidated").getOrCreate()

    source_frames: dict[str, DataFrame] = {}
    output_counts: dict[str, int] = {}
    try:
        for source in SOURCE_SCHEMAS:
            frame = load_source(spark, input_root, source).persist(StorageLevel.MEMORY_AND_DISK)
            source_frames[source] = frame
            LOGGER.info("Loaded %s %s rows", source, frame.count())

        if all(source_frames[source].rdd.isEmpty() for source in ("okta", "ad", "app_usage", "saviynt")):
            raise RuntimeError("No IAM activity files were found; refusing to replace output tables")

        detections = build_detections(source_frames, run_id)
        for table, frame in selected_tables(detections, requested_tables):
            cached = frame.persist(StorageLevel.MEMORY_AND_DISK)
            row_count = cached.count()
            output_counts[table] = row_count
            LOGGER.info("Detection %s produced %s rows", table, row_count)
            if not dry_run:
                write_bigquery(
                    cached,
                    project_id=bq_project,
                    dataset=bq_dataset,
                    table=table,
                    write_method=write_method,
                    temporary_gcs_bucket=temporary_gcs_bucket,
                )
            cached.unpersist()
    finally:
        for frame in source_frames.values():
            frame.unpersist()

    return output_counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=os.getenv("PIPELINE_ENV", "dev"))
    parser.add_argument("--gcs_bucket", help="Bucket containing source/<name>/*.parquet")
    parser.add_argument("--input_root", help="Override the full source root, including file://")
    parser.add_argument(
        "--bq_project",
        default=os.getenv("GOOGLE_CLOUD_PROJECT"),
        required=os.getenv("GOOGLE_CLOUD_PROJECT") is None,
    )
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--write_method", choices=("direct", "indirect"), default="indirect")
    parser.add_argument("--temporary_gcs_bucket")
    parser.add_argument("--tables", help="Optional comma-separated output-table allowlist")
    parser.add_argument("--dry_run", action="store_true")
    args = parser.parse_args()

    if not args.input_root:
        if not args.gcs_bucket:
            parser.error("one of --gcs_bucket or --input_root is required")
        args.input_root = f"gs://{args.gcs_bucket}/source"
    return args


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName(f"IAM-{args.env}-consolidated").getOrCreate()
    try:
        run(
            env=args.env,
            input_root=args.input_root,
            bq_project=args.bq_project,
            bq_dataset=args.bq_dataset,
            run_id=args.run_id,
            write_method=args.write_method,
            temporary_gcs_bucket=args.temporary_gcs_bucket,
            requested_tables=args.tables,
            dry_run=args.dry_run,
            spark=spark,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
