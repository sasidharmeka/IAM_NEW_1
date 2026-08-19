"""Publish synthetic IAM source records to one environment-scoped Pub/Sub topic."""

from __future__ import annotations

import csv
import json
import logging
import os
from collections import Counter
from pathlib import Path
from typing import Any

from google.cloud import pubsub_v1

from pipeline_config import SOURCE_REQUIRED_COLUMNS, data_files, safe_identifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("iam-producer")


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise OSError(f"{name} environment variable is required")
    return value


def validate_source_files(data_dir: str | Path) -> list[tuple[str, Path]]:
    """Validate every configured CSV before publishing any messages."""

    configured_files = data_files(data_dir)
    missing_files = [str(path) for _, path in configured_files if not path.is_file()]
    if missing_files:
        raise FileNotFoundError(
            "Missing configured source files: " + ", ".join(sorted(missing_files))
        )

    for source, path in configured_files:
        with path.open(newline="", encoding="utf-8", errors="strict") as csv_file:
            reader = csv.DictReader(csv_file)
            actual_columns = set(reader.fieldnames or ())
            missing_columns = set(SOURCE_REQUIRED_COLUMNS[source]) - actual_columns
            if missing_columns:
                missing = ", ".join(sorted(missing_columns))
                raise ValueError(f"{path} is missing required columns: {missing}")

            for line_number, row in enumerate(reader, start=2):
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(f"{path}:{line_number} has a malformed CSV row")

    return configured_files


def publish_records(
    *,
    project_id: str,
    topic_id: str,
    data_dir: str | Path,
    pipeline_env: str,
    run_id: str,
    publisher: Any | None = None,
) -> dict[str, int]:
    """Publish every configured CSV row and wait for all publish operations.

    A single topic is sufficient because the source message attribute lets the
    consumer route each record. The environment and run ID make runs observable
    and prevent dev/prod subscriptions from being mixed.
    """

    publisher = publisher or pubsub_v1.PublisherClient(
        batch_settings=pubsub_v1.types.BatchSettings(
            max_bytes=1_000_000,
            max_latency=0.1,
            max_messages=1_000,
        )
    )
    topic_path = publisher.topic_path(project_id, topic_id)
    normalized_env = safe_identifier(pipeline_env)
    normalized_run_id = safe_identifier(run_id)
    futures: list[tuple[str, Any]] = []
    counts: Counter[str] = Counter()

    for source, path in validate_source_files(data_dir):
        with path.open(newline="", encoding="utf-8", errors="strict") as csv_file:
            for row in csv.DictReader(csv_file):
                payload = json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode(
                    "utf-8"
                )
                future = publisher.publish(
                    topic_path,
                    data=payload,
                    source=source,
                    pipeline_env=normalized_env,
                    run_id=normalized_run_id,
                )
                futures.append((source, future))
                counts[source] += 1

    for source, future in futures:
        try:
            future.result(timeout=60)
        except Exception:
            LOGGER.exception("Failed to publish a %s record", source)
            raise

    LOGGER.info("Published %s messages to %s", sum(counts.values()), topic_path)
    for source in sorted(counts):
        LOGGER.info("  %s: %s", source, counts[source])
    return dict(counts)


def main() -> None:
    publish_records(
        project_id=required_env("PROJECT_ID"),
        topic_id=os.getenv("PUBSUB_TOPIC", "iam-events"),
        data_dir=os.getenv("DATA_DIR", "data"),
        pipeline_env=os.getenv("PIPELINE_ENV", "dev"),
        run_id=os.getenv("PIPELINE_RUN_ID", "manual"),
    )


if __name__ == "__main__":
    main()
