"""Pull IAM events from Pub/Sub and persist durable, deduplicated Parquet files."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import pubsub_v1, storage

from pipeline_config import KNOWN_SOURCES, SOURCE_ID_COLUMNS, safe_identifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("iam-consumer")


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise OSError(f"{name} environment variable is required")
    return value


def subscription_path(subscriber: Any, project_id: str, subscription: str) -> str:
    if subscription.startswith("projects/"):
        return subscription
    return subscriber.subscription_path(project_id, subscription)


def pull_messages(
    *,
    subscriber: Any,
    sub_path: str,
    expected_environment: str,
    max_messages: int,
    pull_timeout: float,
    empty_pull_limit: int,
) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], list[str]]:
    """Pull and group messages without acknowledging them."""

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    ack_ids: list[str] = []
    empty_pulls = 0
    normalized_env = safe_identifier(expected_environment)

    while len(ack_ids) < max_messages and empty_pulls < empty_pull_limit:
        request_size = min(1_000, max_messages - len(ack_ids))
        try:
            response = subscriber.pull(
                subscription=sub_path,
                max_messages=request_size,
                timeout=pull_timeout,
            )
        except DeadlineExceeded:
            empty_pulls += 1
            continue

        if not response.received_messages:
            empty_pulls += 1
            continue

        empty_pulls = 0
        for received in response.received_messages:
            attributes = dict(received.message.attributes)
            source = attributes.get("source", "")
            message_env = safe_identifier(attributes.get("pipeline_env", normalized_env))
            run_id = safe_identifier(attributes.get("run_id", "unlabelled"))

            if source not in KNOWN_SOURCES:
                raise ValueError(
                    f"Message {received.message.message_id} has unknown source {source!r}"
                )
            if message_env != normalized_env:
                raise ValueError(
                    f"Message environment {message_env!r} does not match {normalized_env!r}"
                )

            payload = json.loads(received.message.data.decode("utf-8"))
            payload["_pipeline_run_id"] = run_id
            payload["_pubsub_message_id"] = received.message.message_id
            payload["_ingested_at"] = datetime.now(UTC).isoformat()
            grouped[(source, run_id)].append(payload)
            ack_ids.append(received.ack_id)

    return dict(grouped), ack_ids


def dataframe_for_source(source: str, rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    keys = [column for column in SOURCE_ID_COLUMNS[source] if column in frame.columns]
    if keys:
        frame = frame.drop_duplicates(subset=keys, keep="last")
    return frame.reset_index(drop=True)


def upload_parquet(
    *,
    storage_client: Any,
    bucket_name: str,
    source: str,
    run_id: str,
    rows: list[dict[str, Any]],
    row_group_size: int,
) -> str:
    """Write one source/run group to Parquet and return its GCS object name."""

    frame = dataframe_for_source(source, rows)
    date_prefix = datetime.now(UTC).strftime("%Y%m%d")
    batch_id = uuid.uuid4().hex[:12]
    object_name = (
        f"source/{source}/{date_prefix}-{safe_identifier(run_id)}-{batch_id}.parquet"
    )
    temporary_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as temporary:
            temporary_path = Path(temporary.name)

        table = pa.Table.from_pandas(frame, preserve_index=False)
        pq.write_table(table, temporary_path, row_group_size=row_group_size)
        bucket = storage_client.bucket(bucket_name)
        bucket.blob(object_name).upload_from_filename(str(temporary_path))
    finally:
        if temporary_path and temporary_path.exists():
            temporary_path.unlink()

    LOGGER.info("Uploaded %s rows to gs://%s/%s", len(frame), bucket_name, object_name)
    return object_name


def consume_to_gcs(
    *,
    project_id: str,
    subscription: str,
    bucket_name: str,
    pipeline_env: str,
    max_messages: int = 50_000,
    pull_timeout: float = 10,
    empty_pull_limit: int = 3,
    row_group_size: int = 100_000,
    subscriber: Any | None = None,
    storage_client: Any | None = None,
) -> list[str]:
    owns_subscriber = subscriber is None
    subscriber = subscriber or pubsub_v1.SubscriberClient()
    storage_client = storage_client or storage.Client()
    try:
        sub_path = subscription_path(subscriber, project_id, subscription)

        grouped, ack_ids = pull_messages(
            subscriber=subscriber,
            sub_path=sub_path,
            expected_environment=pipeline_env,
            max_messages=max_messages,
            pull_timeout=pull_timeout,
            empty_pull_limit=empty_pull_limit,
        )
        if not ack_ids:
            LOGGER.info("No messages available on %s", sub_path)
            return []

        uploaded = [
            upload_parquet(
                storage_client=storage_client,
                bucket_name=bucket_name,
                source=source,
                run_id=run_id,
                rows=rows,
                row_group_size=row_group_size,
            )
            for (source, run_id), rows in sorted(grouped.items())
        ]

        subscriber.acknowledge(subscription=sub_path, ack_ids=ack_ids)
        LOGGER.info(
            "Acknowledged %s messages after %s successful uploads",
            len(ack_ids),
            len(uploaded),
        )
        return uploaded
    finally:
        if owns_subscriber:
            subscriber.close()


def main() -> None:
    consume_to_gcs(
        project_id=required_env("PROJECT_ID"),
        subscription=required_env("PUBSUB_SUBSCRIPTION"),
        bucket_name=required_env("BUCKET_NAME"),
        pipeline_env=os.getenv("PIPELINE_ENV", "dev"),
        max_messages=int(os.getenv("MAX_MESSAGES", "50000")),
        pull_timeout=float(os.getenv("PULL_TIMEOUT", "10")),
        empty_pull_limit=int(os.getenv("EMPTY_PULL_LIMIT", "3")),
        row_group_size=int(os.getenv("PARQUET_ROW_GROUP", "100000")),
    )


if __name__ == "__main__":
    main()
