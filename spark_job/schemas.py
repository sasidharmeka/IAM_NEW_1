"""Source schemas and loading contracts for IAM Parquet data."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


def string_schema(*columns: str) -> StructType:
    return StructType([StructField(column, StringType(), True) for column in columns])


SOURCE_SCHEMAS: dict[str, StructType] = {
    "okta": string_schema(
        "req_id",
        "user",
        "ts",
        "device",
        "ip",
        "country",
        "mfa",
        "status",
        "session_id",
        "_pipeline_run_id",
        "_pubsub_message_id",
        "_ingested_at",
    ),
    "ad": string_schema(
        "event_id",
        "ts",
        "user",
        "group",
        "action",
        "initiator",
        "ip",
        "request_id",
        "_pipeline_run_id",
        "_pubsub_message_id",
        "_ingested_at",
    ),
    "app_usage": string_schema(
        "usage_id",
        "ts",
        "user",
        "app",
        "action",
        "duration",
        "data_mb",
        "session_id",
        "request_id",
        "_pipeline_run_id",
        "_pubsub_message_id",
        "_ingested_at",
    ),
    "saviynt": string_schema(
        "req_id",
        "user",
        "ts",
        "app",
        "role",
        "status",
        "approver",
        "justification",
        "_pipeline_run_id",
        "_pubsub_message_id",
        "_ingested_at",
    ),
    "hrlifecycle": string_schema(
        "user",
        "join_date",
        "move_date1",
        "move_date2",
        "termination_date",
        "department",
        "job_role",
        "location",
        "_pipeline_run_id",
        "_pubsub_message_id",
        "_ingested_at",
    ),
    "anomaly_key": string_schema(
        "user",
        "anomaly_description",
        "_pipeline_run_id",
        "_pubsub_message_id",
        "_ingested_at",
    ),
}

SOURCE_ID_COLUMNS: dict[str, tuple[str, ...]] = {
    "okta": ("req_id",),
    "ad": ("event_id",),
    "app_usage": ("usage_id",),
    "saviynt": ("req_id",),
    "hrlifecycle": ("user", "join_date"),
    "anomaly_key": ("user", "anomaly_description"),
}
