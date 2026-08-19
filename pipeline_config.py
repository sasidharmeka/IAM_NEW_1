"""Shared configuration for the IAM event-ingestion pipeline."""

from __future__ import annotations

import re
from pathlib import Path

SOURCE_FILES: dict[str, str] = {
    "okta": "okta_logins.csv",
    "ad": "ad_group_events.csv",
    "app_usage": "app_usage.csv",
    "saviynt": "saviynt_requests.csv",
    "hrlifecycle": "hr_lifecycle.csv",
    "anomaly_key": "anomaly_key.csv",
}

SOURCE_ID_COLUMNS: dict[str, tuple[str, ...]] = {
    "okta": ("req_id",),
    "ad": ("event_id",),
    "app_usage": ("usage_id",),
    "saviynt": ("req_id",),
    "hrlifecycle": ("user", "join_date"),
    "anomaly_key": ("user", "anomaly_description"),
}

SOURCE_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "okta": (
        "req_id",
        "user",
        "ts",
        "device",
        "ip",
        "country",
        "mfa",
        "status",
        "session_id",
    ),
    "ad": (
        "event_id",
        "ts",
        "user",
        "group",
        "action",
        "initiator",
        "ip",
        "request_id",
    ),
    "app_usage": (
        "usage_id",
        "ts",
        "user",
        "app",
        "action",
        "duration",
        "data_mb",
        "session_id",
        "request_id",
    ),
    "saviynt": (
        "req_id",
        "user",
        "ts",
        "app",
        "role",
        "status",
        "approver",
        "justification",
    ),
    "hrlifecycle": (
        "user",
        "join_date",
        "move_date1",
        "move_date2",
        "termination_date",
        "department",
        "job_role",
        "location",
    ),
    "anomaly_key": ("user", "anomaly_description"),
}

KNOWN_SOURCES = frozenset(SOURCE_FILES)


def data_files(data_dir: str | Path) -> list[tuple[str, Path]]:
    """Return configured source files in deterministic order."""

    root = Path(data_dir)
    return [(source, root / filename) for source, filename in SOURCE_FILES.items()]


def safe_identifier(value: str, *, fallback: str = "manual") -> str:
    """Convert a run/environment identifier into a safe GCP object-name segment."""

    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip()).strip("-_")
    return normalized[:63] or fallback
