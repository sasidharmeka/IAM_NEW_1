from __future__ import annotations

from types import SimpleNamespace

import pytest

from spark_job.spark_transformation_job import (
    validate_bigquery_dataset,
    write_bigquery,
)


class FakeWriter:
    def __init__(self) -> None:
        self.options: dict[str, str] = {}
        self.format_name = ""
        self.mode_name = ""
        self.saved = False

    def format(self, name: str) -> FakeWriter:
        self.format_name = name
        return self

    def option(self, name: str, value: str) -> FakeWriter:
        self.options[name] = value
        return self

    def mode(self, name: str) -> FakeWriter:
        self.mode_name = name
        return self

    def save(self) -> None:
        self.saved = True


def fake_frame() -> tuple[SimpleNamespace, FakeWriter]:
    writer = FakeWriter()
    return SimpleNamespace(columns=["event_date"], write=writer), writer


def test_indirect_write_creates_partitioned_truncated_table() -> None:
    frame, writer = fake_frame()

    write_bigquery(
        frame,
        project_id="project",
        dataset="iam_data_dev",
        table="risk_scores",
        write_method="indirect",
        temporary_gcs_bucket="bucket",
    )

    assert writer.format_name == "bigquery"
    assert writer.mode_name == "overwrite"
    assert writer.saved
    assert writer.options["writeDisposition"] == "WRITE_TRUNCATE"
    assert writer.options["partitionField"] == "event_date"
    assert writer.options["partitionType"] == "DAY"
    assert writer.options["temporaryGcsBucket"] == "bucket"


def test_direct_write_does_not_request_unsupported_partition_creation() -> None:
    frame, writer = fake_frame()

    write_bigquery(
        frame,
        project_id="project",
        dataset="iam_data_dev",
        table="risk_scores",
        write_method="direct",
        temporary_gcs_bucket=None,
    )

    assert "partitionField" not in writer.options
    assert "temporaryGcsBucket" not in writer.options
    assert "writeDisposition" not in writer.options


def test_bigquery_dataset_validation() -> None:
    assert validate_bigquery_dataset("iam_data_dev") == "iam_data_dev"
    with pytest.raises(ValueError, match="letters, numbers, and underscores"):
        validate_bigquery_dataset("iam-data-dev")
