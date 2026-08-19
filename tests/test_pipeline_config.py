import csv
from pathlib import Path

from pipeline_config import (
    KNOWN_SOURCES,
    SOURCE_FILES,
    SOURCE_REQUIRED_COLUMNS,
    safe_identifier,
)

ROOT = Path(__file__).resolve().parents[1]


def test_source_contract_includes_every_repository_dataset() -> None:
    assert KNOWN_SOURCES == {
        "okta",
        "ad",
        "app_usage",
        "saviynt",
        "hrlifecycle",
        "anomaly_key",
    }
    assert SOURCE_FILES["app_usage"] == "app_usage.csv"

    for source, filename in SOURCE_FILES.items():
        with (ROOT / "data" / filename).open(newline="", encoding="utf-8") as source_file:
            columns = set(csv.DictReader(source_file).fieldnames or ())
        assert set(SOURCE_REQUIRED_COLUMNS[source]).issubset(columns)


def test_safe_identifier_removes_cloud_unsafe_characters() -> None:
    assert safe_identifier(" run/with spaces ") == "run-with-spaces"
    assert safe_identifier("") == "manual"
    assert len(safe_identifier("x" * 100)) == 63
