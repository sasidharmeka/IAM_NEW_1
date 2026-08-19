from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/run-pipeline.yaml"


def test_repository_configuration_files_parse() -> None:
    assert yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))

    lifecycle = json.loads(
        (ROOT / "config/gcs-lifecycle.json").read_text(encoding="utf-8")
    )
    assert lifecycle == {
        "rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]
    }


def test_only_one_manual_cloud_workflow_remains() -> None:
    workflow_files = sorted((ROOT / ".github/workflows").glob("*.yaml"))
    assert workflow_files == [WORKFLOW]

    workflow = WORKFLOW.read_text(encoding="utf-8")
    lowered = workflow.lower()
    assert "workflow_dispatch:" in workflow
    assert "push:" not in lowered
    assert "schedule:" not in lowered
    assert "default: validate-only" in workflow


def test_cloud_execution_is_bounded_and_supports_both_auth_paths() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    lowered = workflow.lower()

    assert workflow.count("gcloud dataproc batches submit pyspark") == 1
    assert "--ttl=30m" in workflow
    assert "spark.dynamicAllocation.maxExecutors=2" in workflow
    assert '--staging-bucket="${BUCKET_NAME}"' in workflow
    assert "--soft-delete-duration=0s" in workflow
    assert "timeout-minutes: 45" in workflow
    assert "workload_identity_provider" in workflow
    assert "credentials_json" in workflow
    assert "gcp_sa_key" in lowered
    assert "composer environments" not in lowered
    assert "run services" not in lowered
    assert "workflows deploy" not in lowered


def test_retired_orchestration_and_generators_are_removed() -> None:
    for retired_directory in ("airflow_job", "generator_code", "infra", "variables"):
        retired_path = ROOT / retired_directory
        assert not retired_path.exists() or not any(
            path.is_file() for path in retired_path.rglob("*")
        )
