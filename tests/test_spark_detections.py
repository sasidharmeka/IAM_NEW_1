from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from spark_job.detections import build_detections
from spark_job.schemas import SOURCE_ID_COLUMNS, SOURCE_SCHEMAS

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_CSVS = {
    "okta": "okta_logins.csv",
    "ad": "ad_group_events.csv",
    "app_usage": "app_usage.csv",
    "saviynt": "saviynt_requests.csv",
    "hrlifecycle": "hr_lifecycle.csv",
    "anomaly_key": "anomaly_key.csv",
}
EXPECTED_OUTPUTS = {
    "impossible_login_anomalies",
    "mfa_bypass_events",
    "mfa_bypass_events_all_apps",
    "impossible_travel_events",
    "privilege_escalation_without_request",
    "ad_volume_spikes",
    "rejected_but_executed",
    "high_risk_sequence",
    "excessive_data_transferred",
    "role_drift",
    "stale_access",
    "multi_app_single_session",
    "mfa_drift",
    "suspicious_approver_behavior",
    "ad_group_churn_rate",
    "orphan_event_detection",
    "orphan_user_detection",
    "cleanest_users",
    "shadow_access_events",
    "risk_scores",
    "identity_timelines",
    "suspicious_time_warp",
}


def source_frames(spark: SparkSession) -> dict[str, DataFrame]:
    frames: dict[str, DataFrame] = {}
    for source, filename in SOURCE_CSVS.items():
        frame = (
            spark.read.option("header", True)
            .schema(SOURCE_SCHEMAS[source])
            .csv(str(REPOSITORY_ROOT / "data" / filename))
            .dropDuplicates(list(SOURCE_ID_COLUMNS[source]))
            .cache()
        )
        frame.count()
        frames[source] = frame
    return frames


def test_all_detections_execute_against_repository_data(spark: SparkSession) -> None:
    frames = source_frames(spark)
    try:
        detections = build_detections(frames, "test-run")
        assert set(detections) == EXPECTED_OUTPUTS

        counts: dict[str, int] = {}
        for name, frame in detections.items():
            assert {
                "detection_name",
                "finding_id",
                "event_date",
                "pipeline_run_id",
                "detected_at",
            }.issubset(frame.columns)
            cached = frame.cache()
            counts[name] = cached.count()
            assert cached.filter("finding_id IS NULL").count() == 0
            assert cached.select("finding_id").distinct().count() == counts[name]
            cached.unpersist()

        expected_timeline_rows = sum(
            frames[source].count() for source in ("okta", "saviynt", "ad", "app_usage")
        )
        assert counts["identity_timelines"] == expected_timeline_rows
        assert counts["risk_scores"] == 50
        assert counts["cleanest_users"] == 10
    finally:
        for frame in frames.values():
            frame.unpersist()
