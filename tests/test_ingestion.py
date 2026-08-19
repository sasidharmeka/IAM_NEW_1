from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from consumer import consume_to_gcs
from pipeline_config import SOURCE_FILES, SOURCE_REQUIRED_COLUMNS
from producer import publish_records


class FakeFuture:
    def result(self, timeout: int) -> str:
        assert timeout == 60
        return "message-id"


class FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str]]] = []

    def topic_path(self, project_id: str, topic_id: str) -> str:
        return f"projects/{project_id}/topics/{topic_id}"

    def publish(self, topic: str, data: bytes, **attributes: str) -> FakeFuture:
        self.published.append((topic, data, attributes))
        return FakeFuture()


def write_valid_sources(data_dir: Path) -> None:
    for source, filename in SOURCE_FILES.items():
        columns = SOURCE_REQUIRED_COLUMNS[source]
        header = ",".join(columns)
        values = ",".join(f"{source}-{column}" for column in columns)
        (data_dir / filename).write_text(f"{header}\n{values}\n", encoding="utf-8")


def test_producer_routes_all_sources_over_one_topic(tmp_path: Path) -> None:
    write_valid_sources(tmp_path)
    publisher = FakePublisher()

    counts = publish_records(
        project_id="project",
        topic_id="iam-events-dev",
        data_dir=tmp_path,
        pipeline_env="dev",
        run_id="run 1",
        publisher=publisher,
    )

    assert counts == {source: 1 for source in SOURCE_FILES}
    assert {item[2]["source"] for item in publisher.published} == set(SOURCE_FILES)
    assert {item[2]["run_id"] for item in publisher.published} == {"run-1"}


def test_producer_rejects_missing_input_before_publishing(tmp_path: Path) -> None:
    write_valid_sources(tmp_path)
    (tmp_path / SOURCE_FILES["okta"]).unlink()
    publisher = FakePublisher()

    with pytest.raises(FileNotFoundError, match="okta_logins.csv"):
        publish_records(
            project_id="project",
            topic_id="iam-events-dev",
            data_dir=tmp_path,
            pipeline_env="dev",
            run_id="run-1",
            publisher=publisher,
        )

    assert publisher.published == []


class FakeSubscriber:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls = 0
        self.acknowledged: list[str] = []

    def subscription_path(self, project: str, subscription: str) -> str:
        return f"projects/{project}/subscriptions/{subscription}"

    def pull(self, **_: object) -> SimpleNamespace:
        self.calls += 1
        if self.calls > 1:
            return SimpleNamespace(received_messages=[])

        def received(ack_id: str, message_id: str) -> SimpleNamespace:
            message = SimpleNamespace(
                attributes={
                    "source": "okta",
                    "pipeline_env": "dev",
                    "run_id": "run-1",
                },
                data=json.dumps(
                    {"req_id": "same-id", "user": "user1", "ts": "2026-01-01 00:00:00"}
                ).encode(),
                message_id=message_id,
            )
            return SimpleNamespace(ack_id=ack_id, message=message)

        return SimpleNamespace(
            received_messages=[
                received("ack-1", "message-1"),
                received("ack-2", "message-2"),
            ]
        )

    def acknowledge(self, *, subscription: str, ack_ids: list[str]) -> None:
        self.events.append("ack")
        self.acknowledged = ack_ids


class FakeBlob:
    def __init__(self, events: list[str], uploads: dict[str, int], name: str) -> None:
        self.events = events
        self.uploads = uploads
        self.name = name

    def upload_from_filename(self, filename: str) -> None:
        self.events.append("upload")
        self.uploads[self.name] = Path(filename).stat().st_size


class FailingBlob(FakeBlob):
    def upload_from_filename(self, filename: str) -> None:
        raise RuntimeError("simulated upload failure")


class FakeBucket:
    def __init__(self, events: list[str], uploads: dict[str, int]) -> None:
        self.events = events
        self.uploads = uploads

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self.events, self.uploads, name)


class FailingBucket(FakeBucket):
    def blob(self, name: str) -> FakeBlob:
        return FailingBlob(self.events, self.uploads, name)


class FakeStorage:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.uploads: dict[str, int] = {}

    def bucket(self, _: str) -> FakeBucket:
        return FakeBucket(self.events, self.uploads)


class FailingStorage(FakeStorage):
    def bucket(self, _: str) -> FakeBucket:
        return FailingBucket(self.events, self.uploads)


def test_consumer_deduplicates_and_acks_after_upload() -> None:
    events: list[str] = []
    subscriber = FakeSubscriber(events)
    storage = FakeStorage(events)

    uploaded = consume_to_gcs(
        project_id="project",
        subscription="iam-events-dev-pipeline",
        bucket_name="bucket",
        pipeline_env="dev",
        empty_pull_limit=1,
        subscriber=subscriber,
        storage_client=storage,
    )

    assert len(uploaded) == 1
    assert uploaded[0].startswith("source/okta/")
    assert storage.uploads[uploaded[0]] > 0
    assert subscriber.acknowledged == ["ack-1", "ack-2"]
    assert events == ["upload", "ack"]


def test_consumer_does_not_ack_when_upload_fails() -> None:
    events: list[str] = []
    subscriber = FakeSubscriber(events)

    try:
        consume_to_gcs(
            project_id="project",
            subscription="iam-events-dev-pipeline",
            bucket_name="bucket",
            pipeline_env="dev",
            empty_pull_limit=1,
            subscriber=subscriber,
            storage_client=FailingStorage(events),
        )
    except RuntimeError as exc:
        assert str(exc) == "simulated upload failure"
    else:
        raise AssertionError("Expected the simulated upload to fail")

    assert subscriber.acknowledged == []
    assert "ack" not in events
