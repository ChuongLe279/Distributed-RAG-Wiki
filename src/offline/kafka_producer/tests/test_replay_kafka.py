"""Test tích hợp producer: replay fixture vào Kafka thật rồi đọc lại."""
from collections import defaultdict
import json
from pathlib import Path
import uuid

import pytest

from common import config
from producer import wiki_kafka_producer as wp

pytestmark = pytest.mark.integration
FIXTURE = Path(__file__).parent / "fixtures" / "replay_sample.jsonl"


def _consume_all(topic: str, expected: int, timeout: float = 30.0) -> list:
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": f"test-{uuid.uuid4().hex}",
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    records, idle = [], 0.0
    try:
        while len(records) < expected and idle < timeout:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1.0
                continue
            assert message.error() is None, message.error()
            records.append(message)
            idle = 0.0
        extra = consumer.poll(2.0)  # không được có message thừa
        assert extra is None
    finally:
        consumer.close()
    return records


@pytest.fixture
def temp_topics(monkeypatch):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    suffix = uuid.uuid4().hex[:8]
    names = {"TOPIC_NEW": f"test_new_{suffix}", "TOPIC_UPDATED": f"test_updated_{suffix}"}
    for future in admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1) for name in names.values()]).values():
        future.result(timeout=30)
    for attribute, name in names.items():
        monkeypatch.setattr(config, attribute, name)
    yield names
    for future in admin.delete_topics(list(names.values())).values():
        future.result(timeout=30)


def test_compose_topics_exist_with_three_partitions():
    from confluent_kafka.admin import AdminClient

    topics = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP}).list_topics(timeout=15).topics
    for name in ("new_article", "updated_article", "article_dlq"):
        assert name in topics, f"thiếu topic {name}"
        assert len(topics[name].partitions) == 3


def test_replay_reaches_kafka_in_order_per_article(temp_topics):
    expected = list(wp.read_replay(FIXTURE))
    publisher = wp.Publisher(wp.make_kafka_producer())
    wp.run_replay(FIXTURE, publisher)
    assert (publisher.sent, publisher.failed, publisher.skipped) == (len(expected), 0, 0)

    new_expected = [m for m in expected if m["event_type"] == "NEW"]
    updated_expected = [m for m in expected if m["event_type"] == "UPDATED"]
    new_records = _consume_all(temp_topics["TOPIC_NEW"], len(new_expected))
    updated_records = _consume_all(temp_topics["TOPIC_UPDATED"], len(updated_expected))
    assert len(new_records) == len(new_expected)
    assert len(updated_records) == len(updated_expected)

    per_key = defaultdict(list)
    for record in new_records + updated_records:
        body = json.loads(record.value())
        assert record.key().decode() == str(body["article_id"])      # key = article_id
        assert body["source"] == "replay"
        per_key[(record.topic(), record.partition(), body["article_id"])].append((record.offset(), body["revision_id"]))
    # Cùng một bài luôn rơi vào cùng partition của topic, và giữ thứ tự revision.
    for entries in per_key.values():
        revisions = [revision for _, revision in sorted(entries)]
        assert revisions == sorted(revisions)
    partitions = defaultdict(set)
    for topic, partition, article_id in per_key:
        partitions[(topic, article_id)].add(partition)
    assert all(len(found) == 1 for found in partitions.values())

    sent = sorted((m["article_id"], m["revision_id"]) for m in expected)
    received = sorted((json.loads(r.value())["article_id"], json.loads(r.value())["revision_id"]) for r in new_records + updated_records)
    assert received == sent
