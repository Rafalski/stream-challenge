import json
import os
import subprocess
import sys
import uuid
from types import SimpleNamespace

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_INVALID
from confluent_kafka.admin import AdminClient, NewTopic

from stream_challenge.ingest import consume_records


pytestmark = pytest.mark.integration


@pytest.fixture
def kafka_topic():
    topic = f"test-{uuid.uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
    try:
        admin.list_topics(timeout=5)
    except Exception as error:
        pytest.fail(f"Start Kafka using the README instructions before running integration tests: {error}")
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(timeout=15)
    resources = SimpleNamespace(topic=topic, groups=[])
    try:
        yield resources
    finally:
        # Only remove resources this test created, even if an assertion failed.
        try:
            if resources.groups:
                for future in admin.delete_consumer_groups(resources.groups).values():
                    future.result(timeout=15)
        finally:
            admin.delete_topics([topic])[topic].result(timeout=15)


def new_consumer(resources, suffix="raw"):
    group = f"{resources.topic}-{suffix}"
    if group not in resources.groups:
        resources.groups.append(group)
    return Consumer({
        "bootstrap.servers": "127.0.0.1:9092", "group.id": group,
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })


def publish_payloads(topic, payloads):
    producer = Producer({"bootstrap.servers": "127.0.0.1:9092", "delivery.timeout.ms": 10000})
    errors = []
    for payload in payloads:
        producer.produce(topic, value=payload, on_delivery=lambda error, message: errors.append(error))
    assert producer.flush(15) == 0
    assert errors == [None] * len(payloads)


def run_cli(module, *arguments):
    """Exercise the real command with machine-readable logs for assertions."""
    result = subprocess.run(
        [sys.executable, "-m", module, *arguments], capture_output=True, text=True,
        timeout=30, env={**os.environ, "LOG_FORMAT": "json"},
    )
    assert result.returncode == 0, result.stderr
    for line in result.stderr.splitlines():
        assert isinstance(json.loads(line), dict)
    return result


def test_real_producer_and_raw_ingestion_commands(kafka_topic, tmp_path):
    run_cli("stream_challenge.producer", "--topic", kafka_topic.topic, "--count", "1",
            "--start-timestamp", "1611662684", "--interval", "0")
    group = f"{kafka_topic.topic}-cli"
    kafka_topic.groups.append(group)
    run_cli("stream_challenge.ingest", "--topic", kafka_topic.topic, "--group-id", group,
            "--output-dir", str(tmp_path), "--max-records", "1", "--idle-timeout", "15")
    assert (tmp_path / kafka_topic.topic / "0" / "0.bin").read_bytes() == (
        b'{"user_id":1234,"postcode":"SW19","webpage":"www.website.com/index.html","timestamp":1611662684}'
    )


def test_raw_bytes_nulls_and_repeated_values_are_preserved(kafka_topic, tmp_path):
    payloads = [b' {"postcode": "SW19"}\n', b"invalid JSON\n\n", b"\xff\x00", b"", None, b"repeat", b"repeat"]
    publish_payloads(kafka_topic.topic, payloads)
    consumer = new_consumer(kafka_topic)
    try:
        assert consume_records(consumer, kafka_topic.topic, tmp_path, len(payloads), 15) == len(payloads)
        assert consumer.committed([TopicPartition(kafka_topic.topic, 0)], timeout=5)[0].offset == len(payloads)
    finally:
        consumer.close()
    for offset, payload in enumerate(payloads):
        suffix = ".null" if payload is None else ".bin"
        assert (tmp_path / kafka_topic.topic / "0" / f"{offset}{suffix}").read_bytes() == (b"" if payload is None else payload)
    assert len(list(tmp_path.rglob("*.bin"))) == len(payloads) - 1


def test_write_failure_leaves_offset_available_for_retry(kafka_topic, tmp_path):
    publish_payloads(kafka_topic.topic, [b"original"])
    blocked = tmp_path / "blocked"
    blocked.write_text("This is a file, so it cannot hold an archive directory.")
    consumer = new_consumer(kafka_topic)
    try:
        with pytest.raises(OSError):
            consume_records(consumer, kafka_topic.topic, blocked, 1, 15)
        assert consumer.committed([TopicPartition(kafka_topic.topic, 0)], timeout=5)[0].offset == OFFSET_INVALID
    finally:
        consumer.close()

    retry = new_consumer(kafka_topic)
    try:
        assert consume_records(retry, kafka_topic.topic, tmp_path / "retry", 1, 15) == 1
    finally:
        retry.close()
    assert (tmp_path / "retry" / kafka_topic.topic / "0" / "0.bin").read_bytes() == b"original"
