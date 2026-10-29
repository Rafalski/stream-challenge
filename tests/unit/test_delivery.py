from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from stream_challenge.ingest import consume_records
from stream_challenge.events import generate_events
from stream_challenge.producer import publish_events


def message(payload=b"original"):
    return SimpleNamespace(
        topic=lambda: "pageviews", partition=lambda: 0, offset=lambda: 0,
        value=lambda: payload, error=lambda: None,
    )


def test_file_exists_before_offset_is_committed(tmp_path):
    consumer = Mock()
    consumer.poll.return_value = message()

    def commit(**kwargs):
        assert (tmp_path / "pageviews" / "0" / "0.bin").read_bytes() == b"original"
        assert kwargs["asynchronous"] is False
        return []

    consumer.commit.side_effect = commit
    assert consume_records(consumer, "pageviews", tmp_path, max_records=1) == 1
    consumer.commit.assert_called_once()


def test_write_failure_does_not_commit_the_offset(tmp_path, monkeypatch):
    consumer = Mock()
    consumer.poll.return_value = message()
    monkeypatch.setattr("stream_challenge.ingest.archive_record", Mock(side_effect=OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        consume_records(consumer, "pageviews", tmp_path, max_records=1)
    consumer.commit.assert_not_called()


def test_commit_failure_leaves_a_replayable_file(tmp_path):
    consumer = Mock()
    consumer.poll.return_value = message()
    consumer.commit.side_effect = RuntimeError("commit failed")
    with pytest.raises(RuntimeError, match="commit failed"):
        consume_records(consumer, "pageviews", tmp_path, max_records=1)
    assert (tmp_path / "pageviews" / "0" / "0.bin").read_bytes() == b"original"


def test_missing_input_reaches_an_observable_deadline(tmp_path):
    consumer = Mock()
    consumer.poll.return_value = None
    with pytest.raises(TimeoutError):
        consume_records(consumer, "pageviews", tmp_path, max_records=1, idle_timeout=0.001)
    consumer.commit.assert_not_called()


@pytest.mark.parametrize("delivery_error", [None, RuntimeError("broker rejected record")])
def test_producer_requires_a_successful_delivery_report(delivery_error):
    producer = Mock()
    producer.produce.side_effect = lambda **kwargs: kwargs["on_delivery"](delivery_error, message())
    producer.flush.return_value = 0
    event = {"user_id": 1234, "postcode": "SW19", "webpage": "www.website.com/index.html", "timestamp": 1611662684}
    if delivery_error:
        with pytest.raises(RuntimeError, match="broker rejected record"):
            publish_events(producer, "pageviews", [event])
    else:
        assert publish_events(producer, "pageviews", [event]) == 1


def test_undelivered_messages_are_not_reported_as_success():
    producer = Mock()
    producer.flush.return_value = 1
    event = {"user_id": 1, "postcode": "SW19", "webpage": "www.website.com/index.html", "timestamp": 1611662684}
    with pytest.raises(TimeoutError):
        publish_events(producer, "pageviews", [event])


def test_producer_duration_limit_prevents_the_next_send(monkeypatch):
    now = [0]
    monkeypatch.setattr("stream_challenge.producer.time.monotonic", lambda: now[0])
    producer = Mock()

    def deliver(**kwargs):
        kwargs["on_delivery"](None, message())
        now[0] += 1

    producer.produce.side_effect = deliver
    producer.flush.return_value = 0
    assert publish_events(producer, "pageviews", generate_events(3), max_duration=1) == 1
    producer.produce.assert_called_once()
