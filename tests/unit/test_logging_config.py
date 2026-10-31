"""Keep structured diagnostics when a runtime has installed its own handler."""

import io
import json
import logging

from stream_challenge.logging_config import configure_logging


def test_existing_handler_keeps_diagnostic_fields(monkeypatch):
    """Flink's forwarding handler needs our formatter too."""
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [handler])
    monkeypatch.setattr(root, "level", logging.WARNING)
    monkeypatch.setenv("LOG_FORMAT", "json")
    configure_logging()

    logging.getLogger("aggregation").warning("invalid_event", extra={"details": {
        "reason": "invalid_json", "topic": "test", "partition": 0, "offset": 3,
    }})
    entry = json.loads(output.getvalue())
    assert entry["event"] == "invalid_event"
    assert entry["reason"] == "invalid_json"
    assert entry["offset"] == 3
    assert "\033" not in output.getvalue()
    assert root.handlers == [handler]
