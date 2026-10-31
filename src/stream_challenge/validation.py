"""Small, explicit DQ checks for the aggregation input."""

import json
import time
from datetime import datetime, timedelta, timezone

from stream_challenge.events import FIELD_TYPES, validate_event


EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def reject_json_constant(value: str) -> None:
    """Python accepts NaN by default, but JSON does not."""
    raise ValueError("invalid_json")


def parse_pageview(payload: bytes | None, now: float | None = None,
                   max_future_seconds: int = 300) -> dict:
    """Return an unchanged valid event, or a safe rejection reason."""
    if payload is None:
        raise ValueError("null_payload")
    try:
        text = bytes(payload).decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("invalid_utf8") from None
    try:
        event = json.loads(text, parse_constant=reject_json_constant)
    except (ValueError, RecursionError):
        raise ValueError("invalid_json") from None

    # Check the source contract before looking at event time.
    validate_event(event)
    if event.keys() != FIELD_TYPES.keys():
        raise ValueError("unexpected_fields")
    for field in ("postcode", "webpage"):
        if not event[field].strip():
            raise ValueError(f"blank_field:{field}")

    timestamp = event["timestamp"]
    if not -62135596800 <= timestamp <= 253402300799:
        raise ValueError("timestamp_out_of_range")
    if timestamp > (time.time() if now is None else now) + max_future_seconds:
        raise ValueError("future_timestamp")
    return event


def utc_window_start(timestamp: int) -> str:
    """Format a UTC minute without platform-dependent epoch conversion."""
    start = EPOCH + timedelta(seconds=(timestamp // 60) * 60)
    return start.isoformat(timespec="seconds").replace("+00:00", "Z")
