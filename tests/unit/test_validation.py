"""Check DQ policy without starting a JVM or Docker."""

import json

import pytest

from stream_challenge.validation import parse_pageview, utc_window_start


SAMPLE = {"user_id": 1234, "postcode": "SW19", "webpage": "www.website.com/index.html",
          "timestamp": 1611662684}


def test_original_sample_is_accepted_without_changes():
    """Historical timestamps and the supplied postcode/URL remain valid."""
    assert parse_pageview(json.dumps(SAMPLE).encode(), now=1700000000) == SAMPLE
    assert utc_window_start(1611662684) == "2021-01-26T12:04:00Z"


@pytest.mark.parametrize("payload, reason", [
    (None, "null_payload"), (b"\xff", "invalid_utf8"), (b"{", "invalid_json"),
    (b"[]", "not_an_object"), (b'{"timestamp": NaN}', "invalid_json"),
])
def test_unreadable_payloads_have_safe_reasons(payload, reason):
    """Reasons never contain the original data."""
    with pytest.raises(ValueError, match=f"^{reason}$"):
        parse_pageview(payload, now=1700000000)


@pytest.mark.parametrize("changes, reason", [
    ({"postcode": "  "}, "blank_field:postcode"),
    ({"webpage": ""}, "blank_field:webpage"),
    ({"user_id": True}, "wrong_type:user_id"),
    ({"timestamp": 1611662684000}, "timestamp_out_of_range"),
    ({"timestamp": 1700000301}, "future_timestamp"),
    ({"extra": 1}, "unexpected_fields"),
])
def test_invalid_fields_are_rejected(changes, reason):
    """No coercion or silent removal of fields is allowed."""
    with pytest.raises(ValueError, match=f"^{reason}$"):
        parse_pageview(json.dumps({**SAMPLE, **changes}).encode(), now=1700000000)


def test_timestamp_boundary_and_nonblank_strings_are_preserved():
    """The future allowance includes its boundary; strings are not trimmed."""
    event = {**SAMPLE, "timestamp": 1700000300, "postcode": " SW19 "}
    assert parse_pageview(json.dumps(event).encode(), now=1700000000) == event
    assert utc_window_start(59) == "1970-01-01T00:00:00Z"
    assert utc_window_start(60) == "1970-01-01T00:01:00Z"
    assert utc_window_start(-1) == "1969-12-31T23:59:00Z"
