"""Generate and check the four-field pageview events."""

import itertools
import json
import time
from collections.abc import Iterator


FIELD_TYPES = {"user_id": int, "postcode": str, "webpage": str, "timestamp": int}


def validate_event(event: object) -> None:
    """Check required field types without changing the event."""
    if not isinstance(event, dict):
        raise ValueError("not_an_object")

    for field, expected_type in FIELD_TYPES.items():
        if field not in event:
            raise ValueError(f"missing_field:{field}")
        # Python calls booleans integers, but the event contract doesn't.
        if type(event[field]) is not expected_type:
            raise ValueError(f"wrong_type:{field}")


def serialize_event(event: dict) -> bytes:
    """Encode a generated pageview as compact UTF-8 JSON."""
    validate_event(event)
    return json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def generate_events(count: int | None, start_timestamp: int | None = None) -> Iterator[dict]:
    """Yield sample visits, or keep going when count is None."""
    if count is not None and count < 1:
        raise ValueError("count must be positive")

    # A 3:2:1 mix gives the live demo visibly different postcode counts.
    postcodes = ("SW19", "SW19", "SW19", "SW1A", "SW1A", "M1")
    webpages = ("index.html", "about.html", "contact.html")

    for index in itertools.count():
        if count is not None and index >= count:
            return
        yield {
            "user_id": 1234 + index % 10,
            "postcode": postcodes[index % len(postcodes)],
            "webpage": f"www.website.com/{webpages[index % len(webpages)]}",
            "timestamp": int(time.time()) if start_timestamp is None else start_timestamp + index,
        }
