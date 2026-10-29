import json

import pytest

from stream_challenge.events import generate_events, serialize_event, validate_event


SAMPLE = {
    "user_id": 1234,
    "postcode": "SW19",
    "webpage": "www.website.com/index.html",
    "timestamp": 1611662684,
}


def test_first_generated_event_matches_the_brief():
    events = list(generate_events(2, start_timestamp=1611662684))
    assert events[0] == SAMPLE
    assert events[1]["timestamp"] == 1611662685
    assert all(set(event) == set(SAMPLE) for event in events)


def test_serialization_preserves_field_values():
    event = {**SAMPLE, "webpage": "www.website.com/café"}
    assert json.loads(serialize_event(event)) == event


@pytest.mark.parametrize("field", SAMPLE)
def test_missing_fields_are_rejected(field):
    event = SAMPLE.copy()
    del event[field]
    with pytest.raises(ValueError, match=f"missing_field:{field}"):
        validate_event(event)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("user_id", True),
        ("user_id", "1234"),
        ("user_id", 1.5),
        ("postcode", 1234),
        ("webpage", []),
        ("timestamp", True),
        ("timestamp", "1611662684"),
        ("timestamp", 1611662684.0),
        *((field, None) for field in SAMPLE),
    ],
)
def test_wrong_types_are_rejected_without_coercion(field, value):
    event = {**SAMPLE, field: value}
    with pytest.raises(ValueError, match=f"wrong_type:{field}"):
        validate_event(event)


@pytest.mark.parametrize("event", [None, [], "not an object"])
def test_non_objects_are_rejected(event):
    with pytest.raises(ValueError, match="not_an_object"):
        validate_event(event)


def test_historical_sample_and_partial_postcode_are_valid():
    validate_event(SAMPLE)


def test_generator_requires_a_positive_count():
    with pytest.raises(ValueError):
        list(generate_events(0))
