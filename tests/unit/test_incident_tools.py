"""Keep the demo evidence adapter scoped to one captured incident."""

import pytest

from demos.archive_incident import evidence_result, with_incident_preset


@pytest.fixture
def snapshot():
    """Use synthetic identifiers; no Docker, network or model call is needed."""
    return ({"id": "incident-a", "job_name": "raw-ingester-demo", "job_run_id": "run-a"},
            {"jobs.get_run": {"state": "FAILED", "archived_records": 2}})


def request():
    """A permitted evidence read bound to the captured run."""
    return {"tool_id": "jobs.get_run", "arguments": {
        "job_name": "raw-ingester-demo", "job_run_id": "run-a", "region": "local"},
        "context": {"incident": {"id": "incident-a"}}}


def test_recorded_read_has_explicit_demo_provenance(snapshot):
    result = evidence_result(snapshot, request())
    assert result["synthetic"] is True
    assert result["data"]["archived_records"] == 2
    assert result["data"]["provenance"] == "recorded isolated demo evidence"


def test_ui_preset_preserves_input_and_existing_configuration():
    """The designer gets the incident without changing the original workflow."""
    original = {"configuration": {"description": "Keep this", "payload_presets": [
        {"id": "other", "name": "Other", "content": {}}]}}
    payload = {"incident": {"id": "incident-a"}, "available_tools": []}
    prepared = with_incident_preset(original, payload)
    repeated = with_incident_preset(prepared, payload)
    config = repeated["configuration"]
    assert config["default_payload_preset_id"] == "archive_failure"
    assert config["payload_presets"][-1]["content"] == payload
    assert config["description"] == "Keep this" and len(config["payload_presets"]) == 2
    assert len(original["configuration"]["payload_presets"]) == 1


@pytest.mark.parametrize("change", ["tool", "incident", "run", "extra_argument"])
def test_unrelated_resources_and_mutations_are_rejected(snapshot, change):
    payload = request()
    if change == "tool":
        payload["tool_id"] = "demo.jobs.restart"
    elif change == "incident":
        payload["context"]["incident"]["id"] = "healthy-pipeline"
    elif change == "run":
        payload["arguments"]["job_run_id"] = "another-run"
    else:
        payload["arguments"]["path"] = "../secrets"
    with pytest.raises(ValueError):
        evidence_result(snapshot, payload)
