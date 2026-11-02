"""Keep the demo evidence adapter scoped to one captured incident."""

import pytest

from demos.archive_incident import evidence_result, load_snapshot, mark_ready, prepared_workflow, with_incident_preset


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


def test_enhanced_copy_has_a_human_gate_without_recovery_actions():
    """Triage and review must not turn a proposal into a restart operation."""
    payload = {"incident": {"id": "incident-a"}, "available_tools": []}
    simple = prepared_workflow(payload)
    enhanced = prepared_workflow(payload, enhanced=True)
    assert simple["definition"]["max_ai_calls"] == 2
    assert enhanced["definition"]["max_ai_calls"] == 3
    choices = [stage for stage in enhanced["definition"]["stages"] if stage["kind"] == "choice"]
    assert len(choices) == 1 and choices[0]["require_comment"] is True
    assert choices[0]["allow_multiple"] is False
    assert all(stage.get("action", {}).get("name") in (None, "set_fields", "collect_incident_evidence")
               for stage in enhanced["definition"]["stages"])
    support = enhanced["configuration"]["payload_presets"][-1]["content"]
    assert support["incident"] == payload["incident"]
    assert support["operator_request"] == "support_review"


def test_incident_root_serves_only_completed_unchanged_capture(tmp_path):
    """New incident files must not become tools until cleanup/completion is published."""
    import json

    identity = "archive-limit-" + "a" * 32
    folder = tmp_path / identity
    folder.mkdir()
    payload = {"incident": {"id": identity}}
    (folder / "incident.json").write_text(json.dumps(payload))
    (folder / "evidence.json").write_text("{}")
    request = {"context": payload}
    with pytest.raises(ValueError, match="incomplete"):
        load_snapshot(tmp_path, request)
    mark_ready(folder)
    assert load_snapshot(tmp_path, request)[0] == payload["incident"]
    (folder / "evidence.json").write_text('{"changed":true}')
    with pytest.raises(ValueError, match="changed"):
        load_snapshot(tmp_path, request)


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
