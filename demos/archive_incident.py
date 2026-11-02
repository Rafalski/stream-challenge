"""Create an isolated archive incident and serve its read-only evidence to Agent Forge."""

import argparse
import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ("jobs.get_definition", "jobs.get_run", "jobs.get_log_excerpt", "jobs.get_related_runs")


def docker(*args):
    """Use argument arrays, never a shell or the healthy service's container."""
    result = subprocess.run(["docker", *args], cwd=ROOT, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"docker {args[0]} failed: {result.stderr[-1500:]}")
    return result.stdout + result.stderr if args[0] == "logs" else result.stdout


def wait_for(probe, description, timeout=60):
    """Wait for observable readiness or exit, with a bounded deadline."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result:
            return result
        time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for {description}")


def save(path, value):
    """Keep only synthetic scenario data and bounded operational evidence."""
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def mark_ready(folder):
    """Publish completion atomically, only after capture cleanup succeeds."""
    marker = {"incident_sha256": hashlib.sha256((folder / "incident.json").read_bytes()).hexdigest(),
              "evidence_sha256": hashlib.sha256((folder / "evidence.json").read_bytes()).hexdigest()}
    temporary = folder / "ready.tmp"
    save(temporary, marker)
    temporary.replace(folder / "ready.json")


def with_incident_preset(workflow, payload):
    """Give the designer the same explicit incident input used by the API run."""
    workflow = copy.deepcopy(workflow)
    configuration = workflow.setdefault("configuration", {})
    presets = configuration.setdefault("payload_presets", [])
    presets[:] = [preset for preset in presets if preset["id"] != "archive_failure"]
    presets.append({"id": "archive_failure", "name": "Captured archive file-limit failure",
                    "content": copy.deepcopy(payload)})
    configuration["default_payload_preset_id"] = "archive_failure"
    return workflow


def prepared_workflow(payload, enhanced=False):
    """Keep the reusable template generic and bind one captured incident per copy."""
    filename = "archive-investigation-gated.json" if enhanced else "archive-investigation.json"
    template = json.loads((ROOT / "demos" / filename).read_text())
    workflow = with_incident_preset(template, payload)
    if enhanced:
        support = copy.deepcopy(payload)
        support["operator_request"] = "support_review"
        workflow["configuration"]["payload_presets"].append({
            "id": "direct_support", "name": "Same incident: request direct support review", "content": support})
    return workflow


def capture(bootstrap):
    """Archive two known records, observe a real blocked third write, then clean up Kafka."""
    # Only capture needs Kafka libraries; preset preparation and evidence reads do not.
    from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    from stream_challenge.events import generate_events, serialize_event
    from stream_challenge.producer import publish_events

    identity = "archive-limit-" + uuid4().hex
    folder = ROOT / "data" / "incidents" / identity
    folder.mkdir(parents=True)
    container = identity
    admin = AdminClient({"bootstrap.servers": bootstrap})
    created = False
    started = False
    try:
        admin.create_topics([NewTopic(identity, 1, 1)])[identity].result(15)
        created = True
        docker("compose", "run", "-d", "--no-deps", "--name", container,
               "-e", "LOG_FORMAT=json", "-e", "LOG_COLOUR=never", "raw-ingester",
               "--topic", identity, "--group-id", identity,
               "--output-dir", f"/app/data/incidents/{identity}/raw",
               "--max-archive-files", "2", "--min-free-mib", "0", "--idle-timeout", "30")
        started = True

        def state():
            return json.loads(docker("inspect", "--format", "{{json .State}}", container))

        def ready():
            assert state()["Running"], docker("logs", container)
            return "ingestion_started" in docker("logs", container)

        wait_for(ready, "isolated ingester readiness")
        events = list(generate_events(3))
        publish_events(Producer({"bootstrap.servers": bootstrap, "acks": "all"}), identity, events,
                       interval_range=(0.05, 0.1))
        wait_for(lambda: not state()["Running"], "archive budget failure")
        assert state()["ExitCode"] == 1, "Expected the real ingester to fail on its file limit"
        logs = [json.loads(line) for line in docker("logs", container).splitlines() if line.startswith("{")]
        failure = next(item for item in logs if item.get("event") == "ingestion_failed")
        assert "archive_file_limit" in failure["error"]
        raw = folder / "raw" / identity / "0"
        assert sorted(path.name for path in raw.iterdir()) == ["0.bin", "1.bin"]
        assert [(raw / f"{index}.bin").read_bytes() for index in range(2)] == [
            serialize_event(event) for event in events[:2]]
        consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": identity,
                             "enable.auto.commit": False})
        try:
            committed = consumer.committed([TopicPartition(identity, 0)], timeout=10)[0].offset
        finally:
            consumer.close()
        assert committed == 2, "The blocked record must remain uncommitted"

        reported = datetime.now(timezone.utc).isoformat()
        incident = {"id": identity, "title": "Raw archive stopped during a bounded demo",
                    "description": "A separate Python raw ingester exited while archiving three synthetic pageviews. Investigate the configured limits, logs and committed offsets. The healthy pipeline is separate. This is recorded evidence: the temporary topic and consumer group are deleted after capture, so the blocked record is NOT currently available for replay.",
                    "reported_at": reported, "job_name": "raw-ingester-demo", "job_run_id": identity,
                    "region": "local", "reported_error": failure["error"], "source": "isolated_live_demo"}
        available = [{"id": tool, "description": "Read recorded evidence for this isolated archive run",
                      "access": "read_only", "required_arguments": ["job_name", "region"] +
                      (["job_run_id"] if tool in TOOLS[1:3] else [])} for tool in TOOLS]
        evidence = {
            "jobs.get_definition": {"job_name": incident["job_name"], "engine": "Python raw ingester",
                "source": {"type": "Kafka", "topic": identity}, "archive_format": "one unchanged value per file",
                "max_archive_files": 2, "offset_policy": "commit synchronously after a successful archive write",
                "note": "No Flink checkpoint or cloud table belongs to this Python process"},
            "jobs.get_run": {"state": "FAILED", "job_run_id": identity, "exit_code": 1,
                "published_records": 3, "archived_records": 2, "committed_next_offset": committed,
                "blocked_offset": 2, "error": failure["error"],
                "offset_semantics": "Next offset 2 means records 0 and 1 were committed; record 2 was not",
                "current_replay_available": False,
                "cleanup": "The isolated topic and group are deleted after capture; no Kafka replay is available now"},
            "jobs.get_log_excerpt": {"lines": logs[-8:], "truncated": len(logs) > 8},
            "jobs.get_related_runs": {"active_runs": [], "scope": "isolated consumer only",
                "note": "Healthy raw-archive and postcode-aggregation groups were not modified"},
        }
        payload = {"incident": incident, "available_tools": available}
        save(folder / "incident.json", payload)
        save(folder / "evidence.json", evidence)
        save(folder / "workflow.json", prepared_workflow(payload))
        save(folder / "workflow-gated.json", prepared_workflow(payload, enhanced=True))
    finally:
        if started:
            docker("rm", "-f", container)
        if created:
            # These names are generated here and never refer to the main topic/groups.
            admin.delete_topics([identity])[identity].result(15)
            for future in admin.delete_consumer_groups([identity]).values():
                try:
                    future.result(15)
                except KafkaException as error:
                    if error.args[0].code() != KafkaError.GROUP_ID_NOT_FOUND:
                        raise
    mark_ready(folder)
    print(f"PASS: 3 published, 2 byte-exact archives, offset 2 uncommitted, exit code 1\nIncident: {folder / 'incident.json'}")
    return folder


def evidence_result(snapshot, request):
    """Allow only four exact read operations for the captured incident identity."""
    incident, evidence = snapshot
    tool, args = request.get("tool_id"), request.get("arguments", {})
    context = request.get("context", {}).get("incident", {})
    if context.get("id") != incident["id"] or tool not in TOOLS or not isinstance(args, dict):
        raise ValueError("incident or tool not allowed")
    expected = {"job_name": incident["job_name"], "region": "local"}
    if tool in TOOLS[1:3]:
        expected["job_run_id"] = incident["job_run_id"]
    if args != expected:
        raise ValueError("arguments do not match this captured incident")
    # Synthetic provenance means replayed demo evidence, never live infrastructure access.
    return {"synthetic": True, "data": {**evidence[tool], "provenance": "recorded isolated demo evidence"}}


def load_snapshot(folder, request):
    """Serve either one fixed capture or a completed incident directly under a root."""
    if not (folder / "incident.json").is_file():
        identity = request.get("context", {}).get("incident", {}).get("id", "")
        if not isinstance(identity, str) or not identity.startswith("archive-limit-"):
            raise ValueError("unknown incident")
        child = folder / identity
        if child.resolve().parent != folder.resolve() or not (child / "ready.json").is_file():
            raise ValueError("incident is outside this root or incomplete")
        for filename in ("ready.json", "incident.json", "evidence.json"):
            path = child / filename
            if path.resolve().parent != child.resolve() or path.stat().st_size > 32768:
                raise ValueError("invalid captured file path or size")
        marker = json.loads((child / "ready.json").read_text())
        for name in ("incident", "evidence"):
            if hashlib.sha256((child / f"{name}.json").read_bytes()).hexdigest() != marker[f"{name}_sha256"]:
                raise ValueError("captured evidence changed after completion")
        folder = child
    payload = json.loads((folder / "incident.json").read_text(encoding="utf-8"))
    return payload["incident"], json.loads((folder / "evidence.json").read_text(encoding="utf-8"))


def serve(folder, port):
    """Serve a capture or completed incident root on loopback, without mutation tools."""
    token = os.environ.get("INCIDENT_TOOLS_TOKEN", "")
    if len(token) < 32:
        raise ValueError("set INCIDENT_TOOLS_TOKEN to a secret of at least 32 characters")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            """Do not print authorization headers or request bodies."""

        def do_POST(self):
            """Match Agent Forge's existing authenticated incident tool interface."""
            if not hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + token):
                self.send_error(401)
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if self.path != "/tools/invoke" or not 0 < length <= 32768:
                    raise ValueError("invalid request")
                request = json.loads(self.rfile.read(length))
                result = evidence_result(load_snapshot(folder, request), request)
                body = json.dumps(result).encode()
                if len(body) > 4096:
                    raise ValueError("evidence exceeds Agent Forge's per-tool limit")
            except (ValueError, TypeError, AttributeError, OSError, KeyError):
                self.send_error(400)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    print(f"Read-only recorded evidence: http://127.0.0.1:{port}/tools/invoke; Ctrl+C stops")
    with ThreadingHTTPServer(("127.0.0.1", port), Handler) as server:
        server.serve_forever()


def submit(folder, api, enhanced=False):
    """Invoke the saved two-call workflow once; retries reuse the same identity."""
    if api not in ("http://127.0.0.1:8000", "http://localhost:8000"):
        raise ValueError("this demo targets local Agent Forge on port 8000 only")
    token = os.environ.get("AGENTFORGE_API_TOKEN", "")
    if not token:
        raise ValueError("set AGENTFORGE_API_TOKEN to an authorized local Agent Forge token")
    payload = json.loads((folder / "incident.json").read_text(encoding="utf-8"))
    identity = payload["incident"]["id"]

    def request(path, body=None, key=None):
        headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        data = None if body is None else json.dumps(body).encode()
        with urlopen(Request(api + path, data=data, headers=headers), timeout=120) as response:
            return json.load(response)

    suffix = "-gated" if enhanced else ""
    receipt = folder / f"submission{suffix}.json"
    if receipt.exists():
        existing = json.loads(receipt.read_text())
        result = request("/api/runs/" + existing["run_id"])
    else:
        workflow = prepared_workflow(payload, enhanced)
        save(folder / f"workflow{suffix}.json", workflow)
        saved = request("/api/workflows", workflow, identity + suffix + "-workflow")
        result = request(f"/api/workflows/{saved['id']}/runs",
                         {"version_number": 1, "input": payload}, identity + suffix + "-investigation")
        save(receipt, {"api": api, "workflow_id": saved["id"], "run_id": result["id"]})
    # Persist a compact report, not the entire API run with unrelated metadata.
    save(folder / f"investigation{suffix}.json", {"run_id": result["id"], "status": result["status"],
         "result": result.get("result"), "ai_calls": result.get("ai_calls", []),
         "tool_calls": result.get("tool_calls", []), "waiting_task_id": result.get("waiting_task_id")})
    print(f"Agent Forge run {result['id']}: {result['status']}\nReport: {folder / ('investigation' + suffix + '.json')}")
    if result["status"] == "waiting":
        print("Open Agent Forge Tasks to review the proposed next step; no recovery action is enabled.")
    elif result["status"] != "succeeded":
        raise RuntimeError("investigation did not succeed; inspect Agent Forge run history before retrying")


def main():
    """Capture and serve separately so rerunning AI never repeats the failure."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture_parser = commands.add_parser("capture")
    capture_parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    serve_parser = commands.add_parser("serve")
    serve_parser.add_argument("folder", type=Path)
    serve_parser.add_argument("--port", type=int, default=8011)
    submit_parser = commands.add_parser("submit")
    submit_parser.add_argument("folder", type=Path)
    submit_parser.add_argument("--api", default="http://127.0.0.1:8000")
    submit_parser.add_argument("--enhanced", action="store_true", help="use the separate triage and human-review copy")
    args = parser.parse_args()
    try:
        if args.command == "capture":
            capture(args.bootstrap_servers)
        elif args.command == "serve":
            serve(args.folder, args.port)
        else:
            submit(args.folder, args.api, args.enhanced)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
