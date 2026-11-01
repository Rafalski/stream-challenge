"""Exercise the continuous Flink job, not a second aggregation implementation."""

import json
import os
import shutil
import stat
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from test_ingestion import kafka_topic, publish_payloads


pytestmark = [pytest.mark.integration, pytest.mark.flink]
ROOT = Path(__file__).resolve().parents[2]


def docker(*arguments, check=True):
    """Run the existing services without adding a separate demo runner."""
    result = subprocess.run(["docker", *arguments], cwd=ROOT, capture_output=True, text=True, timeout=30)
    if check:
        assert result.returncode == 0, f"docker {' '.join(arguments)}\n{result.stderr}"
    # Python logs go to stderr, which docker logs forwards separately.
    return result.stdout + result.stderr if arguments[0] == "logs" else result.stdout


def rest(path, method="GET"):
    """Use Flink's existing REST interface for observable progress."""
    request = urllib.request.Request(f"http://127.0.0.1:8081/{path}", method=method)
    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read()
        return json.loads(body) if body else None


def wait_for(description, probe, timeout=180):
    """Poll an explicit condition, with a deadline and useful failure message."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = probe()
        if last:
            return last
        threading.Event().wait(1)
    pytest.fail(f"Timed out waiting for {description}; last result: {last!r}")


def completed_records(directory):
    """Ignore Flink's hidden pending files until checkpoints commit them."""
    return [json.loads(line) for path in directory.glob("part-*.jsonl")
            for line in path.read_text(encoding="utf-8").splitlines()]


def remove_readonly(function, path, error):
    """Docker-created folders can inherit OneDrive's Windows read-only flag."""
    if not isinstance(error[1], PermissionError):
        raise error[1]
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    function(path)


def test_continuous_windows_dq_lateness_and_raw_preservation(kafka_topic):
    """Arrange isolated consumers, act with known input, assert real file output.

    Two act/assert rounds make lateness deterministic: first prove a minute has
    closed, then publish an event for that minute. Finally check DQ, raw bytes
    and the quiet final window. Cleanup runs even when an assertion fails.
    """
    # Arrange: check the shared cluster and reserve names for this scenario.
    try:
        overview = rest("overview")
    except OSError as error:
        pytest.fail(f"Start the Flink cluster using the README instructions: {error}")
    assert overview["taskmanagers"] == 1, "The local test expects one registered TaskManager"
    assert overview["slots-available"] >= 1, "Stop the demo aggregation job before this isolated test"

    run_dir = ROOT / "data" / kafka_topic.topic
    container = f"flink-{kafka_topic.topic}"
    raw_container = f"raw-{kafka_topic.topic}"
    containers = []
    job_id = None
    run_dir.mkdir(parents=True)
    output = run_dir / "aggregates"
    relative = f"/app/data/{kafka_topic.topic}"
    group = f"{kafka_topic.topic}-flink"
    raw_group = f"{kafka_topic.topic}-raw"
    kafka_topic.groups.extend([group, raw_group])
    observed_counters = {}

    def job():
        """Find only the job created by this scenario."""
        return next((item for item in rest("jobs/overview")["jobs"] if item["name"] == container), None)

    def window(postcode, minute, count):
        """Keep expected results literal and easy to review."""
        return {"postcode": postcode, "window_start": f"2021-01-26T12:{minute}:00Z", "pageview_count": count}

    def event(seconds, postcode="SW19"):
        """Prepare synthetic source data with the original four fields."""
        return json.dumps({"user_id": 1234, "postcode": postcode,
                           "webpage": "www.website.com/index.html", "timestamp": 1611662640 + seconds}).encode()

    def raw_matches(payloads):
        """Check original bytes by offset, including empty values and null markers."""
        directory = run_dir / "raw" / kafka_topic.topic / "0"
        expected = {f"{offset}{'.null' if payload is None else '.bin'}": payload or b""
                    for offset, payload in enumerate(payloads)}
        if not all((directory / name).is_file() for name in expected):
            return False
        actual = {path.name: path.read_bytes() for path in directory.iterdir() if path.is_file()}
        assert actual == expected, "Raw archive differs from the published payloads"
        return True

    try:
        # Arrange: five valid visits and five invalid payloads, then two later phases.
        # Invalid timestamps must never move the watermark. Expected rows stay literal.
        initial = [event(1), event(59), event(58), event(0, "M1"), event(60),
                   b"{bad JSON", b"\xff", b"", None,
                   event(10).replace(b"1611662650", b"9999999999")]
        progress = [event(66, "M1")]
        final = [event(20), event(126)]
        payloads = initial + progress + final

        # Act: start both real applications and wait until they can consume input.
        print(f"\nE2E: starting Flink for isolated topic {kafka_topic.topic}", flush=True)
        containers.append(container)
        docker("compose", "run", "-d", "--no-deps", "--name", container,
            "-e", "LOG_FORMAT=json", "aggregation", "--topic", kafka_topic.topic,
            "--group-id", group, "--job-name", container,
            "--output-dir", f"{relative}/aggregates", "--checkpoint-dir", f"{relative}/checkpoints")

        def submitted():
            """Surface submission errors before waiting for the full deadline."""
            found = job()
            if found:
                return found
            state = json.loads(docker("inspect", "--format", "{{json .State}}", container))
            assert state["Running"], docker("logs", container)

        discovered = wait_for("job submission", submitted)
        job_id = discovered["jid"]

        def running():
            """Fail immediately for job errors rather than waiting for output."""
            details = rest(f"jobs/{job_id}")
            state = details["state"]
            assert state not in ("FAILED", "CANCELED", "FINISHED"), rest(f"jobs/{job_id}/exceptions")
            return state == "RUNNING" and all(vertex["status"] == "RUNNING" for vertex in details["vertices"])

        wait_for("running Flink job", running)

        # Start the actual raw application before publishing anything.
        containers.append(raw_container)
        docker("compose", "run", "-d", "--no-deps", "--name", raw_container,
               "-e", "LOG_FORMAT=json", "raw-ingester", "--topic", kafka_topic.topic,
               "--group-id", raw_group, "--output-dir", f"{relative}/raw",
               "--max-records", str(len(payloads)), "--idle-timeout", "180")

        def raw_ready():
            """Wait for application readiness, rather than a running container alone."""
            state = json.loads(docker("inspect", "--format", "{{json .State}}", raw_container))
            assert state["Running"], docker("logs", raw_container)
            entries = [json.loads(line) for line in docker("logs", raw_container).splitlines() if line.startswith("{")]
            return any(entry.get("event") == "ingestion_started" for entry in entries)

        wait_for("raw ingester readiness", raw_ready, 30)
        # Act, round 1: the 66-second event advances progress beyond the first minute.
        print("E2E: raw ingestion and Flink are ready; publishing 11 records", flush=True)
        publish_payloads(kafka_topic.topic, initial + progress)
        first = [window("SW19", "04", 3), window("M1", "04", 1)]

        def first_window():
            """Only a committed part file establishes readable output."""
            running()
            records = completed_records(output)
            return records if len(records) >= 2 else None

        # Assert, round 1: exact first-minute counts and unchanged raw payloads.
        records = wait_for("committed first minute", first_window)
        assert sorted(records, key=lambda row: row["postcode"]) == sorted(first, key=lambda row: row["postcode"])
        assert rest(f"jobs/{job_id}/checkpoints")["counts"]["completed"] > 0
        wait_for("first 11 raw payloads", lambda: raw_matches(initial + progress), 30)
        assert running(), "Flink must still be running while raw output is checked"
        print("E2E: first minute committed and 11 raw payloads verified while Flink runs", flush=True)

        # Act, round 2: the first file proves closure, so the 20-second event is late.
        # The 126-second event closes the second minute and stays in the third.
        print("E2E: publishing one late record and one event to advance the watermark", flush=True)
        publish_payloads(kafka_topic.topic, final)
        expected = first + [window("SW19", "05", 1), window("M1", "05", 1)]

        def second_window():
            """Wait for the next checkpoint to make the second minute visible."""
            running()
            records = completed_records(output)
            return records if len(records) >= 4 else None

        # Assert, round 2: four final rows, then independent DQ and archive checks.
        actual = wait_for("committed second minute", second_window)
        sort_key = lambda row: (row["window_start"], row["postcode"])
        assert sorted(actual, key=sort_key) == sorted(expected, key=sort_key)

        def counters():
            """Read the application's existing Flink counters, without an exporter."""
            running()
            totals = {"valid_events": 0, "invalid_events": 0, "late_events": 0, "aggregate_records": 0}
            for vertex in rest(f"jobs/{job_id}")["vertices"]:
                endpoint = f"jobs/{job_id}/vertices/{vertex['id']}/subtasks/0/metrics"
                names = [item["id"] for item in rest(endpoint)
                         if item["id"].split(".")[-1] in totals or item["id"].endswith("Writer.numRecordsIn")]
                if names:
                    for item in rest(endpoint + "?get=" + ",".join(names)):
                        name = "aggregate_records" if item["id"].endswith("Writer.numRecordsIn") else item["id"].split(".")[-1]
                        totals[name] += int(item["value"])
            return totals

        expected_counters = {"valid_events": 8, "invalid_events": 5, "late_events": 1, "aggregate_records": 4}

        def counts_match():
            """Retain observed counts for a useful failure report."""
            observed_counters.update(counters())
            return observed_counters == expected_counters

        wait_for(f"DQ and output counters {expected_counters}", counts_match)

        def raw_finished():
            """The finite ingester must finish all records successfully."""
            state = json.loads(docker("inspect", "--format", "{{json .State}}", raw_container))
            if state["Running"]:
                return False
            assert state["ExitCode"] == 0, docker("logs", raw_container)
            return True

        wait_for("successful raw ingestion completion", raw_finished, 30)
        assert raw_matches(payloads)
        raw_logs = docker("logs", raw_container)
        entries = [json.loads(line) for line in raw_logs.splitlines() if line.startswith("{")]
        assert any(entry.get("event") == "ingestion_finished" and entry.get("count") == 13 for entry in entries)
        print("E2E: all 13 raw payloads match byte-for-byte; raw ingester exited successfully", flush=True)

        # A further checkpoint during silence must not close the final event-time window.
        checkpoint = rest(f"jobs/{job_id}/checkpoints")["counts"]["completed"]
        wait_for("checkpoint while input is quiet", lambda:
                 rest(f"jobs/{job_id}/checkpoints")["counts"]["completed"] > checkpoint, 45)
        assert sorted(completed_records(output), key=sort_key) == sorted(expected, key=sort_key)

        # Logs identify rejections without exposing malformed payloads.
        logs = docker("compose", "logs", "--no-color", "taskmanager")
        logs = "\n".join(line for line in logs.splitlines() if kafka_topic.topic in line)
        assert "invalid_event" in logs and "future_timestamp" in logs and "late_event" in logs
        assert "{bad JSON" not in logs
        print("E2E: four aggregate records match the expected postcode/minute counts", flush=True)
        for record in sorted(actual, key=sort_key):
            print(f"  {record['window_start']}  {record['postcode']:<4}  {record['pageview_count']}", flush=True)
        print("E2E: 13 inputs = 5 invalid + 1 late + 6 counted + 1 in the open final window", flush=True)
        print("E2E: another checkpoint during silence left the final window open, as expected", flush=True)
    except BaseException:
        # Keep failure diagnostics in pytest's captured output, before cleanup.
        print(f"E2E: observed counters: {observed_counters}")
        for name in containers:
            print(f"E2E: logs for {name}\n{docker('logs', name, check=False)}")
        print(docker("compose", "logs", "--tail", "150", "taskmanager", check=False))
        raise
    finally:
        # Cleanup: remove only this scenario's job, containers and files.
        try:
            current = job() if job_id is None else {"jid": job_id}
            if current is not None:
                state = rest(f"jobs/{current['jid']}")["state"]
                if state not in ("CANCELED", "FAILED", "FINISHED"):
                    rest(f"jobs/{current['jid']}", "PATCH")
                    wait_for("test job cancellation", lambda:
                             rest(f"jobs/{current['jid']}")["state"] in ("CANCELED", "FAILED"), 30)
        finally:
            existing = set(docker("ps", "-a", "--format", "{{.Names}}").splitlines())
            remaining = [name for name in containers if name in existing]
            if raw_container in remaining:
                # Let the consumer leave its group before fixture cleanup deletes it.
                docker("stop", "--time", "10", raw_container)
            if remaining:
                docker("rm", "-f", *remaining)
            # This resolved directory belongs exclusively to the UUID test topic.
            assert run_dir.resolve().parent == (ROOT / "data").resolve()
            shutil.rmtree(run_dir, onerror=remove_readonly)
        print("E2E: test job, containers and files cleaned up; shared services kept running", flush=True)
