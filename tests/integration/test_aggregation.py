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

from test_ingestion import kafka_topic, new_consumer, publish_payloads
from stream_challenge.ingest import consume_records


pytestmark = [pytest.mark.integration, pytest.mark.flink]
ROOT = Path(__file__).resolve().parents[2]


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
        threading.Event().wait(0.2)
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
    """Known inputs produce exact counts and leave the quiet final minute open."""
    try:
        overview = rest("overview")
    except OSError as error:
        pytest.fail(f"Start the Flink cluster using the README instructions: {error}")
    assert overview["taskmanagers"] == 1, "The local test expects one registered TaskManager"
    assert overview["slots-available"] >= 1, "Stop the demo aggregation job before this isolated test"

    run_dir = ROOT / "data" / kafka_topic.topic
    container = f"flink-{kafka_topic.topic}"
    job_id = None
    run_dir.mkdir(parents=True)
    output = run_dir / "aggregates"
    relative = f"/app/data/{kafka_topic.topic}"
    group = f"{kafka_topic.topic}-flink"
    kafka_topic.groups.append(group)

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

    try:
        # Start with invalids too: none may poison the event-time watermark.
        initial = [event(1), event(59), event(58), event(0, "M1"), event(60),
                   b"{bad JSON", b"\xff", b"", None,
                   event(10).replace(b"1611662650", b"9999999999")]
        publish_payloads(kafka_topic.topic, initial)
        result = subprocess.run([
            "docker", "compose", "run", "-d", "--no-deps", "--name", container,
            "-e", "LOG_FORMAT=json", "aggregation", "--topic", kafka_topic.topic,
            "--group-id", group, "--job-name", container,
            "--output-dir", f"{relative}/aggregates", "--checkpoint-dir", f"{relative}/checkpoints",
        ], cwd=ROOT, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        discovered = wait_for("job submission", job)
        job_id = discovered["jid"]

        def running():
            """Fail immediately for job errors rather than waiting for output."""
            state = rest(f"jobs/{job_id}")["state"]
            assert state not in ("FAILED", "CANCELED", "FINISHED"), rest(f"jobs/{job_id}/exceptions")
            return state == "RUNNING"

        wait_for("running Flink job", running)
        publish_payloads(kafka_topic.topic, [event(66, "M1")])
        first = [window("SW19", "04", 3), window("M1", "04", 1)]

        def first_window():
            """Only a committed part file establishes readable output."""
            running()
            records = completed_records(output)
            return records if len(records) >= 2 else None

        records = wait_for("committed first minute", first_window)
        assert sorted(records, key=lambda row: row["postcode"]) == sorted(first, key=lambda row: row["postcode"])
        assert rest(f"jobs/{job_id}/checkpoints")["counts"]["completed"] > 0

        # The first file proves the watermark passed the old minute. Now send a late record.
        final = [event(20), event(126)]
        publish_payloads(kafka_topic.topic, final)
        expected = first + [window("SW19", "05", 1), window("M1", "05", 1)]

        def second_window():
            """Wait for the next checkpoint to make the second minute visible."""
            running()
            records = completed_records(output)
            return records if len(records) >= 4 else None

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
        wait_for("DQ and output counters", lambda: counters() == expected_counters)

        # A further checkpoint during silence must not close the final event-time window.
        checkpoint = rest(f"jobs/{job_id}/checkpoints")["counts"]["completed"]
        wait_for("checkpoint while input is quiet", lambda:
                 rest(f"jobs/{job_id}/checkpoints")["counts"]["completed"] > checkpoint, 45)
        assert sorted(completed_records(output), key=sort_key) == sorted(expected, key=sort_key)

        # Independently archive every byte, including invalid and excluded late input.
        payloads = initial + [event(66, "M1")] + final
        consumer = new_consumer(kafka_topic)
        try:
            assert consume_records(consumer, kafka_topic.topic, run_dir / "raw", len(payloads), 15) == len(payloads)
        finally:
            consumer.close()
        for offset, payload in enumerate(payloads):
            suffix = ".null" if payload is None else ".bin"
            assert (run_dir / "raw" / kafka_topic.topic / "0" / f"{offset}{suffix}").read_bytes() == (payload or b"")

        # Logs identify rejections without exposing malformed payloads.
        logs = subprocess.run(["docker", "compose", "logs", "--no-color", "taskmanager"],
                              cwd=ROOT, capture_output=True, text=True, timeout=15).stdout
        assert "invalid_event" in logs and "future_timestamp" in logs and "late_event" in logs
        assert kafka_topic.topic in logs
        assert "{bad JSON" not in logs
    except BaseException:
        # Keep failure diagnostics in pytest's captured output, before cleanup.
        for command in (["docker", "logs", container], ["docker", "compose", "logs", "--tail", "150", "taskmanager"]):
            diagnostics = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=15)
            print(diagnostics.stdout, diagnostics.stderr)
        raise
    finally:
        current = job() if job_id is None else {"jid": job_id}
        if current is not None:
            state = rest(f"jobs/{current['jid']}")["state"]
            if state not in ("CANCELED", "FAILED", "FINISHED"):
                rest(f"jobs/{current['jid']}", "PATCH")
                wait_for("test job cancellation", lambda:
                         rest(f"jobs/{current['jid']}")["state"] in ("CANCELED", "FAILED"), 30)
        subprocess.run(["docker", "rm", "-f", container], cwd=ROOT, capture_output=True, timeout=15)
        # This resolved directory was created exclusively for the UUID test topic.
        assert run_dir.resolve().parent == (ROOT / "data").resolve()
        shutil.rmtree(run_dir, onerror=remove_readonly)
