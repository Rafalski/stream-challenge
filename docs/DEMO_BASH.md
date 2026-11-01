# Live demo: Bash

Use Git Bash on Windows, Linux Bash, or Ubuntu in WSL 2 with Docker Desktop integration enabled. Open the repository root in the terminal. The pipeline needs no host Python installation. Stop after any failed command.

Prepare each new terminal before running Docker commands:

```bash
export MSYS_NO_PATHCONV=1
docker compose version
```

Git Bash otherwise rewrites container paths such as `/app/data` into Windows paths under `C:/Program Files/Git`, causing checkpoint-directory failures. `MSYS_NO_PATHCONV` disables that conversion in this shell; it is harmless in Linux/WSL. Quoting a path alone does not prevent conversion.

If WSL reports that Docker is unavailable, enable the Ubuntu distribution under Docker Desktop **Settings Ã¢â€ â€™ Resources Ã¢â€ â€™ WSL Integration**, then reopen the terminal.

## 1. Start services

Skip this step if the pipeline is already running. Initial image builds can take several minutes.

```bash
mkdir -p data
docker compose up -d --wait --wait-timeout 120
docker compose wait kafka-init
docker compose build raw-ingester jobmanager
docker compose up -d --wait --wait-timeout 180 raw-ingester jobmanager taskmanager
docker compose up -d --wait --wait-timeout 120 kafka-ui kafka-exporter prometheus grafana
```

## 2. Check or submit the aggregation job

Open [Flink](http://127.0.0.1:8081). If `postcode-pageviews` is already running, reuse it. Set the output path to the directory used for that submission; the default is:

```bash
aggregate_dir=/app/data/aggregates
```

Only if no aggregation job is running, submit one with fresh directories:

```bash
run=$(date -u +%Y%m%dT%H%M%S)-$RANDOM
aggregate_dir=/app/data/demo-$run/aggregates
docker compose run -d --no-deps aggregation --output-dir "$aggregate_dir" --checkpoint-dir "/app/data/demo-$run/checkpoints"
```

Wait until all three Flink boxes show running tasks. The separate `raw-ingester` container archives originals; it is not another Flink job.

If submission fails with a checkpoint path containing `C:/Program Files/Git`, run `export MSYS_NO_PATHCONV=1` and repeat the fresh-directory submission block above. Keep earlier files; no Kafka reset is needed. To inspect a failed submission, use the Flink job's **Exceptions** tab.

## 3. Open the live views

- [Grafana](http://127.0.0.1:3000/d/streaming-demo): leave `postcode-pageviews` and Job ID **All** selected. Watch throughput, checkpoints and consumer lag.
- [Kafka browser](http://127.0.0.1:8080): select **local Ã¢â€ â€™ Topics Ã¢â€ â€™ pageviews Ã¢â€ â€™ Messages Ã¢â€ â€™ Live**. Expand a record to inspect its four fields.
- [Flink](http://127.0.0.1:8081): inspect the running job and its checkpoint history.

A fresh submission replays retained Kafka input. Before producing demo traffic, let `postcode-aggregation` lag settle to zero and confirm completed checkpoints are increasing. Older output minutes during replay are expected.

## 4. Produce low, busy and medium traffic

Run in order. Each phase stops after about one minute; Ctrl+C stops production early.

```bash
docker compose run --rm --no-deps producer --continuous --interval-range 0.2 0.4 --max-duration 60
docker compose run --rm --no-deps producer --continuous --interval-range 0.06 0.12 --max-duration 60
docker compose run --rm --no-deps producer --continuous --interval-range 0.1 0.2 --max-duration 60
```

Rates are smoothed, so graphs respond gradually. The producer reports actual counts. Flink consumer lag can rise between checkpoints; raw archival commits each record separately.

## 5. Inspect finished results

In the same terminal, after a minute window closes and a checkpoint completes:

```bash
docker compose exec -T taskmanager sh -c "cat $aggregate_dir/part-*.jsonl"
docker compose logs --tail 10 raw-ingester
```

Each JSONL row contains `postcode`, `window_start` in UTC and `pageview_count`. Files appear on the host under the matching `data/` directory. Repeat the command to see later results. The final quiet minute can remain open; [timing explained](ENGINEERING_GUIDE.md#checkpoints-and-event-time).

For a table and keyboard screen, use the host Python environment prepared in step 6. Install the optional screen extra once. Git Bash:

```bash
.venv/Scripts/python.exe -m pip install -e '.[viewer]'
results_dir="${aggregate_dir#/app/}"
winpty .venv/Scripts/python.exe -m stream_challenge.results "$results_dir" --interactive
```

Use Tab to move between sections, 1/2 from the table to edit filters, Enter to apply/view, arrows to select, R to refresh and Q to quit. The `winpty` prefix supplies the console bridge needed by Git Bash/mintty. Omit it in Windows Terminal if unnecessary; do not use it for Linux/WSL. For a plain filtered preview:

```bash
.venv/Scripts/python.exe -m stream_challenge.results "$results_dir" --postcode SW19 --minute 2026-11-01T19:46
```

Replace the example UTC minute with one present in the output; omit either filter to show all matching rows. Linux/WSL uses `.venv-linux/bin/python` for the install and viewer commands. [Viewer reference](../README.md#aggregate-results-viewer).

## 6. Optional automated checks

Git Bash uses Windows Python 3.11. Prepare a Windows environment once, using forward slashes when calling its executables from Bash:

```bash
py -3.11 -m venv .venv
.venv/Scripts/python.exe -m pip install -e '.[test]'
.venv/Scripts/python.exe -m pytest
```

Linux/WSL uses Linux Python 3.11 and its venv support, with a separate environment:

```bash
python3.11 -m venv .venv-linux
.venv-linux/bin/python -m pip install -e '.[test]'
.venv-linux/bin/python -m pytest
```

If `python3.11` is missing, resolve that host prerequisite first. Docker demos still work without it. Native WSL test execution has not been verified.

For the real end-to-end scenario, cancel the demo job in Flink and wait for **CANCELED** and an available task slot. Leave the shared services running. Git Bash:

```bash
.venv/Scripts/python.exe -m pytest -m flink -v -s
```

Linux/WSL:

```bash
.venv-linux/bin/python -m pytest -m flink -v -s
```

Expect four matching aggregate rows, 13 byte-exact raw records and **1 passed**. The test cleans up its isolated resources; it does not restart the manual demo job.

## 7. Stop

```bash
docker compose --profile "*" down
```

Kafka data and host output files are retained. Further setup and safeguards are in the [README](../README.md).

## 8. Optional complete reset

This discards the local demo history. Stop producers and close the viewer first, then run:

```bash
docker compose --profile "*" down --volumes --remove-orphans
```

This removes project containers, Kafka topics/consumer offsets and Prometheus history. It does not delete host files. In the file manager, delete only this repository's `data` folder, including raw files, aggregates, checkpoints, per-run folders and diagnostic logs. Keep source files, `.venv`, `.venv-linux` and `.local-notes`.

Recreate the empty output root and follow steps 1 and 2 for a fresh pipeline and job:

```bash
mkdir -p data
```

No reset is needed just to repeat traffic. For an independent aggregation submission, cancel the old job and use the fresh-directory command in step 2. Reset Kafka and raw storage together: retaining old consumer offsets while deleting raw files does not rebuild the archive.
