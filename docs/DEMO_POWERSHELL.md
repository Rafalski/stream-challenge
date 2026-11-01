# Live demo: PowerShell

Use PowerShell at the repository root, with Docker Desktop running Linux containers. The pipeline needs no host Python installation. Stop after any failed command.

## 1. Start services

Skip this step if the pipeline is already running. Initial image builds can take several minutes.

```powershell
New-Item -ItemType Directory -Force data | Out-Null
docker compose up -d --wait --wait-timeout 120
docker compose wait kafka-init
docker compose build raw-ingester jobmanager
docker compose up -d --wait --wait-timeout 180 raw-ingester jobmanager taskmanager
docker compose up -d --wait --wait-timeout 120 kafka-ui kafka-exporter prometheus grafana
```

## 2. Check or submit the aggregation job

Open [Flink](http://127.0.0.1:8081). If `postcode-pageviews` is already running, reuse it. Set the output path to the directory used for that submission; the default is:

```powershell
$aggregateDir = "/app/data/aggregates"
```

Only if no aggregation job is running, submit one with fresh directories:

```powershell
$run = [guid]::NewGuid().ToString("N").Substring(0, 8)
$aggregateDir = "/app/data/demo-$run/aggregates"
docker compose run -d --no-deps aggregation --output-dir $aggregateDir --checkpoint-dir "/app/data/demo-$run/checkpoints"
```

Wait until all three Flink boxes show running tasks. The separate `raw-ingester` container archives originals; it is not another Flink job.

## 3. Open the live views

- [Grafana](http://127.0.0.1:3000/d/streaming-demo): leave `postcode-pageviews` and Job ID **All** selected. Watch throughput, checkpoints and consumer lag.
- [Kafka browser](http://127.0.0.1:8080): select **local → Topics → pageviews → Messages → Live**. Expand a record to inspect its four fields.
- [Flink](http://127.0.0.1:8081): inspect the running job and its checkpoint history.

## 4. Produce low, busy and medium traffic

Run in order. Each phase stops after about one minute; Ctrl+C stops production early.

```powershell
docker compose run --rm --no-deps producer --continuous --interval-range 0.2 0.4 --max-duration 60
docker compose run --rm --no-deps producer --continuous --interval-range 0.06 0.12 --max-duration 60
docker compose run --rm --no-deps producer --continuous --interval-range 0.1 0.2 --max-duration 60
```

Rates are smoothed, so graphs respond gradually. The producer reports actual counts. Flink consumer lag can rise between checkpoints; raw archival commits each record separately.

## 5. Inspect finished results

In the same terminal, after a minute window closes and a checkpoint completes:

```powershell
docker compose exec -T taskmanager sh -c "cat $aggregateDir/part-*.jsonl"
docker compose logs --tail 10 raw-ingester
```

Each JSONL row contains `postcode`, `window_start` in UTC and `pageview_count`. Files appear on the host under the matching `data/` directory. Repeat the command to see later results. The final quiet minute can remain open; [timing explained](ENGINEERING_GUIDE.md#checkpoints-and-event-time).

## 6. Optional automated checks

Native tests require Windows Python 3.11. Prepare a Windows environment once:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest
```

For the real end-to-end scenario, cancel the demo job in Flink and wait for **CANCELED** and an available task slot. Leave the shared services running:

```powershell
.\.venv\Scripts\python.exe -m pytest -m flink -v -s
```

Expect four matching aggregate rows, 13 byte-exact raw records and **1 passed**. The test cleans up its isolated resources; it does not restart the manual demo job.

## 7. Stop

```powershell
docker compose --profile "*" down
```

Kafka data and host output files are retained. Further setup and safeguards are in the [README](../README.md).
