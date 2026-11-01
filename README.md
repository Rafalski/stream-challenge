# stream-challenge

A local Python streaming exercise: generate website pageviews, publish them to Kafka, archive the original events and use PyFlink to count pageviews by postcode in one-minute event-time windows. Expected volume is fewer than 100,000 pageviews a day.

## Start here

The pipeline and concurrent end-to-end scenario are implemented and verified locally. Optional Kafka browsing and a [provisioned Grafana dashboard](#optional-live-dashboard) support the live demonstration. CI is not implemented yet; milestone review status is in the roadmap.

Prerequisites: Docker with a running Linux-container engine, a Compose plugin supporting `up --wait` and `wait`, and host ports 9092 and 8081 available. On Windows, use Docker Desktop in Linux-container mode. No host Python, Java or Kafka installation is needed to run through Docker. Local tests use a Python virtual environment.

For the main live demonstration, start raw ingestion and Flink below, open the Kafka browser, then run the three traffic phases in [Aggregate pageviews](#aggregate-pageviews). The ten-record producer command is only a small connectivity check. The optional Kafka browser also needs host port 8080.

Run commands from the repository root. Blocks labelled `text` work in both PowerShell and Bash; shell-specific commands have separate examples. Stop after any failed command and inspect its error before continuing.

For Bash on Windows, use Ubuntu in WSL 2 with [Docker Desktop WSL integration](https://docs.docker.com/desktop/features/wsl/) enabled for that distribution. Check `docker compose version` in Ubuntu before starting. The same Compose configuration and containers are used from either shell. Git Bash is not the documented Bash environment because its Windows path conversion can affect container arguments.

The current Windows checkout can be accessed through WSL under `/mnt/c/`. There is no need to move it for this walkthrough. For regular Linux development, prefer a checkout in the Linux filesystem outside synchronised folders, following [Docker's filesystem guidance](https://docs.docker.com/desktop/features/wsl/best-practices/). Moving the checkout is a separate setup task. Do not share a Python virtual environment between Windows and Linux.

## Start Kafka

```text
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
docker compose wait kafka-init
```

The first start downloads the pinned `apache/kafka:4.3.2` image. The final command waits for topic initialization and must return success. Startup is safe to repeat. It creates the `pageviews` topic with one partition and replication factor 1; it does not generate events.

Kafka is reachable at `127.0.0.1:9092` from the host and `kafka:29092` from Compose services. Using the explicit IPv4 host address avoids Windows resolving `localhost` to an unbound IPv6 address.

## Produce and archive events

Create the output directory before starting the ingester. PowerShell:

```powershell
New-Item -ItemType Directory -Force data | Out-Null
```

Bash:

```bash
mkdir -p data
```

On the Linux filesystem, ensure `data/` is writable by the image's UID 1000. Host permissions on other platforms have not been verified.

```text
docker compose up -d --build raw-ingester
docker compose run --rm --build producer --count 10 --interval 0.2
docker compose logs --tail 20 raw-ingester
```

The ingester runs continuously in the background. It writes original Kafka values under `data/raw/<topic>/<partition>/<offset>.bin` on the host. Console logs identify published and archived offsets without printing payloads. A null Kafka value has a separate empty `.null` marker; an empty byte value has an empty `.bin` file. Malformed values are archived unchanged too.

The producer defaults to ten events, one second apart, using the current epoch time. Its sample postcode mix is `SW19:SW1A:M1 = 3:2:1`, so aggregates show different counts without changing the four source fields. For ad hoc demos, use a fixed interval or a random range, in seconds:

```text
docker compose run --rm producer --continuous --interval 2
docker compose run --rm producer --continuous --interval 0.1
docker compose run --rm producer --continuous --interval-range 0.2 1.5 --seed 42
```

Run one producer at a time and use Ctrl+C to stop continuous production early. By default it stops starting new sends after 15 minutes. The range gives variable pauses between acknowledged sends; the actual rate also includes broker latency. The seed repeats the chosen delays, not wall-clock timestamps or exact arrival times. Use `--count N` for a finite run or `--interval 0` for no deliberate delay. `--start-timestamp 1611662684` gives deterministic event timestamps, advancing by one second per record.

To inspect raw files from PowerShell:

```powershell
Get-ChildItem data/raw/pageviews/0/*.bin
Get-Content data/raw/pageviews/0/0.bin
```

Bash:

```bash
find data/raw/pageviews/0 -maxdepth 1 -type f -name '*.bin'
cat data/raw/pageviews/0/0.bin
```

Wait for the corresponding `record_archived` log before expecting a file. The first event in a fresh topic is offset 0.

## Aggregate pageviews

Flink runs in Docker, keeping Java and PyFlink out of the host Python environment. The first build downloads a sizeable runtime and can take several minutes. Allow roughly 4 GiB of available Docker memory for the local pipeline, plus headroom for image builds. The image currently targets Linux x86-64.

```text
docker compose up -d --build --wait --wait-timeout 180 jobmanager taskmanager
docker compose up -d aggregation
docker compose logs --tail 50 aggregation
```

Open the [Flink UI](http://127.0.0.1:8081) and check that `postcode-pageviews` is **RUNNING**. Container startup alone does not establish that the job is running. Run one aggregation job at a time; the local TaskManager has one slot.

In Docker Desktop, expand the `stream-challenge` project. Expect `kafka`, `raw-ingester`, `jobmanager`, `taskmanager` and the `aggregation` submission client to be running, with `kafka-init` exited successfully. Kafka and both Flink managers have health checks; the other running containers do not. In the Flink UI, expect one registered TaskManager and one total task slot. Open the job and check that its tasks are running, not just the overall job state. Job startup can take a minute or more.

If submission fails with a non-empty-directory message, use the fresh-directory rerun command below. Do not delete earlier output or reset Kafka just to repeat the demonstration.

To repeat the demo while a job is already running, keep that job and the raw ingester running and repeat only the producer commands below. Do not submit another Flink job. Select its Job ID in Grafana and inspect the output directory used when it was submitted; a fresh-directory rerun does not write into `data/aggregates`. Totals include earlier traffic and replay, so watch the changes during this run.

Before starting the producer, open the three views and select the current job in Grafana:

| View | Watch | Explain |
| --- | --- | --- |
| [Grafana](http://127.0.0.1:3000/d/streaming-demo) | Throughput, accepted records, DQ, output, checkpoints and consumer lag | The busy phase increases rate; totals include earlier input. Lag compares broker and committed offsets, while output waits for event-time progress and checkpoints. |
| [Kafka browser](http://127.0.0.1:8080) | `local → Topics → pageviews → Messages`, then **Live** | Inspect the original four-field JSON and increasing offsets. **Stop loading** freezes inspection without stopping the producer. |
| [Flink UI](http://127.0.0.1:8081) | Running job, all tasks running, checkpoint history | Independent raw and aggregation consumers progress separately. Successful checkpoints commit finished files; they do not close an event-time window by themselves. |

With the raw ingester also running and all Flink tasks **RUNNING**, use a three-minute live demonstration. Run these commands one after another in the same terminal. Each phase stops itself after 60 seconds; no records are sent after the last phase finishes.

```text
docker compose run --rm --no-deps producer --continuous --interval-range 0.2 0.4 --max-duration 60
docker compose run --rm --no-deps producer --continuous --interval-range 0.06 0.12 --max-duration 60
docker compose run --rm --no-deps producer --continuous --interval-range 0.1 0.2 --max-duration 60
```

The phases are low, busy and medium traffic: roughly 200, 650 and 400 events per minute before broker/delivery overhead. Actual throughput is lower and is reported by each producer's `production_finished` count. Kafka is already initialised, so `--no-deps` avoids restarting its setup service between phases. This is paced current-time traffic, not a batch of historical timestamps. The 3:2:1 postcode mix makes `SW19` busiest, then `SW1A`, then `M1`. No additional producer mode or Python dependencies are needed.

Keep a second terminal at the repository root for live logs while the producer runs:

```text
docker compose logs -f --tail 20 raw-ingester taskmanager
```

Expect `record_published` in the producer, `record_archived` in the ingester and eventually `aggregate_emitted` in the TaskManager logs. Ctrl+C in the log-following terminal stops viewing logs, not the services. The one-off producer container disappears after its bounded run because `--rm` is set.

Windows are fixed UTC clock minutes, such as `[12:04:00, 12:05:00)`, rather than rolling counts over the last 60 seconds. They finish as event time advances beyond the window end plus the five-second disorder allowance. Completed files become visible after a successful ten-second checkpoint, so output is not immediate. A three-minute run shows several completed minutes; its first and last minutes may be partial. Traffic phases start when their commands run and need not line up with clock-minute boundaries. If traffic stops, the final minute stays open until later valid events advance event time. A checkpoint alone does not close that window.

Inspect completed output in PowerShell:

```powershell
Get-ChildItem data/aggregates/part-*.jsonl | Get-Content | ConvertFrom-Json | Sort-Object window_start, postcode | Format-Table window_start, postcode, pageview_count -AutoSize
```

Bash:

```bash
find data/aggregates -maxdepth 1 -type f -name 'part-*.jsonl' -exec cat {} +
```

For a live view in a separate PowerShell terminal, refresh the table every two seconds. Ctrl+C stops the view, not the pipeline:

```powershell
while ($true) {
    Clear-Host
    Get-ChildItem data/aggregates/part-*.jsonl -ErrorAction SilentlyContinue | Get-Content | ConvertFrom-Json | Sort-Object window_start, postcode | Format-Table window_start, postcode, pageview_count -AutoSize
    Start-Sleep -Seconds 2
}
```

In Ubuntu Bash, the equivalent live JSONL view uses `watch`:

```bash
watch -n 2 'find data/aggregates -maxdepth 1 -type f -name "part-*.jsonl" -exec cat {} +'
```

Watch the Kafka browser for incoming JSON, Flink for job health and checkpoints, and this table for completed results. The Flink UI shows processing metrics; it is not a browser for the aggregate file contents. Select the source node and add `numRecordsOutPerSecond` in its Metrics view to see processing rate. `valid_events` should rise, the file writer's `numRecordsIn` should increase when windows emit, and completed checkpoints should continue advancing. Counts are cumulative, so expect a faster increase during the busy phase. Untouched zero-valued Python counters may not appear until used. The optional Grafana dashboard below brings these signals together.

Each JSON line contains `postcode`, UTC `window_start` and `pageview_count`. Ignore hidden in-progress files. Flink writes multiple aggregates per part file and stores checkpoints separately in `data/checkpoints`. The UI exposes checkpoint status, application counters (`valid_events`, `invalid_events`, `late_events`) and the file writer's `numRecordsIn` for aggregate output. An `aggregate_emitted` log means computed, not yet committed to a readable file.

During the normal demo, `valid_events` should rise and completed checkpoints should continue increasing. Inspect operator metrics for validation, late diagnostics and the file writer. Current-time sample traffic should normally have no invalid events. The job replays retained Kafka input too, so earlier traffic can affect counts and late exclusions. Compare exact counts using the isolated automated scenario below, rather than assuming the manual demo starts from an empty topic. Checkpoint files are Flink's internal state, not pageview results.

Invalid records and arrivals for closed windows are excluded with diagnostic reasons; raw ingestion still preserves their bytes. See the [agreed policies](docs/ENGINEERING_GUIDE.md#technical-decisions). Flink's Java runtime logs keep their own format; Python application log settings remain selectable.

Fresh submissions replay retained Kafka data from the earliest offsets. They require empty output/checkpoint directories and do not automatically restore previous state. For another run, stop the existing job through the UI, then use new directories:

```text
docker compose run --rm aggregation --output-dir /app/data/aggregates-next --checkpoint-dir /app/data/checkpoints-next
```

Use a different fresh pair for each independent rerun. Do not run a second job alongside the first or combine output from replayed runs when counting pageviews. Tested checkpoint restoration and broader failure recovery remain milestone 5 work.

## Browse Kafka messages

Start the optional [Kafbat UI](https://github.com/kafbat/kafka-ui/releases/tag/v1.5.0). It is pinned to version 1.5.0, bound to loopback and configured read-only; dynamic configuration is disabled.

```text
docker compose up -d --wait --wait-timeout 120 kafka-ui
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080), then select **local ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Topics ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ pageviews ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ Messages**. Change the **Newest** dropdown to **Live** while the producer runs. Use **Stop loading** to freeze the view, then expand a row to inspect its JSON value; switch back to **Newest** and **Refresh** to inspect the most recent retained messages. Offsets identify Kafka records; the `timestamp` inside the value is our event time in epoch seconds. Browsing messages does not remove them from Kafka or change the application's consumer offsets.

The **Consumers** page shows `raw-archive` and, once a checkpoint commits offsets, `postcode-aggregation`. Inspect committed offsets and lag there. Flink can use explicit partition assignment without an active group member, so an empty group state alone does not mean the Flink job has stopped. Use the Flink UI for job health. Broker byte-rate charts require additional broker metrics and may show zero in this setup; use message offsets and Flink counters to demonstrate progress.

If you prefer the terminal, read at most ten retained records with their partition/offset metadata. This inspection consumer does not join either application group or commit offsets:

```text
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic pageviews --from-beginning --max-messages 10 --timeout-ms 10000 --command-property enable.auto.commit=false --formatter-property print.partition=true --formatter-property print.offset=true
```

Old records can appear when reading from the beginning. Use the browser's newest/live controls to watch the current run. To stop only the browser:

```text
docker compose stop kafka-ui
```

## Optional live dashboard

With the Flink cluster running, start monitoring. These commands work in PowerShell and Bash:

```text
docker compose --profile monitoring up -d --wait --wait-timeout 120 kafka-exporter prometheus grafana
```

Open [Grafana](http://127.0.0.1:3000/d/streaming-demo) for the **Streaming demo** dashboard. No login or manual import is needed. Select the current Flink **Job ID** and run the three traffic phases above. Allow a few scrapes for graphs to appear; they refresh every five seconds. Expect faster processing during the busy phase, aggregate output in minute-sized bursts, and completed checkpoints roughly every ten seconds. Startup can register failed checkpoint triggers while Python tasks initialise; check whether that count keeps rising after all tasks run, using the Flink UI and logs for the cause. The final quiet minute still remains open.

Panels cover reporter availability, running jobs, registered workers, accepted/invalid/late records, processing rate, aggregate rows reaching the writer and checkpoint health. Aggregate metrics are not proof of committed file contents; inspect `part-*.jsonl` or run the end-to-end test for that. Totals apply to the selected job attempt, not a durable audit history. Missing DQ counters display zero because PyFlink may only expose them after use; check availability and job health alongside them.

[Prometheus targets](http://127.0.0.1:9090/targets) should show both Flink managers and the Kafka exporter **UP**. The dashboard keeps the two Flink reporter targets and Kafka exporter availability separate. A reachable reporter does not mean a job is running. If upgrading an already running cluster to this configuration, cancel its job in the Flink UI, stop the `aggregation` client, restart `jobmanager` and `taskmanager`, and submit a new job using fresh output/checkpoint directories as described above. No checkpoint restoration is implied.

Grafana is an anonymous read-only local viewer. Ports 3000 and 9090 bind to loopback; do not expose this setup publicly. Prometheus retains about one day or 256 MB of time-series blocks, with additional WAL/head overhead. Grafana is disposable and its dashboard is provisioned from files. Source throughput includes replay of retained input; totals are not restricted to the latest producer run. Allow extra Docker memory for these two services. A small Kafka exporter adds consumer lag for the two demo groups. There is no raw-ingester metric endpoint, alerting or log collection; use application logs alongside the dashboard.

The **Consumer lag (records)** graph compares `raw-archive` and `postcode-aggregation`, summed across partitions. Lag is the difference between the latest broker offset and the group's committed offset, not elapsed seconds. The raw ingester commits after each archived record; Flink commits after successful checkpoints, so its line can rise between checkpoints and fall afterwards. Scrape timing can show small raw-ingester spikes too. Sustained growth needs investigation. Zero lag does not mean the final event-time window has closed. Missing offsets or an unavailable exporter produce no data, not a fabricated zero. These group metrics are independent of the dashboard's Job ID selector and can outlive a job; check Flink job health as well.

To stop only monitoring, leaving the pipeline running:

```text
docker compose stop grafana prometheus kafka-exporter
```

## Local run safeguards

Three lightweight safeguards are enabled by default and can be overridden or disabled explicitly:

| Safeguard | Default | Options |
| --- | --- | --- |
| Publishing duration | 900 seconds, including continuous mode | Producer `--max-duration SECONDS` |
| Archive budget | 10,000 files or 100 MiB of logical file contents, including existing files in the output directory | Ingester `--max-archive-files N`, `--max-archive-mib N` |
| Free-space reserve | 1 GiB on the archive filesystem, checked at startup and about every five seconds | Ingester `--min-free-mib N` |

The ingester warns once per signal at 80% of a file/byte budget, or below twice the free-space reserve. It fails before a write that would exceed a budget, or when a free-space check reports less than the reserve. The blocked record's offset is not committed. Existing files are never deleted or automatically compressed. Producer duration limits finish the current delivery attempt, which can take up to another ten seconds. Reaching the duration limit normally completes continuous mode; a finite run that cannot deliver its requested count exits unsuccessfully.

For example, run a busier stream for one minute:

```text
docker compose run --rm producer --continuous --interval 0.1 --max-duration 60
```

Pass `--no-safeguards` to the relevant producer or ingester command to opt out; this logs a warning. On the producer it disables only the duration limit, not `--count`. On the ingester it disables both archive budgets and free-space checks. Use one active writer per archive directory. To run an ingester with custom flags, stop the background ingester first and use `docker compose run --rm raw-ingester` with those flags.

These are local checks, not distributed quotas: file counters include the startup scan plus this writer's changes, byte totals exclude filesystem metadata, and free-space checks are periodic. Archive limits cover the raw archive, not aggregate files, checkpoints or all Docker storage. Keep Flink demonstrations bounded too; production output retention and file sizing remain deferred. The Docker bind-mount and host free-space readings matched in this Windows environment; that has not been verified on other platforms.

If a limit stops the ingester, the producer and Kafka keep running independently. Stop the producer and review the error rather than treating the run as complete. Kafka can retain the blocked records only within its configured retention policy. Retention and byte-preserving rolled batches must be designed and tested before sustained-volume or production use; these guardrails do not make one file per event a production archive design.

## Log display

Console logs use blue `INFO`, yellow `STEP`, green `OK`, magenta `WARN` and red `ERROR` entries, with coloured level badges. Both published and archived success messages are green. Coloured message text has an explicit black background so it remains readable in light log viewers. The labels are presentation only; JSON retains standard logging levels.

Docker enables console colours explicitly so they appear in `docker compose logs`. Native Python detects an interactive terminal. Set `LOG_COLOUR=never` for plain text, or select colour-free JSON for automation:

```text
docker compose run --rm -e LOG_FORMAT=json producer --count 1 --interval 0
```

The available settings are `LOG_FORMAT=console|json` and `LOG_COLOUR=auto|always|never`. Shell settings override Compose defaults. These formatters apply to our Python applications, not Kafka's broker logs. Flink forwards formatted Python messages inside its Java log entries, so selecting JSON does not make the entire TaskManager log a JSON stream. Logs use UTC timestamps and Kafka coordinates; event payloads are not logged.

## Verify and inspect

```text
docker compose ps -a
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic pageviews
docker compose run --rm kafka-smoke-test
```

Expect `kafka` to be healthy, `kafka-init` to have exited with code 0, and the topic description to show one partition and one replica. The smoke test creates a unique temporary topic, publishes the sample JSON, checks the consumed value and deletes its topic on exit. Look for `PASS` and a successful command exit. This checks Kafka round trips; the Flink scenario below checks aggregation.

For startup diagnostics:

```text
docker compose logs --tail 100 kafka kafka-init
```

## Local Python tests

Use Python 3.11, which is also used by the application image:

PowerShell (Windows Python):

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest
```

Bash (Linux Python 3.11 with virtual-environment support installed):

```bash
python3.11 -m venv .venv-linux
.venv-linux/bin/python -m pip install -e '.[test]'
.venv-linux/bin/python -m pytest
```

The environments have different names so both can coexist in a Windows checkout accessed through WSL. Neither requires activation. A missing `python3.11` or `venv` module is a host prerequisite to resolve before running native Bash tests; it does not prevent the Docker demonstration. Native Linux test execution remains unverified until run in that environment.

The default run covers core unit tests without Docker or PyFlink. With Kafka running, run its integration checks locally:

```powershell
.\.venv\Scripts\python.exe -m pytest -m "integration and not flink"
```

Bash:

```bash
.venv-linux/bin/python -m pytest -m 'integration and not flink'
```

## End-to-end demonstration

This single scenario starts the actual raw ingester and Flink job as separate Docker containers with independent consumer groups. Both are ready before it publishes input. It checks raw files while Flink is running, then verifies committed aggregates, DQ outcomes and the quiet final window. It uses isolated resources and does not write to the demo's `pageviews` topic or output directories.

Prepare the shared services and current application images. These commands work in PowerShell and Bash:

```text
docker compose up -d --wait --wait-timeout 120
docker compose build raw-ingester jobmanager
docker compose up -d --wait --wait-timeout 180 jobmanager taskmanager
```

Do not start the manual `aggregation` service for this test. If a demo job is already running, cancel it in the Flink UI and wait for `CANCELED` and an available slot. Stopping only its submission client does not establish that its job stopped.

Run with `-s` to see the progress and reconciliation report. PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pytest -m flink -v -s
```

Bash:

```bash
.venv-linux/bin/python -m pytest -m flink -v -s
```

Expect progress messages followed by these results (other pytest output omitted):

```text
E2E: all 13 raw payloads match byte-for-byte; raw ingester exited successfully
E2E: four aggregate records match the expected postcode/minute counts
  2021-01-26T12:04:00Z  M1    1
  2021-01-26T12:04:00Z  SW19  3
  2021-01-26T12:05:00Z  M1    1
  2021-01-26T12:05:00Z  SW19  1
E2E: 13 inputs = 5 invalid + 1 late + 6 counted + 1 in the open final window
```

The final pytest result should be **1 passed**. Verified locally on Windows: 59 unit cases, four Kafka checks and this concurrent end-to-end scenario passed. The scenario can take several minutes on a cold runtime; it waits for observable results with bounded deadlines, not a fixed sleep. A further successful checkpoint during silence confirms that the final window remains open.

In Docker Desktop, temporary `flink-test-...` and `raw-test-...` containers appear. In the Flink UI, the `flink-test-...` job runs and becomes `CANCELED` during normal cleanup. The finite raw ingester exits successfully after 13 records. Test containers, files, consumer groups and the topic are removed; shared Kafka/Flink services remain running. Normal demo output is retained. Failures show assertion differences, observed counters and relevant container logs before cleanup. Run the test again without resetting Kafka or manually deleting output.

## Stop or reset

```text
docker compose --profile "*" down
```

Enabling all profiles here also removes application and Flink containers, while retaining Kafka records in the project's named volume. Stop any foreground producer first with Ctrl+C. Shutdown is not a final-window flush or a savepoint. To discard Kafka records and start with an empty broker instead:

```text
docker compose --profile "*" down --volumes
```

The reset deletes this Compose project's Kafka data and Prometheus history, but not raw files, aggregate files or checkpoints under host `data/`. Restart with the commands above to recreate the topic. Use fresh Flink output/checkpoint directories for a new submission. After resetting Kafka, use a fresh archive directory rather than mixing reused offsets with an older topic's files, for example:

```text
docker compose run --rm raw-ingester --output-dir data/raw-next
```

Run this instead of the background ingester, not alongside it with the same consumer group. If archive files are removed while Kafka offsets are retained, use a new `--group-id` to replay available records. Kafka can only replay data still within its retention period.

## Status and limitations

Verified on Windows with Docker Engine 29.9.2 and Compose 5.6.0: Kafka startup/persistence/reset, native Python tests, Docker production/archival and continuous Kafka/Flink aggregation with host-visible committed output. Flink 2.2.1, Kafka connector `5.0.0-2.2`, Java 17 and Python 3.11 work together in the local image. Local tests use Python 3.11.2; the images use 3.11.18. Other host platforms have not been verified.

Archival uses one file per record for clarity and byte preservation. Files accumulate quickly; retention and batching need review for sustained use. Files are flushed, fsynced and atomically renamed before synchronous offset commits. Matching replays reuse the same file; conflicting contents fail visibly. There is no atomic transaction between filesystem writes and Kafka, or verified power-loss guarantee. Broader recovery testing belongs to milestone 5.

This is a single local broker without authentication, TLS or redundancy. Only the host listener is published, on loopback. The input topic retains segments for approximately one day or 256 MiB per partition, subject to segment rolling and cleanup; these settings are not a strict disk cap or the future raw archive's retention policy.

[SPEC.md](SPEC.md) defines the requirements. [ROADMAP.md](ROADMAP.md) tracks milestone gates. [ENGINEERING_GUIDE.md](docs/ENGINEERING_GUIDE.md) records design choices and the AI-assisted workflow. [AGENTS.md](AGENTS.md) contains actionable rules for coding agents.

Prometheus/Grafana are optional and available through the monitoring profile. Agent Forge remains deferred.
