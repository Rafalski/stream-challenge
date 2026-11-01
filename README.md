# stream-challenge

A local Python streaming exercise: generate website pageviews, publish them to Kafka, archive the original events and use PyFlink to count pageviews by postcode in one-minute event-time windows. Expected volume is fewer than 100,000 pageviews a day.

## Start here

Kafka, the Python producer, independent raw ingester and PyFlink aggregation are implemented and verified locally. Milestone 3 awaits engineer review. CI is not implemented yet.

Prerequisites: Docker with a running Linux-container engine, a Compose plugin supporting `up --wait` and `wait`, and host ports 9092 and 8081 available. On Windows, use Docker Desktop in Linux-container mode. No host Python, Java or Kafka installation is needed to run through Docker. Local tests use a Python virtual environment.

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

The producer defaults to ten events, one second apart, using the current epoch time. For demos, use a fixed interval or a random range, in seconds:

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

With the raw ingester also running, generate current-time traffic for a short demonstration:

```text
docker compose run --rm producer --continuous --interval-range 0.2 1.5 --max-duration 150
docker compose logs --tail 50 taskmanager
```

Keep a second terminal at the repository root for live logs while the producer runs:

```text
docker compose logs -f --tail 20 raw-ingester taskmanager
```

Expect `record_published` in the producer, `record_archived` in the ingester and eventually `aggregate_emitted` in the TaskManager logs. Ctrl+C in the log-following terminal stops viewing logs, not the services. The one-off producer container disappears after its bounded run because `--rm` is set.

Windows finish as event time advances beyond the minute plus the five-second disorder allowance. Completed files become visible after a successful ten-second checkpoint, so output is not immediate. If traffic stops, the final minute stays open until later valid events advance event time. A checkpoint alone does not close that window.

Inspect completed output in PowerShell:

```powershell
Get-ChildItem data/aggregates/part-*.jsonl | Get-Content
```

Bash:

```bash
find data/aggregates -maxdepth 1 -type f -name 'part-*.jsonl' -exec cat {} +
```

Each JSON line contains `postcode`, UTC `window_start` and `pageview_count`. Ignore hidden in-progress files. Flink writes multiple aggregates per part file and stores checkpoints separately in `data/checkpoints`. The UI exposes checkpoint status, application counters (`valid_events`, `invalid_events`, `late_events`) and the file writer's `numRecordsIn` for aggregate output. An `aggregate_emitted` log means computed, not yet committed to a readable file.

During the normal demo, `valid_events` should rise and completed checkpoints should continue increasing. Inspect operator metrics for validation, late diagnostics and the file writer. Current-time sample traffic should normally have no invalid events. The job replays retained Kafka input too, so earlier traffic can affect counts and late exclusions. Compare exact counts using the isolated automated scenario below, rather than assuming the manual demo starts from an empty topic. Checkpoint files are Flink's internal state, not pageview results.

Invalid records and arrivals for closed windows are excluded with diagnostic reasons; raw ingestion still preserves their bytes. See the [agreed policies](docs/ENGINEERING_GUIDE.md#technical-decisions). Flink's Java runtime logs keep their own format; Python application log settings remain selectable.

Fresh submissions replay retained Kafka data from the earliest offsets. They require empty output/checkpoint directories and do not automatically restore previous state. For another run, stop the existing job through the UI, then use new directories:

```text
docker compose run --rm aggregation --output-dir /app/data/aggregates-next --checkpoint-dir /app/data/checkpoints-next
```

Use a different fresh pair for each independent rerun. Do not run a second job alongside the first or combine output from replayed runs when counting pageviews. Tested checkpoint restoration and broader failure recovery remain milestone 5 work.

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

With the Flink cluster healthy and no demo aggregation job occupying its slot, select the streaming scenario:

```powershell
.\.venv\Scripts\python.exe -m pytest -m flink -v
```

Bash:

```bash
.venv-linux/bin/python -m pytest -m flink -v
```

Before the Flink test, cancel any manual demo job in the Flink UI and wait until it is `CANCELED` and the slot is available. Stopping only the submission client is not proof that the job has stopped. During the test, a job named `flink-test-...` appears, produces checked results, then becomes `CANCELED` during cleanup. Expect 59 passing unit cases, four Kafka checks and one Flink scenario with the current suite. The scenario can take several minutes on a cold runtime; it uses observable deadlines rather than a fixed sleep.

Integration checks create isolated topics, groups and directories. The Flink scenario exercises the real continuous job, compares literal expected postcode/minute counts, verifies invalid and late outcomes, preserves raw bytes and observes checkpoint-committed files. It also checks that a quiet final minute remains pending. Its job and test resources are cleaned up; the shared Kafka/Flink services remain running. The broader scenario harness is milestone 4 work.

## Stop or reset

```text
docker compose --profile "*" down
```

Enabling all profiles here also removes application and Flink containers, while retaining Kafka records in the project's named volume. Stop any foreground producer first with Ctrl+C. Shutdown is not a final-window flush or a savepoint. To discard Kafka records and start with an empty broker instead:

```text
docker compose --profile "*" down --volumes
```

The reset deletes this Compose project's Kafka data, but not raw files, aggregate files or checkpoints under host `data/`. Restart with the commands above to recreate the topic. Use fresh Flink output/checkpoint directories for a new submission. After resetting Kafka, use a fresh archive directory rather than mixing reused offsets with an older topic's files, for example:

```text
docker compose run --rm raw-ingester --output-dir data/raw-next
```

Run this instead of the background ingester, not alongside it with the same consumer group. If archive files are removed while Kafka offsets are retained, use a new `--group-id` to replay available records. Kafka can only replay data still within its retention period.

## Status and limitations

Verified on Windows with Docker Engine 29.9.2 and Compose 5.6.0: Kafka startup/persistence/reset, native Python tests, Docker production/archival and continuous Kafka/Flink aggregation with host-visible committed output. Flink 2.2.1, Kafka connector `5.0.0-2.2`, Java 17 and Python 3.11 work together in the local image. Local tests use Python 3.11.2; the images use 3.11.18. Other host platforms have not been verified.

Archival uses one file per record for clarity and byte preservation. Files accumulate quickly; retention and batching need review for sustained use. Files are flushed, fsynced and atomically renamed before synchronous offset commits. Matching replays reuse the same file; conflicting contents fail visibly. There is no atomic transaction between filesystem writes and Kafka, or verified power-loss guarantee. Broader recovery testing belongs to milestone 5.

This is a single local broker without authentication, TLS or redundancy. Only the host listener is published, on loopback. The input topic retains segments for approximately one day or 256 MiB per partition, subject to segment rolling and cleanup; these settings are not a strict disk cap or the future raw archive's retention policy.

[SPEC.md](SPEC.md) defines the requirements. [ROADMAP.md](ROADMAP.md) tracks milestone gates. [ENGINEERING_GUIDE.md](docs/ENGINEERING_GUIDE.md) records design choices and the AI-assisted workflow. [AGENTS.md](AGENTS.md) contains actionable rules for coding agents.

Prometheus/Grafana and the later Agent Forge investigation are optional. Neither is implemented.
