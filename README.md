# stream-challenge

A local Python streaming exercise: generate website pageviews, publish them to Kafka, archive the original events and use PyFlink to count pageviews by postcode in one-minute event-time windows. Expected volume is fewer than 100,000 pageviews a day.

## Start here

Kafka, the Python producer and independent raw ingester are implemented, with local unit tests and Kafka integration checks. Flink aggregation and CI are not implemented yet.

Prerequisites: Docker with a running Linux-container engine, a Compose plugin supporting `up --wait` and `wait`, and host port 9092 available. On Windows, use Docker Desktop in Linux-container mode. No host Python, Java or Kafka installation is needed to run through Docker. Local tests use a Python virtual environment. Run commands from the repository root; Python setup and file inspection examples below use PowerShell.

## Start Kafka

```text
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
docker compose wait kafka-init
```

The first start downloads the pinned `apache/kafka:4.3.2` image. The final command waits for topic initialization and must return success. Startup is safe to repeat. It creates the `pageviews` topic with one partition and replication factor 1; it does not generate events.

Kafka is reachable at `127.0.0.1:9092` from the host and `kafka:29092` from Compose services. Using the explicit IPv4 host address avoids Windows resolving `localhost` to an unbound IPv6 address.

## Produce and archive events

Create the output directory before starting the ingester. In PowerShell:

```powershell
New-Item -ItemType Directory -Force data | Out-Null
```

On Linux, create `data/` using your host account and ensure it is writable by the image's UID 1000. Host permissions on other platforms have not been verified.

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

Wait for the corresponding `record_archived` log before expecting a file. The first event in a fresh topic is offset 0. Flink aggregate files are not available yet.

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

These are local checks, not distributed quotas: file counters include the startup scan plus this writer's changes, byte totals exclude filesystem metadata, and free-space checks are periodic. They cover the raw archive, not all Docker storage or future aggregate output. The Docker bind-mount and host free-space readings matched in this Windows environment; that has not been verified on other platforms.

If a limit stops the ingester, the producer and Kafka keep running independently. Stop the producer and review the error rather than treating the run as complete. Kafka can retain the blocked records only within its configured retention policy. Retention and byte-preserving rolled batches must be designed and tested before sustained-volume or production use; these guardrails do not make one file per event a production archive design.

## Log display

Console logs use blue `INFO`, yellow `STEP`, green `OK`, magenta `WARN` and red `ERROR` entries, with coloured level badges. Both published and archived success messages are green. Coloured message text has an explicit black background so it remains readable in light log viewers. The labels are presentation only; JSON retains standard logging levels.

Docker enables console colours explicitly so they appear in `docker compose logs`. Native Python detects an interactive terminal. Set `LOG_COLOUR=never` for plain text, or select colour-free JSON for automation:

```text
docker compose run --rm -e LOG_FORMAT=json producer --count 1 --interval 0
```

The available settings are `LOG_FORMAT=console|json` and `LOG_COLOUR=auto|always|never`. Shell settings override Compose defaults. These formatters apply to our Python applications, not Kafka's broker logs. Logs use UTC timestamps and Kafka coordinates; event payloads are not logged.

## Verify and inspect

```text
docker compose ps -a
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic pageviews
docker compose run --rm kafka-smoke-test
```

Expect `kafka` to be healthy, `kafka-init` to have exited with code 0, and the topic description to show one partition and one replica. The smoke test creates a unique temporary topic, publishes the sample JSON, checks the consumed value and deletes its topic on exit. Look for `PASS` and a successful command exit. This tests real Kafka, not the future streaming application.

For startup diagnostics:

```text
docker compose logs --tail 100 kafka kafka-init
```

## Local Python tests

Use Python 3.11, which is also used by the application image:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[test]"
.\.venv\Scripts\python.exe -m pytest
```

The default run covers core unit tests without Docker. With Kafka running, run the integration checks locally:

```powershell
.\.venv\Scripts\python.exe -m pytest -m integration
```

These create isolated topics, groups and temporary directories. They exercise the real producer/ingester commands, preserve malformed/empty/null/repeated values and verify that a failed write or archive limit leaves its offset available for retry. This is not yet an end-to-end aggregation test suite.

## Stop or reset

```text
docker compose --profile "*" down
```

Enabling all profiles here also removes application containers, while retaining Kafka records in the project's named volume. Stop any foreground producer first with Ctrl+C. To discard Kafka records and start with an empty broker instead:

```text
docker compose --profile "*" down --volumes
```

The reset deletes this Compose project's Kafka data, but not host files under `data/`. Restart with the commands above to recreate the topic. After resetting Kafka, use a fresh archive directory rather than mixing reused offsets with an older topic's files, for example:

```text
docker compose run --rm raw-ingester --output-dir data/raw-next
```

Run this instead of the background ingester, not alongside it with the same consumer group. If archive files are removed while Kafka offsets are retained, use a new `--group-id` to replay available records. Kafka can only replay data still within its retention period.

## Status and limitations

Verified on Windows with Docker Engine 29.9.2 and Compose 5.6.0: Kafka startup/persistence/reset, native Python tests and Kafka integration, Docker production/archival and host-visible output. Local tests use Python 3.11.2; the image uses 3.11.18. Other host platforms have not been verified.

Archival uses one file per record for clarity and byte preservation. Files accumulate quickly; retention and batching need review for sustained use. Files are flushed, fsynced and atomically renamed before synchronous offset commits. Matching replays reuse the same file; conflicting contents fail visibly. There is no atomic transaction between filesystem writes and Kafka, or verified power-loss guarantee. Broader recovery testing belongs to milestone 5.

This is a single local broker without authentication, TLS or redundancy. Only the host listener is published, on loopback. The input topic retains segments for approximately one day or 256 MiB per partition, subject to segment rolling and cleanup; these settings are not a strict disk cap or the future raw archive's retention policy.

[SPEC.md](SPEC.md) defines the requirements. [ROADMAP.md](ROADMAP.md) tracks milestone gates. [ENGINEERING_GUIDE.md](docs/ENGINEERING_GUIDE.md) records design choices and the AI-assisted workflow. [AGENTS.md](AGENTS.md) contains actionable rules for coding agents.

Prometheus/Grafana and the later Agent Forge investigation are optional. Neither is implemented.
