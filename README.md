# stream-challenge

A local Python streaming exercise: generate website pageviews, publish them to Kafka, archive the original events and use PyFlink to count pageviews by postcode in one-minute event-time windows. Expected volume is fewer than 100,000 pageviews a day.

## Start here

Milestone 1 provides local Kafka infrastructure and a broker smoke test. The Python producer, raw archive, Flink job, unit tests and CI are not implemented yet.

Prerequisites: Docker with a running Linux-container engine, a Compose plugin supporting `up --wait` and `wait`, and localhost port 9092 available. On Windows, use Docker Desktop in Linux-container mode. No host Python, Java or Kafka installation is needed for this milestone. Commands below run from the repository root in PowerShell or a POSIX shell.

## Start Kafka

```text
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
docker compose wait kafka-init
```

The first start downloads the pinned `apache/kafka:4.3.2` image. The final command waits for topic initialization and must return success. Startup is safe to repeat. It creates the `pageviews` topic with one partition and replication factor 1; it does not generate events.

Kafka is reachable at `localhost:9092` from the host and `kafka:29092` from services on the Compose network.

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

There are no raw or aggregate output files yet. Event generation and output inspection commands will be added when implemented. Local pytest unit tests will be added with Python logic; no unit-test command is available yet.

## Stop or reset

```text
docker compose down
```

This removes the containers and network but retains Kafka records in the project's named volume. To discard those records and start with an empty broker instead:

```text
docker compose down --volumes
```

The reset deletes this Compose project's Kafka data. Restart with the commands above to recreate the topic.

## Status and limitations

Verified on Windows with Docker Engine 29.9.2 and Compose 5.6.0: clean startup, topic initialization, repeat startup, broker publication/consumption, localhost connectivity, persistence across container recreation, shutdown and reset. Other host platforms have not been verified.

This is a single local broker without authentication, TLS or redundancy. Only the host listener is published, on loopback. The input topic retains segments for approximately one day or 256 MiB per partition, subject to segment rolling and cleanup; these settings are not a strict disk cap or the future raw archive's retention policy.

[SPEC.md](SPEC.md) defines the requirements. [ROADMAP.md](ROADMAP.md) tracks milestone gates. [ENGINEERING_GUIDE.md](docs/ENGINEERING_GUIDE.md) records design choices and the AI-assisted workflow. [AGENTS.md](AGENTS.md) contains actionable rules for coding agents.

Prometheus/Grafana and the later Agent Forge investigation are optional. Neither is implemented.
