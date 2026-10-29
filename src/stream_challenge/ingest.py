"""Archive raw Kafka values independently of stream aggregation."""

import argparse
import logging
import math
import os
import signal
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

from stream_challenge.archive import ArchiveGuard, archive_record
from stream_challenge.logging_config import configure_logging


logger = logging.getLogger("raw_ingester")


def consume_records(
    consumer: Consumer,
    topic: str,
    output_dir: Path,
    max_records: int = 0,
    idle_timeout: float | None = None,
    guard: ArchiveGuard | None = None,
) -> int:
    """Archive records and commit offsets only after successful writes."""
    consumer.subscribe([topic])
    count = 0
    last_record = time.monotonic()

    while not max_records or count < max_records:
        if guard is not None:
            guard.check()

        # A deadline is useful for finite demos; normal ingestion just waits.
        remaining = None if idle_timeout is None else idle_timeout - (time.monotonic() - last_record)
        if remaining is not None and remaining <= 0:
            raise TimeoutError(f"no record received within {idle_timeout} seconds")
        message = consumer.poll(1 if remaining is None else min(1, remaining))
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())

        # Keep even malformed payloads. Flink will handle validation separately.
        archive_record(output_dir, message.topic(), message.partition(), message.offset(), message.value(), guard=guard)

        # If a write fails, leave the offset alone so the record can be retried.
        committed = consumer.commit(message=message, asynchronous=False)
        for partition in committed:
            if partition.error:
                raise KafkaException(partition.error)

        count += 1
        last_record = time.monotonic()
        logger.info("record_archived", extra={"details": {
            "topic": message.topic(), "partition": message.partition(), "offset": message.offset(),
            "bytes": None if message.value() is None else len(message.value()),
        }})
    return count


def stop(signum, frame):
    """Let Docker's stop signal use the normal cleanup path."""
    raise KeyboardInterrupt


def main() -> int:
    """Read local settings and run the raw consumer until stopped."""
    # Connection, archive location and optional limits for finite runs.
    parser = argparse.ArgumentParser(description="Archive original Kafka values without parsing them.")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    parser.add_argument("--topic", default="pageviews")
    parser.add_argument("--group-id", default="raw-archive")
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--max-records", type=int, default=0, help="Stop after this many records; 0 runs continuously.")
    parser.add_argument("--idle-timeout", type=float, help="Fail if no record arrives within this many seconds.")

    # Small local budgets are enabled by default, with an explicit opt-out.
    parser.add_argument("--max-archive-files", type=int, default=10000)
    parser.add_argument("--max-archive-mib", type=int, default=100)
    parser.add_argument("--min-free-mib", type=int, default=1024)
    parser.add_argument("--no-safeguards", action="store_true", help="Disable archive budgets and free-space checks.")

    args = parser.parse_args()
    if args.max_records < 0:
        parser.error("--max-records must be non-negative")
    if args.idle_timeout is not None and (not math.isfinite(args.idle_timeout) or args.idle_timeout <= 0):
        parser.error("--idle-timeout must be finite and positive")
    if args.max_archive_files < 1 or args.max_archive_mib < 1 or args.min_free_mib < 0:
        parser.error("archive budgets must be positive and --min-free-mib cannot be negative")

    configure_logging()
    signal.signal(signal.SIGTERM, stop)
    consumer = None
    try:
        # We own offset commits here; closing the consumer must not advance them.
        consumer = Consumer({
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "socket.timeout.ms": 10000,
        }, logger=logging.getLogger("kafka"))
        metadata = consumer.list_topics(args.topic, timeout=10)
        if args.topic not in metadata.topics or metadata.topics[args.topic].error:
            raise RuntimeError("input topic is unavailable; run Kafka initialization first")

        guard = None
        if args.no_safeguards:
            logger.warning("safeguards_disabled", extra={"details": {"scope": "archive"}})
        else:
            guard = ArchiveGuard(
                args.output_dir, max_files=args.max_archive_files,
                max_bytes=args.max_archive_mib * 1024**2, min_free_bytes=args.min_free_mib * 1024**2,
            )
            logger.info("archive_guard_enabled", extra={"details": {
                "existing_files": guard.files, "existing_bytes": guard.bytes,
                "max_files": guard.max_files, "max_bytes": guard.max_bytes,
                "min_free_bytes": guard.min_free_bytes,
            }})
        logger.info("ingestion_started", extra={"details": {"topic": args.topic, "group": args.group_id}})
        count = consume_records(consumer, args.topic, args.output_dir, args.max_records, args.idle_timeout, guard=guard)
        logger.info("ingestion_finished", extra={"details": {"count": count}})
        return 0
    except KeyboardInterrupt:
        logger.info("ingestion_stopped")
        return 0
    except (KafkaException, OSError, RuntimeError, TimeoutError, ValueError) as error:
        logger.error("ingestion_failed", extra={"details": {"error": str(error)}})
        return 1
    finally:
        # Leave the group cleanly even when a write or commit fails.
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())
