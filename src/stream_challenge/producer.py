"""Publish a finite batch or a paced stream of synthetic pageviews."""

import argparse
import logging
import math
import os
import random
import time
from collections.abc import Iterable

from confluent_kafka import KafkaException, Producer

from stream_challenge.events import generate_events, serialize_event
from stream_challenge.logging_config import configure_logging


logger = logging.getLogger("producer")


def publish_events(
    producer: Producer,
    topic: str,
    events: Iterable[dict],
    timeout: float = 10,
    interval_range: tuple[float, float] = (0, 0),
    seed: int | None = None,
    max_duration: float | None = 900,
) -> int:
    """Publish events and count only successful broker acknowledgements."""
    minimum, maximum = interval_range
    if not all(math.isfinite(value) for value in interval_range) or not 0 <= minimum <= maximum:
        raise ValueError("intervals must be finite and satisfy 0 <= minimum <= maximum")
    if max_duration is not None and (not math.isfinite(max_duration) or max_duration <= 0):
        raise ValueError("max_duration must be finite and positive")

    rng = random.Random(seed)
    count = 0
    deadline = None if max_duration is None else time.monotonic() + max_duration

    for event in events:
        # Keep demo pacing within the run budget, including long pauses.
        if count and maximum:
            delay = rng.uniform(minimum, maximum)
            if deadline is not None:
                delay = min(delay, max(0, deadline - time.monotonic()))
            time.sleep(delay)
        if deadline is not None and time.monotonic() >= deadline:
            logger.warning("production_duration_limit", extra={"details": {"seconds": max_duration, "count": count}})
            break

        # Prepare the event only when we're still allowed to send it.
        payload = serialize_event(event)
        delivery = []

        def delivered(error, message):
            """Keep the delivery report for the record we're waiting on."""
            delivery.append((error, message))

        producer.produce(topic=topic, value=payload, on_delivery=delivered)
        # Queued isn't delivered. Wait for the broker before reporting success.
        if producer.flush(timeout):
            raise TimeoutError("delivery deadline exceeded; the record may still reach Kafka")
        if not delivery:
            raise RuntimeError("missing delivery report")
        error, message = delivery[0]
        if error is not None:
            raise RuntimeError(str(error))

        count += 1
        logger.info("record_published", extra={"details": {
            "topic": message.topic(), "partition": message.partition(), "offset": message.offset(),
        }})
    return count


def main() -> int:
    """Read demo options, connect to Kafka and run the producer."""
    # Connection and sample settings.
    parser = argparse.ArgumentParser(description="Publish synthetic pageviews to local Kafka.")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    parser.add_argument("--topic", default="pageviews")

    volume = parser.add_mutually_exclusive_group()
    volume.add_argument("--count", type=int, default=10)
    volume.add_argument("--continuous", action="store_true", help="Publish until stopped or the duration limit is reached.")
    parser.add_argument("--start-timestamp", type=int, help="Use this epoch second, advancing one second per event.")

    # Fixed pacing and random pacing are alternatives, not cumulative delays.
    pacing = parser.add_mutually_exclusive_group()
    pacing.add_argument("--interval", type=float, default=1, help="Seconds between events (default: 1).")
    pacing.add_argument("--interval-range", type=float, nargs=2, metavar=("MIN", "MAX"), help="Random seconds between events.")
    parser.add_argument("--seed", type=int, help="Make random pacing repeatable.")
    parser.add_argument("--max-duration", type=float, default=900, help="Publishing duration in seconds (default: 900).")
    parser.add_argument("--no-safeguards", action="store_true", help="Disable the publishing duration limit.")

    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    intervals = tuple(args.interval_range) if args.interval_range else (args.interval, args.interval)
    if not all(math.isfinite(value) for value in intervals) or not 0 <= intervals[0] <= intervals[1]:
        parser.error("intervals must be finite and satisfy 0 <= minimum <= maximum")
    if not math.isfinite(args.max_duration) or args.max_duration <= 0:
        parser.error("--max-duration must be finite and positive")

    configure_logging()
    try:
        # Check the topic before starting, so setup errors fail clearly.
        producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 10000,
            "socket.timeout.ms": 10000,
        }, logger=logging.getLogger("kafka"))
        metadata = producer.list_topics(args.topic, timeout=10)
        if args.topic not in metadata.topics or metadata.topics[args.topic].error:
            raise RuntimeError("input topic is unavailable; run Kafka initialization first")

        event_count = None if args.continuous else args.count
        events = generate_events(event_count, args.start_timestamp)
        duration = None if args.no_safeguards else args.max_duration
        if args.no_safeguards:
            logger.warning("safeguards_disabled", extra={"details": {"scope": "producer"}})
        logger.info("production_started", extra={"details": {
            "topic": args.topic, "count": event_count, "max_duration": duration,
        }})
        count = publish_events(producer, args.topic, events, interval_range=intervals, seed=args.seed, max_duration=duration)
        if event_count is not None and count < event_count:
            logger.error("production_incomplete", extra={"details": {"count": count, "requested": event_count}})
            return 1
        logger.info("production_finished", extra={"details": {"count": count}})
        return 0
    except KeyboardInterrupt:
        logger.warning("production_interrupted", extra={"details": {"delivery_status": "check Kafka before retrying"}})
        return 130
    except (KafkaException, RuntimeError, TimeoutError, ValueError, BufferError) as error:
        logger.error("production_failed", extra={"details": {"error": str(error)}})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
