"""Validate Kafka pageviews and count them in event-time minute windows."""

import argparse
import json
import logging
import os
from pathlib import Path

from pyflink.common import Configuration, Duration, Encoder, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.file_system import FileSink, BucketAssigner, RollingPolicy, OutputFileConfig
from pyflink.datastream.functions import AggregateFunction, FlatMapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment, TableDescriptor

from stream_challenge.logging_config import configure_logging
from stream_challenge.validation import parse_pageview, utc_window_start


logger = logging.getLogger("aggregation")
# Kafka coordinates travel alongside the event internally, never in the source JSON.
EVENT_TYPE = Types.TUPLE([Types.STRING(), Types.LONG(), Types.STRING(), Types.INT(), Types.LONG()])


class ValidatePageview(FlatMapFunction):
    """Reject individual bad records before they can affect watermarks."""

    def __init__(self, max_future_seconds: int):
        """Keep the agreed future-clock allowance configurable."""
        self.max_future_seconds = max_future_seconds

    def open(self, runtime_context):
        """Expose counters in Flink and configure the Python worker logs."""
        configure_logging()
        metrics = runtime_context.get_metrics_group()
        self.accepted = metrics.counter("valid_events")
        self.invalid = metrics.counter("invalid_events")

    def flat_map(self, record):
        """Carry just the postcode, event time and diagnostic coordinates."""
        payload, topic, partition, offset = record
        try:
            event = parse_pageview(payload, max_future_seconds=self.max_future_seconds)
        except ValueError as error:
            self.invalid.inc()
            logger.warning("invalid_event", extra={"details": {
                "reason": str(error), "topic": topic, "partition": partition, "offset": offset,
            }})
            return

        self.accepted.inc()
        yield event["postcode"], event["timestamp"], topic, partition, offset


class PageviewTimestamp(TimestampAssigner):
    """Convert epoch seconds to the milliseconds Flink expects."""

    def extract_timestamp(self, value, record_timestamp):
        """Use the validated event time, not the Kafka ingestion time."""
        return value[1] * 1000


class CountPageviews(AggregateFunction):
    """Keep a count rather than retaining every event in a window."""

    def create_accumulator(self):
        """Start an empty postcode/window count."""
        return 0

    def add(self, value, accumulator):
        """Every accepted pageview contributes one, including repeats."""
        return accumulator + 1

    def get_result(self, accumulator):
        """Pass the completed count to the window formatter."""
        return accumulator

    def merge(self, first, second):
        """Combine counts if the window operator needs to merge them."""
        return first + second


class FormatWindow(ProcessWindowFunction):
    """Attach the postcode and UTC minute to each final count."""

    def open(self, runtime_context):
        """Keep window logs on the application's selected formatter."""
        configure_logging()

    def process(self, key, context, elements):
        """Emit one JSON line per completed postcode/window."""
        count = next(iter(elements))
        # This log means computed, not yet committed by the file sink.
        logger.info("aggregate_emitted", extra={"details": {"pageview_count": count}})
        yield json.dumps({"postcode": key, "window_start": utc_window_start(context.window().start // 1000),
                          "pageview_count": count}, ensure_ascii=False, separators=(",", ":"))


class ReportLateEvent(FlatMapFunction):
    """Account for excluded late events without creating another data store."""

    def open(self, runtime_context):
        """Keep a dedicated counter for the late side output."""
        configure_logging()
        self.late = runtime_context.get_metrics_group().counter("late_events")

    def flat_map(self, value):
        """Log safe coordinates, then emit nothing into the aggregate sink."""
        self.late.inc()
        logger.warning("late_event", extra={"details": {
            "reason": "window_closed", "topic": value[2], "partition": value[3], "offset": value[4],
        }})
        return []


def kafka_records(env, args):
    """Use the supported table connector to expose raw bytes and metadata."""
    table_env = StreamTableEnvironment.create(env)
    schema = (Schema.new_builder().column("payload", DataTypes.BYTES())
              .column_by_metadata("topic", DataTypes.STRING(), "topic")
              .column_by_metadata("partition_id", DataTypes.INT(), "partition")
              .column_by_metadata("record_offset", DataTypes.BIGINT(), "offset").build())
    descriptor = (TableDescriptor.for_connector("kafka").schema(schema)
                  .option("topic", args.topic).option("properties.bootstrap.servers", args.bootstrap_servers)
                  .option("properties.group.id", args.group_id).option("properties.enable.auto.commit", "false")
                  .option("properties.allow.auto.create.topics", "false")
                  .option("scan.startup.mode", "earliest-offset").format("raw").build())
    return table_env.to_data_stream(table_env.from_descriptor(descriptor))


def build_pipeline(args):
    """Wire the real Kafka source, event-time windows and checkpointed sink."""
    config = Configuration()
    config.set_string("python.executable", "python")
    config.set_integer("python.fn-execution.bundle.size", 1)
    config.set_integer("python.fn-execution.bundle.time", 100)
    config.set_string("restart-strategy.type", "none")
    config.set_string("execution.checkpointing.storage", "filesystem")
    config.set_string("execution.checkpointing.dir", Path(args.checkpoint_dir).resolve().as_uri())
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    env.enable_checkpointing(10000)

    # Validate first. Invalid timestamps must never influence stream progress.
    valid = kafka_records(env, args).flat_map(ValidatePageview(args.max_future_seconds), EVENT_TYPE).name("validate")
    watermarks = (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(args.disorder_seconds))
                  .with_timestamp_assigner(PageviewTimestamp()))
    timed = valid.assign_timestamps_and_watermarks(watermarks)
    # A separate pickled side-output type avoids sharing JVM-backed type objects.
    late_tag = OutputTag("late-pageviews")
    aggregates = (timed.key_by(lambda event: event[0], key_type=Types.STRING())
                  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                  .side_output_late_data(late_tag)
                  .aggregate(CountPageviews(), FormatWindow(), accumulator_type=Types.LONG(),
                             output_type=Types.STRING()).name("postcode-minute-counts"))

    # Route late diagnostics through the same sink as an empty stream.
    late = aggregates.get_side_output(late_tag).flat_map(ReportLateEvent(), Types.STRING()).name("late-diagnostics")
    sink = (FileSink.for_row_format(Path(args.output_dir).resolve().as_uri(), Encoder.simple_string_encoder())
            .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner())
            .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy())
            .with_output_file_config(OutputFileConfig.builder().with_part_prefix("part")
                                     .with_part_suffix(".jsonl").build()).build())
    aggregates.union(late).sink_to(sink).name("aggregate-files")
    return env


def main():
    """Submit the continuous local job with a small set of explicit policies."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    parser.add_argument("--topic", default="pageviews")
    parser.add_argument("--group-id", default="postcode-aggregation")
    parser.add_argument("--job-name", default="postcode-pageviews")
    parser.add_argument("--output-dir", default="/app/data/aggregates")
    parser.add_argument("--checkpoint-dir", default="/app/data/checkpoints")
    parser.add_argument("--disorder-seconds", type=int, default=5)
    parser.add_argument("--max-future-seconds", type=int, default=300)
    args = parser.parse_args()
    if args.disorder_seconds < 0 or args.max_future_seconds < 0:
        parser.error("time allowances cannot be negative")

    configure_logging()
    # A fresh submission starts at the beginning, not from a restored checkpoint.
    # Refuse to mix its results with a previous run's files.
    for directory in (args.output_dir, args.checkpoint_dir):
        path = Path(directory)
        if path.exists() and any(path.iterdir()):
            parser.error(f"use an empty directory for a fresh submission: {directory}")
    logger.info("aggregation_started", extra={"details": {
        "topic": args.topic, "group": args.group_id, "disorder_seconds": args.disorder_seconds,
    }})
    # Let operational failures reach Flink and fail the job visibly.
    build_pipeline(args).execute(args.job_name)


if __name__ == "__main__":
    main()
