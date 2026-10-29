#!/bin/sh
set -eu

broker=kafka:29092
topic="smoke-$(cat /proc/sys/kernel/random/uuid)"
record='{"user_id":1234,"postcode":"SW19","webpage":"www.website.com/index.html","timestamp":1611662684}'

cleanup() {
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$broker" \
        --delete --if-exists --topic "$topic"
}

# Use an isolated topic so verification does not add records to pageviews.
trap cleanup EXIT
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$broker" \
    --create --topic "$topic" --partitions 1 --replication-factor 1

printf '%s\n' "$record" | /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$broker" --topic "$topic" --sync \
    --command-property max.block.ms=10000 \
    --command-property request.timeout.ms=10000 \
    --command-property delivery.timeout.ms=30000

actual=$(/opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$broker" --topic "$topic" --partition 0 \
    --offset earliest --max-messages 1 --timeout-ms 15000)

if [ "$actual" != "$record" ]; then
    printf '%s\n' 'FAIL: consumed record differs from published record' >&2
    exit 1
fi

printf '%s\n' 'PASS: Kafka topic creation, publication and consumption'
