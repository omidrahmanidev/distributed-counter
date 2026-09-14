#!/bin/sh
set -eu
for topic in video-view-events counter-shard-snapshots video-total-snapshots video-view-events.DLT counter-shard-snapshots.DLT video-total-snapshots.DLT; do
 policy=delete
 case "$topic" in video-total-snapshots) policy=compact ;; esac
 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" --create --if-not-exists   --topic "$topic" --partitions "${KAFKA_PARTITIONS:-24}" --replication-factor "${KAFKA_REPLICATION_FACTOR:-1}"   --config cleanup.policy="$policy" --config retention.ms=1209600000
done
