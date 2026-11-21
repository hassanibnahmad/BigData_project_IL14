"""Read a Kafka snapshot, validate events, and verify stop-time joins."""

import argparse
from collections import Counter
import json
import time

from jsonschema import Draft202012Validator, FormatChecker
from kafka import KafkaConsumer, TopicPartition

from kafka_ingestion.producer import BROKER, SCHEMA_PATH
from kafka_ingestion.stop_times_producer import TOPICS, validator_for
from simulator.stop_times import STOP_FIELDS


def join_key(event):
    if event["schedule_id"] and event["service_date"]:
        return (event["schedule_id"], event["service_date"], *(event[key] for key in STOP_FIELDS))
    return None


def read_snapshot(consumer, topic, timeout):
    partitions = [
        TopicPartition(topic, number)
        for number in sorted(consumer.partitions_for_topic(topic) or [])
    ]
    if not partitions:
        raise SystemExit(f"Cannot find Kafka partitions for {topic}")
    consumer.assign(partitions)
    ends = consumer.end_offsets(partitions)
    consumer.seek_to_beginning(*partitions)
    deadline = time.monotonic() + timeout
    while any(consumer.position(partition) < ends[partition] for partition in partitions):
        if time.monotonic() >= deadline:
            raise SystemExit(f"Timed out reading {topic}")
        for partition, messages in consumer.poll(timeout_ms=500).items():
            for message in messages:
                if message.offset < ends[partition]:
                    yield json.loads(message.value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vehicles", action="store_true", help="Also validate the vehicle topic")
    parser.add_argument("--timeout", type=int, default=60, help="Read deadline in seconds per topic")
    args = parser.parse_args()
    topic_validators = {TOPICS[kind]: validator_for(kind) for kind in TOPICS}
    if args.vehicles:
        topic_validators["positions_vehicules"] = Draft202012Validator(
            json.loads(SCHEMA_PATH.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )

    schedules = {}
    sample = None
    vehicle_sample = None
    counts = Counter()
    consumer = KafkaConsumer(
        bootstrap_servers=BROKER.split(","),
        group_id=None,
        enable_auto_commit=False,
        fetch_max_bytes=1024 * 1024,
        max_partition_fetch_bytes=256 * 1024,
        receive_message_max_bytes=2 * 1024 * 1024,
    )
    try:
        for topic, validator in topic_validators.items():
            for event in read_snapshot(consumer, topic, args.timeout):
                validator.validate(event)
                counts[topic] += 1
                if topic == TOPICS["schedule"]:
                    schedules[join_key(event)] = event
                elif topic == TOPICS["prediction"]:
                    schedule = schedules.get(join_key(event))
                    if schedule and any(
                        schedule[f"scheduled_{side}"] and event[f"predicted_{side}"]
                        for side in ("arrival", "departure")
                    ):
                        counts["comparable_predictions"] += 1
                        sample = {"prediction": event, "schedule": schedule}
                else:
                    vehicle_sample = event
    finally:
        consumer.close()

    print(json.dumps({"counts": counts, "matched_sample": sample, "vehicle_sample": vehicle_sample}, indent=2))
    if not sample or (args.vehicles and not vehicle_sample):
        raise SystemExit("Expected Kafka evidence was missing")


if __name__ == "__main__":
    main()
