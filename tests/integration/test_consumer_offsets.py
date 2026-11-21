"""Step 6.6: Kafka consumer offsets, restart, and delivery tests."""
import json
import os
import time
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import OffsetAndMetadata

BROKER = os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


@pytest.fixture
def kafka_topic():
    name = f"il14-offset-test-{uuid.uuid4().hex[:12]}"
    admin = KafkaAdminClient(bootstrap_servers=BROKER)
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)])
        yield name
    finally:
        admin.close()


def publish_events(topic, count):
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
    try:
        return [
            producer.send(topic, value={"sequence": i}).get(timeout=15)
            for i in range(count)
        ]
    finally:
        producer.close()


def make_consumer(group):
    return KafkaConsumer(
        bootstrap_servers=BROKER,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def read_n(consumer, expected, timeout=15):
    messages = []
    deadline = time.monotonic() + timeout
    while len(messages) < expected and time.monotonic() < deadline:
        for batch in consumer.poll(timeout_ms=500).values():
            messages.extend(batch)
    assert len(messages) == expected, f"Expected {expected} records, got {len(messages)}"
    return messages


def test_explicit_offset_commit(kafka_topic):
    """An explicit commit stores the next offset, not the last processed offset."""
    metadata = publish_events(kafka_topic, 3)
    group = f"il14-offset-group-{uuid.uuid4().hex[:12]}"
    consumer = make_consumer(group)
    try:
        tp = TopicPartition(kafka_topic, 0)
        consumer.assign([tp])
        messages = read_n(consumer, 3)
        assert [m.value["sequence"] for m in messages] == [0, 1, 2]
        next_offset = metadata[-1].offset + 1
        consumer.commit({tp: OffsetAndMetadata(next_offset, "", -1)})
        assert consumer.committed(tp) == next_offset
        print(f"\nExplicit commit PASSED: next offset={next_offset}")
    finally:
        consumer.close()


def test_restart_resumes_from_committed_offset(kafka_topic):
    """A restarted consumer group skips committed messages and reads new ones."""
    publish_events(kafka_topic, 2)
    group = f"il14-restart-group-{uuid.uuid4().hex[:12]}"
    tp = TopicPartition(kafka_topic, 0)

    first = make_consumer(group)
    try:
        first.assign([tp])
        initial = read_n(first, 2)
        assert [m.value["sequence"] for m in initial] == [0, 1]
        first.commit({tp: OffsetAndMetadata(initial[-1].offset + 1, "", -1)})
    finally:
        first.close()

    publish_events(kafka_topic, 1)  # This new record has sequence 0.
    second = make_consumer(group)
    try:
        second.assign([tp])
        resumed = read_n(second, 1)
        assert resumed[0].offset == initial[-1].offset + 1
        assert resumed[0].value == {"sequence": 0}
    finally:
        second.close()
