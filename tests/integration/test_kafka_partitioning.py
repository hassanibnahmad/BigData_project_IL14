"""Step 6.5: Kafka partition consistency and per-partition ordering."""
import json
import os
import time
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

BROKER = os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


@pytest.fixture
def kafka_topic():
    name = f"il14-partition-test-{uuid.uuid4().hex[:12]}"
    admin = KafkaAdminClient(bootstrap_servers=BROKER)
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=3, replication_factor=1)])
        yield name
    finally:
        admin.close()


def make_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )


def read_partition(topic, partition, expected_count):
    """Read from a known partition without consumer-group assignment races."""
    consumer = KafkaConsumer(
        bootstrap_servers=BROKER,
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    try:
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        consumer.seek_to_beginning(tp)
        messages = []
        deadline = time.monotonic() + 15
        while len(messages) < expected_count and time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=500).values():
                messages.extend(batch)
        return messages
    finally:
        consumer.close()


def test_same_vehicle_key_uses_same_partition(kafka_topic):
    """All messages for one vehicle key must map to the same partition."""
    producer = make_producer()
    try:
        metadata = [
            producer.send(
                kafka_topic,
                key="vehicle-66",
                value={"vehicle_id": "vehicle-66", "sequence": seq},
            ).get(timeout=15)
            for seq in range(5)
        ]
    finally:
        producer.close()

    partitions = {item.partition for item in metadata}
    assert len(partitions) == 1, f"Same key mapped to multiple partitions: {partitions}"
    print(f"\nPartition consistency PASSED: partition={metadata[0].partition}")


def test_messages_keep_order_within_partition(kafka_topic):
    """Messages for the same vehicle are consumed in publish order."""
    producer = make_producer()
    try:
        metadata = [
            producer.send(
                kafka_topic,
                key="vehicle-77",
                value={"vehicle_id": "vehicle-77", "sequence": seq},
            ).get(timeout=15)
            for seq in range(5)
        ]
    finally:
        producer.close()

    partition = metadata[0].partition
    assert all(item.partition == partition for item in metadata)
    messages = read_partition(kafka_topic, partition, expected_count=5)
    assert len(messages) == 5, f"Expected 5 messages, received {len(messages)}"
    assert [msg.value["sequence"] for msg in messages] == list(range(5))
    assert [msg.offset for msg in messages] == [item.offset for item in metadata]
    print(f"\nOrdering PASSED: partition={partition}, sequences=0..4")
