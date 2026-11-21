import json
import uuid

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

from kafka_ingestion import stop_times_producer as stop_times
from kafka_ingestion.producer import create_producer
from tests.integration.test_kafka_integration import BROKER


def test_stop_time_publish_and_consume(monkeypatch):
    suffix = uuid.uuid4().hex
    topics = {kind: f"il14-{kind}-test-{suffix}" for kind in stop_times.TOPICS}
    monkeypatch.setattr(stop_times, "TOPICS", topics)
    monkeypatch.setattr(stop_times, "BROKER", BROKER)
    monkeypatch.setattr("kafka_ingestion.producer.BROKER", BROKER)
    stop_times.ensure_topics()
    stop_times.ensure_topics()
    admin = KafkaAdminClient(bootstrap_servers=BROKER)
    try:
        for description in admin.describe_topics(list(topics.values())):
            assert len(description["partitions"]) == 3
            assert all(len(partition["replicas"]) == 1 for partition in description["partitions"])
    finally:
        admin.close()

    producer = create_producer()
    consumer = KafkaConsumer(bootstrap_servers=BROKER, group_id=None, enable_auto_commit=False)
    try:
        publisher = stop_times.StopTimePublisher(producer)
        for kind, topic in topics.items():
            path = stop_times.ROOT / "contracts" / f"{kind}.example.json"
            event = json.loads(path.read_text(encoding="utf-8"))
            assert publisher.publish(kind, [event, event]) == {"published": 1, "duplicate": 1}
            partitions = [TopicPartition(topic, index) for index in range(3)]
            consumer.assign(partitions)
            consumer.seek_to_beginning(*partitions)
            batches = consumer.poll(timeout_ms=10000)
            messages = [message for batch in batches.values() for message in batch]
            assert len(messages) == 1
            assert messages[0].key.decode() == event["schedule_id"]
            incoming = json.loads(messages[0].value)
            publisher.validators[kind].validate(incoming)
            assert incoming == event
    finally:
        producer.close()
        consumer.close()
