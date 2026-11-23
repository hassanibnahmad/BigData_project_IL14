from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata

from scripts.bronze.kafka_consumer import (
    DEFAULT_CONSUMER_GROUP_ID,
    KafkaBronzeConsumer,
)


def make_consumer(group_id=None):
    factory = patch("scripts.bronze.kafka_consumer.KafkaConsumer")
    kafka_consumer_class = factory.start()
    kafka_client = MagicMock()
    kafka_consumer_class.return_value = kafka_client
    consumer = KafkaBronzeConsumer(
        topic="positions_vehicules",
        bootstrap_servers="broker:9092",
        group_id=group_id,
    )
    return factory, kafka_consumer_class, kafka_client, consumer


def test_uses_stable_default_group_and_disables_auto_commit():
    factory, kafka_consumer_class, _, consumer = make_consumer(group_id=None)
    try:
        assert consumer.group_id == DEFAULT_CONSUMER_GROUP_ID
        assert kafka_consumer_class.call_args.kwargs["group_id"] == (
            DEFAULT_CONSUMER_GROUP_ID
        )
        assert kafka_consumer_class.call_args.kwargs["enable_auto_commit"] is False
    finally:
        factory.stop()


def test_allows_a_configured_group_id():
    factory, kafka_consumer_class, _, consumer = make_consumer(
        group_id="il14-bronze-test"
    )
    try:
        assert consumer.group_id == "il14-bronze-test"
        assert kafka_consumer_class.call_args.kwargs["group_id"] == (
            "il14-bronze-test"
        )
    finally:
        factory.stop()


def test_consume_retries_an_empty_poll_and_preserves_message_fields():
    factory, _, kafka_client, consumer = make_consumer()
    partition = TopicPartition("positions_vehicules", 2)
    kafka_message = SimpleNamespace(
        topic="positions_vehicules",
        partition=2,
        offset=41,
        timestamp=1730000000000,
        value={"vehicle_id": "bus-17"},
    )
    kafka_client.poll.side_effect = [
        {},
        {partition: [kafka_message]},
        {},
    ]

    try:
        messages = consumer.consume(max_messages=1, timeout_ms=1000)

        assert messages == [
            {
                "topic": "positions_vehicules",
                "partition": 2,
                "offset": 41,
                "timestamp": 1730000000000,
                "value": {"vehicle_id": "bus-17"},
            }
        ]
        assert kafka_client.poll.call_count == 2
        assert kafka_client.poll.call_args_list[0].kwargs["max_records"] == 1
    finally:
        factory.stop()


def test_consume_returns_empty_list_after_empty_poll():
    factory, _, kafka_client, consumer = make_consumer()
    kafka_client.poll.return_value = {}

    try:
        assert consumer.consume(timeout_ms=0) == []
        kafka_client.poll.assert_called_once_with(timeout_ms=0, max_records=100)
    finally:
        factory.stop()


def test_commit_uses_next_offset_for_each_partition():
    factory, _, kafka_client, consumer = make_consumer()
    messages = [
        {"topic": "positions_vehicules", "partition": 0, "offset": 4},
        {"topic": "positions_vehicules", "partition": 0, "offset": 6},
        {"topic": "predictions_arrets", "partition": 1, "offset": 9},
    ]

    try:
        consumer.commit(messages)

        kafka_client.commit.assert_called_once_with(
            offsets={
                TopicPartition("positions_vehicules", 0): OffsetAndMetadata(
                    7, "", -1
                ),
                TopicPartition("predictions_arrets", 1): OffsetAndMetadata(
                    10, "", -1
                ),
            }
        )
    finally:
        factory.stop()


def test_commit_is_a_noop_for_empty_messages():
    factory, _, kafka_client, consumer = make_consumer()

    try:
        consumer.commit([])
        kafka_client.commit.assert_not_called()
    finally:
        factory.stop()
