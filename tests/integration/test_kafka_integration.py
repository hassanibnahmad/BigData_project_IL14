#this file is an integration test that verifies the end-to-end functionality of producing and consuming vehicle position events using Kafka. It performs the following steps:
import json
import time
import uuid
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from kafka import KafkaConsumer, KafkaProducer


# ============================================================
# 1. CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = (
    ROOT / "contracts" / "vehicle_position.schema.json"
)

EXAMPLE_PATH = (
    ROOT / "contracts" / "vehicle_position.example.json"
)

BROKER = "localhost:9092"
TOPIC = "positions_vehicules_test"


# ============================================================
# 2. INTEGRATION TEST: PRODUCE -> KAFKA -> CONSUME
# ============================================================

def test_kafka_publish_consume():

    # STEP 1: Load JSON Schema
    schema = json.loads(
        SCHEMA_PATH.read_text(encoding="utf-8")
    )

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )

    # STEP 2: Load example vehicle event
    event = json.loads(
        EXAMPLE_PATH.read_text(encoding="utf-8")
    )

    # Unique event ID to identify this test message
    event["event_id"] = str(uuid.uuid4())

    # Validate event before publishing
    validator.validate(event)

    print("\n[1] JSON Schema validation PASSED")

    # STEP 3: Create Kafka consumer
    consumer = KafkaConsumer(
        bootstrap_servers=BROKER,
        group_id=f"il14-integration-{uuid.uuid4()}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        )
    )

    producer = None

    try:
        # STEP 4: Subscribe to the test topic
        consumer.subscribe([TOPIC])

        # Wait until partitions are assigned
        deadline = time.monotonic() + 15

        while not consumer.assignment():
            consumer.poll(timeout_ms=500)

            if time.monotonic() >= deadline:
                raise AssertionError(
                    "Kafka partition assignment timed out"
                )

        partitions = consumer.assignment()

        print("[2] Assigned partitions:", partitions)

        # Skip messages from previous test runs
        consumer.seek_to_end(*partitions)

        # Resolve positions before publishing
        for partition in partitions:
            consumer.position(partition)

        print("[3] Consumer ready")

        # STEP 5: Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all"
        )

        # STEP 6: Publish event
        metadata = producer.send(
            TOPIC,
            key=event["vehicle_id"],
            value=event
        ).get(timeout=15)

        print(
            f"[4] Published | "
            f"Partition={metadata.partition} | "
            f"Offset={metadata.offset}"
        )

        # STEP 7: Consume event
        received = None
        received_partition = None
        received_offset = None

        deadline = time.monotonic() + 15

        while time.monotonic() < deadline:
            records = consumer.poll(timeout_ms=1000)

            for messages in records.values():
                for message in messages:

                    if (
                        message.value.get("event_id")
                        == event["event_id"]
                    ):
                        received = message.value
                        received_partition = message.partition
                        received_offset = message.offset
                        break

                if received is not None:
                    break

            if received is not None:
                break

        # STEP 8: Assertions
        assert received is not None, (
            "Kafka did not return the published test event"
        )

        validator.validate(received)

        assert received == event, (
            "Received event differs from published event"
        )

        assert received_partition == metadata.partition
        assert received_offset == metadata.offset

        print("[5] Event successfully consumed")
        print("[6] JSON Schema validation PASSED")
        print("[7] Published and received events MATCH")

        print("\nVehicle:", received["vehicle_id"])
        print("Route:", received["route_id"])
        print("Latitude:", received["latitude"])
        print("Longitude:", received["longitude"])
        print("Partition:", received_partition)
        print("Offset:", received_offset)

        print("\nINTEGRATION TEST PASSED")

    finally:
        if producer is not None:
            producer.close()

        consumer.close()
