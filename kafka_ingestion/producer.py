#this code is used to get the vehicle positions from the MBTA API and publish them to a Kafka topic. It uses the kafka-python library to create a Kafka producer and send messages to the specified topic. The code also includes error handling and logging to track the status of the producer and any issues that may arise during execution.
#vehicles are normalized using the normalize_vehicle function from the simulator.normalizer module, which converts the raw vehicle data into a standardized format that can be validated against a JSON schema. The producer runs in an infinite loop, polling the MBTA API at regular intervals and publishing valid vehicle events to Kafka while skipping duplicates and invalid records.

import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from jsonschema import Draft202012Validator, FormatChecker
from kafka import KafkaProducer
from kafka.errors import KafkaError

from simulator.mbta_client import MBTAClient
from simulator.normalizer import normalize_vehicle


load_dotenv()

ROOT = Path(__file__).resolve().parents[1]

SCHEMA_PATH = (
    ROOT / "contracts" / "vehicle_position.schema.json"
)

BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC", "positions_vehicules"
)

POLL_INTERVAL = int(
    os.getenv("MBTA_POLL_INTERVAL", "20")
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER.split(","),
        value_serializer=lambda event: json.dumps(
            event
        ).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=3,
        request_timeout_ms=30000
    )


def main():
    schema = json.loads(
        SCHEMA_PATH.read_text(encoding="utf-8")
    )

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )

    client = MBTAClient()
    producer = create_producer()

    # Prevent publishing the same observation
    # repeatedly during this process.
    seen_events = set()

    logger.info("MBTA Kafka producer started")
    logger.info("Broker: %s", BROKER)
    logger.info("Topic: %s", TOPIC)
    logger.info("Polling interval: %s seconds", POLL_INTERVAL)

    try:
        while True:
            try:
                vehicles = client.get_vehicles()

                published = 0
                duplicates = 0
                skipped = 0
                pending = []

                for vehicle in vehicles:
                    try:
                        event = normalize_vehicle(vehicle)

                        if event is None:
                            skipped += 1
                            continue

                        validator.validate(event)

                        event_id = event["event_id"]

                        if event_id in seen_events:
                            duplicates += 1
                            continue

                        future = producer.send(
                            TOPIC,
                            key=event["vehicle_id"],
                            value=event
                        )

                        pending.append((event_id, future))

                    except (ValueError, TypeError) as error:
                        skipped += 1
                        logger.warning(
                            "Invalid vehicle record: %s", error
                        )

                # Wait for Kafka acknowledgments before
                # marking observations as published.
                for event_id, future in pending:
                    try:
                        future.get(timeout=30)
                        seen_events.add(event_id)
                        published += 1
                    except KafkaError as error:
                        logger.error(
                            "Kafka delivery failed: %s", error
                        )

                logger.info(
                    "Received=%d | Published=%d | "
                    "Duplicates=%d | Skipped=%d",
                    len(vehicles),
                    published,
                    duplicates,
                    skipped
                )

            except Exception as error:
                logger.error(
                    "Polling cycle failed: %s", error
                )

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Stopping producer...")

    finally:
        producer.flush(timeout=30)
        producer.close()
        logger.info("Producer stopped")


if __name__ == "__main__":
    main()
