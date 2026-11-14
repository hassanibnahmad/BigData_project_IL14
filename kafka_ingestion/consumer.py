#this script is a Kafka consumer that reads vehicle position events from a Kafka topic, validates them against a JSON schema (./contracts/vehicle_position.schema.json), and logs the results. It uses the `kafka-python` library to connect to the Kafka broker and consume messages. The consumer is configured to read from a specified topic, with options for group ID and offset reset behavior. Valid messages are logged with their details, while invalid messages are logged as warnings without committing their offsets. The script also handles graceful shutdown on keyboard interrupt and logs summary statistics of processed and invalid messages.

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from jsonschema import Draft202012Validator, FormatChecker
from kafka import KafkaConsumer
from kafka.errors import KafkaError


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

GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP", "il14-python-consumer"
)

OFFSET_RESET = os.getenv(
    "KAFKA_AUTO_OFFSET_RESET", "earliest"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER.split(","),
        group_id=GROUP_ID,
        auto_offset_reset=OFFSET_RESET,
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=1000
    )


def main():
    schema = json.loads(
        SCHEMA_PATH.read_text(encoding="utf-8")
    )

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker()
    )

    consumer = create_consumer()

    processed = 0
    invalid = 0

    logger.info("Kafka consumer started")
    logger.info("Broker: %s", BROKER)
    logger.info("Topic: %s", TOPIC)
    logger.info("Group: %s", GROUP_ID)

    try:
        while True:
            for message in consumer:
                try:
                    event = message.value

                    validator.validate(event)

                    processed += 1

                    logger.info(
                        "Vehicle=%s | Route=%s | "
                        "Lat=%s | Lon=%s | "
                        "Partition=%s | Offset=%s",
                        event["vehicle_id"],
                        event["route_id"],
                        event["latitude"],
                        event["longitude"],
                        message.partition,
                        message.offset
                    )

                    # Commit after successful validation.
                    consumer.commit()

                except Exception as error:
                    invalid += 1
                    logger.warning(
                        "Invalid message at partition=%s "
                        "offset=%s: %s",
                        message.partition,
                        message.offset,
                        error
                    )

                    # Do not commit invalid messages.
                    # A production consumer should use a
                    # dead-letter topic and explicit retries.

            if processed or invalid:
                logger.info(
                    "Summary | Processed=%d | Invalid=%d",
                    processed,
                    invalid
                )

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    except KafkaError as error:
        logger.error("Kafka consumer error: %s", error)

    finally:
        consumer.close()
        logger.info(
            "Consumer stopped | Processed=%d | Invalid=%d",
            processed,
            invalid
        )


if __name__ == "__main__":
    main()
