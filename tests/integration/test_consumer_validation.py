
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


ROOT = Path(__file__).resolve().parents[2]
BROKER = os.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

EXAMPLE_PATH = (
    ROOT / "contracts" / "vehicle_position.example.json"
)


def test_consumer_rejects_invalid_event():
    topic = f"il14-consumer-test-{uuid.uuid4().hex[:12]}"
    group = f"il14-consumer-test-{uuid.uuid4().hex[:12]}"

    # STEP 1: Create isolated Kafka topic
    admin = KafkaAdminClient(
        bootstrap_servers=BROKER
    )

    producer = None
    process = None

    try:
        admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=1,
                replication_factor=1
            )
        ])

        print("\n[1] Test topic created:", topic)

        # STEP 2: Prepare test events
        valid_event = json.loads(
            EXAMPLE_PATH.read_text(encoding="utf-8")
        )

        valid_event["event_id"] = str(uuid.uuid4())

        invalid_event = valid_event.copy()
        invalid_event["event_id"] = str(uuid.uuid4())
        invalid_event["latitude"] = 120

        # STEP 3: Start the actual consumer
        env = os.environ.copy()
        env["KAFKA_BOOTSTRAP_SERVERS"] = BROKER
        env["KAFKA_TOPIC"] = topic
        env["KAFKA_CONSUMER_GROUP"] = group
        env["KAFKA_AUTO_OFFSET_RESET"] = "earliest"

        with tempfile.TemporaryFile(mode="w+t") as log_file:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-u",
                    str(ROOT / "kafka_ingestion" / "consumer.py")
                ],
                cwd=ROOT,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT
            )

            print("[2] Real consumer started")

            # STEP 4: Publish invalid and valid events
            producer = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda value: json.dumps(
                    value
                ).encode("utf-8"),
                acks="all"
            )

            producer.send(
                topic,
                value=invalid_event
            ).get(timeout=15)

            producer.send(
                topic,
                value=valid_event
            ).get(timeout=15)

            print("[3] Invalid event published")
            print("[4] Valid event published")

            # STEP 5: Wait for consumer logs
            deadline = time.monotonic() + 30
            output = ""

            while time.monotonic() < deadline:
                log_file.seek(0)
                output = log_file.read()

                if (
                    "Invalid message at partition=" in output
                    and f"Vehicle={valid_event['vehicle_id']}" in output
                    and "Summary | Processed=1 | Invalid=1" in output
                ):
                    break

                if process.poll() is not None:
                    break

                time.sleep(0.5)

            print("\n--- CONSUMER LOGS ---")
            print(output)
            print("--- END LOGS ---\n")

            # STEP 6: Verify results
            assert "Invalid message at partition=" in output, (
                "Consumer did not report the invalid event"
            )

            assert f"Vehicle={valid_event['vehicle_id']}" in output, (
                "Consumer did not process the valid event"
            )

            assert "Processed=1" in output, (
                "Expected one valid event to be processed"
            )

            assert "Invalid=1" in output, (
                "Expected one invalid event to be detected"
            )

            print("[5] Invalid event correctly rejected")
            print("[6] Valid event correctly processed")
            print("\nCONSUMER VALIDATION TEST PASSED")

    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

        if producer is not None:
            producer.close()

        admin.close()
