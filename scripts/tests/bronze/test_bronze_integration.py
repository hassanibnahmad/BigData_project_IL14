import sys
from pathlib import Path

BRONZE_DIR = Path(__file__).resolve().parents[2] / "bronze"
sys.path.insert(0, str(BRONZE_DIR))

from kafka_consumer import KafkaBronzeConsumer
from bronze_writer import BronzeWriter

KAFKA_BOOTSTRAP_SERVERS = "100.77.236.48:39092"
TOPIC = "predictions_arrets"
BATCH_SIZE = 5


def main():
    consumer = KafkaBronzeConsumer(
        topic=TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=None,
    )

    writer = BronzeWriter()

    try:
        print("Starting Bronze integration test...")
        print(f"Topic: {TOPIC}")

        # Step 1: Consume messages from Kafka
        messages = consumer.consume(max_messages=BATCH_SIZE)

        print(f"Messages received: {len(messages)}")

        if not messages:
            print("No messages received. Nothing to write or commit.")
            return

        # Step 2: Write messages to MinIO
        file_path = writer.write_batch(
            topic=TOPIC,
            messages=messages,
        )

        if not file_path:
            raise RuntimeError("MinIO write failed: no file path returned.")

        print(f"MinIO write successful: {file_path}")

        # Step 3: Commit Kafka offsets after successful storage
        consumer.commit(messages)

        print("Kafka commit successful.")
        print("BRONZE INTEGRATION TEST PASSED.")

    except Exception as e:
        print(f"BRONZE INTEGRATION TEST FAILED: {e}")
        raise

    finally:
        consumer.close()


if __name__ == "__main__":
    main()