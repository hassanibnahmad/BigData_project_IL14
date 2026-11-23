import time

from kafka_consumer import KafkaBronzeConsumer
from bronze_writer import BronzeWriter


KAFKA_BOOTSTRAP_SERVERS = "100.77.236.48:39092"

TOPICS = [
    "predictions_arrets",
    "horaires_theoriques",
    "positions_vehicules",
]

BATCH_SIZE = 100
SLEEP_SECONDS = 5


def process_topic(topic, consumer, writer):
    print(f"\nProcessing topic: {topic}")

    messages = consumer.consume(max_messages=BATCH_SIZE)

    if not messages:
        print(f"No messages found for topic {topic}")
        return

    print(f"Messages retrieved: {len(messages)}")

    # Step 1: Write the batch to MinIO
    file_path = writer.write_batch(
        topic=topic,
        messages=messages,
    )

    print(f"Batch successfully stored in MinIO: {file_path}")

    # Step 2: Commit Kafka offsets only after MinIO succeeds
    consumer.commit(messages)

    print(f"Kafka offsets successfully committed for {topic}")
    print(f"Batch processed: {len(messages)} messages")


def main():
    writer = BronzeWriter()

    while True:
        for topic in TOPICS:

            consumer = KafkaBronzeConsumer(
                topic=topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=None,
            )

            try:
                process_topic(topic, consumer, writer)

            except Exception as e:
                print(f"ERROR processing topic {topic}: {e}")

            finally:
                consumer.close()

        print(f"\nSleeping for {SLEEP_SECONDS} seconds...")
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()