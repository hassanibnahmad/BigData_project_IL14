import json
from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = "100.105.157.123:39092"
TOPIC = "horaires_theoriques"

def consume_sample():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        consumer_timeout_ms=10000
    )

    try:
        partitions = consumer.partitions_for_topic(TOPIC)

        if not partitions:
            print(f"No partitions found for topic: {TOPIC}")
            return

        topic_partitions = [
            TopicPartition(TOPIC, partition)
            for partition in sorted(partitions)
        ]

        consumer.assign(topic_partitions)

        for tp in topic_partitions:
            consumer.seek(tp, 0)

        print(f"Topic: {TOPIC}")
        print(f"Partitions: {len(topic_partitions)}")
        print("\nReading messages...\n")

        records = consumer.poll(
            timeout_ms=10000,
            max_records=5
        )

        count = 0

        for partition, messages in records.items():
            for message in messages:
                print(f"Message {count + 1}")
                print(f"Partition: {message.partition}")
                print(f"Offset: {message.offset}")

                try:
                    data = json.loads(message.value.decode("utf-8"))
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    print(message.value)

                print("-" * 60)
                count += 1

        print(f"\nTotal messages read: {count}")

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_sample()