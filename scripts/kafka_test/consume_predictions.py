import json
from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = "100.105.157.123:39092"
TOPIC = "predictions_arrets"

def consume_sample():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        consumer_timeout_ms=15000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    try:
        partitions = consumer.partitions_for_topic(TOPIC)

        topic_partitions = [
            TopicPartition(TOPIC, partition)
            for partition in partitions
        ]

        consumer.assign(topic_partitions)

        # Lire explicitement à partir du début de chaque partition
        for partition in topic_partitions:
            consumer.seek(partition, 0)

        print(f"Topic: {TOPIC}")
        print(f"Partitions: {len(topic_partitions)}")
        print("Reading first messages...\n")

        count = 0

        for message in consumer:
            print(f"Message {count + 1}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(json.dumps(message.value, indent=2, ensure_ascii=False))
            print("-" * 60)

            count += 1

            if count >= 5:
                break

        print(f"\nTotal messages read: {count}")

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_sample()