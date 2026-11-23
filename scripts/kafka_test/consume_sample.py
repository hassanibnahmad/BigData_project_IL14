import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "100.105.157.123:39092"
TOPIC = "positions_vehicules"

def consume_sample():
    print(f"Connecting to Kafka...")
    print(f"Topic: {TOPIC}")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="student_b_sample_reader",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    print("\nKafka connection established!")
    print("Reading messages...\n")

    count = 0

    try:
        for message in consumer:
            print(f"Message {count + 1}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print("Data:")

            print(json.dumps(
                message.value,
                indent=2,
                ensure_ascii=False
            ))

            print("-" * 60)

            count += 1

            if count >= 5:
                break

    finally:
        consumer.close()

    print(f"\nTotal messages read: {count}")

if __name__ == "__main__":
    consume_sample()