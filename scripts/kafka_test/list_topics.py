from kafka.admin import KafkaAdminClient

BOOTSTRAP_SERVERS = "100.105.157.123:39092"

def list_kafka_topics():
    print("Connecting to Kafka broker...")

    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="student_b_topic_checker",
        request_timeout_ms=10000
    )

    topics = admin_client.list_topics()

    print("\nConnection successful!")
    print("\nAvailable Kafka topics:")

    for topic in sorted(topics):
        print(f"- {topic}")

    admin_client.close()

if __name__ == "__main__":
    list_kafka_topics()