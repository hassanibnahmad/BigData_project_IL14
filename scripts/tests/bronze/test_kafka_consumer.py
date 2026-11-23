from kafka_consumer import KafkaBronzeConsumer

KAFKA_BOOTSTRAP_SERVERS = "100.77.236.48:39092"

consumer = KafkaBronzeConsumer(
    topic="predictions_arrets",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=None,
)

try:
    messages = consumer.consume(max_messages=5, timeout_ms=5000)

    print(f"\nMessages récupérés : {len(messages)}")

    for message in messages:
        print("\n----------------------------")
        print(f"Topic     : {message['topic']}")
        print(f"Partition : {message['partition']}")
        print(f"Offset   : {message['offset']}")
        print(f"Data    : {message['value']}")

finally:
    consumer.close()