from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = "100.105.157.123:39092"

TOPICS = [
    "positions_vehicules",
    "predictions_arrets",
    "horaires_theoriques",
]

consumer = KafkaConsumer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    enable_auto_commit=False,
)

print("Kafka connected successfully!\n")

for topic in TOPICS:
    partitions = consumer.partitions_for_topic(topic)

    if not partitions:
        print(f"\nTopic: {topic}")
        print("No partitions found.")
        continue

    print(f"\nTopic: {topic}")

    topic_partitions = [
        TopicPartition(topic, partition)
        for partition in partitions
    ]

    beginning_offsets = consumer.beginning_offsets(topic_partitions)
    end_offsets = consumer.end_offsets(topic_partitions)

    total_messages = 0

    for tp in topic_partitions:
        start = beginning_offsets[tp]
        end = end_offsets[tp]
        count = end - start

        total_messages += count

        print(
            f"Partition {tp.partition}: "
            f"beginning={start}, end={end}, messages={count}"
        )

    print(f"Total messages in topic: {total_messages}")

consumer.close()