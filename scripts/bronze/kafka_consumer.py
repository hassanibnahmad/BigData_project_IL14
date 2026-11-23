import json
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata


DEFAULT_CONSUMER_GROUP_ID = "il14-bronze"
_POLL_SLICE_MS = 500


class KafkaBronzeConsumer:
    def __init__(self, topic, bootstrap_servers, group_id=DEFAULT_CONSUMER_GROUP_ID):
        self.topic = topic
        self.group_id = group_id or DEFAULT_CONSUMER_GROUP_ID

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            consumer_timeout_ms=1000,
        )

    def consume(self, max_messages=100, timeout_ms=5000):
        """Poll assigned partitions and return up to ``max_messages`` records.

        The timeout is the maximum time to wait for records when the consumer
        has not received any yet. Empty polls during group assignment are safe:
        polling continues until records arrive or the timeout expires.
        """
        messages = []
        if max_messages <= 0:
            return messages

        timeout_ms = max(0, timeout_ms)
        deadline = time.monotonic() + timeout_ms / 1000
        first_poll = True

        while len(messages) < max_messages:
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            if not first_poll and remaining_ms == 0:
                break

            records = self.consumer.poll(
                timeout_ms=min(_POLL_SLICE_MS, remaining_ms),
                max_records=max_messages - len(messages),
            )
            first_poll = False

            if not records:
                if messages or time.monotonic() >= deadline:
                    break
                continue

            for partition_records in records.values():
                for message in partition_records:
                    messages.append(
                        {
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "timestamp": message.timestamp,
                            "value": message.value,
                        }
                    )

                    if len(messages) >= max_messages:
                        break

                if len(messages) >= max_messages:
                    break

        return messages

    def commit(self, messages):
        """Commit the next offset for each partition represented in messages.

        Call this only after the supplied records have been durably written.
        Empty input is a no-op. Commit failures are allowed to propagate so the
        caller can report them and Kafka can redeliver the uncommitted records.
        """
        offsets = {}
        for message in messages:
            partition = TopicPartition(message["topic"], message["partition"])
            next_offset = message["offset"] + 1
            current = offsets.get(partition)
            if current is None or next_offset > current.offset:
                offsets[partition] = OffsetAndMetadata(next_offset, "", -1)

        if offsets:
            self.consumer.commit(offsets=offsets)

    def close(self):
        self.consumer.close()
