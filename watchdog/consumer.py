from __future__ import annotations

import time
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from watchdog.logging_setup import get_logger

if TYPE_CHECKING:
    from watchdog.config import WatchDogConfig


class MicroBatchConsumer:
    def __init__(self, config: WatchDogConfig) -> None:
        self.config = config
        self.logger = get_logger("watchdog.consumer")

        consumer_conf = {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "group.id": config.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 600000,
            "session.timeout.ms": 30000,
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([config.input_topic])
        self.batch_size = config.batch_size
        self.batch_timeout_ms = config.batch_timeout_ms

        self._ensure_topics_exist()

    def _ensure_topics_exist(self) -> None:
        admin = AdminClient({"bootstrap.servers": self.config.kafka_bootstrap_servers})
        topics = [
            self.config.input_topic,
            self.config.clean_topic,
            self.config.error_topic,
        ]
        existing = set(admin.list_topics(timeout=10).topics.keys())
        new_topics = [
            NewTopic(t, num_partitions=3, replication_factor=1)
            for t in topics
            if t not in existing
        ]
        if new_topics:
            futures = admin.create_topics(new_topics)
            for topic_name, future in futures.items():
                try:
                    future.result()
                    self.logger.info("topic_created", topic=topic_name)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        self.logger.warning(
                            "topic_create_failed", topic=topic_name, error=str(e)
                        )

    def poll_batch(self) -> list[Message]:
        messages: list[Message] = []
        deadline = time.monotonic() + (self.batch_timeout_ms / 1000.0)

        while len(messages) < self.batch_size:
            remaining = (deadline - time.monotonic()) * 1000
            if remaining <= 0:
                break

            msg = self.consumer.poll(timeout=max(remaining, 0.0) / 1000.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            messages.append(msg)

        return messages

    def commit(self) -> None:
        self.consumer.commit(asynchronous=False)

    def close(self) -> None:
        try:
            self.consumer.close()
        except Exception:
            pass
