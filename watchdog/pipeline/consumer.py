from __future__ import annotations

import time
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from watchdog.core.logging_setup import get_logger
from watchdog.monitoring.metrics import BACKPRESSURE_ACTIVE

if TYPE_CHECKING:
    from watchdog.core.config import WatchDogConfig


class MicroBatchConsumer:
    def __init__(self, config: WatchDogConfig) -> None:
        self.config = config
        self.logger = get_logger("watchdog.pipeline.consumer")

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
        self._paused = False
        self._last_check = time.monotonic()
        self._records_this_second = 0
        self._rate_window_start = time.monotonic()

        self._ensure_topics_exist()

    def _ensure_topics_exist(self) -> None:
        admin = AdminClient({"bootstrap.servers": self.config.kafka_bootstrap_servers})
        topics = [
            self.config.input_topic,
            self.config.clean_topic,
            self.config.error_topic,
            self.config.rollout.shadow_topic,
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
        self._apply_backpressure()

        messages: list[Message] = []
        deadline = time.monotonic() + (self.batch_timeout_ms / 1000.0)

        while len(messages) < self.batch_size:
            if self._rate_limited():
                time.sleep(0.05)
                if time.monotonic() > deadline:
                    break
                continue

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
            self._records_this_second += 1

        return messages

    def _apply_backpressure(self) -> None:
        bp = self.config.backpressure
        should_pause = False

        if bp.throttle_on_lag > 0:
            lag = self._consumer_lag()
            if lag > bp.throttle_on_lag:
                should_pause = True

        if should_pause and not self._paused:
            partitions = self.consumer.assignment()
            if partitions:
                self.consumer.pause(partitions)
                self._paused = True
                BACKPRESSURE_ACTIVE.set(1)
                self.logger.warning(
                    "backpressure_engaged",
                    lag=self._consumer_lag(),
                )
        elif not should_pause and self._paused:
            partitions = self.consumer.assignment()
            if partitions:
                self.consumer.resume(partitions)
                self._paused = False
                BACKPRESSURE_ACTIVE.set(0)
                self.logger.info("backpressure_released")

    def _consumer_lag(self) -> int:
        try:
            partitions = self.consumer.assignment()
            if not partitions:
                return 0
            committed = self.consumer.committed(partitions, timeout=5)
            if not committed:
                return 0

            committed_offsets: dict[str, int] = {}
            for tp in committed:
                key = f"{tp.topic}-{tp.partition}"
                committed_offsets[key] = tp.offset

            lag = 0
            for tp in partitions:
                lo, hi = self.consumer.get_watermark_offsets(
                    tp, timeout=5, cached=False
                )
                key = f"{tp.topic}-{tp.partition}"
                cur_off = committed_offsets.get(key, 0)
                lag += max(0, hi - cur_off)
            return lag
        except Exception:
            return 0

    def _rate_limited(self) -> bool:
        limit = self.config.backpressure.rate_limit_records_per_sec
        if limit <= 0:
            return False

        now = time.monotonic()
        elapsed = now - self._rate_window_start
        if elapsed >= 1.0:
            self._records_this_second = 0
            self._rate_window_start = now
            return False

        return self._records_this_second >= limit

    def commit(self) -> None:
        self.consumer.commit(asynchronous=False)

    def close(self) -> None:
        try:
            self.consumer.close()
        except Exception:
            pass
