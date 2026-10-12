"""Publishing trades to the `trades` topic.

- Each feed has its own partition, chosen explicitly from config/feeds.toml rather than by
  hashing the key, so two feeds can never share a partition.
- The producer is idempotent: if a send is retried after a timeout, the broker discards the
  repeat, so retries can't duplicate or reorder messages within a partition. That covers
  one producer session only. A trade received twice from the exchange (for example on two
  overlapping connections) is still sent twice; removing those duplicates is the job of the
  stream processor and of idempotent storage downstream.
- The Kafka timestamp of each message is the publish time, set by the client.
"""

from __future__ import annotations

import logging

from confluent_kafka import KafkaError, Message, Producer

from ticklab import avro, metrics
from ticklab.model import Trade
from ticklab.settings import Settings, load_feeds

log = logging.getLogger(__name__)


class TradePublisher:
    def __init__(self, settings: Settings, client_id: str) -> None:
        schema_text, parsed = avro.load_schema()
        topic = settings.trades_topic
        schema_id = avro.register(settings.schema_registry_url, avro.subject(topic), schema_text)
        self._encoder = avro.TradeEncoder(schema_id, parsed)
        self._topic = topic
        self._feeds = {(feed.exchange, feed.symbol): feed for feed in load_feeds()}
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "client.id": client_id,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
                "compression.type": "zstd",
            }
        )
        self.schema_id = schema_id
        self.delivered = 0
        self.failed = 0

    def publish(self, trade: Trade) -> None:
        self.publish_encoded(*self.encode(trade))

    def encode(self, trade: Trade) -> tuple[int, bytes, bytes]:
        """The trade as it goes on the topic: (partition, key, value)."""
        feed = self._feeds[(trade.exchange, trade.symbol)]
        return feed.partition, feed.key, self._encoder.encode(trade)

    def publish_encoded(self, partition: int, key: bytes, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    self._topic,
                    value=value,
                    key=key,
                    partition=partition,
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:  # the local queue is full: let deliveries make room
                self._producer.poll(0.1)
        self._producer.poll(0)

    def poll(self, timeout: float = 0) -> None:
        self._producer.poll(timeout)

    def flush(self, timeout: float = 30) -> int:
        """Wait for outstanding deliveries; returns how many are still undelivered."""
        return self._producer.flush(timeout)

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        exchange = (message.key() or b"?:").decode().split(":")[0]
        if error is None:
            self.delivered += 1
            metrics.DELIVERED.labels(exchange).inc()
        else:
            self.failed += 1
            metrics.DELIVERY_ERRORS.labels(exchange).inc()
            log.error("delivery failed: %s", error)
