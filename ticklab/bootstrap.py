"""Prepares a stack before anything is published: topic, schema and ClickHouse tables.

It runs at every start (the Compose `init` service) and is safe to repeat: existing objects
are left alone, and the topic only ever gains partitions.
"""

from __future__ import annotations

import logging
import time

from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

from ticklab import avro
from ticklab.clickhouse import ClickHouse
from ticklab.settings import ROOT, Settings, load_feeds

log = logging.getLogger(__name__)

SQL_DIR = ROOT / "infra" / "clickhouse" / "sql"
TRADES_RETENTION_MS = 24 * 3600 * 1000
# Topics the Flink jobs write to; one partition each is plenty for a few rows a second.
OUTPUT_TOPICS = ("candles", "late-trades", "gaps", "feed-health", "spread-1s", "alerts")


def run(settings: Settings) -> None:
    partitions = len(load_feeds())
    ensure_topic(settings, settings.trades_topic, partitions)
    for topic in OUTPUT_TOPICS:
        ensure_topic(settings, topic, 1)
    schema_text, _ = avro.load_schema()
    topic_subject = avro.subject(settings.trades_topic)
    schema_id = avro.register(settings.schema_registry_url, topic_subject, schema_text)
    log.info("schema %s registered with ID %d", topic_subject, schema_id)
    apply_sql(ClickHouse(settings), settings.stack)


def ensure_topic(settings: Settings, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    metadata = admin.list_topics(timeout=30)
    existing = metadata.topics.get(topic)
    if existing is None or existing.error is not None:
        new = NewTopic(
            topic,
            num_partitions=partitions,
            replication_factor=1,
            config={"retention.ms": str(TRADES_RETENTION_MS), "cleanup.policy": "delete"},
        )
        admin.create_topics([new])[topic].result()
        log.info("created topic %s with %d partitions", topic, partitions)
    elif len(existing.partitions) < partitions:
        admin.create_partitions([NewPartitions(topic, partitions)])[topic].result()
        log.info("topic %s now has %d partitions", topic, partitions)
    else:
        log.info("topic %s exists with %d partitions", topic, len(existing.partitions))


def apply_sql(clickhouse: ClickHouse, stack: str) -> None:
    """Run every statement in infra/clickhouse/sql, in file order, then label the stack."""
    for path in sorted(SQL_DIR.glob("*.sql")):
        for statement in path.read_text().split(";\n"):
            if _has_code(statement):
                clickhouse.execute(statement)
        log.info("applied %s", path.name)
    # Lets tools check which stack they are talking to before writing anything.
    clickhouse.execute("TRUNCATE TABLE ticklab.stack")
    clickhouse.execute("INSERT INTO ticklab.stack SELECT {name:String}", name=stack)


def _has_code(statement: str) -> bool:
    return any(
        line.strip() and not line.strip().startswith("--") for line in statement.splitlines()
    )


def wait_and_run(settings: Settings, attempts: int = 30) -> None:
    for attempt in range(1, attempts + 1):
        try:
            run(settings)
            return
        except Exception as exc:
            if attempt == attempts:
                raise
            log.warning("init attempt %d failed (%r); retrying in 2s", attempt, exc)
            time.sleep(2)
