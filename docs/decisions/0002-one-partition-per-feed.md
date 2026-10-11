# 0002: One partition per feed

**Status:** accepted, milestone 1

## Context

Kafka orders messages only within a partition. Flink tracks event time per partition with a
watermark, and gap detection relies on trade IDs arriving per market in order. With the default
partitioner, a partition is chosen by hashing the key, so two markets can share a partition and
interleave in one log.

## Decision

- The `trades` topic has exactly one partition per feed (one market on one exchange).
- Partition numbers are fixed in `config/feeds.toml` and chosen explicitly by the producer
  (`ticklab/kafka.py`). The key `exchange:symbol` still travels with every message.
- A feed's partition never changes once data exists. New feeds get new partitions, which the
  init job adds (`ticklab/bootstrap.py`); partitions are never removed.

## Consequences

- Each partition is one ordered log with one watermark, which keeps event-time reasoning
  simple.
- Parallelism is limited to the number of feeds (7 in the end). That is plenty for a few
  thousand trades per second.
- A quiet feed holds back its partition's watermark, which Flink's idleness setting handles
  from milestone 2.
