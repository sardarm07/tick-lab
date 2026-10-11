# 0001: Avro through the schema registry, with exact decimals

**Status:** accepted, milestone 1

## Context

Trades are produced in Python and read by Flink (Java) and ClickHouse. All three must agree on
the format, and the project's main claim is that its candles equal the exchanges' candles
exactly. Exchanges publish prices and quantities as decimal text (Binance: `"83040.01000000"`).
Binary floating point can't represent most of those values, so sums of float quantities
drift, and an exact match becomes impossible to prove.

## Decision

- Messages are Avro records (`schemas/trade.avsc`) in the Confluent wire format: a zero byte,
  the schema ID, then the record (`ticklab/avro.py`). The schema lives in Redpanda's schema
  registry under the subject `trades-value`.
- `price` and `quantity` are Avro `decimal(28, 12)`: up to 16 digits before the point and 12
  after. Values outside that range are rejected when parsed (`ticklab/model.py`), never rounded.
- Timestamps are `timestamp-micros`.
- The Avro encoding is written with `fastavro`, and the schema is registered over the
  registry's HTTP API with `httpx`. `confluent-kafka` is used only for its Kafka client.

## Consequences

- Python uses `Decimal`, ClickHouse `Decimal(28, 12)`, and Flink will use `BigDecimal`.
  Floats appear only in statistics.
- ClickHouse reads the topic directly with its `AvroConfluent` format, resolving schemas by ID.
- Adding a field with a default is backward compatible; renaming or retyping one needs a new
  schema version.
