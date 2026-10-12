# 0017: The signals job: Flink SQL for bars and joins, DataStream for alerts

**Status:** accepted, milestone 4

## Context

The signals compare the same assets on two exchanges, each second: BTC, ETH and SOL against
USDT on Binance, against USD on Kraken. Kraken's prices have to be converted with the USDT/USD
rate, and the rate (like every price here) changes over time, so "the rate" only has a meaning
at a given moment. Alerts then watch the spread and each feed's minutes.

## Decision

- **A second job**, `ticklab-signals` (`SignalsJob.java`), on the same cluster and jar
  (decision 0010). It reads `trades` under its own consumer group and deduplicates them the same
  way as the core job (decision 0009). Live, its first start begins at the newest trade: a day of
  backlog would be worked through for nothing. Replays and tests start at the beginning.
- **Flink SQL for the bars and joins** (`SignalsPipeline.java`):
  - 1-second bars per feed with a tumbling window. The last price is the trade with the highest
    ID, which takes a small aggregate function (`LastPriceById.java`): SQL's `LAST_VALUE` would
    mean the last to arrive.
  - Kraken's USDT/USD bars become a **versioned table**: deduplicating on the newest time per key
    makes Flink keep every version, so it can answer "what was the rate at time t".
  - Each Kraken bar is converted with an **event-time temporal join**
    (`FOR SYSTEM_TIME AS OF k.bar_time`): the rate as of that bar, not the latest rate.
  - The converted Kraken prices become a second versioned table, and each Binance second is
    joined with the Kraken price as of that second.
  - 1-minute bars are cascaded from the 1-second ones, for the price and volume alerts.
- **Exact arithmetic in Java**, after the SQL: the conversion and the spread in basis points,
  rounded half-even to 12 decimal places (`schemas/spread.avsc`). A Kraken price more than
  10 s old gives no spread.
- **Alerts in the DataStream API**, with keyed state and event-time timers
  (`DivergenceAlerts.java`, `PriceMoveAlerts.java`, `VolumeSpikeAlerts.java`), each a small
  state machine with hysteresis: different thresholds for opening and closing.
- **Inputs and timers in event-time order** (`InEventTimeOrder.java`): each element waits until
  the watermark passes it, and is then handled in the same timer queue as the alerts' own
  timers. Handled on arrival, an element could overtake an earlier timer whenever the watermark
  jumps more than a second at once, as it does in a fast replay, and two replays would disagree.

## Consequences

- The SQL is short and states the time semantics directly; the planner keeps the versioned
  tables' state and clears old versions as the watermark passes them.
- 1-second windows drop trades that come later than the watermark, such as repaired ones: the
  spread is a live signal. Candles, which accept late data for 5 minutes, stay the exact record.
- Flink SQL generates its operators' IDs from the query, so after a change to the query a
  savepoint's SQL state has no operator to go to. The signals job is therefore resumed with
  `allowNonRestoredState` (`ticklab/flinkctl.py`): the operators with fixed IDs (the Kafka
  source's offsets, deduplication, the alerts' state machines) get their state back, and the SQL
  operators' state is dropped. It holds a few seconds of bars and the latest prices, so little
  is lost. Milestone 5 needed this the first time the query changed.
- In milestone 4 the planner computed the 1-second bars once for each query that read them, so
  the signals job did more work per trade than the core job and ran minutes behind after a
  fast replay. Milestone 5 computes them once, through a DataStream. Draining stops a source
  where it has read to, so `flink drain` first waits until every job has read all of its input
  (milestone 4 found this: two replays of a day disagreed until it did).
- The thresholds come from a week of history (decision 0018).
