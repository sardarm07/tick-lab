# 0016: Kraken's timestamps, and repairing gaps by time

**Status:** accepted, milestone 4

## Context

Kraken's WebSocket gives each trade a timestamp in microseconds; its REST `Trades` endpoint
gives a time in seconds with up to seven decimal places, and pages by time (`since`), not by
trade ID. Gap repair and the history importer use REST, so both questions matter: do REST times
equal WebSocket times, and how does a gap of missing IDs become a time to page from?

Checked live on 2026-10-11, with BTC/USD (`ticklab/kraken.py`, `ticklab/kraken_rest.py`):

- Kraken keeps trade times in **nanoseconds**. A page's `last` value is the exact time of its
  last trade, so a one-trade page reveals any trade's exact time.
- The **WebSocket truncates** them to microseconds: 50 of 50 trades.
- REST sends **binary floating-point seconds**: about 0.24 µs apart at today's dates, so within
  0.12 µs of the exact time. Truncated to microseconds, they equal the WebSocket's value for 43
  of 50 trades and are 1 µs off for 7.
- **`since` is inclusive**, in nanoseconds: a trade at exactly `since` is returned, so a page
  starting at the previous page's `last` repeats its last trade.

## Decision

- REST times are truncated to microseconds and accepted as they are. A repaired or historical
  Kraken trade can be 1 µs off its WebSocket time. It changes nothing that is computed: candles
  order trades by ID, and a trade could only land in another minute if it happened within
  0.12 µs of the minute's end.
- **Gap repair** starts from the trade just before the gap, already stored in ClickHouse: its
  time, minus 1 µs to cover the possible error, in nanoseconds. It pages forward until past the
  gap's last ID, collecting trades by ID (pages overlap by one), at most one request a second.
- **History** is kept as raw pages, overlap included; reading it drops repeats by ID.
- The Kraken producer replaces a connection that delivers nothing for 10 s: Kraken sends a
  heartbeat every second when there is nothing else (decision 0006).

## Consequences

- Kraken gap repair needs ClickHouse as well as Kafka, and a trade before the gap. A feed's very
  first trades can't be repaired this way; deduplication's warm-up never reports gaps there.
- WebSocket and REST copies of one trade can differ by 1 µs in `trades`. Storage keeps one of
  the two (decision 0003); nothing downstream depends on the difference.
