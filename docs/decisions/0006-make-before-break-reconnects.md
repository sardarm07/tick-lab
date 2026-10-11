# 0006: Make-before-break reconnects

**Status:** accepted, milestone 1

## Context

Binance closes every WebSocket connection after 24 hours, and may send `serverShutdown` when a
server is about to go away. Closing and then reopening loses every trade made in between.
Those could be fetched again from REST (gap repair, milestone 3), but a planned reconnect
shouldn't create gaps at all.

## Decision

`ticklab/stream.py`:

- Replaces each connection after 23 hours (`BINANCE_MAX_CONNECTION_AGE_S`), and immediately
  after `serverShutdown`.
- Opens the new connection first, waits for its first trade (at most 10 s) plus 2 more seconds,
  and only then closes the old one. If the new connection can't open, the old one is kept and
  the replacement retried with backoff.
- On errors, reconnects with exponential backoff (1 s doubling to 60 s) with "equal jitter":
  half of each delay is random.
- Replaces a connection that has delivered nothing for 60 s (`BINANCE_STALE_AFTER_S`). The data
  study shows that no pair goes 60 s without a trade, and the combined stream carries three
  pairs.
- Kraken (from milestone 4) documents no limit on a connection's age, so its connections aren't
  replaced on a schedule. It sends a heartbeat each second when there is nothing else to send,
  so a Kraken connection that delivers nothing for 10 s (`KRAKEN_STALE_AFTER_S`) is replaced.
  Each new subscription starts with a snapshot of the last 50 trades of each pair, which
  overlaps what the previous connection delivered.

## Consequences

- Planned reconnects leave no gaps. Trades from the overlap arrive twice, which the
  idempotent storage and, from milestone 2, Flink's deduplication absorb.
- During a handover, `ticklab_ws_connected` shows 2 for a few seconds.
