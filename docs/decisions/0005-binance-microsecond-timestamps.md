# 0005: Binance times in microseconds everywhere

**Status:** accepted, milestone 1

## Context

Binance's daily history files give trade times in microseconds (from 2025 on), but its
WebSocket streams and REST API default to milliseconds. Comparing live data with history or
REST would then need truncation, and 64–84% of Binance trades share their millisecond with
another trade.

Binance's API changelog (2024-12-17) added an opt-in: the URL parameter `timeUnit=MICROSECOND`
for WebSocket streams, and the header `X-MBX-TIME-UNIT: MICROSECOND` for REST.

## Decision

- The live stream URL always includes `timeUnit=MICROSECOND` (`ticklab/binance.py`).
- REST calls send `X-MBX-TIME-UNIT: MICROSECOND` (`ticklab/binance_rest.py`).
- The normaliser takes the unit from the URL a frame came from, which the recorder stores, so
  recordings made in milliseconds would still replay correctly.

## Consequences

- Live, REST and history agree on trade times to the microsecond. The golden test checks this
  against REST for real frames and history rows.
- Ordering within a minute still uses trade IDs, not times, because some trades share even a
  microsecond.
