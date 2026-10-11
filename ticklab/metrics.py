"""Prometheus metrics of the producers and the recorder.

Counters reset to zero when a process restarts. Prometheus treats a drop as a reset, but a
restart that comes back to the same value (a fresh process opening its first connection,
like the one before it) looks like no change at all. Restarts are therefore counted from
`process_start_time_seconds`, which the client library exports on Linux.
"""

from prometheus_client import Counter, Gauge

FRAMES = Counter("ticklab_ws_frames", "WebSocket frames received", ["exchange", "kind"])
TRADES = Counter("ticklab_trades", "Trades normalised from live frames", ["exchange", "symbol"])
CONNECTS = Counter(
    "ticklab_ws_connects",
    "WebSocket connections opened, by reason: startup, error, stale, planned, server_shutdown",
    ["exchange", "reason"],
)
CONNECTED = Gauge("ticklab_ws_connected", "Open WebSocket connections", ["exchange"])
PARSE_ERRORS = Counter("ticklab_parse_errors", "Frames that could not be normalised", ["exchange"])
DELIVERED = Counter("ticklab_kafka_delivered", "Trades acknowledged by Kafka", ["exchange"])
DELIVERY_ERRORS = Counter(
    "ticklab_kafka_delivery_errors", "Trades Kafka failed to store", ["exchange"]
)
LAST_TRADE_TIME = Gauge(
    "ticklab_last_trade_time_seconds", "Trade time of the newest trade", ["exchange", "symbol"]
)
RECORDED_LINES = Counter("ticklab_recorder_lines", "Lines written by the recorder", ["exchange"])
