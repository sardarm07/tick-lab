"""Configuration: the feeds come from config/feeds.toml, everything else from the environment.

The defaults suit commands run on the host next to a running stack (Kafka on 127.0.0.1:19092
and so on: the ports are bound to IPv4 only). Inside the Compose network, the services get
the internal addresses instead.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Feed:
    """One market on one exchange, published to its own partition."""

    exchange: str  # BINANCE
    symbol: str  # BTC/USDT
    native: str  # BTCUSDT, as the exchange spells it
    partition: int

    @property
    def base(self) -> str:
        return self.symbol.split("/")[0]

    @property
    def quote(self) -> str:
        return self.symbol.split("/")[1]

    @property
    def key(self) -> bytes:
        """The Kafka message key: `exchange:symbol`."""
        return f"{self.exchange}:{self.symbol}".encode()


def load_feeds(path: Path | None = None) -> list[Feed]:
    path = path or Path(os.environ.get("TICKLAB_FEEDS", ROOT / "config" / "feeds.toml"))
    with open(path, "rb") as f:
        feeds = [Feed(**item) for item in tomllib.load(f)["feeds"]]
    if sorted(feed.partition for feed in feeds) != list(range(len(feeds))):
        raise ValueError(f"{path}: partitions must be 0..{len(feeds) - 1}, each used once")
    for feed in feeds:
        if feed.symbol.count("/") != 1:
            raise ValueError(f"{path}: symbol {feed.symbol!r} is not BASE/QUOTE")
    if len({(feed.exchange, feed.native) for feed in feeds}) != len(feeds):
        raise ValueError(f"{path}: a market is listed twice")
    return feeds


def feeds_for(exchange: str) -> list[Feed]:
    return [feed for feed in load_feeds() if feed.exchange == exchange]


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    schema_registry_url: str
    trades_topic: str
    clickhouse_url: str
    clickhouse_user: str
    clickhouse_password: str
    recordings_dir: Path
    history_dir: Path
    binance_ws_url: str
    binance_rest_url: str
    binance_history_url: str
    binance_max_connection_age_s: float
    stale_after_s: float
    metrics_port: int
    stack: str

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get
        return cls(
            kafka_bootstrap=env("KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            schema_registry_url=env("SCHEMA_REGISTRY_URL", "http://127.0.0.1:18081"),
            trades_topic=env("TRADES_TOPIC", "trades"),
            clickhouse_url=env("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
            clickhouse_user=env("CLICKHOUSE_USER", "ticklab"),
            clickhouse_password=env("CLICKHOUSE_PASSWORD", "ticklab"),
            recordings_dir=Path(env("RECORDINGS_DIR", str(ROOT / "data" / "recordings"))),
            history_dir=Path(env("HISTORY_DIR", str(ROOT / "data" / "history"))),
            binance_ws_url=env("BINANCE_WS_URL", "wss://stream.binance.com:9443"),
            binance_rest_url=env("BINANCE_REST_URL", "https://api.binance.com"),
            binance_history_url=env("BINANCE_HISTORY_URL", "https://data.binance.vision"),
            # Binance closes connections after 24 hours; replace them well before that.
            binance_max_connection_age_s=float(env("BINANCE_MAX_CONNECTION_AGE_S", "82800")),
            stale_after_s=float(env("FEED_STALE_AFTER_S", "60")),
            metrics_port=int(env("METRICS_PORT", "8000")),
            stack=env("TICKLAB_STACK", "live"),
        )
