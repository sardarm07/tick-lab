"""Builds the Flink pipeline test fixture. Run it from the repo root, after `make history`:

    PYTHONPATH=. uv run python flink/src/test/resources/fixtures/build.py 2026-10-10T00:00 3

It writes, for BTCUSDT and SOLUSDT:

- <name>.trades.csv: every trade of those minutes, copied unmodified from Binance's daily
  history file, with the market in front;
- <name>.klines.csv: Binance's official 1-minute candles for the same minutes, from
  `GET /api/v3/klines` on data-api.binance.vision.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx

from ticklab import binance, history
from ticklab.settings import Settings, feeds_for

HERE = Path(__file__).parent
MARKETS = ("BTCUSDT", "SOLUSDT")


def main(start: datetime, minutes: int) -> None:
    settings = Settings.from_env()
    feeds = {feed.native: feed for feed in feeds_for(binance.EXCHANGE)}
    start_us = int(start.timestamp() * 1_000_000)
    end_us = start_us + minutes * 60_000_000
    name = f"binance-{start:%Y-%m-%dT%H%M}-{minutes}m"
    trades = ["# native,trade_id,price,qty,quote_qty,time_us,is_buyer_maker,is_best_match"]
    klines = [
        "# native,open_time_ms,open,high,low,close,volume,close_time_ms,quote_volume,count,"
        "taker_buy_volume,taker_buy_quote_volume"
    ]
    with httpx.Client(timeout=30) as client:
        for native in MARKETS:
            path = history.local_path(settings.history_dir, feeds[native], start.date())
            for row in history.read_rows(path):
                if start_us <= int(row[4]) < end_us:
                    trades.append(",".join([native, *row]))
            response = client.get(
                "https://data-api.binance.vision/api/v3/klines",
                params={
                    "symbol": native,
                    "interval": "1m",
                    "startTime": start_us // 1000,
                    "endTime": end_us // 1000 - 1,
                },
            )
            response.raise_for_status()
            for k in response.json():
                klines.append(",".join(str(v) for v in [native, *k[:11]]))
    (HERE / f"{name}.trades.csv").write_text("\n".join(trades) + "\n")
    (HERE / f"{name}.klines.csv").write_text("\n".join(klines) + "\n")
    print(f"{name}: {len(trades) - 1} trades, {len(klines) - 1} candles")


if __name__ == "__main__":
    when = datetime.fromisoformat(sys.argv[1]).replace(tzinfo=UTC)
    main(when, int(sys.argv[2]) if len(sys.argv) > 2 else 3)
