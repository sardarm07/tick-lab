"""A 1-minute candle as both sides of a reconciliation see it, and the comparison of the two."""

from __future__ import annotations

from dataclasses import dataclass, fields
from decimal import Decimal


@dataclass(frozen=True)
class Candle:
    """Fields an exchange doesn't publish are None, and aren't compared (Binance has no VWAP,
    Kraken no quote volume or taker-buy volumes)."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None
    taker_buy_volume: Decimal | None
    taker_buy_quote_volume: Decimal | None
    trade_count: int


def compare(ours: Candle | None, theirs: Candle) -> list[str]:
    """`field: ours / theirs` for every field that differs; empty means an exact match.

    A minute without a candle of ours counts as zero trades (rule 7): Flink never opens a window
    without data, while exchanges may publish candles for minutes without trades.
    """
    if ours is None:
        return [] if theirs.trade_count == 0 else [f"trade_count: 0 / {theirs.trade_count}"]
    differences = []
    for field in fields(Candle):
        reference = getattr(theirs, field.name)
        if reference is not None and getattr(ours, field.name) != reference:
            differences.append(f"{field.name}: {getattr(ours, field.name)} / {reference}")
    return differences
