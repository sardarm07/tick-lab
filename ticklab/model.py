"""The normalised trade that every source is turned into (schemas/trade.avsc)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

# Limits of decimal(28,12) in the Avro schema: 12 digits after the point, 16 before it.
MAX_SCALE = 12
MAX_INTEGER_DIGITS = 16


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class Source(StrEnum):
    WS = "WS"
    WS_SNAPSHOT = "WS_SNAPSHOT"
    REST = "REST"
    HISTORY = "HISTORY"


@dataclass(frozen=True, slots=True)
class Trade:
    exchange: str
    symbol: str
    base: str
    quote: str
    trade_id: int
    price: Decimal
    quantity: Decimal
    taker_side: Side
    trade_time: int  # microseconds since the Unix epoch, UTC
    received_time: int | None  # microseconds; None when nothing arrived live (history)
    source: Source

    def to_record(self) -> dict:
        """This trade as an Avro record."""
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "base": self.base,
            "quote": self.quote,
            "trade_id": self.trade_id,
            "price": self.price,
            "quantity": self.quantity,
            "taker_side": self.taker_side.value,
            "trade_time": self.trade_time,
            "received_time": self.received_time,
            "source": self.source.value,
        }

    def to_json(self) -> dict:
        """This trade with exact decimals written as strings, for fixtures and reports."""
        return {**self.to_record(), "price": str(self.price), "quantity": str(self.quantity)}


@dataclass(frozen=True, slots=True)
class Frame:
    """What one WebSocket message contained, once normalised."""

    trades: tuple[Trade, ...] = ()
    shutdown: bool = False  # the server announced it is about to close the connection
    block_trade: bool = False  # Binance off-exchange block trade: recorded, not a trade


def to_decimal(text: str) -> Decimal:
    """Parse an exchange's decimal text exactly, rejecting what the schema can't hold.

    Floats never appear on this path: 0.1 has no exact binary form, and candles are
    only provably right if every price and quantity is stored exactly as published.
    """
    value = Decimal(text)
    if not value.is_finite() or value <= 0:
        raise ValueError(f"not a positive number: {text!r}")
    exponent = value.as_tuple().exponent
    assert isinstance(exponent, int)
    if -exponent > MAX_SCALE:
        raise ValueError(f"more than {MAX_SCALE} decimal places: {text!r}")
    if value.adjusted() + 1 > MAX_INTEGER_DIGITS:
        raise ValueError(f"more than {MAX_INTEGER_DIGITS} digits before the point: {text!r}")
    return value
