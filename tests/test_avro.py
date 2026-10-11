from datetime import UTC, datetime
from decimal import Decimal

from ticklab import avro
from ticklab.model import Side, Source, Trade


def trade(**changes) -> Trade:
    values = dict(
        exchange="BINANCE",
        symbol="BTC/USDT",
        base="BTC",
        quote="USDT",
        trade_id=6752423791,
        price=Decimal("83040.01000000"),
        quantity=Decimal("0.000000000001"),
        taker_side=Side.SELL,
        trade_time=1791675558429090,
        received_time=1791675558592190,
        source=Source.WS,
    )
    return Trade(**(values | changes))


def test_messages_use_the_confluent_wire_format():
    _, schema = avro.load_schema()
    value = avro.TradeEncoder(7, schema).encode(trade())
    assert value[0] == 0
    assert int.from_bytes(value[1:5], "big") == 7


def test_round_trip_keeps_every_field_exact():
    _, schema = avro.load_schema()
    schema_id, record = avro.decode(avro.TradeEncoder(7, schema).encode(trade()), schema)
    assert schema_id == 7
    assert record["price"] == Decimal("83040.01")
    assert record["quantity"] == Decimal("0.000000000001")
    assert record["taker_side"] == "SELL"
    assert record["trade_time"] == datetime(2026, 10, 10, 23, 39, 18, 429090, tzinfo=UTC)
    assert record["received_time"] == datetime(2026, 10, 10, 23, 39, 18, 592190, tzinfo=UTC)


def test_history_trades_have_no_received_time():
    _, schema = avro.load_schema()
    encoded = avro.TradeEncoder(1, schema).encode(trade(received_time=None, source=Source.HISTORY))
    _, record = avro.decode(encoded, schema)
    assert record["received_time"] is None
    assert record["source"] == "HISTORY"


def test_the_registered_text_is_compact_json_of_the_schema_file():
    text, _ = avro.load_schema()
    assert text.startswith('{"type":"record","name":"Trade","namespace":"ticklab.v1"')
    assert avro.subject("trades") == "trades-value"
