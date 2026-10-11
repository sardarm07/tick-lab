"""Avro in the Confluent wire format, and the schema registry.

Every Kafka message value is laid out as:

    byte 0       magic byte, always 0
    bytes 1-4    schema ID, big-endian, assigned by the schema registry
    bytes 5-     the record, Avro binary encoding

A reader (Flink, ClickHouse, a test) takes the ID, fetches that schema from the registry
once, and decodes. Decimals travel as Avro `decimal` (an unscaled integer plus the scale
from the schema), so 0.1 stays exactly 0.1 from exchange to database.
"""

from __future__ import annotations

import io
import json
import struct

import fastavro
import httpx

from ticklab.model import Trade
from ticklab.settings import ROOT

SCHEMA_FILE = ROOT / "schemas" / "trade.avsc"
MAGIC = b"\x00"
REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def subject(topic: str) -> str:
    """Registry subject for a topic's message values (the TopicNameStrategy)."""
    return f"{topic}-value"


def load_schema() -> tuple[str, dict]:
    """The trade schema as compact JSON text (what gets registered) and in parsed form."""
    raw = json.loads(SCHEMA_FILE.read_text())
    return json.dumps(raw, separators=(",", ":")), fastavro.parse_schema(raw)


def register(registry_url: str, subject_name: str, schema_text: str) -> int:
    """Register the schema (a no-op if it already is) and return its ID."""
    response = httpx.post(
        f"{registry_url}/subjects/{subject_name}/versions",
        content=json.dumps({"schema": schema_text}),
        headers={"Content-Type": REGISTRY_CONTENT_TYPE},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["id"]


class TradeEncoder:
    def __init__(self, schema_id: int, parsed_schema: dict) -> None:
        self._header = MAGIC + struct.pack(">I", schema_id)
        self._schema = parsed_schema

    def encode(self, trade: Trade) -> bytes:
        buffer = io.BytesIO()
        buffer.write(self._header)
        fastavro.schemaless_writer(buffer, self._schema, trade.to_record())
        return buffer.getvalue()


def decode(value: bytes, parsed_schema: dict) -> tuple[int, dict]:
    """Split a message value into its schema ID and decoded record."""
    if value[:1] != MAGIC:
        raise ValueError("not in the Confluent wire format")
    (schema_id,) = struct.unpack(">I", value[1:5])
    record = fastavro.schemaless_reader(io.BytesIO(value[5:]), parsed_schema, None)
    return schema_id, record
