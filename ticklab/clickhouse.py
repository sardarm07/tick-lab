"""A small client for ClickHouse's HTTP interface."""

from __future__ import annotations

import json
from typing import Any

import httpx

from ticklab.settings import Settings


class ClickHouse:
    def __init__(self, settings: Settings, timeout: float = 300) -> None:
        self._client = httpx.Client(
            base_url=settings.clickhouse_url,
            headers={
                "X-ClickHouse-User": settings.clickhouse_user,
                "X-ClickHouse-Key": settings.clickhouse_password,
            },
            timeout=timeout,
        )

    def execute(self, sql: str, **params: Any) -> str:
        """Run one statement. Parameters fill `{name:Type}` placeholders server-side."""
        response = self._client.post(
            "/", content=sql, params={f"param_{k}": v for k, v in params.items()}
        )
        if response.status_code != 200:
            raise RuntimeError(f"ClickHouse: {response.text.strip()}\n-- in: {sql.strip()}")
        return response.text

    def rows(self, sql: str, **params: Any) -> list[dict]:
        """Rows as dicts. Decimals and 64-bit integers come back as strings, so nothing passes
        through a float on the way."""
        query = sql.rstrip().rstrip(";")
        query += "\nSETTINGS output_format_json_quote_decimals = 1\nFORMAT JSONEachRow"
        text = self.execute(query, **params)
        return [json.loads(line) for line in text.splitlines() if line]

    def value(self, sql: str, **params: Any) -> Any:
        (row,) = self.rows(sql, **params)
        (value,) = row.values()
        return value
