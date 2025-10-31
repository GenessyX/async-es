from __future__ import annotations

import dataclasses
import importlib
import json
import re
from typing import TYPE_CHECKING, Any, cast

import asyncpg

from async_es.protocols import EventStore

if TYPE_CHECKING:
    from async_es.event import DomainEvent
    from async_es.types import AggregateId


_DEFAULT_TABLE = "events"


def _quote_ident(name: str) -> str:
    # Strict validation: allow only unqualified identifiers [A-Za-z_][A-Za-z0-9_]*
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        msg = "Invalid SQL identifier"
        raise ValueError(msg)
    return f'"{name}"'


class AsyncPGEventStore(EventStore):
    def __init__(self, pool: asyncpg.pool.Pool, *, table_name: str = _DEFAULT_TABLE) -> None:
        self._pool = pool
        self._table = table_name
        self._table_sql = _quote_ident(table_name)
        self._index_name = f"idx_{table_name}_agg"
        self._index_sql = _quote_ident(self._index_name)

    @classmethod
    async def create(
        cls,
        *,
        dsn: str | None = None,
        table_name: str = _DEFAULT_TABLE,
        **connect_kwargs: Any,  # noqa: ANN401
    ) -> "AsyncPGEventStore":
        pool = await asyncpg.create_pool(dsn=dsn, **connect_kwargs)
        return cls(pool, table_name=table_name)

    async def close(self) -> None:
        await self._pool.close()

    async def ensure_schema(self) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self._table_sql} (
            id UUID PRIMARY KEY,
            aggregate_type TEXT NOT NULL,
            aggregate_id UUID NOT NULL,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            payload_module TEXT NOT NULL,
            payload_type TEXT NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            published_at TIMESTAMPTZ,
            metadata JSONB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS {self._index_sql} ON {self._table_sql} (aggregate_type, aggregate_id, occurred_at);
        """
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    async def append(self, events: list["DomainEvent[Any, Any]"]) -> None:
        if not events:
            return
        sql = f"""
        INSERT INTO {self._table_sql} (
            id, aggregate_type, aggregate_id, event_type,
            payload, payload_module, payload_type,
            occurred_at, published_at, metadata
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        )
        ON CONFLICT (id) DO NOTHING
        """  # noqa: S608
        records = [self._to_record(e) for e in events]
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.executemany(sql, records)

    async def load(self, aggregate_type: str, aggregate_id: "AggregateId") -> list["DomainEvent[Any, Any]"]:
        sql = f"""
        SELECT id, aggregate_type, aggregate_id, event_type,
               payload, payload_module, payload_type,
               occurred_at, published_at, metadata
        FROM {self._table_sql}
        WHERE aggregate_type = $1 AND aggregate_id = $2
        ORDER BY occurred_at ASC, id ASC
        """  # noqa: S608
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, aggregate_type, aggregate_id)
        return [self._from_row(row) for row in rows]

    @staticmethod
    def _payload_to_json(payload: object) -> str:
        if dataclasses.is_dataclass(payload) and not isinstance(payload, type):
            return json.dumps(dataclasses.asdict(payload), default=str)
        return json.dumps(payload, default=str)

    @staticmethod
    def _payload_from_json(payload_module: str, payload_type: str, data: object) -> object:
        module = importlib.import_module(payload_module)
        payload_cls = getattr(module, payload_type)
        obj = json.loads(data) if isinstance(data, str) else data
        if dataclasses.is_dataclass(payload_cls):
            payload_type_obj = cast("type[object]", payload_cls)
            return payload_type_obj(**cast("dict[str, object]", obj))
        return obj

    def _to_record(self, e: "DomainEvent[Any, Any]") -> tuple[object, ...]:
        payload = e.payload
        payload_module = type(payload).__module__
        payload_type = type(payload).__qualname__
        return (
            e.id,
            e.aggregate_type,
            e.aggregate_id,
            e.event_type,
            self._payload_to_json(payload),
            payload_module,
            payload_type,
            e.occurred_at,
            e.published_at,
            json.dumps(e.metadata, default=str),
        )

    @staticmethod
    def _from_row(row: asyncpg.Record) -> "DomainEvent[Any, Any]":
        from async_es.event import DomainEvent  # noqa: PLC0415

        payload = AsyncPGEventStore._payload_from_json(row["payload_module"], row["payload_type"], row["payload"])
        metadata_value = row["metadata"]
        metadata = json.loads(metadata_value) if isinstance(metadata_value, str) else metadata_value
        return DomainEvent(
            id=row["id"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            event_type=row["event_type"],
            payload=payload,
            occurred_at=row["occurred_at"],
            published_at=row["published_at"],
            metadata=metadata,
        )
