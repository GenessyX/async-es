import asyncio
import dataclasses
import json as _json
import os
from typing import TYPE_CHECKING, cast

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from async_es.aggregate import Aggregate, aggregate, event
from async_es.application import Application
from async_es.event_store.postgres import AsyncPGEventStore
from async_es.repository.eventsourced import EventSourcedRepository
from async_es.types import AggregateId

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


class CounterId(AggregateId): ...


@dataclasses.dataclass
class Increment:
    amount: int


@aggregate
class Counter(Aggregate[CounterId]):
    id: CounterId
    value: int = 0

    @event("incremented", Increment)
    def add(self, amount: int) -> None:
        self.value += amount


async def _wait_pg_ready(dsn: str) -> None:
    for _ in range(30):
        try:
            conn = await asyncpg.connect(dsn)
            await conn.close()
        except Exception:  # noqa: BLE001
            await asyncio.sleep(1)
        else:
            return
    msg = "PostgreSQL not ready"
    raise RuntimeError(msg)


@pytest.fixture
def pg_dsn() -> "Iterator[str]":
    env_dsn = os.getenv("POSTGRES_DSN")
    if env_dsn:
        yield env_dsn
        return
    with PostgresContainer("postgres:16-alpine") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        user = pg.username
        password = pg.password
        db = pg.dbname
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        yield dsn


@pytest_asyncio.fixture
async def pg_store(pg_dsn: str) -> "AsyncIterator[AsyncPGEventStore]":
    await _wait_pg_ready(pg_dsn)
    store = await AsyncPGEventStore.create(dsn=pg_dsn)
    await store.ensure_schema()
    try:
        yield store
    finally:
        await store.close()


def make_repo(store: AsyncPGEventStore) -> EventSourcedRepository[CounterId]:
    return EventSourcedRepository[CounterId](
        aggregate_type=Counter.aggregate_name,
        event_store=store,
        aggregate_cls=Counter,
    )


@pytest.mark.asyncio
async def test_application_saves_and_restores_with_postgres(pg_store: AsyncPGEventStore, pg_dsn: str) -> None:
    repo = make_repo(pg_store)
    app = Application[CounterId](repository=repo)

    counter_id = CounterId.generate()

    aggregate = await app.load(counter_id)
    counter = cast("Counter", aggregate)
    counter.add(3)
    counter.add(4)
    await app.save(counter)

    # direct DB verification
    conn = await asyncpg.connect(pg_dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT aggregate_type, aggregate_id, event_type, payload, payload_module, payload_type,
                   occurred_at, published_at, metadata
            FROM events
            WHERE aggregate_id = $1
            ORDER BY occurred_at ASC, event_type ASC
            """,
            counter_id,
        )
    finally:
        await conn.close()

    assert len(rows) == 3
    assert {r["event_type"] for r in rows} == {"created", "incremented"}

    inc_rows = [r for r in rows if r["event_type"] == "incremented"]
    amounts: set[int] = set()
    for r in inc_rows:
        payload = r["payload"]
        if isinstance(payload, str):
            payload = _json.loads(payload)
        amounts.add(cast("int", payload["amount"]))
    assert amounts == {3, 4}

    # re-load via repository
    new_repo = make_repo(pg_store)
    new_app = Application[CounterId](repository=new_repo)
    reloaded = await new_app.load(counter_id)
    reloaded_counter = cast("Counter", reloaded)
    assert reloaded_counter.value == 7


@pytest.mark.asyncio
async def test_events_buffer_cleared_after_save_with_postgres(pg_store: AsyncPGEventStore, pg_dsn: str) -> None:
    repo = make_repo(pg_store)
    app = Application[CounterId](repository=repo)

    counter_id = CounterId.generate()

    aggregate = await app.load(counter_id)
    counter = cast("Counter", aggregate)
    counter.add(10)
    counter.add(20)
    await app.save(counter)

    # After save, aggregate should have no pending events buffered
    assert counter.events == []

    # direct DB verification of three rows and increment amounts
    conn = await asyncpg.connect(pg_dsn)
    try:
        rows = await conn.fetch(
            """
            SELECT event_type, payload
            FROM events
            WHERE aggregate_id = $1
            ORDER BY occurred_at ASC, event_type ASC
            """,
            counter_id,
        )
    finally:
        await conn.close()

    assert len(rows) == 3
    inc_rows = [r for r in rows if r["event_type"] == "incremented"]
    amounts: set[int] = set()
    for r in inc_rows:
        payload = r["payload"]
        if isinstance(payload, str):
            payload = _json.loads(payload)
        amounts.add(cast("int", payload["amount"]))
    assert amounts == {10, 20}
