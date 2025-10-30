import dataclasses
import json as _json
from typing import TYPE_CHECKING, Any, cast

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

    from async_es.event import DomainEvent


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


@pytest.fixture
def pg_dsn() -> "Iterator[str]":
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
    store = await AsyncPGEventStore.create(dsn=pg_dsn)
    await store.ensure_schema()
    try:
        yield store
    finally:
        await store.close()


def make_repo(store: AsyncPGEventStore) -> EventSourcedRepository[CounterId]:
    def factory(counter_id: CounterId) -> Counter:
        return Counter(id=counter_id)

    def apply(counter: Aggregate[CounterId], domain_event: "DomainEvent[CounterId, Any]") -> None:
        concrete = cast("Counter", counter)
        if domain_event.event_type == "incremented":
            concrete.value += domain_event.payload.amount

    return EventSourcedRepository[CounterId](
        aggregate_type=Counter.aggregate_name,
        event_store=store,
        factory=factory,
        apply=apply,
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

    assert len(rows) == 2
    assert {r["event_type"] for r in rows} == {"incremented"}
    for r in rows:
        # payload may be str or dict depending on driver version
        payload = r["payload"]
        if isinstance(payload, str):
            payload = _json.loads(payload)
        assert payload["amount"] in {3, 4}
        assert r["payload_type"] == "Increment"
        assert r["aggregate_type"] == Counter.aggregate_name
        assert r["aggregate_id"] == counter_id
        assert r["occurred_at"] is not None

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

    # direct DB verification of two rows and amounts
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

    assert len(rows) == 2
    amounts: set[int] = set()
    for r in rows:
        payload = r["payload"]
        if isinstance(payload, str):
            payload = _json.loads(payload)
        amounts.add(cast("int", payload["amount"]))
    assert amounts == {10, 20}
