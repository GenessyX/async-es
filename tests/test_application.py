import dataclasses
from typing import TYPE_CHECKING, Any, cast

import pytest

from async_es.aggregate import Aggregate, aggregate, event
from async_es.application import Application
from async_es.event_store.inmemory import InMemoryEventStore
from async_es.repository.eventsourced import EventSourcedRepository
from async_es.types import AggregateId

if TYPE_CHECKING:
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


def make_repo(store: InMemoryEventStore) -> EventSourcedRepository[CounterId]:
    return EventSourcedRepository[CounterId](
        aggregate_type=Counter.aggregate_name,
        event_store=store,
        aggregate_cls=Counter,
    )


class DummyNotifier:
    def __init__(self) -> None:
        self.received: list[list["DomainEvent[Any, Any]"]] = []

    async def notify(self, events: list["DomainEvent[Any, Any]"]) -> None:
        self.received.append(list(events))


@pytest.mark.asyncio
async def test_application_saves_and_restores() -> None:
    store = InMemoryEventStore()
    repo = make_repo(store)
    notifier = DummyNotifier()
    app = Application[CounterId](repository=repo, notifier=notifier)

    counter_id = CounterId.generate()

    aggregate = await app.load(counter_id)
    counter = cast("Counter", aggregate)
    counter.add(1)
    counter.add(2)
    await app.save(counter)

    # re-load via a fresh repository using the same store → state from events
    new_repo = make_repo(store)
    new_app = Application[CounterId](repository=new_repo)
    reloaded = await new_app.load(counter_id)
    reloaded_counter = cast("Counter", reloaded)

    assert reloaded_counter.value == 3
    # notifier received one batch of three events (created + two increments)
    assert len(notifier.received) == 1
    batch = notifier.received[0]
    assert [e.event_type for e in batch] == ["created", "incremented", "incremented"]
    assert all(e.aggregate_id == counter_id for e in batch)


@pytest.mark.asyncio
async def test_events_buffer_cleared_after_save() -> None:
    store = InMemoryEventStore()
    repo = make_repo(store)
    app = Application[CounterId](repository=repo)

    counter_id = CounterId.generate()

    aggregate = await app.load(counter_id)
    counter = cast("Counter", aggregate)
    counter.add(5)
    counter.add(7)
    await app.save(counter)

    # After save, aggregate should have no pending events buffered
    assert counter.events == []
