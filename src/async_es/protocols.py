from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from async_es.types import AggregateId

if TYPE_CHECKING:
    from async_es.aggregate import Aggregate
    from async_es.event import DomainEvent


class Repository[IdT: AggregateId](Protocol):
    async def get(self, aggregate_id: IdT) -> Aggregate[IdT]: ...

    async def save(self, aggregate: Aggregate[IdT]) -> None: ...


class EventStore(Protocol):
    async def append(self, events: list[DomainEvent[Any, Any]]) -> None: ...

    async def load(self, aggregate_type: str, aggregate_id: AggregateId) -> list[DomainEvent[Any, Any]]: ...


class ApplicationNotifier(Protocol):
    async def notify(self, events: list[DomainEvent[Any, Any]]) -> None: ...
