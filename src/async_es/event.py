import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid7

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

from async_es.types import AggregateId

type EventId = UUID


@dataclass(kw_only=True)
class DomainEvent[AggregateIdT: AggregateId, PayloadT: "DataclassInstance"]:
    id: EventId = field(default_factory=lambda: uuid7())
    aggregate_type: str
    aggregate_id: AggregateIdT
    event_type: str
    payload: PayloadT
    occurred_at: datetime.datetime = field(default_factory=lambda: datetime.datetime.now(datetime.UTC))
    published_at: datetime.datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def publish(self) -> None:
        self.published_at = datetime.datetime.now(datetime.UTC)
