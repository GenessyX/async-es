import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, dataclass_transform

from async_es.event import DomainEvent
from async_es.sentinel import NO_ARG
from async_es.types import AggregateId

if TYPE_CHECKING:
    from collections.abc import Callable

    from _typeshed import DataclassInstance


@dataclass(kw_only=True, eq=True)
class Aggregate[AggregateIdT: AggregateId]:
    aggregate_name: ClassVar[str] = "Aggregate"

    id: AggregateIdT
    created_at: datetime.datetime = field(default=NO_ARG)  # type: ignore[assignment]
    updated_at: datetime.datetime = field(default=NO_ARG)  # type: ignore[assignment]
    deleted_at: datetime.datetime | None = None
    _events: list[DomainEvent[AggregateIdT, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.created_at is NO_ARG:
            self.created_at = datetime.datetime.now(datetime.UTC)
            self.updated_at = self.created_at

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.id == other.id

    def _raise_event(self, event_type: str, event: "DataclassInstance") -> None:
        self._events.append(
            DomainEvent(
                aggregate_id=self.id,
                aggregate_type=self.aggregate_name,
                event_type=event_type,
                payload=event,
            ),
        )

    @property
    def events(self) -> list[DomainEvent[AggregateIdT, Any]]:
        return list(self._events)

    def publish_events(self) -> list[DomainEvent[AggregateIdT, Any]]:
        for event in self._events:
            event.publish()
        return list(self._events)

    def clear_events(self) -> None:
        self._events.clear()

    def delete(self) -> None:
        self.deleted_at = datetime.datetime.now(datetime.UTC)


@dataclass_transform(kw_only_default=True, eq_default=True, order_default=False, frozen_default=False)
def aggregate[T](  # noqa: PLR0913
    cls: type[T] | None = None,
    /,
    *,
    init: bool = True,
    repr: bool = True,  # noqa: A002
    eq: bool = False,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    match_args: bool = True,
    kw_only: bool = True,
    slots: bool = False,
    weakref_slot: bool = False,
) -> Callable[[type[T]], type[T]]:
    """
    Like @dataclass, but defaults to eq=False, kw_only=True.
    Allows overriding by passing eq=..., kw_only=...,
    """
    if cls is None:
        return dataclass(
            init=init,
            repr=repr,
            eq=eq,
            order=order,
            unsafe_hash=unsafe_hash,
            frozen=frozen,
            match_args=match_args,
            kw_only=kw_only,
            slots=slots,
            weakref_slot=weakref_slot,
        )
    return dataclass(
        init=init,
        repr=repr,
        eq=eq,
        order=order,
        unsafe_hash=unsafe_hash,
        frozen=frozen,
        match_args=match_args,
        kw_only=kw_only,
        slots=slots,
        weakref_slot=weakref_slot,
    )(cls)  # type: ignore[arg-type]
