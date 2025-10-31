import datetime
import functools
import inspect
from contextlib import AbstractContextManager, contextmanager
from dataclasses import MISSING, Field, dataclass, field, fields, is_dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Concatenate, dataclass_transform

from async_es.event import DomainEvent
from async_es.sentinel import NO_ARG
from async_es.types import AggregateId

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from _typeshed import DataclassInstance


@dataclass(kw_only=True, eq=True)
class Aggregate[AggregateIdT: AggregateId]:
    aggregate_name: ClassVar[str] = "Aggregate"

    id: AggregateIdT
    created_at: datetime.datetime = field(default=NO_ARG)  # type: ignore[assignment]
    updated_at: datetime.datetime = field(default=NO_ARG)  # type: ignore[assignment]
    deleted_at: datetime.datetime | None = None
    _events: list[DomainEvent[AggregateIdT, Any]] = field(default_factory=list)
    _suppress_events: bool = False

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

    def suppress_events(self) -> AbstractContextManager[None]:
        @contextmanager
        def _cm() -> "Iterator[None]":
            self._suppress_events = True
            try:
                yield None
            finally:
                self._suppress_events = False

        return _cm()

    def _raise_event(self, event_type: str, event: "DataclassInstance") -> None:
        if self._suppress_events:
            return
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

    def get_event_as[PayloadT: "DataclassInstance"](
        self,
        payload_type: type[PayloadT],  # noqa: ARG002
        index: int = -1,
    ) -> DomainEvent[AggregateIdT, PayloadT]:
        return self.events[index]

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


def _has_default(f: Field[Any]) -> bool:
    # dataclasses.Field.default_factory is MISSING by default; attribute always exists
    return (f.default is not MISSING) or (f.default_factory is not MISSING)


def event[T: Aggregate[Any], R, **P](  # noqa: C901
    event_type: str,
    payload: type | Callable[..., Any],
    *,
    aliases: dict[str, str] | None = None,
    field_from_return: str | None = None,
) -> Callable[[Callable[Concatenate[T, P], R]], Callable[Concatenate[T, P], R]]:
    """
    Decorate an instance method of an Aggregate so that after it runs,
    an event is raised via self._raise_event(event_type, payload_instance).

    payload:
      - a dataclass **type** → payload is constructed from method args/self attrs
      - or a **builder callable** (self, *args, **kwargs) -> dataclass instance

    aliases: map payload_field -> method_param when names differ
    field_from_return: if set, inject method return value into that payload field
    """

    def decorator(func: Callable[Concatenate[T, P], R]) -> Callable[Concatenate[T, P], R]:  # noqa: C901
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(self: T, *args: P.args, **kwargs: P.kwargs) -> R:
            # run original method
            result = func(self, *args, **kwargs)

            # build payload instance
            if callable(payload) and not (isinstance(payload, type) and is_dataclass(payload)):
                # custom builder path
                payload_instance = payload(self, *args, **kwargs)
            else:
                payload_type = payload
                if not (isinstance(payload_type, type) and is_dataclass(payload_type)):
                    msg = "event(...): 'payload' must be a dataclass type or a builder callable"
                    raise TypeError(msg)

                bound = sig.bind(self, *args, **kwargs)
                bound.apply_defaults()
                bound_args = dict(bound.arguments)  # includes 'self'
                # Exclude 'self' from param lookup
                param_names = [n for n in bound_args if n != "self"]

                flds = list(fields(payload_type))
                data: dict[str, Any] = {}

                # Special case: single-field payload + single non-self arg → map automatically
                if not aliases and len(flds) == 1 and len(param_names) == 1:
                    only_field = flds[0].name
                    only_param = param_names[0]
                    data[only_field] = bound_args[only_param]
                    if field_from_return and field_from_return == only_field:
                        data[only_field] = result  # return value overrides
                else:
                    for f in flds:
                        target_name = f.name
                        source_name = (aliases or {}).get(target_name, target_name)

                        if source_name in bound_args:
                            data[target_name] = bound_args[source_name]
                        elif hasattr(self, source_name):
                            data[target_name] = getattr(self, source_name)
                        elif field_from_return and target_name == field_from_return:
                            data[target_name] = result
                        elif _has_default(f):
                            # allow dataclass to fill its own default by omission
                            pass
                        else:
                            msg = (
                                f"event(...): cannot populate payload field '{target_name}': "
                                f"no method arg, no self.{source_name}, and no default"
                            )
                            raise TypeError(msg)

                payload_instance = payload_type(**data)

            # only raise when not suppressing (rehydration mode)
            self._raise_event(event_type=event_type, event=payload_instance)
            return result

        # annotate wrapper so repository can map event_type -> method
        wrapper.__event_type__ = event_type  # type: ignore[attr-defined]
        wrapper.__payload_type__ = payload  # type: ignore[attr-defined]

        return wrapper  # type: ignore[return-value]

    return decorator
