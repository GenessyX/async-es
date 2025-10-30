from dataclasses import dataclass, field

from async_es.aggregate import Aggregate, aggregate, event
from async_es.event import DomainEvent
from async_es.types import AggregateId


class DogId(AggregateId): ...


@dataclass(kw_only=True)
class TrickAddedEvent:
    trick: str


@dataclass(kw_only=True)
class TrickAddedEventV2:
    name: str


@dataclass(kw_only=True)
class FooBarChangedEvent:
    bar_alias: str
    foo_alias: str


@aggregate
class Dog(Aggregate[DogId]):
    aggregate_name = "Dog"

    id: DogId = field(default_factory=DogId.generate)
    name: str
    tricks: list[str] = field(default_factory=list)
    foo: str = ""
    bar: str = ""

    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)
        self._raise_event(event_type="TRICK_ADDED", event=TrickAddedEvent(trick=trick))

    @event("TRICK_ADDED", TrickAddedEvent)
    def add_trick_decorated(self, trick: str) -> None:
        self.tricks.append(trick)

    @event("TRICK_ADDED", TrickAddedEventV2)
    def add_trick_decorated_args(self, trick: str) -> None:
        self.tricks.append(trick)

    @event(
        "FOO_BAR_CHANGED",
        FooBarChangedEvent,
        aliases={"foo_alias": "foo", "bar_alias": "bar"},
    )
    def change_foo_bar(self, foo: str, bar: str) -> None:
        self.foo = foo
        self.bar = bar


def test_event_raised() -> None:
    dog = Dog(name="Bobby")
    dog.add_trick("roll over")
    assert dog.tricks == ["roll over"]
    trick_added = dog.get_event_as(TrickAddedEvent)
    assert isinstance(trick_added, DomainEvent)
    assert trick_added.aggregate_id == dog.id
    assert trick_added.aggregate_type == Dog.aggregate_name
    assert trick_added.event_type == "TRICK_ADDED"
    assert isinstance(trick_added.payload, TrickAddedEvent)
    assert trick_added.payload.trick == "roll over"


def test_event_raised_decorated() -> None:
    dog = Dog(name="Bobby")
    dog.add_trick_decorated("roll over")
    assert dog.tricks == ["roll over"]
    trick_added = dog.get_event_as(TrickAddedEvent)
    assert isinstance(trick_added, DomainEvent)
    assert trick_added.aggregate_id == dog.id
    assert trick_added.aggregate_type == Dog.aggregate_name
    assert trick_added.event_type == "TRICK_ADDED"
    assert isinstance(trick_added.payload, TrickAddedEvent)
    assert trick_added.payload.trick == "roll over"


def test_event_raised_decorated_args() -> None:
    dog = Dog(name="Bobby")
    dog.add_trick_decorated_args("roll over")
    assert dog.tricks == ["roll over"]
    trick_added = dog.get_event_as(TrickAddedEventV2)
    assert isinstance(trick_added, DomainEvent)
    assert trick_added.aggregate_id == dog.id
    assert trick_added.aggregate_type == Dog.aggregate_name
    assert trick_added.event_type == "TRICK_ADDED"
    assert isinstance(trick_added.payload, TrickAddedEventV2)
    assert trick_added.payload.name == "roll over"


def test_event_raised_decorated_alias() -> None:
    dog = Dog(name="Bobby")
    dog.change_foo_bar(foo="foo", bar="bar")
    assert dog.foo == "foo"
    assert dog.bar == "bar"
    changed = dog.get_event_as(FooBarChangedEvent)
    assert isinstance(changed, DomainEvent)
    assert changed.aggregate_id == dog.id
    assert changed.aggregate_type == Dog.aggregate_name
    assert changed.event_type == "FOO_BAR_CHANGED"
    assert isinstance(changed.payload, FooBarChangedEvent)
    assert changed.payload.foo_alias == "foo"
    assert changed.payload.bar_alias == "bar"
