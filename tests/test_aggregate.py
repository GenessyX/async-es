from dataclasses import dataclass, field

from async_es.aggregate import Aggregate, aggregate
from async_es.event import DomainEvent
from async_es.types import AggregateId


class DogId(AggregateId): ...


@dataclass(kw_only=True)
class TrickAddedEvent:
    name: str


@aggregate
class Dog(Aggregate[DogId]):
    aggregate_name = "Dog"

    id: DogId = field(default_factory=DogId.generate)
    name: str
    tricks: list[str] = field(default_factory=list)

    def add_trick(self, trick: str) -> None:
        self.tricks.append(trick)
        self._raise_event(event_type="TRICK_ADDED", event=TrickAddedEvent(name=trick))


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
    assert trick_added.payload.name == "roll over"
