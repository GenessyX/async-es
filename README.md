# async-es

Async-first event sourcing toolkit for Python 3.14 with lightweight ergonomics:
- Dataclass-friendly `Aggregate` base and `@aggregate` decorator
- Simple `@event` method decorator to emit domain events
- Pluggable `EventStore` protocol with in-memory and PostgreSQL (asyncpg) backends
- `Repository` for rehydration + `Application` for save/notify orchestration
- Type-checked (mypy --strict) and linted (ruff), with CI on GitHub Actions

## Requirements
- Python 3.14
- For PostgreSQL tests/store: Docker (for testcontainers) and a running Docker daemon

## Install
Using uv (recommended):

```bash
uv sync --all-groups
```

Or pip:

```bash
pip install -e .
```

## Quickstart

### 1) Define an Aggregate and an Event
```python
from dataclasses import dataclass, field

from async_es.aggregate import Aggregate, aggregate, event
from async_es.types import AggregateId

class CounterId(AggregateId): ...

@dataclass
class Increment:
    amount: int

@aggregate
class Counter(Aggregate[CounterId]):
    id: CounterId = field(default_factory=CounterId.generate)
    value: int = 0

    @event("incremented", Increment)
    def add(self, amount: int) -> None:
        self.value += amount
```

### 2) Choose an Event Store
- In-memory (great for unit tests):
```python
from async_es.event_store.inmemory import InMemoryEventStore
store = InMemoryEventStore()
```

- PostgreSQL (asyncpg):
```python
from async_es.event_store.postgres import AsyncPGEventStore

store = await AsyncPGEventStore.create(dsn="postgresql://user:pass@host:5432/db")
await store.ensure_schema()  # one-time setup
```

### 3) Wire Repository and Application
```python
from typing import Any, cast

from async_es.application import Application
from async_es.repository.eventsourced import EventSourcedRepository

def make_repo(store) -> EventSourcedRepository[CounterId]:
    def factory(counter_id: CounterId) -> Counter:
        return Counter(id=counter_id)

    def apply(counter: Aggregate[CounterId], evt) -> None:
        concrete = cast(Counter, counter)
        if evt.event_type == "incremented":
            concrete.value += evt.payload.amount

    return EventSourcedRepository[CounterId](
        aggregate_type=Counter.aggregate_name,
        event_store=store,
        factory=factory,
        apply=apply,
    )

repo = make_repo(store)
app = Application[CounterId](repository=repo)
```

### 4) Use the Application
```python
counter_id = CounterId.generate()

counter = await app.load(counter_id)
counter.add(1)
counter.add(2)
await app.save(counter)

reloaded = await app.load(counter_id)
assert cast(Counter, reloaded).value == 3
```

## Notifications
`Application` accepts an optional `ApplicationNotifier` to publish saved events (e.g., Kafka). A reference `KafkaNotifier` stub is provided at `async_es/notifiers/kafka.py` for you to adapt.

```python
from async_es.notifiers.kafka import KafkaNotifier
app = Application[CounterId](repository=repo, notifier=KafkaNotifier(topic="domain-events"))
```

## Testing
- Unit tests can use `InMemoryEventStore`.
- Integration tests use PostgreSQL via `testcontainers` and `pytest-asyncio`.

See `tests/test_application.py` and `tests/test_application_postgres.py` for examples.

Run tests and tooling:

```bash
uv run ruff check .
uv run mypy . --strict
uv run pytest -q
```

## CI
GitHub Actions workflow (`.github/workflows/ci.yml`) runs ruff, mypy (strict), and pytest on push/PR with caching for uv, ruff, and mypy.

## License
MIT
