# async-es

Async-first event sourcing toolkit for Python 3.14 with lightweight ergonomics:
- Dataclass-friendly `Aggregate` base and `@aggregate` decorator
- Simple `@event` method decorator to emit domain events
- Pluggable `EventStore` protocol with in-memory and PostgreSQL (asyncpg) backends
- Event-sourced `Repository` that rebuilds state by calling aggregate methods
- `Application` for load/save orchestration and optional notifications via `ApplicationNotifier`
- Type-checked (mypy --strict) and linted (ruff), with CI on GitHub Actions

## Requirements
- Python 3.14
- For PostgreSQL/Kafka tests/store: Docker (for testcontainers) and a running Docker daemon

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

### 1) Define an Aggregate and Events
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

Notes:
- On first construction, every `Aggregate` emits a `created` event with all initialization fields inside `Created.data`.
- Rehydration calls your original `@event` methods under suppression (so they don’t emit new events during replay).

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
from async_es.application import Application
from async_es.repository.eventsourced import EventSourcedRepository

repo = EventSourcedRepository[CounterId](
    aggregate_type=Counter.aggregate_name,
    event_store=store,
    aggregate_cls=Counter,  # no custom factory/apply needed
)

app = Application[CounterId](repository=repo)
```

### 4) Use the Application
```python
counter_id = CounterId.generate()

counter = await app.load(counter_id)     # emits a 'created' event on first construct
counter.add(1)
counter.add(2)
await app.save(counter)                  # persists ['created','incremented','incremented']

reloaded = await app.load(counter_id)
assert reloaded.value == 3
```

## Notifications (Kafka)
`Application` accepts an optional `ApplicationNotifier` to publish saved events. A Kafka implementation is provided.

```python
from async_es.notifiers.kafka import KafkaNotifier

notifier = KafkaNotifier(
    bootstrap_servers="localhost:9092",
    topic="domain-events",
)
app = Application[CounterId](repository=repo, notifier=notifier)
```

The Kafka payload is a compact JSON with: `id`, `aggregate_type`, `aggregate_id`, `event_type`, `payload` (dataclass serialized), `occurred_at`, `published_at`, `metadata`.

> Production note: For strong guarantees (no lost/duplicate notifications), consider a Transactional Outbox. Write events + an outbox row in the same DB transaction, and use a background publisher to deliver and mark rows as sent.

## Local infrastructure (optional)
Use `docker-compose` for fast local cycles. It provides Postgres, Kafka (KRaft), and Kafka UI.

```bash
docker compose up -d postgres kafka kafka-ui
```
- Kafka UI: http://localhost:8080
- Defaults used across tests/tools:
  - Postgres DSN: `postgresql://test:test@localhost:5432/testdb`
  - Kafka bootstrap: `localhost:9092`

## Testing
- Tests prefer existing infra via environment, otherwise fall back to testcontainers.
- Configure environment via `pytest-dotenv` (loads `tests/.env`):
  - `POSTGRES_DSN=postgresql://test:test@localhost:5432/testdb`
  - `KAFKA_BOOTSTRAP=localhost:9092`
  - `KAFKA_TEST_TOPIC=test-events` (optional)

Behavior:
- Postgres tests: if `POSTGRES_DSN` is set, the fixture cleans `events` rows for the test aggregate after each test.
- Kafka tests: if `KAFKA_BOOTSTRAP` is set, the fixture deletes and recreates the test topic before and after tests to avoid interference.

Run tests and tooling:

```bash
uv run ruff check .
uv run mypy . --strict
uv run pytest -q
```

## CI
GitHub Actions workflow (`.github/workflows/ci.yml`) runs ruff, mypy (strict), and pytest on push/PR with caching. Python 3.14 is used. Testcontainers-based integration tests run automatically.

## License
MIT
