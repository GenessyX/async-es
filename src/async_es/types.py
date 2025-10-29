from typing import Self
from uuid import UUID, uuid7


class AggregateId(UUID):
    @classmethod
    def generate(cls) -> Self:
        return cls(hex=uuid7().hex)
