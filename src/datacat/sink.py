"""Data destinations"""
from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from datacat import helpers
from datacat.typing import AsyncData

if TYPE_CHECKING:
    from datacat.serializer import Serializer
    from datacat.timestamper import Timestamper


class Sink(abc.ABC):
    """An object that outputs datasets into some format"""

    @abc.abstractmethod
    async def output(self, data: AsyncData):
        ...


class ConsoleSink(Sink):
    """A sink that outputs the file to the console"""

    supports_streaming: bool = True

    def __init__(
        self,
        serializer: Serializer,
        timestamper: Timestamper,
        *,
        n: int | None = None,
        add_timestamp: bool = True,
        timestamp_field: str = "timestamp",
    ):
        self.serializer = serializer
        self.timestamper = timestamper
        self.n = n
        self.add_timestamp = add_timestamp
        self.timestamp_field = timestamp_field

    async def output(self, data: AsyncData):
        data = data if self.n is None else helpers.aislice(data, self.n)
        async for row in data:
            if self.add_timestamp:
                row[self.timestamp_field] = self.timestamper.timestamp().isoformat()

            serialized = self.serializer.serialize(row)

            # Actually output to the console
            print(serialized)
