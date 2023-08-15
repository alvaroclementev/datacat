"""Data destinations"""
from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from datacat import helpers
from datacat.config import Configuration
from datacat.typing import AsyncData

if TYPE_CHECKING:
    from datacat.serializer import Serializer
    from datacat.timestamper import Timestamper


def build(
    conf: Configuration,
    serializer: Serializer,
    timestamper: Timestamper,
    n: int | None = None,
) -> Sink:
    """Build the right `Sink` for the given configuration"""

    if conf.sink.type == "console":
        return ConsoleSink(serializer, timestamper, n=n)
    raise ValueError("Unknown sink configuration")


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
    ):
        self.serializer = serializer
        self.timestamper = timestamper
        self.n = n

    async def output(self, data: AsyncData):
        data = data if self.n is None else helpers.aislice(data, self.n)
        async for row in data:
            ts = self.timestamper.timestamp()
            if ts is not None:
                row[self.timestamper.field_name] = ts.isoformat()

            serialized = self.serializer.serialize(row)

            # Actually output to the console
            print(serialized)
