"""Data destinations"""
from __future__ import annotations

import abc

from datacat.config import Configuration
from datacat.typing import RawRow

# TODO(alvaro): Maybe serialization should be tied to the Sink?


def build(conf: Configuration) -> Sink:
    """Build the right `Sink` for the given configuration"""

    if conf.sink.type == "console":
        return ConsoleSink()
    raise ValueError("Unknown sink configuration")


class Sink(abc.ABC):
    """An object that outputs datasets into some format"""

    @abc.abstractmethod
    async def output(self, row: RawRow):
        ...


class ConsoleSink(Sink):
    """A sink that outputs the file to the console"""

    async def output(self, row: RawRow):
        print(row)
