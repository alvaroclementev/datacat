"""Data sources"""
from __future__ import annotations

import abc
import csv
from pathlib import Path

from datacat.config import Configuration
from datacat.typing import Data


def build(conf: Configuration) -> Source:
    """Build the right `Source` for the given configuration"""

    if conf.source.type == "csv":
        return CsvSource(conf.source.path)
    raise ValueError("Unknown source configuration")


class Source(abc.ABC):
    """An object that encapsulates the source of some data"""

    @abc.abstractmethod
    def load(self) -> Data:
        ...


class CsvSource(Source):
    """A source that comes from a CSV file"""

    supports_streaming: bool = True

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> Data:
        # TODO(alvaro): Add support for limiting the number of rows to load
        with self.path.open("r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)
