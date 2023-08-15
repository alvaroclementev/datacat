"""Data sources"""
from __future__ import annotations

import abc
from pathlib import Path

from datacat.config import Configuration
from datacat.typing import Data


def build(conf: Configuration) -> Source:
    """Build the right `Source` for the given configuration"""

    if conf.source.type == "csv":
        return CsvSource(conf.source.path)
    elif conf.source.type == "parquet":
        return ParquetSource(conf.source.path)
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
        import pyarrow.csv

        # TODO(alvaro): Add support for limiting the number of rows to load
        table = pyarrow.csv.read_csv(self.path)
        return table.to_pylist()


class ParquetSource(Source):
    """A source that comes from a parquet file"""

    supports_streaming: bool = True

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> Data:
        import pyarrow.parquet

        # TODO(alvaro): Add support for limiting the number of rows to load
        table = pyarrow.parquet.read_table(self.path)
        return table.to_pylist()
