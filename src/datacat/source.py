"""Data sources"""
from __future__ import annotations

import abc
from pathlib import Path

from datacat.config import Configuration
from datacat.typing import Data


def build(conf: Configuration) -> Source:
    """Build the right `Source` for the given configuration"""
    try:
        if conf.source.type == "glob":
            source_class = FILE_SOURCE_TYPE_MAP[conf.source.source_type]
            return GlobFileSource(glob=conf.source.glob, source_class=source_class)

        cls = FILE_SOURCE_TYPE_MAP[conf.source.type]
        if issubclass(cls, FileSource):
            return cls(path=conf.source.path)
        raise AssertionError("unreachable")
    except KeyError:
        raise ValueError("Unknown source configuration")


class Source(abc.ABC):
    """An object that encapsulates the source of some data"""

    @abc.abstractmethod
    def load(self) -> Data:
        ...


class FileSource(Source):
    """A source"""

    def __init__(self, path: Path):
        self.path = path

    @abc.abstractmethod
    def load(self) -> Data:
        ...


class CsvSource(FileSource):
    """A source that comes from a CSV file"""

    def load(self) -> Data:
        import pyarrow.csv

        # TODO(alvaro): Add support for limiting the number of rows to load
        table = pyarrow.csv.read_csv(self.path)
        return table.to_pylist()


class ParquetSource(FileSource):
    """A source that comes from a parquet file"""

    def load(self) -> Data:
        import pyarrow.parquet

        # TODO(alvaro): Add support for limiting the number of rows to load
        table = pyarrow.parquet.read_table(self.path)
        return table.to_pylist()


class NdJsonSource(FileSource):
    """A source that comes from a NdJSON (newline delimited JSON) file"""

    def load(self) -> Data:
        import json

        with self.path.open("r") as f:
            data = [json.loads(line.strip()) for line in f.readlines()]

        return data


class JsonSource(FileSource):
    """A source that comes from a file that contains JSON array of objects"""

    def load(self) -> Data:
        import json

        with self.path.open("r") as f:
            data = json.load(f)

        if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
            raise ValueError(
                "invalid format for JSON source: it should be an array of objects"
            )
        return data


class GlobFileSource(Source):
    """A source that represents a glob of files that should be loaded"""

    # NOTE(alvaro): Technically we could support loading a glob of different file types
    # and detect the relevant source for each... but not interested for now
    def __init__(self, glob: str, source_class: type[FileSource]):
        self.glob = glob
        self.source_class = source_class

    def load(self) -> Data:
        import glob

        data = []
        for result in glob.glob(self.glob, recursive=True):
            path = Path(result)

            # TODO(alvaro): Proper file validation
            assert path.exists()
            if not path.is_file():
                raise RuntimeError("glob must only return files")

            source = self.source_class(path=path)
            source_data = source.load()
            data.extend(source_data)
        return data


FILE_SOURCE_TYPE_MAP: dict[str, type[FileSource]] = {
    "csv": CsvSource,
    "parquet": ParquetSource,
    "ndjson": NdJsonSource,
    "json": JsonSource,
}
