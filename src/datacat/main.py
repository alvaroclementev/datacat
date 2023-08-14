"""Entrypoints for datacat"""

import abc
import argparse
import asyncio
import csv
import datetime
import itertools
import json
import sys
from pathlib import Path
from typing import Iterable

Row = dict
Data = list[Row]
LazyData = Iterable[Row]

# TODO(alvaro): Make this work in a streaming fashion (async source and sink, maybe AsyncIterator?)
# TODO(alvaro): Make source and sink async so that everything can work asynchronously
# TODO(alvaro): Add some simple way of configuring the generation (yaml?, use pydantic)
# TODO(alvaro): Add a way to configure the timing of the rows
#       - Regular interval
#       - Bursts / batches
#       - Custom Random Distribution


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


class Timestamper(abc.ABC):
    """A generator of timestamps for rows"""

    @abc.abstractmethod
    def timestamp(self) -> datetime.datetime:
        ...


class NowTimestamper(Timestamper):
    """A Timestamper that uses the current time"""

    def timestamp(self) -> datetime.datetime:
        return datetime.datetime.now()


class Sink(abc.ABC):
    """An object that outputs datasets into some format"""

    @abc.abstractmethod
    def output(self, data: LazyData):
        ...


class Serializer(abc.ABC):
    """An object that transforms a row into a serialized format"""

    @abc.abstractmethod
    def serialize(self, row: Row) -> str:
        ...


class JsonSerializer(Serializer):
    """A serializer that represents each row as a json object"""

    def serialize(self, row: Row) -> str:
        return json.dumps(row)


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

    def output(self, data: LazyData):
        data = data if self.n is None else itertools.islice(data, self.n)
        for row in data:
            if self.add_timestamp:
                row[self.timestamp_field] = self.timestamper.timestamp().isoformat()

            serialized = self.serializer.serialize(row)

            # Actually output to the console
            print(serialized)


def main() -> int:
    parser = argparse.ArgumentParser(description="Production like data generator")
    parser.add_argument("path", type=Path)
    parser.add_argument("-n", type=int, default=None)

    args = parser.parse_args()
    path = args.path
    n = args.n

    # Validate the path
    if not path.exists():
        print("data path not found", file=sys.stderr)
        return 1

    asyncio.run(generate_data(path, n))
    return 0


async def generate_data(source_path: Path, n: int | None = None):
    """Generate the data"""

    # Prepare the generator given the configuration
    # TODO(alvaro): Actually take a configuration
    source = CsvSource(source_path)
    serializer = JsonSerializer()
    timestamper = NowTimestamper()
    sink = ConsoleSink(serializer, timestamper, n=n, add_timestamp=True)

    # Run the generation
    data = source.load()
    sink.output(data)


if __name__ == "__main__":
    raise SystemExit(main())
