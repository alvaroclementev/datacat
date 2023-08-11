"""Entrypoints for datacat"""

import argparse
import asyncio
import csv
import datetime
import itertools
import json
import sys
from pathlib import Path

Data = list[dict]

# TODO(alvaro): Add some simple way of configuring the generation (yaml?, use pydantic)
# TODO(alvaro): Add the concept of `Serializer` that controls the format of the generation (i.e: ndjson)
# TODO(alvaro): Add a concept that represents a timestamp generation
# TODO(alvaro): Add a concept of `Source`
# TODO(alvaro): Formaize the concept of `Sink`
# TODO(alvaro): Make this work in a streaming fashion (async source and sink, maybe AsyncIterator?)


class ConsoleSink:
    """A sink that outputs the file to the console"""

    def __init__(
        self,
        data: Data,
        n: int | None = None,
        add_timestamp: bool = True,
        timestamp_field: str = "timestamp",
    ):
        self.data = data
        self.n = n
        self.add_timestamp = add_timestamp
        self.timestamp_field = timestamp_field

    def output(self):
        data = self.data if self.n is None else itertools.islice(self.data, self.n)
        for row in data:
            if self.add_timestamp:
                row[self.timestamp_field] = datetime.datetime.now().isoformat()

            print(json.dumps(row))


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


async def generate_data(source: Path, n: int | None = None):
    """Generate the data"""
    data = load_csv(source)

    sink = ConsoleSink(data, n, add_timestamp=True)
    sink.output()


def load_csv(path: Path) -> Data:
    """Load the data from a CSV file"""
    with path.open("r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)


if __name__ == "__main__":
    raise SystemExit(main())
