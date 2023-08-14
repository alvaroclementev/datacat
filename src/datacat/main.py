"""Entrypoints for datacat"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from datacat.conductor import FixedRateConductor
from datacat.serializer import JsonSerializer
from datacat.sink import ConsoleSink
from datacat.source import CsvSource
from datacat.timestamper import NowTimestamper

# TODO(alvaro): Make source and sink async so that everything can work asynchronously
# TODO(alvaro): Add the concept of ticks so that we can give more precise timings
#       - Make it have dynamic precision (e.g: 10ticks/s vs 1000ticks/s, depending on
#           the maximum precision requested)
# TODO(alvaro): Add some simple way of configuring the generation (yaml?, use pydantic)


# FIXME(alvaro): Make this configurable
ROWS_PER_S = 25
VERBOSE = False


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

    conductor = FixedRateConductor(ROWS_PER_S, verbose=VERBOSE)

    # Run the generation
    data = source.load()
    stream = conductor.conduct(data)
    await sink.output(stream)


if __name__ == "__main__":
    raise SystemExit(main())
