"""Entrypoints for datacat"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from datacat import config
from datacat.conductor import FixedRateConductor
from datacat.serializer import JsonSerializer
from datacat.sink import ConsoleSink
from datacat.source import CsvSource
from datacat.timestamper import NowTimestamper

# TODO(alvaro): Make source and sink async so that everything can work asynchronously
# TODO(alvaro): Add some simple way of configuring the generation (yaml?, use pydantic)
# TODO(alvaro): Add the concept of ticks so that we can give more precise timings
#       - Make it have dynamic precision (e.g: 10ticks/s vs 1000ticks/s, depending on
#           the maximum precision requested)


# FIXME(alvaro): Make this configurable
ROWS_PER_S = 25
VERBOSE = False


def main() -> int:
    parser = argparse.ArgumentParser(description="Production like data generator")
    parser.add_argument("path", type=Path, help="Path to source file")
    parser.add_argument(
        "-n", type=int, default=None, help="Maximum number of rows to generate"
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default="datacat.yaml",
        help="Path to configuration file",
    )

    args = parser.parse_args()
    path = args.path
    n = args.n
    config_path = args.config

    # Validate the path
    if not path.exists():
        print("data path not found", file=sys.stderr)
        return 1

    # Load the configuration
    # TODO(alvaro): Can we provide sane defaults to that this just works? Maybe
    # require a path to a file and that's it (default to csv, now, no delay, json,
    # console)
    if not config_path.exists():
        print("no configuration file exists", file=sys.stderr)
        return 1

    # TODO(alvaro): Merge the config from the command line (and environment?)
    conf = config.load(config_path)

    asyncio.run(generate_data(conf, n))
    return 0


async def generate_data(conf: config.Configuration, n: int | None = None):
    """Generate the data based on the given configuration"""

    # Prepare the generator given the configuration
    # TODO(alvaro): Extract the configuration from here
    if conf.source.type == "csv":
        source = CsvSource(conf.source.path)

    if conf.format.type == "json":
        serializer = JsonSerializer()

    if conf.timestamp.type == "now":
        timestamper = NowTimestamper()

    if conf.sink.type == "console":
        sink = ConsoleSink(serializer, timestamper, n=n, add_timestamp=True)

    if conf.conductor.type == "rate":
        conductor = FixedRateConductor(conf.conductor.rate, verbose=VERBOSE)

    # Run the generation
    data = source.load()
    stream = conductor.conduct(data)
    await sink.output(stream)


if __name__ == "__main__":
    raise SystemExit(main())
