"""Entrypoints for datacat"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from datacat import conductor, config, serializer, sink, source, timestamper

# TODO(alvaro): Make source and sink async so that everything can work asynchronously
# TODO(alvaro): Add the concept of ticks so that we can give more precise timings
#       - Make it have dynamic precision (e.g: 10ticks/s vs 1000ticks/s, depending on
#           the maximum precision requested)

# For debugging purposes
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

    # TODO(alvaro): Merge the config coming from the command line (and environment?)
    conf = config.load(config_path)

    asyncio.run(generate_data(conf, n))
    return 0


async def generate_data(conf: config.Configuration, n: int | None = None):
    """Generate the data based on the given configuration"""

    # Prepare the generator given the configuration
    # TODO(alvaro): Extract the configuration from here
    gen_source = source.build(conf)
    gen_serializer = serializer.build(conf)
    gen_timestamper = timestamper.build(conf)
    gen_conductor = conductor.build(conf, verbose=VERBOSE)

    gen_sink = sink.build(conf, gen_serializer, gen_timestamper, n=n)

    # Run the generation
    data = gen_source.load()
    stream = gen_conductor.conduct(data)
    await gen_sink.output(stream)


if __name__ == "__main__":
    raise SystemExit(main())
