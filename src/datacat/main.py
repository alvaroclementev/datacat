"""Entrypoints for datacat"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from datacat import conductor, config, helpers, serializer, sink, source, timestamper

# TODO(alvaro): Make source and sink async so that everything can work asynchronously
# TODO(alvaro): Add the concept of ticks so that we can give more precise timings
#       - Make it have dynamic precision (e.g: 10ticks/s vs 1000ticks/s, depending on
#           the maximum precision requested)

# For debugging purposes
VERBOSE = False


def main() -> int:
    parser = argparse.ArgumentParser(description="Production like data generator")
    parser.add_argument(
        "path", nargs="?", type=Path, default=None, help="Path to source file"
    )
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
    n = args.n

    # Load the configuration
    conf = config.compile(args.config, args)

    # TODO(alvaro): Proper error handling
    asyncio.run(generate_data(conf, n))
    return 0


async def generate_data(conf: config.Configuration, n: int | None = None):
    """Generate the data based on the given configuration"""

    # Prepare the generator given the configuration
    gen_source = source.build(conf)
    gen_serializer = serializer.build(conf)
    gen_timestamper = timestamper.build(conf)
    gen_conductor = conductor.build(conf, verbose=VERBOSE)
    gen_sink = sink.build(conf)

    # Run the generation engine
    data = gen_source.load()
    stream = gen_conductor.conduct(data)
    # TODO(alvaro): Add support for batch output
    stream = stream if n is None else helpers.aislice(stream, n)
    async for row in stream:
        ts = gen_timestamper.timestamp()
        if ts is not None:
            row[gen_timestamper.field_name] = ts.isoformat()
        serialized = gen_serializer.serialize(row)
        await gen_sink.output(serialized)


if __name__ == "__main__":
    raise SystemExit(main())
