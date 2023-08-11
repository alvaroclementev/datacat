"""Entrypoints for datacat"""

import argparse
import csv
import json
import sys
from pathlib import Path

Data = list[dict]


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

    data = load_csv(path)
    show_data(data, n)
    return 0


def load_csv(path: Path) -> Data:
    """Load the data from a CSV file"""
    with path.open("r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)


def show_data(data: Data, n: int | None = None):
    """Show the data through the console"""
    if n is not None:
        data = data[:n]

    for row in data:
        print(json.dumps(row))


if __name__ == "__main__":
    raise SystemExit(main())
