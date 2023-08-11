"""Entrypoints for datacat"""

import csv
import json

Data = list[dict]


def main() -> int:
    path = "data/iris.csv"
    data = load_csv(path)
    show_data(data)
    return 0


def load_csv(path: str) -> Data:
    """Load the data from a CSV file"""
    with open(path, "r", newline="") as csvfile:
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
