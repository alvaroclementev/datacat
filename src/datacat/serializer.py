"""Data output serialization"""
from __future__ import annotations

import abc
import json

from datacat.config import Configuration
from datacat.typing import Row


def build(conf: Configuration) -> Serializer:
    """Build the right `Serializer` for the given configuration"""

    if conf.format.type == "json":
        return JsonSerializer()
    raise ValueError("Unknown serializer configuration")


class Serializer(abc.ABC):
    """An object that transforms a row into a serialized format"""

    @abc.abstractmethod
    def serialize(self, row: Row) -> str:
        ...


class JsonSerializer(Serializer):
    """A serializer that represents each row as a json object"""

    def serialize(self, row: Row) -> str:
        return json.dumps(row)
