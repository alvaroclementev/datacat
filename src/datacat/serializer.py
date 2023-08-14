"""Data output serialization"""
from __future__ import annotations

import abc
import json

from datacat.typing import Row


class Serializer(abc.ABC):
    """An object that transforms a row into a serialized format"""

    @abc.abstractmethod
    def serialize(self, row: Row) -> str:
        ...


class JsonSerializer(Serializer):
    """A serializer that represents each row as a json object"""

    def serialize(self, row: Row) -> str:
        return json.dumps(row)
