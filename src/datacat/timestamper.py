"""Timestamp generation"""
from __future__ import annotations

import abc
import datetime

from datacat.config import Configuration


def build(conf: Configuration) -> Timestamper:
    """Build the right `Timestamper` for the given configuration"""

    if conf.timestamp.type == "now":
        return NowTimestamper(field_name=conf.timestamp.field_name)
    elif conf.timestamp.type == "none":
        return NoneTimestamper()
    raise ValueError("Unknown timestamp configuration")


class Timestamper(abc.ABC):
    """A generator of timestamps for rows"""

    def __init__(self, field_name: str = "timestamp"):
        self.field_name = field_name

    @abc.abstractmethod
    def timestamp(self) -> datetime.datetime | None:
        ...


class NoneTimestamper(Timestamper):
    """A Timestamper that does not include the timestamp"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def timestamp(self) -> datetime.datetime | None:
        return None


class NowTimestamper(Timestamper):
    """A Timestamper that uses the current time"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def timestamp(self) -> datetime.datetime | None:
        return datetime.datetime.now()
