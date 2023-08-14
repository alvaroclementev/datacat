"""Timestamp generation"""
from __future__ import annotations

import abc
import datetime


class Timestamper(abc.ABC):
    """A generator of timestamps for rows"""

    @abc.abstractmethod
    def timestamp(self) -> datetime.datetime:
        ...


class NowTimestamper(Timestamper):
    """A Timestamper that uses the current time"""

    def timestamp(self) -> datetime.datetime:
        return datetime.datetime.now()
