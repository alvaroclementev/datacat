"""Objects that manage the timings for data generators.
They connect the Source and the Sink
"""
from __future__ import annotations

import abc
import asyncio
import datetime
import time

from datacat.config import Configuration
from datacat.typing import AsyncData, LazyData

# TODO(alvaro): Add different conductors
#   - Burst / Batches
#   - Custom Random Distributions


def build(conf: Configuration, verbose: bool = False) -> Conductor:
    """Build the right `Conductor` for the given configuration"""

    if conf.conductor.type == "rate":
        return FixedRateConductor(conf.conductor.rate, verbose=verbose)
    elif conf.conductor.type == "original":
        return OriginalRateConductor(
            conf.conductor.field_name, conf.conductor.format, verbose=verbose
        )
    raise ValueError("Unknown source configuration")


class Conductor(abc.ABC):
    """An object that handles the `Source` and `Sink` orchestrating the
    generation of data with a given Timing
    """

    @abc.abstractmethod
    def conduct(self, data: LazyData) -> AsyncData:
        ...


class FixedRateConductor(Conductor):
    """Timing Generator that yields rows at a fixed rate (rows/s)"""

    def __init__(self, rows_per_s: float, *, verbose: bool = False):
        assert rows_per_s > 0

        self.rows_per_s = rows_per_s
        self.row_period = 1 / rows_per_s
        self.verbose = verbose

    def conduct(self, data: LazyData) -> AsyncData:
        return FixedRateConductorIterator(data, self.row_period, verbose=self.verbose)


class FixedRateConductorIterator:
    """An AsyncIterator that produces the rows at a fixed rate"""

    def __init__(self, data: LazyData, row_period: float, verbose: bool = False):
        self._inner_iter = iter(data)
        self.row_period = row_period
        self.verbose = verbose

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(self.row_period)
        if self.verbose:
            print(f"{self.__class__.__name__} slept for {self.row_period:.3f}s")
        try:
            return next(self._inner_iter)
        except StopIteration:
            raise StopAsyncIteration


class OriginalRateConductor(Conductor):
    """A Timing Generator that maintains the original rate from the source for
    data generation.

    For this, it looks at a given `timestamp_field` and produces the data to match
    the given timestamp field
    """

    def __init__(
        self,
        timestamp_field: str,
        datetime_format: str | None = None,
        *,
        verbose: bool = False,
    ):
        self.timestamp_field = timestamp_field
        self.datetime_format = datetime_format
        self.verbose = verbose

    def conduct(self, data: LazyData) -> AsyncData:
        return OriginalRateConductorIterator(
            data, self.timestamp_field, self.datetime_format, verbose=self.verbose
        )


class OriginalRateConductorIterator:
    """An AsyncIterator that produces the rows maintaing the original time rate"""

    def __init__(
        self,
        data: LazyData,
        timestamp_field: str,
        datetime_format: str | None,
        verbose: bool = False,
    ):
        self._inner_iter = iter(data)
        self.timestamp_field = timestamp_field
        self.datetime_format = datetime_format
        self.verbose = verbose
        self.previous_timestamp = None
        self.previous_tick = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Pull the next value
        try:
            next_value = next(self._inner_iter)
        except StopIteration:
            raise StopAsyncIteration

        # Check the timestamp to maintain the rate
        tick = time.monotonic_ns()
        timestamp = self._to_datetime(next_value[self.timestamp_field])

        if not self.previous_timestamp:
            # We don't have a previous timestamp, so we yield the value straight away
            self.previous_timestamp = timestamp
            self.previous_tick = tick
            return next_value

        # There was a previous timestamp, make sure enough time has passed
        expected_delta_us = round(
            (timestamp - self.previous_timestamp) / datetime.timedelta(microseconds=1)
        )
        passed_delta_us = round((tick - self.previous_tick) / 1000)
        sleep_time_s = max(0, (expected_delta_us - passed_delta_us) / 1_000_000)
        if sleep_time_s > 0:
            await asyncio.sleep(sleep_time_s)
            tick = time.monotonic_ns()
            if self.verbose:
                print(f"{self.__class__.__name__} slept for {sleep_time_s:.3f}s")

        # Now we can yield the result, storing the timing information for the next
        # iteration
        self.previous_timestamp = timestamp
        self.previous_tick = tick
        return next_value

    def _to_datetime(self, datetime_str: str | datetime.datetime) -> datetime.datetime:
        if isinstance(datetime_str, datetime.datetime):
            return datetime_str

        if not self.datetime_format:
            return datetime.datetime.fromisoformat(datetime_str)
        else:
            return datetime.datetime.strptime(datetime_str, self.datetime_format)
