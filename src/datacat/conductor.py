"""Objects that manage the timings for data generators.
They connect the Source and the Sink
"""

import abc
import asyncio

from datacat.typing import AsyncData, LazyData


class Conductor(abc.ABC):
    """An object that handles the `Source` and `Sink` orchestrating the
    generation of data with a given Timing
    """

    @abc.abstractmethod
    def conduct(self, data: LazyData) -> AsyncData:
        ...


class FixedRateConductor(abc.ABC):
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
            print(f"Slept for {self.row_period:.3f}s")
        try:
            return next(self._inner_iter)
        except StopIteration:
            raise StopAsyncIteration
