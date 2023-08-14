"""General helpers"""
import sys
from typing import AsyncIterable, TypeVar

_T = TypeVar("_T")

# TODO(alvaro): There exists async generators so we can make this much simpler
# much closer to how `itertools.islice` is implemented


class SlicedAsyncIterator:
    def __init__(self, inner: AsyncIterable[_T], *args):
        self.inner = aiter(inner)
        s = slice(*args)

        self._start = s.start or 0
        self._stop = s.stop or sys.maxsize
        self._step = s.step or 1
        self._next_idx = 0

        # Make sure the values are sane
        assert self._start >= 0
        assert self._stop >= 0
        assert self._step > 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Consume up to `start`
        while self._next_idx < self._start and self._next_idx < self._stop:
            self._next_idx += 1
            await anext(self.inner)

        # Skip if requested by step
        last_step = self._next_idx
        while self._next_idx - last_step < self._step - 1:
            self._next_idx += 1
            await anext(self.inner)

        # Check stop condition
        if self._next_idx >= self._stop:
            raise StopAsyncIteration

        # Return the actually requested value
        self._next_idx += 1
        return await anext(self.inner)


def aislice(aiterable: AsyncIterable[_T], *args) -> AsyncIterable[_T]:
    """Similar to `itertools.islice` for `AsyncIterable`"""
    # See `itertools.islice` for more information
    # aislice('ABCDEFG', 2) --> A B
    # aislice('ABCDEFG', 2, 4) --> C D
    # aislice('ABCDEFG', 2, None) --> C D E F G
    # aislice('ABCDEFG', 0, None, 2) --> A C E G
    return SlicedAsyncIterator(aiterable, *args)
