"""General helpers"""
from __future__ import annotations

import sys
from typing import AsyncIterable, TypeVar

_T = TypeVar("_T")


async def aenumerate(
    aiterable: AsyncIterable[_T], start: int = 0
) -> AsyncIterable[tuple[int, _T]]:
    assert start >= 0

    i = start
    async for x in aiterable:
        yield i, x
        i += 1


async def aislice(aiterable: AsyncIterable[_T], *args) -> AsyncIterable[_T]:
    """Similar to `itertools.islice` but working for `AsyncIterable`.

    See `itertools.islice` for more information
    """
    # aislice('ABCDEFG', 2) --> A B
    # aislice('ABCDEFG', 2, 4) --> C D
    # aislice('ABCDEFG', 2, None) --> C D E F G
    # aislice('ABCDEFG', 0, None, 2) --> A C E G

    s = slice(*args)
    start, stop, step = s.start or 0, s.stop or sys.maxsize, s.step or 1
    it = iter(range(start, stop, step))
    try:
        nexti = next(it)
    except StopIteration:
        # Consume *iterable* up to the *start* position.
        # for i, element in zip(range(start), iterable):
        #     pass
        return

    async for i, element in aenumerate(aiterable):
        if i == nexti:
            yield element
            try:
                nexti = next(it)
            except StopIteration:
                # # Consume to *stop*.
                # for i, element in zip(range(i + 1, stop), iterable):
                #     pass
                return
