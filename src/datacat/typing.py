"""Shared types across the project"""
from typing import AsyncIterable, Iterable

Row = dict
Data = list[Row]
LazyData = Iterable[Row]
AsyncData = AsyncIterable[Row]
