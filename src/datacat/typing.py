"""Shared types across the project"""
from __future__ import annotations

from typing import AsyncIterable, Iterable

Row = dict
Data = list[Row]
LazyData = Iterable[Row]
AsyncData = AsyncIterable[Row]
