"""
Lazy[T] — pure awaitable wrapper around an ``asyncio.Task``.

``await`` it to get the resolved value::

    user = await lazy_user   # user is a real T

That's the only operation. No proxy, no magic attribute forwarding.
If the task raised, the exception propagates on ``await``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Lazy(Generic[T]):
    """
    Wraps an ``asyncio.Task[T]``.

    ``await`` it to get the resolved value::

        user = await lazy_user   # user is a real T

    That's the only operation. No proxy, no magic attribute forwarding.
    If the task raised, the exception propagates on ``await``.
    """

    __slots__ = ("_task",)

    def __init__(self, task: asyncio.Task[T], *, name: str = "") -> None:
        self._task = task

    def __await__(self) -> Generator[Any, None, T]:
        return self._task.__await__()

    def __repr__(self) -> str:
        if self._task.done() and not self._task.cancelled():
            exc = self._task.exception()
            if exc is None:
                return f"<Lazy resolved={self._task.result()!r}>"
            return f"<Lazy error={exc!r}>"
        return "<Lazy pending>"
