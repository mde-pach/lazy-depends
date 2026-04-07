"""
CachedDepends — TTL-based caching across requests.
"""

from __future__ import annotations

import asyncio
import functools
import time as _time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, overload

try:
    from fastapi import params as _fastapi_params

    _HAS_FASTAPI = True
except ImportError:
    _HAS_FASTAPI = False

T = TypeVar("T")


@overload
def CachedDepends(
    dependency: Callable[..., Awaitable[T]], *, ttl: float, use_cache: bool = True
) -> T: ...


@overload
def CachedDepends(dependency: Callable[..., T], *, ttl: float, use_cache: bool = True) -> T: ...


def CachedDepends(dependency: Any, *, ttl: float, use_cache: bool = True) -> Any:
    """
    TTL-based caching dependency.

    Wraps a callable so that its result is cached for ``ttl`` seconds.
    Subsequent requests within the TTL window receive the cached value
    without invoking the callable again.

    Works with both sync and async callables.

    Usage::

        async def get_config():
            return await fetch_from_db()

        @app.get("/")
        async def root(config=CachedDepends(get_config, ttl=60)):
            ...
    """
    if not _HAS_FASTAPI:
        raise ImportError("fastapi is required for CachedDepends")

    _is_async = asyncio.iscoroutinefunction(dependency)
    _cache: dict = {}
    _lock: asyncio.Lock | None = None

    @functools.wraps(dependency)
    async def _wrapper(*args, **kwargs):
        nonlocal _lock
        now = _time.monotonic()
        if "v" in _cache and now < _cache["exp"]:
            return _cache["v"]
        if _lock is None:
            _lock = asyncio.Lock()
        async with _lock:
            now = _time.monotonic()
            if "v" in _cache and now < _cache["exp"]:
                return _cache["v"]
            result = await dependency(*args, **kwargs) if _is_async else dependency(*args, **kwargs)
            _cache["v"] = result
            _cache["exp"] = now + ttl
            return result

    return _fastapi_params.Depends(dependency=_wrapper, use_cache=use_cache)
