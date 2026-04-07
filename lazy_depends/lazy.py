"""
Lazy[T] wrapper and LazyDepends marker for deferred dependency resolution.

Usage:
    from lazy_depends import Depends, LazyDepends, ConcurrentRoute

    app.router.route_class = ConcurrentRoute

    @app.get("/")
    async def root(
        db=Depends(get_db),                  # eager
        lazy_user=LazyDepends(get_user),     # starts resolving in background
    ):
        await db.execute("SELECT 1")
        await asyncio.sleep(0.5)
        user = await lazy_user               # real value, no proxy
        print(user["name"])
"""

from __future__ import annotations

import asyncio
import inspect
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Generator,
    TypeVar,
    overload,
)

try:
    from fastapi import params as _fastapi_params

    _HAS_FASTAPI = True
except ImportError:
    _HAS_FASTAPI = False

T = TypeVar("T")


# ──────────────────────────────────────────────────────────
# Marker subclass — detected per-route from the signature
# ──────────────────────────────────────────────────────────

if _HAS_FASTAPI:

    class _LazyDependsMarker(_fastapi_params.Depends):
        """Subclass of fastapi.params.Depends used to detect lazy deps."""

        pass


def detect_lazy_dep_names(endpoint: Callable[..., Any]) -> set[str] | None:
    """
    Inspect an endpoint function's signature and return the set of
    parameter names whose default is a ``LazyDepends`` marker.

    Returns ``None`` if no lazy deps are found (avoids allocating
    an empty set on every request for non-lazy routes).
    """
    if not _HAS_FASTAPI:
        return None
    result: set[str] | None = None
    for name, param in inspect.signature(endpoint).parameters.items():
        if isinstance(param.default, _LazyDependsMarker):
            if result is None:
                result = set()
            result.add(name)
    return result


# ──────────────────────────────────────────────────────────
# LazyDepends — drop-in marker (typed to return Lazy[T])
# ──────────────────────────────────────────────────────────


@overload
def LazyDepends(
    dependency: Callable[..., Awaitable[T]], *, use_cache: bool = True
) -> "Lazy[T]": ...


@overload
def LazyDepends(
    dependency: Callable[..., T], *, use_cache: bool = True
) -> "Lazy[T]": ...


def LazyDepends(dependency: Any, *, use_cache: bool = True) -> Any:
    """
    Mark a dependency for deferred (lazy) resolution.

    Returns a ``_LazyDependsMarker`` (subclass of ``fastapi.params.Depends``)
    so FastAPI builds the dependency graph normally.  ``ConcurrentRoute``
    detects the marker from the endpoint signature and injects a ``Lazy[T]``
    that must be awaited to get the real value::

        @app.get("/")
        async def root(lazy_user=LazyDepends(get_user)):
            user = await lazy_user   # real T, no proxy
    """
    if not _HAS_FASTAPI:
        raise ImportError("fastapi is required for LazyDepends")
    return _LazyDependsMarker(dependency=dependency, use_cache=use_cache)


# ──────────────────────────────────────────────────────────
# StaticDepends — resolved at startup via lifespan
# ──────────────────────────────────────────────────────────

# Global registry: callable → resolved value
_STATIC_REGISTRY: list[tuple[Callable[..., Any], bool]] = []
_STATIC_VALUES: dict[int, Any] = {}  # keyed by id(callable)


@overload
def StaticDepends(
    dependency: Callable[..., Awaitable[T]],
) -> T: ...


@overload
def StaticDepends(
    dependency: Callable[..., T],
) -> T: ...


def StaticDepends(dependency: Any) -> Any:
    """
    A dependency resolved **once at startup** via the app lifespan.

    The result is injected directly into endpoints — zero overhead
    at request time (no callable, no lock, just a dict lookup).

    Setup::

        from lazy_depends import StaticDepends

        async def load_config():
            return await fetch_from_db()

        app = FastAPI(lifespan=StaticDepends.lifespan)

        @app.get("/")
        async def root(config=StaticDepends(load_config)):
            ...  # config is the real dict, resolved at startup

    To compose with your own lifespan, use ``StaticDepends.resolve``::

        @asynccontextmanager
        async def lifespan(app):
            await StaticDepends.resolve()
            # ... your own startup logic ...
            yield
            # ... your own shutdown logic ...

    Not suitable for generator (yield) dependencies — use ``Depends``
    for those.
    """
    if not _HAS_FASTAPI:
        raise ImportError("fastapi is required for StaticDepends")

    _is_async = asyncio.iscoroutinefunction(dependency)
    _STATIC_REGISTRY.append((dependency, _is_async))
    dep_id = id(dependency)

    async def _getter() -> Any:
        if dep_id not in _STATIC_VALUES:
            raise RuntimeError(
                f"StaticDepends({getattr(dependency, '__name__', '?')}) was not "
                f"resolved at startup. Make sure your app uses "
                f"StaticDepends.lifespan or calls StaticDepends.resolve() "
                f"in your own lifespan."
            )
        return _STATIC_VALUES[dep_id]

    return _fastapi_params.Depends(dependency=_getter, use_cache=True)


from contextlib import asynccontextmanager as _asynccontextmanager


async def _static_resolve() -> None:
    """
    Resolve all registered static dependencies concurrently.

    Call this during your app's lifespan startup. Results are stored
    and injected at request time with zero overhead.
    """

    async def _run_one(
        fn: Callable[..., Any], is_async: bool
    ) -> tuple[int, Any]:
        if is_async:
            result = await fn()
        else:
            from starlette.concurrency import run_in_threadpool

            result = await run_in_threadpool(fn)
        return id(fn), result

    if not _STATIC_REGISTRY:
        return

    results = await asyncio.gather(
        *[_run_one(fn, is_async) for fn, is_async in _STATIC_REGISTRY]
    )
    for dep_id, value in results:
        _STATIC_VALUES[dep_id] = value


@_asynccontextmanager
async def _static_lifespan(app: Any):
    """
    Ready-to-use lifespan that resolves all static deps at startup::

        app = FastAPI(lifespan=StaticDepends.lifespan)
    """
    await _static_resolve()
    yield


def _static_reset() -> None:
    """Clear all static values and registry (for testing)."""
    _STATIC_VALUES.clear()
    _STATIC_REGISTRY.clear()


# Attach to StaticDepends so the API reads naturally:
#   StaticDepends.lifespan, StaticDepends.resolve(), StaticDepends.reset()
StaticDepends.lifespan = _static_lifespan  # type: ignore[attr-defined]
StaticDepends.resolve = _static_resolve  # type: ignore[attr-defined]
StaticDepends.reset = _static_reset  # type: ignore[attr-defined]


# ──────────────────────────────────────────────────────────
# CachedDepends — TTL-based caching across requests
# ──────────────────────────────────────────────────────────


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

    import functools
    import time as _time

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


# ──────────────────────────────────────────────────────────
# Lazy[T] — pure awaitable, no proxy magic
# ──────────────────────────────────────────────────────────


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
