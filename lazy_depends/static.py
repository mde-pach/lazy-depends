"""
StaticDepends — dependencies resolved once at startup via the app lifespan.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager as _asynccontextmanager
from typing import (
    Any,
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


async def _static_resolve() -> None:
    """
    Resolve all registered static dependencies concurrently.

    Call this during your app's lifespan startup. Results are stored
    and injected at request time with zero overhead.
    """

    async def _run_one(fn: Callable[..., Any], is_async: bool) -> tuple[int, Any]:
        if is_async:
            result = await fn()
        else:
            from starlette.concurrency import run_in_threadpool

            result = await run_in_threadpool(fn)
        return id(fn), result

    if not _STATIC_REGISTRY:
        return

    results = await asyncio.gather(*[_run_one(fn, is_async) for fn, is_async in _STATIC_REGISTRY])
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
