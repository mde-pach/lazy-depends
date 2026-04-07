"""
Dependency marker functions: ``Depends`` and ``LazyDepends``.

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

import inspect
from collections.abc import Awaitable, Callable
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

from lazy_depends.lazy import Lazy

T = TypeVar("T")


def Depends(dependency: Callable, *, use_cache: bool = True):
    """
    Drop-in replacement for fastapi.Depends with concurrent resolution.

    Returns a standard fastapi.params.Depends — no wrapping, no proxying.
    The concurrency is handled entirely by ConcurrentRoute.

    Usage identical to FastAPI — only the import changes:

        from lazy_depends import Depends          # instead of: from fastapi import Depends

        async def get_db(): ...
        async def get_user(db=Depends(get_db)): ...

    Pair with ConcurrentRoute for automatic parallel resolution:

        app.router.route_class = ConcurrentRoute
    """
    if not _HAS_FASTAPI:
        raise ImportError("fastapi is required for lazy_depends.Depends")
    return _fastapi_params.Depends(dependency, use_cache=use_cache)


# ──────────────────────────────────────────────────────────
# Marker subclass — detected per-route from the signature
# ──────────────────────────────────────────────────────────

if _HAS_FASTAPI:

    class _LazyDependsMarker(_fastapi_params.Depends):
        """Subclass of fastapi.params.Depends used to detect lazy deps."""

        pass


def detect_lazy_dep_names(fn: Callable[..., Any]) -> set[str] | None:
    """
    Inspect a function's signature and return the set of
    parameter names whose default is a ``LazyDepends`` marker.

    Returns ``None`` if no lazy deps are found.
    """
    if not _HAS_FASTAPI:
        return None
    result: set[str] | None = None
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return None
    for name, param in sig.parameters.items():
        if isinstance(param.default, _LazyDependsMarker):
            if result is None:
                result = set()
            result.add(name)
    return result


def build_lazy_map(graph: dict) -> dict[Any, set[str]]:
    """
    Scan all nodes in the dependency graph and build a map of
    ``{node_key: set(param_names)}`` for every callable that has
    ``LazyDepends`` params.

    Used by the solver to inject ``Lazy(task)`` instead of awaiting
    for those children.
    """
    lazy_map: dict[Any, set[str]] = {}
    for key, (dep, call, child_keys) in graph.items():
        lazy_names = detect_lazy_dep_names(call)
        if lazy_names:
            lazy_map[key] = lazy_names
    return lazy_map


# ──────────────────────────────────────────────────────────
# LazyDepends — drop-in marker (typed to return Lazy[T])
# ──────────────────────────────────────────────────────────


@overload
def LazyDepends(dependency: Callable[..., Awaitable[T]], *, use_cache: bool = True) -> Lazy[T]: ...


@overload
def LazyDepends(dependency: Callable[..., T], *, use_cache: bool = True) -> Lazy[T]: ...


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
