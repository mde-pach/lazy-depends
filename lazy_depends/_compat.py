"""
Feature-detected compatibility layer for FastAPI internals.

``lazy_depends`` reimplements FastAPI's request handler and dependency
solver, so it necessarily touches private-ish FastAPI internals.  Those
internals move between minor releases.  Rather than branching on a single
"era" boolean, every capability this package needs is probed **once at
import time**, one probe per symbol, with a pure-Python fallback wherever
one is possible.

The result: a FastAPI release that renames or drops one symbol degrades
that single capability instead of breaking the whole package.

Capability flags (all evaluated at import):

``HAS_DEPENDANT_CACHE_KEY``
    ``Dependant.cache_key`` exists (FastAPI <= 0.139).
``HAS_GET_CACHE_KEY``
    ``dependencies.models._get_cache_key()`` exists (FastAPI >= 0.140).
``HAS_MODULE_PREDICATES`` / ``HAS_PRIVATE_MODULE_PREDICATES``
    module-level ``is_coroutine_callable`` (<= 0.115) or
    ``_is_coroutine_callable`` (>= 0.140) callable classifiers.
``HAS_DEPENDANT_PREDICATES``
    ``Dependant.is_coroutine_callable`` properties (0.116 - 0.139).
``HAS_SOLVE_GENERATOR`` / ``HAS_PRIVATE_SOLVE_GENERATOR``
    ``solve_generator(call=...)`` (<= 0.115) or
    ``_solve_generator(dependant=...)`` (>= 0.116).
``HAS_NORMALIZE_ERRORS``
    ``fastapi._compat._normalize_errors`` (<= 0.115).
``HAS_ENDPOINT_CONTEXT``
    ``fastapi.exceptions.EndpointContext`` (>= 0.116).
``RVE_ACCEPTS_ENDPOINT_CTX`` / ``SERIALIZE_ACCEPTS_ENDPOINT_CTX``
    signature probes for the ``endpoint_ctx=`` keyword.
``HAS_DEPENDANT_SCOPE``
    ``Dependant.scope`` ("function" / "request" lifetimes, >= 0.116).
``HAS_OAUTH_SCOPES`` / ``HAS_SECURITY_SCOPES``
    ``own_oauth_scopes``/``parent_oauth_scopes`` (>= 0.116) vs the older
    ``security_scopes`` field (<= 0.115).
``HAS_STRICT_CONTENT_TYPE``
    routes carry ``strict_content_type`` (>= 0.135).
``HAS_STREAMING_ROUTES``
    routes carry ``is_json_stream`` / ``stream_item_field`` (>= 0.135).
``HAS_RUN_ENDPOINT_FUNCTION``
    ``fastapi.routing.run_endpoint_function`` (all supported releases; a
    local equivalent is used if it ever moves).
"""

from __future__ import annotations

import inspect
import sys
from collections.abc import Callable
from contextlib import AsyncExitStack, contextmanager, suppress
from functools import lru_cache, partial
from typing import Any

if sys.version_info >= (3, 13):  # pragma: no cover
    from inspect import iscoroutinefunction as _iscoroutinefunction
else:  # pragma: no cover
    from asyncio import iscoroutinefunction as _iscoroutinefunction

try:
    import fastapi.dependencies.models as _dep_models
    import fastapi.dependencies.utils as _dep_utils
    import fastapi.routing as _fastapi_routing
    from fastapi.dependencies.models import Dependant

    HAS_FASTAPI = True
except ImportError:  # pragma: no cover - fastapi is a hard runtime dep
    HAS_FASTAPI = False


__all__ = [
    "HAS_FASTAPI",
    "build_endpoint_context",
    "build_override_dependant",
    "dep_cache_key",
    "dep_oauth_scopes",
    "dep_scope",
    "is_async_gen_callable",
    "is_coroutine_callable",
    "is_gen_callable",
    "normalize_errors",
    "run_endpoint_function",
    "scope_exit_stack",
    "solve_generator",
]


def _accepts_kwarg(fn: Any, name: str) -> bool:
    """True when ``fn`` accepts a keyword argument called ``name``."""
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):  # pragma: no cover - builtins/C callables
        return False
    if name in params:
        return True
    return any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


if HAS_FASTAPI:
    # ── capability probes (one symbol each, evaluated once) ────────────
    _DATACLASS_FIELDS = getattr(Dependant, "__dataclass_fields__", {})

    # <= 0.115: a dataclass field filled in __post_init__; 0.116 - 0.139: a
    # computed property; >= 0.140: gone, replaced by _get_cache_key().
    HAS_DEPENDANT_CACHE_KEY = hasattr(Dependant, "cache_key") or "cache_key" in _DATACLASS_FIELDS
    HAS_GET_CACHE_KEY = hasattr(_dep_models, "_get_cache_key")

    HAS_MODULE_PREDICATES = hasattr(_dep_utils, "is_coroutine_callable")
    HAS_PRIVATE_MODULE_PREDICATES = hasattr(_dep_models, "_is_coroutine_callable")
    HAS_DEPENDANT_PREDICATES = hasattr(Dependant, "is_coroutine_callable")

    HAS_SOLVE_GENERATOR = hasattr(_dep_utils, "solve_generator")
    HAS_PRIVATE_SOLVE_GENERATOR = hasattr(_dep_utils, "_solve_generator")

    HAS_DEPENDANT_SCOPE = "scope" in _DATACLASS_FIELDS
    HAS_OAUTH_SCOPES = "own_oauth_scopes" in _DATACLASS_FIELDS
    HAS_SECURITY_SCOPES = "security_scopes" in _DATACLASS_FIELDS
    HAS_GET_OAUTH_SCOPES = hasattr(_dep_models, "_get_oauth_scopes")

    HAS_ENDPOINT_CONTEXT = False
    _EndpointContext: Any = None
    _extract_endpoint_context: Any = None
    try:
        from fastapi.exceptions import (
            EndpointContext as _EndpointContext,  # type: ignore[attr-defined,no-redef]
        )

        HAS_ENDPOINT_CONTEXT = True
    except ImportError:
        pass
    if HAS_ENDPOINT_CONTEXT:
        _extract_endpoint_context = getattr(_fastapi_routing, "_extract_endpoint_context", None)

    _normalize_errors: Any = None
    try:
        from fastapi._compat import _normalize_errors  # type: ignore[attr-defined,no-redef]

        HAS_NORMALIZE_ERRORS = True
    except ImportError:
        HAS_NORMALIZE_ERRORS = False

    from fastapi.exceptions import RequestValidationError as _RequestValidationError
    from fastapi.routing import serialize_response as _serialize_response

    RVE_ACCEPTS_ENDPOINT_CTX = HAS_ENDPOINT_CONTEXT and _accepts_kwarg(
        _RequestValidationError.__init__, "endpoint_ctx"
    )
    SERIALIZE_ACCEPTS_ENDPOINT_CTX = HAS_ENDPOINT_CONTEXT and _accepts_kwarg(
        _serialize_response, "endpoint_ctx"
    )
    HAS_STRICT_CONTENT_TYPE = _accepts_kwarg(
        _fastapi_routing.get_request_handler, "strict_content_type"
    )
    HAS_STREAMING_ROUTES = _accepts_kwarg(_fastapi_routing.get_request_handler, "is_json_stream")

    _GET_DEPENDANT_PARAMS = frozenset(inspect.signature(_dep_utils.get_dependant).parameters)
else:  # pragma: no cover - fastapi always present in practice
    HAS_DEPENDANT_CACHE_KEY = HAS_GET_CACHE_KEY = False
    HAS_MODULE_PREDICATES = HAS_PRIVATE_MODULE_PREDICATES = False
    HAS_DEPENDANT_PREDICATES = False
    HAS_SOLVE_GENERATOR = HAS_PRIVATE_SOLVE_GENERATOR = False
    HAS_DEPENDANT_SCOPE = HAS_OAUTH_SCOPES = HAS_SECURITY_SCOPES = False
    HAS_GET_OAUTH_SCOPES = HAS_ENDPOINT_CONTEXT = HAS_NORMALIZE_ERRORS = False
    RVE_ACCEPTS_ENDPOINT_CTX = SERIALIZE_ACCEPTS_ENDPOINT_CTX = False
    HAS_STRICT_CONTENT_TYPE = HAS_STREAMING_ROUTES = False
    _GET_DEPENDANT_PARAMS = frozenset()


# ──────────────────────────────────────────────────────────
# Callable classification
# ──────────────────────────────────────────────────────────


def _impartial(func: Any) -> Any:
    while isinstance(func, partial):
        func = func.func
    return func


def _unwrapped(call: Any) -> Any:
    return inspect.unwrap(_impartial(call))


def _classify(call: Any, predicate: Callable[[Any], bool]) -> bool:
    """
    Fallback classifier mirroring FastAPI's own logic: consider the
    callable itself, its ``functools.partial`` target, its unwrapped
    (``functools.wraps``) target and, for instances, ``__call__``.
    """
    if call is None:
        return False
    if predicate(_impartial(call)) or predicate(_unwrapped(call)):
        return True
    if inspect.isclass(_unwrapped(call)):
        return False
    for base in (_impartial(call), _unwrapped(call)):
        dunder = getattr(base, "__call__", None)  # noqa: B004
        if dunder is None:  # pragma: no cover - every object has __call__
            continue
        if predicate(_impartial(dunder)) or predicate(_unwrapped(dunder)):
            return True
    return False


def _routine_is_coroutine(fn: Any) -> bool:
    return inspect.isroutine(fn) and _iscoroutinefunction(fn)


class _CallIdentity:
    """Hash-by-identity wrapper so arbitrary callables can key an lru_cache."""

    __slots__ = ("call",)

    def __init__(self, call: Any) -> None:
        self.call = call

    def __hash__(self) -> int:
        return id(self.call)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _CallIdentity) and other.call is self.call


def _make_predicate(
    public_name: str,
    private_name: str,
    dependant_attr: str,
    fallback: Callable[[Any], bool],
) -> Callable[[Any, Any], bool]:
    """
    Build one classification helper, binding whichever FastAPI symbol is
    available.  Preference order: module-level function (>= 0.140 private,
    <= 0.115 public) → ``Dependant`` property (0.116 - 0.139) → local
    inspection.

    The local classifier is also consulted as a *union* with FastAPI's
    answer: FastAPI <= 0.115 did not look through ``functools.partial`` /
    ``functools.wraps``, which silently injected un-awaited coroutines.
    Unioning keeps classification identical on every supported release.
    """
    impl = None
    if HAS_FASTAPI:
        for module, name in (
            (_dep_models, private_name),
            (_dep_utils, private_name),
            (_dep_utils, public_name),
        ):
            impl = getattr(module, name, None)
            if impl is not None:
                break
    use_property = impl is None and HAS_FASTAPI and hasattr(Dependant, dependant_attr)
    bound = impl

    @lru_cache(maxsize=2048)
    def _cached(identity: _CallIdentity) -> bool:
        return _classify(identity.call, fallback)

    def _predicate(dep: Any, call: Any) -> bool:
        if bound is not None and bound(call):
            return True
        if (
            use_property
            and dep is not None
            and getattr(dep, "call", None) is call
            and getattr(dep, dependant_attr)
        ):
            return True
        if call is None:
            return False
        return _cached(_CallIdentity(call))

    return _predicate


_is_coroutine_impl = _make_predicate(
    "is_coroutine_callable",
    "_is_coroutine_callable",
    "is_coroutine_callable",
    _routine_is_coroutine,
)
_is_gen_impl = _make_predicate(
    "is_gen_callable",
    "_is_gen_callable",
    "is_gen_callable",
    inspect.isgeneratorfunction,
)
_is_async_gen_impl = _make_predicate(
    "is_async_gen_callable",
    "_is_async_gen_callable",
    "is_async_gen_callable",
    inspect.isasyncgenfunction,
)


def is_coroutine_callable(call: Any, dependant: Any = None) -> bool:
    """True when ``call`` should be awaited directly."""
    return _is_coroutine_impl(dependant, call)


def is_gen_callable(call: Any, dependant: Any = None) -> bool:
    """True when ``call`` is a sync generator (``yield``) dependency."""
    return _is_gen_impl(dependant, call)


def is_async_gen_callable(call: Any, dependant: Any = None) -> bool:
    """True when ``call`` is an async generator (``yield``) dependency."""
    return _is_async_gen_impl(dependant, call)


# ──────────────────────────────────────────────────────────
# Dependency cache keys / scopes
# ──────────────────────────────────────────────────────────


def dep_scope(dependant: Any) -> str | None:
    """The dependency lifetime ("function"/"request"), or None if unsupported."""
    if not HAS_DEPENDANT_SCOPE:
        return None
    return getattr(dependant, "scope", None)


def dep_oauth_scopes(dependant: Any) -> list[str]:
    """OAuth2 scopes visible to ``SecurityScopes`` for this dependant."""
    if HAS_GET_OAUTH_SCOPES:
        return list(_dep_models._get_oauth_scopes(dependant=dependant))  # type: ignore[attr-defined]
    scopes = getattr(dependant, "oauth_scopes", None)
    if scopes is not None:
        return list(scopes)
    scopes = getattr(dependant, "security_scopes", None)
    if scopes is not None:
        return list(scopes)
    return []  # pragma: no cover - future FastAPI


def dep_cache_key(dependant: Any) -> Any:
    """
    Stable identity for a dependency node.

    ``Dependant.cache_key`` up to 0.139; ``_get_cache_key()`` from 0.140;
    a locally computed equivalent otherwise.
    """
    if HAS_DEPENDANT_CACHE_KEY:
        return dependant.cache_key
    if HAS_GET_CACHE_KEY:
        return _dep_models._get_cache_key(dependant=dependant)  # type: ignore[attr-defined]
    # pragma: no cover - future FastAPI
    return (
        getattr(dependant, "call", None),
        tuple(sorted(set(dep_oauth_scopes(dependant)))),
        dep_scope(dependant) or "",
    )


def build_override_dependant(dependant: Any, call: Callable[..., Any]) -> Any:
    """
    Re-analyze an overridden callable, mirroring FastAPI's own override
    path.  The ``get_dependant()`` keyword set changed across releases
    (``security_scopes`` → ``own_oauth_scopes``/``parent_oauth_scopes``
    plus ``scope``), so the kwargs are assembled from its real signature.
    """
    kwargs: dict[str, Any] = {
        "path": getattr(dependant, "path", None) or "",
        "call": call,
        "name": dependant.name,
    }
    if "parent_oauth_scopes" in _GET_DEPENDANT_PARAMS:
        kwargs["parent_oauth_scopes"] = dep_oauth_scopes(dependant)
    elif "security_scopes" in _GET_DEPENDANT_PARAMS:
        kwargs["security_scopes"] = getattr(dependant, "security_scopes", None)
    if "scope" in _GET_DEPENDANT_PARAMS:
        kwargs["scope"] = dep_scope(dependant)
    return _dep_utils.get_dependant(**kwargs)


# ──────────────────────────────────────────────────────────
# Generator dependencies / exit stacks
# ──────────────────────────────────────────────────────────


async def solve_generator(
    *,
    dependant: Any,
    call: Callable[..., Any],
    stack: AsyncExitStack,
    sub_values: dict[str, Any],
) -> Any:
    """Enter a ``yield`` dependency on ``stack`` and return its value."""
    if HAS_PRIVATE_SOLVE_GENERATOR:
        return await _dep_utils._solve_generator(  # type: ignore[attr-defined]
            dependant=dependant, stack=stack, sub_values=sub_values
        )
    if HAS_SOLVE_GENERATOR:
        return await _dep_utils.solve_generator(  # type: ignore[attr-defined]
            call=call, stack=stack, sub_values=sub_values
        )
    # pragma: no cover - future FastAPI
    from fastapi.concurrency import asynccontextmanager, contextmanager_in_threadpool

    if is_async_gen_callable(call, dependant):
        cm = asynccontextmanager(call)(**sub_values)
    else:
        cm = contextmanager_in_threadpool(contextmanager(call)(**sub_values))
    return await stack.enter_async_context(cm)


async def _run_endpoint_function_fallback(
    *, dependant: Any, values: dict[str, Any], is_coroutine: bool
) -> Any:  # pragma: no cover - future FastAPI
    """Local stand-in for ``fastapi.routing.run_endpoint_function``."""
    from starlette.concurrency import run_in_threadpool

    assert dependant.call is not None, "dependant.call must be a function"
    if is_coroutine:
        return await dependant.call(**values)
    return await run_in_threadpool(dependant.call, **values)


#: FastAPI's endpoint dispatcher, or a local equivalent if it ever moves.
run_endpoint_function: Any = (
    getattr(_fastapi_routing, "run_endpoint_function", None) or _run_endpoint_function_fallback
    if HAS_FASTAPI
    else _run_endpoint_function_fallback
)
HAS_RUN_ENDPOINT_FUNCTION = HAS_FASTAPI and hasattr(_fastapi_routing, "run_endpoint_function")


def scope_exit_stack(request: Any, key: str) -> AsyncExitStack | None:
    """
    Fetch one of FastAPI's request-scoped ``AsyncExitStack``s.

    FastAPI >= 0.116 owns the exit stacks and publishes them in the ASGI
    scope (``fastapi_middleware_astack``, ``fastapi_inner_astack``,
    ``fastapi_function_astack``).  Older releases do not, and a future
    release may rename them again — callers fall back to a locally owned
    stack when this returns ``None``.
    """
    stack = getattr(request, "scope", {}).get(key)
    return stack if isinstance(stack, AsyncExitStack) else None


# ──────────────────────────────────────────────────────────
# Errors / endpoint context
# ──────────────────────────────────────────────────────────


def normalize_errors(errors: list[Any]) -> list[Any]:
    """Pydantic error normalization, a no-op where FastAPI dropped it."""
    if HAS_NORMALIZE_ERRORS:
        return list(_normalize_errors(errors))
    return errors


def build_endpoint_context(call: Any, path: str | None, method: str, root_path: str) -> Any:
    """
    Build FastAPI's per-request ``EndpointContext`` used to enrich
    validation error messages.  Returns ``None`` on releases without it,
    and callers then simply omit the ``endpoint_ctx`` keyword.
    """
    if not HAS_ENDPOINT_CONTEXT:
        return None
    if _extract_endpoint_context is not None and call is not None:
        # FastAPI memoizes these dicts by id(func) in a module-level cache;
        # copy before adding the per-request path so concurrent requests and
        # other routes sharing the endpoint never see each other's writes.
        ctx = dict(_extract_endpoint_context(call))
    else:  # pragma: no cover - future FastAPI
        ctx = _EndpointContext()
    if path:
        mount_path = (root_path or "").rstrip("/")
        with suppress(TypeError):  # pragma: no cover - future FastAPI
            ctx["path"] = f"{method} {mount_path}{path}"
    return ctx


def error_kwargs(body: Any, endpoint_ctx: Any) -> dict[str, Any]:
    """Keyword arguments for ``RequestValidationError`` on this release."""
    kwargs: dict[str, Any] = {"body": body}
    if RVE_ACCEPTS_ENDPOINT_CTX and endpoint_ctx is not None:
        kwargs["endpoint_ctx"] = endpoint_ctx
    return kwargs
