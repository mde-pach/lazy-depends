"""
Core concurrent dependency resolver.

Contains ``solve_dependencies_concurrent`` and ``_resolve_single_dep`` —
the ASAP scheduling logic that replaces FastAPI's sequential resolution.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any

from fastapi.background import BackgroundTasks
from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import (
    SolvedDependency,
    request_body_to_args,
    request_params_to_args,
)
from fastapi.dependencies.utils import (
    solve_dependencies as _original_solve_dependencies,
)
from fastapi.exceptions import RequestValidationError
from fastapi.security.oauth2 import SecurityScopes
from starlette.background import BackgroundTasks as StarletteBackgroundTasks
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request as _Request
from starlette.responses import Response as _Response
from starlette.websockets import WebSocket

# Version-adaptive imports: FastAPI >=0.133 moved/removed several helpers
try:
    from fastapi.dependencies.utils import (  # type: ignore[attr-defined]
        is_async_gen_callable,
        is_coroutine_callable,
        is_gen_callable,
        solve_generator,
    )

    _NEW_FASTAPI = False
except ImportError:
    # FastAPI >=0.133: these are now properties on Dependant,
    # and solve_generator became _solve_generator with a different signature
    from fastapi.dependencies.utils import _solve_generator  # type: ignore[attr-defined]

    _NEW_FASTAPI = True

from lazy_depends.graph import _build_graph, _node_key, _topo_levels
from lazy_depends.lazy import Lazy


async def _resolve_single_dep(
    key: Any,
    graph: dict,
    cache: dict,
    request: _Request | WebSocket,
    body: Any,
    background_tasks: StarletteBackgroundTasks | None,
    response: _Response,
    async_exit_stack: AsyncExitStack,
    embed_body_fields: bool,
) -> tuple[Any, list, StarletteBackgroundTasks | None]:
    """
    Resolve a single dependency node and all its non-dep params.

    Mirrors FastAPI's solve_dependencies logic for a single Dependant,
    but sub-deps are already resolved in `cache`.
    """
    dep, call, child_keys = graph[key]
    values: dict[str, Any] = {}
    errors: list = []

    # 1. Inject sub-deps from cache
    for param_name, child_key in child_keys.items():
        values[param_name] = cache[child_key]

    # 2. Extract request params (path, query, header, cookie)
    path_values, path_errors = request_params_to_args(dep.path_params, request.path_params)
    query_values, query_errors = request_params_to_args(dep.query_params, request.query_params)
    header_values, header_errors = request_params_to_args(dep.header_params, request.headers)
    cookie_values, cookie_errors = request_params_to_args(dep.cookie_params, request.cookies)
    values.update(path_values)
    values.update(query_values)
    values.update(header_values)
    values.update(cookie_values)
    errors += path_errors + query_errors + header_errors + cookie_errors

    # 3. Body params
    if dep.body_params:
        body_values, body_errors = await request_body_to_args(
            body_fields=dep.body_params,
            received_body=body,
            embed_body_fields=embed_body_fields,
        )
        values.update(body_values)
        errors.extend(body_errors)

    # 4. Special params
    if dep.http_connection_param_name:
        values[dep.http_connection_param_name] = request
    if dep.request_param_name and isinstance(request, _Request):
        values[dep.request_param_name] = request
    elif dep.websocket_param_name and isinstance(request, WebSocket):
        values[dep.websocket_param_name] = request
    if dep.background_tasks_param_name:
        if background_tasks is None:
            background_tasks = BackgroundTasks()
        values[dep.background_tasks_param_name] = background_tasks
    if dep.response_param_name:
        values[dep.response_param_name] = response
    if dep.security_scopes_param_name:
        _scopes = getattr(dep, "security_scopes", None) or getattr(dep, "security_scopes_value", [])
        values[dep.security_scopes_param_name] = SecurityScopes(scopes=_scopes)

    # 5. Call the dependency (handle generators, async, sync)
    if errors:
        return None, errors, background_tasks

    if _NEW_FASTAPI:
        if dep.is_gen_callable or dep.is_async_gen_callable:
            solved = await _solve_generator(
                dependant=dep, stack=async_exit_stack, sub_values=values
            )
        elif dep.is_coroutine_callable:
            solved = await call(**values)
        else:
            solved = await run_in_threadpool(call, **values)
    else:
        if is_gen_callable(call) or is_async_gen_callable(call):
            solved = await solve_generator(call=call, stack=async_exit_stack, sub_values=values)
        elif is_coroutine_callable(call):
            solved = await call(**values)
        else:
            solved = await run_in_threadpool(call, **values)

    return solved, errors, background_tasks


async def solve_dependencies_concurrent(
    *,
    request: _Request | WebSocket,
    dependant: Dependant,
    body: Any = None,
    background_tasks: StarletteBackgroundTasks | None = None,
    response: _Response | None = None,
    dependency_overrides_provider: Any = None,
    dependency_cache: dict | None = None,
    async_exit_stack: AsyncExitStack,
    embed_body_fields: bool,
    _lazy_dep_names: set[str] | None = None,
    _cached_graph: dict | None = None,
    _cached_key_map: dict | None = None,
    _cached_levels: list[list] | None = None,
    _cached_lazy_map: dict[Any, set[str]] | None = None,
    _trace: Any | None = None,
) -> SolvedDependency:
    """
    Drop-in replacement for FastAPI's solve_dependencies that resolves
    independent deps concurrently by topological level.

    Same signature, same return type, same behavior — just faster.
    """
    values: dict[str, Any] = {}
    errors: list = []

    if response is None:
        response = _Response()
        del response.headers["content-length"]
        response.status_code = None  # type: ignore

    if dependency_cache is None:
        dependency_cache = {}

    # Use pre-computed graph if available and no overrides are active
    _has_overrides = bool(
        dependency_overrides_provider
        and getattr(dependency_overrides_provider, "dependency_overrides", None)
    )
    if _cached_graph is not None and not _has_overrides:
        graph = _cached_graph
        key_to_dependant = _cached_key_map  # type: ignore[assignment]
        levels = _cached_levels  # type: ignore[assignment]
    else:
        graph, key_to_dependant = _build_graph(dependant, dependency_overrides_provider)
        levels = _topo_levels(graph) if graph else []

    if not graph:
        # No deps to resolve — just extract endpoint's own params
        return await _original_solve_dependencies(
            request=request,
            dependant=dependant,
            body=body,
            background_tasks=background_tasks,
            response=response,
            dependency_overrides_provider=dependency_overrides_provider,
            dependency_cache=dependency_cache,
            async_exit_stack=async_exit_stack,
            embed_body_fields=embed_body_fields,
        )

    # Build tracing helpers if enabled
    _tracing = _trace is not None
    if _tracing:
        import time as _time

        _key_to_level: dict[Any, int] = {}
        for lvl_idx, lvl_keys in enumerate(levels):  # type: ignore[arg-type]
            for k in lvl_keys:
                _key_to_level[k] = lvl_idx

        def _dep_name(k: Any) -> str:
            dep, call, _ = graph[k]
            return getattr(call, "__name__", None) or dep.name or str(k)

        def _dep_parents(k: Any) -> list[str]:
            _, _, child_keys = graph[k]
            return [_dep_name(ck) for ck in child_keys.values()]

    # ── Unified ASAP resolution ──────────────────────────
    # Every dep gets a task that awaits only its direct sub-deps.
    # Tasks start as soon as their sub-deps finish — no waiting
    # for unrelated deps at the same "level".
    # Lazy deps are wrapped in Lazy[T]; eager deps are awaited.
    # LazyDepends params inside sub-deps also get Lazy wrappers.

    # Build lazy map if not cached (e.g. when dependency_overrides active)
    if _cached_lazy_map is not None:
        lazy_map = _cached_lazy_map
    else:
        from lazy_depends.depends import build_lazy_map

        lazy_map = build_lazy_map(graph)

    node_tasks: dict[Any, asyncio.Task] = {}

    for level_keys in levels:  # type: ignore[union-attr]
        for key in level_keys:

            async def _resolve_node(_key: Any = key) -> Any:
                nonlocal background_tasks
                dep, call, child_keys = graph[_key]
                local_cache: dict[Any, Any] = {}
                node_lazy_params = lazy_map.get(_key)
                for param_name, child_key in child_keys.items():
                    if node_lazy_params and param_name in node_lazy_params:
                        # This child is LazyDepends in the parent's signature —
                        # pass the running task as Lazy[T] instead of awaiting
                        local_cache[child_key] = Lazy(node_tasks[child_key], name=param_name)
                    else:
                        local_cache[child_key] = await node_tasks[child_key]
                if _tracing:
                    from lazy_depends.tracing import DepSpan

                    _t0 = _time.monotonic()
                solved, dep_errors, bt = await _resolve_single_dep(
                    _key,
                    graph,
                    local_cache,
                    request,
                    body,
                    background_tasks,
                    response,
                    async_exit_stack,
                    embed_body_fields,
                )
                if bt is not None:
                    background_tasks = bt
                if _tracing:
                    _t1 = _time.monotonic()
                    _trace.spans.append(  # type: ignore[union-attr]
                        DepSpan(
                            name=_dep_name(_key),
                            level=_key_to_level.get(_key, 0),
                            start=_t0,
                            end=_t1,
                            parent_names=_dep_parents(_key),
                        )
                    )
                if dep_errors:
                    raise RequestValidationError(dep_errors)
                return solved

            node_tasks[key] = asyncio.create_task(_resolve_node())

    # Map top-level deps → values (eager) or Lazy (deferred)
    for sub_dep in dependant.dependencies:
        key = _node_key(sub_dep)
        if sub_dep.name is None or key not in node_tasks:
            continue
        if _lazy_dep_names and sub_dep.name in _lazy_dep_names:
            values[sub_dep.name] = Lazy(node_tasks[key], name=sub_dep.name)
            if _tracing:
                dep, call, _ = graph[key]
                name = getattr(call, "__name__", None) or sub_dep.name
                for span in _trace.spans:  # type: ignore[union-attr]
                    if span.name == name:
                        span.is_lazy = True
        else:
            values[sub_dep.name] = await node_tasks[key]

    # Extract the endpoint's own non-dep params
    path_values, path_errors = request_params_to_args(dependant.path_params, request.path_params)
    query_values, query_errors = request_params_to_args(
        dependant.query_params, request.query_params
    )
    header_values, header_errors = request_params_to_args(dependant.header_params, request.headers)
    cookie_values, cookie_errors = request_params_to_args(dependant.cookie_params, request.cookies)
    values.update(path_values)
    values.update(query_values)
    values.update(header_values)
    values.update(cookie_values)
    errors += path_errors + query_errors + header_errors + cookie_errors

    if dependant.body_params:
        body_values, body_errors = await request_body_to_args(
            body_fields=dependant.body_params,
            received_body=body,
            embed_body_fields=embed_body_fields,
        )
        values.update(body_values)
        errors.extend(body_errors)

    if dependant.http_connection_param_name:
        values[dependant.http_connection_param_name] = request
    if dependant.request_param_name and isinstance(request, _Request):
        values[dependant.request_param_name] = request
    elif dependant.websocket_param_name and isinstance(request, WebSocket):
        values[dependant.websocket_param_name] = request
    if dependant.background_tasks_param_name:
        if background_tasks is None:
            background_tasks = BackgroundTasks()
        values[dependant.background_tasks_param_name] = background_tasks
    if dependant.response_param_name:
        values[dependant.response_param_name] = response
    if dependant.security_scopes_param_name:
        _scopes = getattr(dependant, "security_scopes", None) or getattr(
            dependant, "security_scopes_value", []
        )
        values[dependant.security_scopes_param_name] = SecurityScopes(scopes=_scopes)

    return SolvedDependency(
        values=values,
        errors=errors,
        background_tasks=background_tasks,
        response=response,
        dependency_cache=dependency_cache,
    )
