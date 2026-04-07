"""
Drop-in concurrent Depends — same API as fastapi.Depends.

Usage:
    from lazy_depends import Depends, ConcurrentRoute

    app = FastAPI(lifespan=lifespan)
    app.router.route_class = ConcurrentRoute

    # Everything else is IDENTICAL to standard FastAPI:
    async def get_db(): ...
    async def get_user(request: Request, db=Depends(get_db)): ...

    @app.get("/")
    async def root(user=Depends(get_user)): ...

Dependencies at the same level of the graph resolve concurrently
instead of FastAPI's default sequential (depth-first) resolution.

This replaces FastAPI's solve_dependencies entirely for routes
using ConcurrentRoute — no wrapper functions, no heuristics.
Works with dependency_overrides, generators, use_cache=False,
Header/Query/Cookie/Security params, and everything else.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Union

try:
    from contextlib import AsyncExitStack, contextmanager
    from typing import cast, Dict, List, Optional, Tuple, Union
    import email.message
    import json

    from fastapi import params as _fastapi_params
    from fastapi._compat import ModelField, Undefined, _normalize_errors
    from fastapi.concurrency import (
        asynccontextmanager as _asynccontextmanager,
        contextmanager_in_threadpool,
    )
    from fastapi.datastructures import Default, DefaultPlaceholder
    from fastapi.dependencies.models import Dependant
    from fastapi.dependencies.utils import (
        get_dependant,
        get_flat_dependant,
        get_body_field,
        is_coroutine_callable,
        is_gen_callable,
        is_async_gen_callable,
        request_body_to_args,
        request_params_to_args,
        solve_dependencies as _original_solve_dependencies,
        SolvedDependency,
        _should_embed_body_fields,
        solve_generator,
    )
    from fastapi.exceptions import (
        FastAPIError,
        RequestValidationError,
    )
    from fastapi.routing import (
        APIRoute as _APIRoute,
        run_endpoint_function,
        serialize_response,
    )
    from fastapi.utils import is_body_allowed_for_status_code
    from fastapi.background import BackgroundTasks
    from fastapi.security.oauth2 import SecurityScopes
    from starlette.background import BackgroundTasks as StarletteBackgroundTasks
    from starlette.concurrency import run_in_threadpool
    from starlette.exceptions import HTTPException
    from starlette.requests import Request as _Request
    from starlette.responses import JSONResponse, Response as _Response
    from starlette.datastructures import FormData
    from starlette.websockets import WebSocket

    _HAS_FASTAPI = True
except ImportError:
    _HAS_FASTAPI = False


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
# Graph construction from FastAPI's Dependant tree
# ──────────────────────────────────────────────────────────

def _node_key(dep: Dependant) -> Any:
    """
    Unique key for a graph node.

    use_cache=True  → shared by cache_key (same fn + scopes = same node)
    use_cache=False → unique per call-site (id of the Dependant object)
    """
    if dep.use_cache:
        return dep.cache_key
    return id(dep)


def _build_graph(
    dependant: Dependant,
    dependency_overrides_provider: Any,
) -> tuple[
    dict,  # graph: {node_key: (Dependant, call, {param_name: child_node_key})}
    dict,  # key_to_dependant: {node_key: Dependant}  (for use_cache dedup)
]:
    """
    Walk the Dependant tree and build a DAG for concurrent resolution.

    Handles dependency_overrides by re-analyzing overridden callables.
    """
    graph: dict = {}
    key_to_dependant: dict = {}
    _visited: set = set()

    def walk(dep: Dependant) -> Any:
        """Process a Dependant node, returns its node_key."""
        dep.call = cast(Callable[..., Any], dep.call)
        dep.cache_key = cast(
            Tuple[Callable[..., Any], Tuple[str]], dep.cache_key
        )

        # Apply dependency overrides (same logic as FastAPI)
        call = dep.call
        use_dep = dep
        if (
            dependency_overrides_provider
            and dependency_overrides_provider.dependency_overrides
        ):
            call = getattr(
                dependency_overrides_provider, "dependency_overrides", {}
            ).get(dep.call, dep.call)
            if call is not dep.call:
                use_dep = get_dependant(
                    path=dep.path,  # type: ignore
                    call=call,
                    name=dep.name,
                    security_scopes=dep.security_scopes,
                )

        key = _node_key(dep)

        # For use_cache=True, skip if already processed
        if dep.use_cache and key in graph:
            return key

        # Cycle detection for use_cache=False
        visit_id = id(dep)
        if visit_id in _visited:
            return key
        _visited.add(visit_id)

        # Recurse into sub-dependencies
        child_keys: dict[str, Any] = {}
        for sub_dep in use_dep.dependencies:
            child_key = walk(sub_dep)
            if sub_dep.name is not None:
                child_keys[sub_dep.name] = child_key

        graph[key] = (use_dep, call, child_keys)
        key_to_dependant[key] = dep
        return key

    # Walk all top-level deps of the endpoint (not the endpoint itself)
    for sub_dep in dependant.dependencies:
        walk(sub_dep)

    return graph, key_to_dependant


def _topo_levels(graph: dict) -> list[list]:
    """Topological sort into levels for concurrent execution."""
    deps_of = {
        key: set(child_keys.values())
        for key, (_, _, child_keys) in graph.items()
    }

    levels: list[list] = []
    resolved: set = set()
    remaining = set(deps_of.keys())

    while remaining:
        level = [
            key for key in remaining
            if deps_of[key].issubset(resolved)
        ]
        if not level:
            break  # circular — bail, FastAPI will handle the error
        levels.append(level)
        resolved.update(level)
        remaining -= set(level)

    return levels


# ──────────────────────────────────────────────────────────
# Concurrent solve_dependencies replacement
# ──────────────────────────────────────────────────────────

async def _resolve_single_dep(
    key: Any,
    graph: dict,
    cache: dict,
    request: Union[_Request, WebSocket],
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
    path_values, path_errors = request_params_to_args(
        dep.path_params, request.path_params
    )
    query_values, query_errors = request_params_to_args(
        dep.query_params, request.query_params
    )
    header_values, header_errors = request_params_to_args(
        dep.header_params, request.headers
    )
    cookie_values, cookie_errors = request_params_to_args(
        dep.cookie_params, request.cookies
    )
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
        values[dep.security_scopes_param_name] = SecurityScopes(
            scopes=dep.security_scopes
        )

    # 5. Call the dependency (handle generators, async, sync)
    if errors:
        return None, errors, background_tasks

    if is_gen_callable(call) or is_async_gen_callable(call):
        solved = await solve_generator(
            call=call, stack=async_exit_stack, sub_values=values
        )
    elif is_coroutine_callable(call):
        solved = await call(**values)
    else:
        solved = await run_in_threadpool(call, **values)

    return solved, errors, background_tasks


async def solve_dependencies_concurrent(
    *,
    request: Union[_Request, WebSocket],
    dependant: Dependant,
    body: Any = None,
    background_tasks: StarletteBackgroundTasks | None = None,
    response: _Response | None = None,
    dependency_overrides_provider: Any = None,
    dependency_cache: dict | None = None,
    async_exit_stack: AsyncExitStack,
    embed_body_fields: bool,
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

    # Build the graph from the Dependant tree
    graph, key_to_dependant = _build_graph(dependant, dependency_overrides_provider)

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

    levels = _topo_levels(graph)
    cache: dict = {}

    # Pre-fill cache with already-resolved deps (from dependency_cache)
    for key, (dep, call, child_keys) in graph.items():
        original_dep = key_to_dependant.get(key, dep)
        if original_dep.use_cache and original_dep.cache_key in dependency_cache:
            cache[key] = dependency_cache[original_dep.cache_key]

    # Resolve level by level, each level concurrently
    for level_keys in levels:
        # Filter out already-cached entries
        to_resolve = [k for k in level_keys if k not in cache]
        if not to_resolve:
            continue

        if len(to_resolve) == 1:
            # Single dep — no gather overhead
            key = to_resolve[0]
            solved, dep_errors, background_tasks = await _resolve_single_dep(
                key, graph, cache, request, body,
                background_tasks, response, async_exit_stack, embed_body_fields,
            )
            if dep_errors:
                errors.extend(dep_errors)
                break  # Stop on error, matching FastAPI behavior
            cache[key] = solved
            # Update FastAPI's dependency_cache
            original_dep = key_to_dependant.get(key)
            if original_dep and original_dep.cache_key not in dependency_cache:
                dependency_cache[original_dep.cache_key] = solved
        else:
            # Multiple deps — resolve concurrently with cancellation on error
            level_errors = []
            level_results: dict = {}

            async def _do_one(k: Any) -> None:
                nonlocal background_tasks
                solved, dep_errors, bt = await _resolve_single_dep(
                    k, graph, cache, request, body,
                    background_tasks, response, async_exit_stack, embed_body_fields,
                )
                if bt is not None:
                    background_tasks = bt
                if dep_errors:
                    level_errors.extend(dep_errors)
                    raise Exception("dep_validation_error")
                level_results[k] = solved

            _level_failed = False
            try:
                async with asyncio.TaskGroup() as tg:
                    for k in to_resolve:
                        tg.create_task(_do_one(k))
            except* Exception as eg:
                # Filter out our sentinel errors vs real exceptions
                real_errors = []
                for exc in eg.exceptions:
                    if str(exc) != "dep_validation_error":
                        real_errors.append(exc)
                if real_errors:
                    raise real_errors[0]
                if level_errors:
                    errors.extend(level_errors)
                _level_failed = True

            if _level_failed:
                break

            # Commit results to cache
            cache.update(level_results)
            for k, solved in level_results.items():
                original_dep = key_to_dependant.get(k)
                if original_dep and original_dep.cache_key not in dependency_cache:
                    dependency_cache[original_dep.cache_key] = solved

    # Map resolved deps back to endpoint param names
    for sub_dep in dependant.dependencies:
        key = _node_key(sub_dep)
        if sub_dep.name is not None and key in cache:
            values[sub_dep.name] = cache[key]

    # Extract the endpoint's own non-dep params
    path_values, path_errors = request_params_to_args(
        dependant.path_params, request.path_params
    )
    query_values, query_errors = request_params_to_args(
        dependant.query_params, request.query_params
    )
    header_values, header_errors = request_params_to_args(
        dependant.header_params, request.headers
    )
    cookie_values, cookie_errors = request_params_to_args(
        dependant.cookie_params, request.cookies
    )
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
        values[dependant.security_scopes_param_name] = SecurityScopes(
            scopes=dependant.security_scopes
        )

    return SolvedDependency(
        values=values,
        errors=errors,
        background_tasks=background_tasks,
        response=response,
        dependency_cache=dependency_cache,
    )


# ──────────────────────────────────────────────────────────
# ConcurrentRoute — drop-in APIRoute replacement
# ──────────────────────────────────────────────────────────

if _HAS_FASTAPI:

    def _get_concurrent_request_handler(
        dependant: Dependant,
        body_field: ModelField | None = None,
        status_code: int | None = None,
        response_class: type[_Response] | DefaultPlaceholder = Default(JSONResponse),
        response_field: ModelField | None = None,
        response_model_include: Any = None,
        response_model_exclude: Any = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        dependency_overrides_provider: Any = None,
        embed_body_fields: bool = False,
    ) -> Callable[[_Request], Any]:
        """
        Replaces FastAPI's get_request_handler with concurrent dep resolution.

        Same structure as FastAPI's version, but calls
        solve_dependencies_concurrent instead of solve_dependencies.
        """
        assert dependant.call is not None, "dependant.call must be a function"
        is_coroutine = asyncio.iscoroutinefunction(dependant.call)
        is_body_form = body_field and isinstance(
            body_field.field_info, _fastapi_params.Form
        )
        if isinstance(response_class, DefaultPlaceholder):
            actual_response_class: type[_Response] = response_class.value
        else:
            actual_response_class = response_class

        async def app(request: _Request) -> _Response:
            response: _Response | None = None
            async with AsyncExitStack() as file_stack:
                try:
                    body: Any = None
                    if body_field:
                        if is_body_form:
                            body = await request.form()
                            file_stack.push_async_callback(body.close)
                        else:
                            body_bytes = await request.body()
                            if body_bytes:
                                json_body: Any = Undefined
                                content_type_value = request.headers.get(
                                    "content-type"
                                )
                                if not content_type_value:
                                    json_body = await request.json()
                                else:
                                    message = email.message.Message()
                                    message["content-type"] = content_type_value
                                    if message.get_content_maintype() == "application":
                                        subtype = message.get_content_subtype()
                                        if subtype == "json" or subtype.endswith(
                                            "+json"
                                        ):
                                            json_body = await request.json()
                                if json_body != Undefined:
                                    body = json_body
                                else:
                                    body = body_bytes
                except json.JSONDecodeError as e:
                    validation_error = RequestValidationError(
                        [
                            {
                                "type": "json_invalid",
                                "loc": ("body", e.pos),
                                "msg": "JSON decode error",
                                "input": {},
                                "ctx": {"error": e.msg},
                            }
                        ],
                        body=e.doc,
                    )
                    raise validation_error from e
                except HTTPException:
                    raise
                except Exception as e:
                    http_error = HTTPException(
                        status_code=400,
                        detail="There was an error parsing the body",
                    )
                    raise http_error from e

                errors: list = []
                async with AsyncExitStack() as async_exit_stack:
                    # ── THE ONLY CHANGE: concurrent instead of sequential ──
                    solved_result = await solve_dependencies_concurrent(
                        request=request,
                        dependant=dependant,
                        body=body,
                        dependency_overrides_provider=dependency_overrides_provider,
                        async_exit_stack=async_exit_stack,
                        embed_body_fields=embed_body_fields,
                    )
                    errors = solved_result.errors
                    if not errors:
                        raw_response = await run_endpoint_function(
                            dependant=dependant,
                            values=solved_result.values,
                            is_coroutine=is_coroutine,
                        )
                        if isinstance(raw_response, _Response):
                            if raw_response.background is None:
                                raw_response.background = (
                                    solved_result.background_tasks
                                )
                            response = raw_response
                        else:
                            response_args: dict[str, Any] = {
                                "background": solved_result.background_tasks
                            }
                            current_status_code = (
                                status_code
                                if status_code
                                else solved_result.response.status_code
                            )
                            if current_status_code is not None:
                                response_args["status_code"] = current_status_code
                            if solved_result.response.status_code:
                                response_args["status_code"] = (
                                    solved_result.response.status_code
                                )
                            content = await serialize_response(
                                field=response_field,
                                response_content=raw_response,
                                include=response_model_include,
                                exclude=response_model_exclude,
                                by_alias=response_model_by_alias,
                                exclude_unset=response_model_exclude_unset,
                                exclude_defaults=response_model_exclude_defaults,
                                exclude_none=response_model_exclude_none,
                                is_coroutine=is_coroutine,
                            )
                            response = actual_response_class(
                                content, **response_args
                            )
                            if not is_body_allowed_for_status_code(
                                response.status_code
                            ):
                                response.body = b""
                            response.headers.raw.extend(
                                solved_result.response.headers.raw
                            )
                    if errors:
                        validation_error = RequestValidationError(
                            _normalize_errors(errors), body=body
                        )
                        raise validation_error
                if response is None:
                    raise FastAPIError(
                        "No response object was returned. There's a high chance that "
                        "the application code is raising an exception and a dependency "
                        "with yield has a block with a bare except, or a block with "
                        "except Exception, and is not raising the exception again. "
                        "Read more about it in the docs: https://fastapi.tiangolo.com/"
                        "tutorial/dependencies/dependencies-with-yield/"
                        "#dependencies-with-yield-and-except"
                    )
                return response

        return app

    class ConcurrentRoute(_APIRoute):
        """
        Drop-in route class that resolves dependencies concurrently.

        Usage:
            app = FastAPI()
            app.router.route_class = ConcurrentRoute

        Dependencies at the same level in the graph run in parallel
        instead of FastAPI's default sequential resolution.

        Fully compatible with:
          - dependency_overrides (testing)
          - Generator (yield) deps
          - use_cache=True/False
          - Header(), Query(), Cookie(), Security() params
          - BackgroundTasks, Response injection
        """

        def get_route_handler(self) -> Callable[[_Request], Any]:
            return _get_concurrent_request_handler(
                dependant=self.dependant,
                body_field=self.body_field,
                status_code=self.status_code,
                response_class=self.response_class,
                response_field=self.secure_cloned_response_field,
                response_model_include=self.response_model_include,
                response_model_exclude=self.response_model_exclude,
                response_model_by_alias=self.response_model_by_alias,
                response_model_exclude_unset=self.response_model_exclude_unset,
                response_model_exclude_defaults=self.response_model_exclude_defaults,
                response_model_exclude_none=self.response_model_exclude_none,
                dependency_overrides_provider=self.dependency_overrides_provider,
                embed_body_fields=self._embed_body_fields,
            )
