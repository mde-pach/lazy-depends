"""
FastAPI integration: request handler factory and route classes.

Contains ``_get_concurrent_request_handler``, ``ConcurrentRoute``,
and ``ConcurrentRouter``.

The handler mirrors ``fastapi.routing.get_request_handler`` but swaps the
sequential dependency solver for ``solve_dependencies_concurrent``.  Every
FastAPI internal it relies on is feature-detected in ``lazy_depends._compat``
so the same code runs on FastAPI 0.114 through the latest release.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

try:
    import email.message
    import json

    from fastapi import params as _fastapi_params
    from fastapi._compat import ModelField, Undefined
    from fastapi.datastructures import Default, DefaultPlaceholder
    from fastapi.dependencies.models import Dependant
    from fastapi.exceptions import RequestValidationError
    from fastapi.routing import (
        APIRoute as _APIRoute,
    )
    from fastapi.routing import serialize_response
    from fastapi.utils import is_body_allowed_for_status_code
    from starlette.exceptions import HTTPException
    from starlette.requests import Request as _Request
    from starlette.responses import JSONResponse
    from starlette.responses import Response as _Response

    _HAS_FASTAPI = True
except ImportError:
    _HAS_FASTAPI = False


if _HAS_FASTAPI:
    from lazy_depends import _compat
    from lazy_depends.depends import detect_lazy_dep_names
    from lazy_depends.graph import _build_graph, _topo_levels
    from lazy_depends.solver import solve_dependencies_concurrent

    #: ASGI scope keys FastAPI (>= 0.116) uses to publish its exit stacks.
    _MIDDLEWARE_ASTACK_KEY = "fastapi_middleware_astack"
    _INNER_ASTACK_KEY = "fastapi_inner_astack"
    _FUNCTION_ASTACK_KEY = "fastapi_function_astack"

    def _get_concurrent_request_handler(
        dependant: Dependant,
        body_field: ModelField | None = None,
        status_code: int | None = None,
        response_class: type[_Response] | DefaultPlaceholder = Default(JSONResponse),  # noqa: B008
        response_field: ModelField | None = None,
        response_model_include: Any = None,
        response_model_exclude: Any = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        dependency_overrides_provider: Any = None,
        embed_body_fields: bool = False,
        strict_content_type: bool | DefaultPlaceholder = False,
    ) -> Callable[[_Request], Any]:
        """
        Replaces FastAPI's get_request_handler with concurrent dep resolution.

        Same structure as FastAPI's version, but calls
        solve_dependencies_concurrent instead of solve_dependencies.
        """
        assert dependant.call is not None, "dependant.call must be a function"
        is_coroutine = _compat.is_coroutine_callable(dependant.call, dependant)
        is_body_form = body_field and isinstance(body_field.field_info, _fastapi_params.Form)
        if isinstance(response_class, DefaultPlaceholder):
            actual_response_class: type[_Response] = response_class.value
        else:
            actual_response_class = response_class
        if isinstance(strict_content_type, DefaultPlaceholder):
            actual_strict_content_type: bool = bool(strict_content_type.value)
        else:
            actual_strict_content_type = bool(strict_content_type)

        # Detect LazyDepends markers at route registration time
        from lazy_depends.depends import build_lazy_map

        lazy_dep_names = (
            detect_lazy_dep_names(dependant.call)
            if dependant.call  # type: ignore[truthy-function]
            else None
        )

        # Pre-compute dependency graph at registration time (no overrides).
        # Reused on every request unless dependency_overrides is active.
        _cached_graph, _cached_key_map = _build_graph(dependant, None)
        _cached_levels = _topo_levels(_cached_graph) if _cached_graph else []

        # Pre-compute lazy param map for ALL nodes in the graph
        _cached_lazy_map = build_lazy_map(_cached_graph) if _cached_graph else {}

        async def app(request: _Request) -> _Response:
            response: _Response | None = None

            # FastAPI >= 0.116 owns the exit stacks and publishes them in the
            # ASGI scope; older (or future) releases leave us to own them.
            file_stack = _compat.scope_exit_stack(request, _MIDDLEWARE_ASTACK_KEY)
            local_file_stack = None
            if file_stack is None:
                file_stack = local_file_stack = AsyncExitStack()

            # Per-request so concurrent requests never share a context dict.
            endpoint_ctx = _compat.build_endpoint_context(
                dependant.call,
                dependant.path,
                request.method,
                request.scope.get("root_path", ""),
            )

            try:
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
                                content_type_value = request.headers.get("content-type")
                                if not content_type_value:
                                    if not actual_strict_content_type:
                                        json_body = await request.json()
                                else:
                                    message = email.message.Message()
                                    message["content-type"] = content_type_value
                                    if message.get_content_maintype() == "application":
                                        subtype = message.get_content_subtype()
                                        if subtype == "json" or subtype.endswith("+json"):
                                            json_body = await request.json()
                                body = json_body if json_body != Undefined else body_bytes
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
                        **_compat.error_kwargs(e.doc, endpoint_ctx),
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

                async_exit_stack = _compat.scope_exit_stack(request, _INNER_ASTACK_KEY)
                local_exit_stack = None
                if async_exit_stack is None:
                    async_exit_stack = local_exit_stack = AsyncExitStack()
                # Dependencies declared with scope="function" (FastAPI >= 0.116)
                # are torn down before the response is sent; when that stack is
                # unavailable they share the request-scoped one.
                function_exit_stack = (
                    _compat.scope_exit_stack(request, _FUNCTION_ASTACK_KEY) or async_exit_stack
                )

                try:
                    # Set up tracing if enabled
                    import time as _time

                    from lazy_depends.tracing import RequestTrace, is_trace_enabled, log_trace

                    _trace_obj = None
                    if is_trace_enabled():
                        _trace_obj = RequestTrace(
                            method=request.method,
                            path=request.url.path,
                            resolve_start=_time.monotonic(),
                        )

                    # ── THE ONLY CHANGE: concurrent instead of sequential ──
                    solved_result = await solve_dependencies_concurrent(
                        request=request,
                        dependant=dependant,
                        body=body,
                        dependency_overrides_provider=dependency_overrides_provider,
                        async_exit_stack=async_exit_stack,
                        function_exit_stack=function_exit_stack,
                        embed_body_fields=embed_body_fields,
                        _lazy_dep_names=lazy_dep_names,
                        _cached_graph=_cached_graph if _cached_graph else None,
                        _cached_key_map=_cached_key_map if _cached_graph else None,
                        _cached_levels=_cached_levels if _cached_graph else None,
                        _cached_lazy_map=_cached_lazy_map if _cached_graph else None,
                        _trace=_trace_obj,
                    )

                    if _trace_obj is not None:
                        _trace_obj.resolve_end = _time.monotonic()
                        log_trace(_trace_obj)
                    errors = solved_result.errors
                    if not errors:
                        raw_response = await _compat.run_endpoint_function(
                            dependant=dependant,
                            values=solved_result.values,
                            is_coroutine=is_coroutine,
                        )
                        if isinstance(raw_response, _Response):
                            if raw_response.background is None:
                                raw_response.background = solved_result.background_tasks
                            response = raw_response
                        else:
                            response_args: dict[str, Any] = {
                                "background": solved_result.background_tasks
                            }
                            current_status_code = (
                                status_code if status_code else solved_result.response.status_code
                            )
                            if current_status_code is not None:
                                response_args["status_code"] = current_status_code
                            if solved_result.response.status_code:
                                response_args["status_code"] = solved_result.response.status_code
                            serialize_kwargs: dict[str, Any] = {
                                "field": response_field,
                                "response_content": raw_response,
                                "include": response_model_include,
                                "exclude": response_model_exclude,
                                "by_alias": response_model_by_alias,
                                "exclude_unset": response_model_exclude_unset,
                                "exclude_defaults": response_model_exclude_defaults,
                                "exclude_none": response_model_exclude_none,
                                "is_coroutine": is_coroutine,
                            }
                            if _compat.SERIALIZE_ACCEPTS_ENDPOINT_CTX and endpoint_ctx is not None:
                                serialize_kwargs["endpoint_ctx"] = endpoint_ctx
                            content = await serialize_response(**serialize_kwargs)
                            response = actual_response_class(content, **response_args)
                            if not is_body_allowed_for_status_code(response.status_code):
                                response.body = b""
                            response.headers.raw.extend(solved_result.response.headers.raw)
                    if errors:
                        validation_error = RequestValidationError(
                            _compat.normalize_errors(errors),
                            **_compat.error_kwargs(body, endpoint_ctx),
                        )
                        raise validation_error

                    assert response is not None, "No response object was returned."
                    return response
                finally:
                    if local_exit_stack is not None:
                        await local_exit_stack.aclose()
            finally:
                if local_file_stack is not None:
                    await local_file_stack.aclose()

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

        Streaming endpoints (JSONL / SSE / generator endpoints, FastAPI
        >= 0.135) delegate to FastAPI's own handler, which owns the
        streaming machinery; their dependencies resolve sequentially.
        """

        def _is_streaming_route(self) -> bool:
            """True for endpoints FastAPI streams itself (JSONL / SSE / generators)."""
            if not _compat.HAS_STREAMING_ROUTES:
                return False
            if getattr(self, "is_json_stream", False):
                return True
            if getattr(self, "stream_item_field", None) is not None:
                return True
            call = self.dependant.call
            return _compat.is_async_gen_callable(call) or _compat.is_gen_callable(call)

        def get_route_handler(self) -> Callable[[_Request], Any]:
            if self._is_streaming_route():
                return super().get_route_handler()
            extra: dict[str, Any] = {}
            if _compat.HAS_STRICT_CONTENT_TYPE:
                extra["strict_content_type"] = self.strict_content_type
            return _get_concurrent_request_handler(
                dependant=self.dependant,
                body_field=self.body_field,
                status_code=self.status_code,
                response_class=self.response_class,
                response_field=self.response_field,
                response_model_include=self.response_model_include,
                response_model_exclude=self.response_model_exclude,
                response_model_by_alias=self.response_model_by_alias,
                response_model_exclude_unset=self.response_model_exclude_unset,
                response_model_exclude_defaults=self.response_model_exclude_defaults,
                response_model_exclude_none=self.response_model_exclude_none,
                dependency_overrides_provider=self.dependency_overrides_provider,
                embed_body_fields=self._embed_body_fields,
                **extra,
            )

    from fastapi import APIRouter as _APIRouter_class

    class ConcurrentRouter(_APIRouter_class):
        """APIRouter subclass that uses ConcurrentRoute by default."""

        def __init__(self, **kwargs: Any) -> None:
            kwargs.setdefault("route_class", ConcurrentRoute)
            super().__init__(**kwargs)
