"""
Tests for the version-adaptive FastAPI layer (``lazy_depends._compat``).

These exercise every capability the compatibility layer probes for, so a
FastAPI release that moves or drops one of them fails loudly here instead
of silently degrading behaviour at runtime.

Every behavioural test is written as a *parity* test where possible: the
same app is built twice, once with FastAPI's stock route class and once
with ``ConcurrentRoute``, and both must agree.
"""

from __future__ import annotations

import asyncio
import functools
import json
from contextlib import AsyncExitStack

import fastapi
import pytest
from fastapi import Depends as FastAPIDepends
from fastapi import FastAPI, Query, Security
from fastapi.dependencies.utils import get_dependant
from fastapi.security import OAuth2PasswordBearer, SecurityScopes
from fastapi.testclient import TestClient
from pydantic import BaseModel

from lazy_depends import ConcurrentRoute, Depends
from lazy_depends import _compat as compat

FASTAPI_VERSION = tuple(int(p) for p in fastapi.__version__.split(".")[:2])


def _apps(**kwargs):
    """Yield (label, app) for a stock FastAPI app and a ConcurrentRoute app."""
    vanilla = FastAPI(**kwargs)
    concurrent = FastAPI(**kwargs)
    concurrent.router.route_class = ConcurrentRoute
    return ("vanilla", vanilla), ("concurrent", concurrent)


# ──────────────────────────────────────────────────────────
# Capability probes — these must keep finding *something*
# ──────────────────────────────────────────────────────────


class TestCapabilityProbes:
    def test_a_cache_key_strategy_is_available(self):
        """Either Dependant.cache_key or _get_cache_key() must exist."""
        assert compat.HAS_DEPENDANT_CACHE_KEY or compat.HAS_GET_CACHE_KEY

    def test_callable_predicates_are_available(self):
        """FastAPI must still expose callable classification somewhere."""
        assert (
            compat.HAS_PRIVATE_MODULE_PREDICATES
            or compat.HAS_MODULE_PREDICATES
            or compat.HAS_DEPENDANT_PREDICATES
        )

    def test_solve_generator_is_available(self):
        assert compat.HAS_SOLVE_GENERATOR or compat.HAS_PRIVATE_SOLVE_GENERATOR

    def test_get_dependant_accepts_a_known_scope_keyword(self):
        """The override path needs one of the two scope keyword spellings."""
        assert (
            "parent_oauth_scopes" in compat._GET_DEPENDANT_PARAMS
            or "security_scopes" in compat._GET_DEPENDANT_PARAMS
        )

    def test_endpoint_context_flags_are_coherent(self):
        """EndpointContext implies both consumers accept endpoint_ctx=."""
        if compat.HAS_ENDPOINT_CONTEXT:
            assert compat.RVE_ACCEPTS_ENDPOINT_CTX
            assert compat.SERIALIZE_ACCEPTS_ENDPOINT_CTX
        else:
            assert not compat.RVE_ACCEPTS_ENDPOINT_CTX
            assert not compat.SERIALIZE_ACCEPTS_ENDPOINT_CTX
            assert compat.HAS_NORMALIZE_ERRORS, "old FastAPI must expose _normalize_errors"

    def test_streaming_flag_implies_strict_content_type_flag(self):
        if compat.HAS_STREAMING_ROUTES:
            assert compat.HAS_STRICT_CONTENT_TYPE

    def test_request_scope_stacks_match_capability(self):
        """FastAPI-owned exit stacks appear in the ASGI scope from 0.116."""
        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        async def dep():
            return 1

        @app.get("/keys")
        async def keys(request: fastapi.Request, _=Depends(dep)):
            return sorted(k for k in request.scope if k.endswith("_astack"))

        found = TestClient(app).get("/keys").json()
        if compat.HAS_ENDPOINT_CONTEXT:
            assert found == [
                "fastapi_function_astack",
                "fastapi_inner_astack",
                "fastapi_middleware_astack",
            ]
        else:
            assert found == []


# ──────────────────────────────────────────────────────────
# Dependency cache keys
# ──────────────────────────────────────────────────────────


class TestDepCacheKey:
    def test_same_callable_yields_equal_keys(self):
        async def dep():
            return 1

        a = get_dependant(path="/", call=dep)
        b = get_dependant(path="/", call=dep)
        assert compat.dep_cache_key(a) == compat.dep_cache_key(b)

    def test_different_callables_yield_different_keys(self):
        async def dep_a():
            return 1

        async def dep_b():
            return 2

        a = get_dependant(path="/", call=dep_a)
        b = get_dependant(path="/", call=dep_b)
        assert compat.dep_cache_key(a) != compat.dep_cache_key(b)

    def test_cache_key_is_hashable(self):
        async def dep():
            return 1

        assert {compat.dep_cache_key(get_dependant(path="/", call=dep)): 1}

    def test_cache_key_shared_across_routes(self):
        """use_cache dedup relies on the key being stable per callable."""
        calls = []

        async def shared():
            calls.append(1)
            return len(calls)

        async def left(v=Depends(shared)):
            return v

        async def right(v=Depends(shared)):
            return v

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(a=Depends(left), b=Depends(right), c=Depends(shared)):
            return {"a": a, "b": b, "c": c}

        body = TestClient(app).get("/").json()
        assert body == {"a": 1, "b": 1, "c": 1}
        assert len(calls) == 1


# ──────────────────────────────────────────────────────────
# Callable classification
# ──────────────────────────────────────────────────────────


async def _async_fn():
    return 1


def _sync_fn():
    return 1


def _sync_gen():
    yield 1


async def _async_gen():
    yield 1


@functools.wraps(_async_fn)
async def _wrapped_async():
    return 1


class _AsyncCallable:
    async def __call__(self):
        return 1


class _SyncCallable:
    def __call__(self):
        return 1


_CLASSIFICATION_CASES = [
    ("async_fn", _async_fn, True, False, False),
    ("sync_fn", _sync_fn, False, False, False),
    ("sync_gen", _sync_gen, False, True, False),
    ("async_gen", _async_gen, False, False, True),
    ("partial_async", functools.partial(_async_fn), True, False, False),
    ("partial_sync_gen", functools.partial(_sync_gen), False, True, False),
    ("wrapped_async", _wrapped_async, True, False, False),
    ("async_callable_obj", _AsyncCallable(), True, False, False),
    ("sync_callable_obj", _SyncCallable(), False, False, False),
]


class TestCallableClassification:
    @pytest.mark.parametrize(
        ("label", "call", "coro", "gen", "agen"),
        _CLASSIFICATION_CASES,
        ids=[c[0] for c in _CLASSIFICATION_CASES],
    )
    def test_classification(self, label, call, coro, gen, agen):
        assert compat.is_coroutine_callable(call) is coro
        assert compat.is_gen_callable(call) is gen
        assert compat.is_async_gen_callable(call) is agen

    @pytest.mark.parametrize(
        ("label", "call", "coro", "gen", "agen"),
        _CLASSIFICATION_CASES,
        ids=[c[0] for c in _CLASSIFICATION_CASES],
    )
    def test_local_fallback_agrees_with_fastapi(self, label, call, coro, gen, agen):
        """
        The pure-Python fallback is what a future FastAPI rename falls back
        to, so it must classify identically to FastAPI's own helpers.
        """
        import inspect

        assert compat._classify(call, compat._routine_is_coroutine) is coro
        assert compat._classify(call, inspect.isgeneratorfunction) is gen
        assert compat._classify(call, inspect.isasyncgenfunction) is agen

    def test_classification_drives_dispatch(self):
        """Sync deps must run in a threadpool, not block the event loop."""
        order = []

        def slow_sync():
            import time

            time.sleep(0.15)
            order.append("sync")
            return "sync"

        async def fast_async():
            await asyncio.sleep(0.01)
            order.append("async")
            return "async"

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(a=Depends(slow_sync), b=Depends(fast_async)):
            return {"a": a, "b": b}

        assert TestClient(app).get("/").json() == {"a": "sync", "b": "async"}
        assert order == ["async", "sync"]


# ──────────────────────────────────────────────────────────
# Exit stacks
# ──────────────────────────────────────────────────────────


class TestExitStacks:
    def test_scope_exit_stack_missing_key(self):
        class _Req:
            scope: dict = {}

        assert compat.scope_exit_stack(_Req(), "fastapi_inner_astack") is None

    def test_scope_exit_stack_wrong_type(self):
        class _Req:
            scope = {"fastapi_inner_astack": object()}

        assert compat.scope_exit_stack(_Req(), "fastapi_inner_astack") is None

    def test_scope_exit_stack_found(self):
        stack = AsyncExitStack()

        class _Req:
            scope = {"fastapi_inner_astack": stack}

        assert compat.scope_exit_stack(_Req(), "fastapi_inner_astack") is stack

    def test_routes_work_without_fastapi_owned_stacks(self, monkeypatch):
        """
        Simulate a FastAPI that renamed the scope keys: the handler must
        fall back to locally owned exit stacks and still clean up.
        """
        events = []

        async def gen_dep():
            events.append("open")
            yield "value"
            events.append("close")

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(v=Depends(gen_dep)):
            events.append("endpoint")
            return {"v": v}

        monkeypatch.setattr(compat, "scope_exit_stack", lambda request, key: None)
        # routing binds the helper through the module object, so patching the
        # attribute is enough for both routing and solver.
        client = TestClient(app)
        assert client.get("/").json() == {"v": "value"}
        assert events == ["open", "endpoint", "close"]

    def test_local_exit_stack_closes_on_endpoint_error(self, monkeypatch):
        """The locally owned stacks must unwind even when the endpoint raises."""
        events = []

        async def gen_dep():
            events.append("open")
            yield "value"
            events.append("close")

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/boom")
        async def boom(v=Depends(gen_dep)):
            raise RuntimeError("boom")

        monkeypatch.setattr(compat, "scope_exit_stack", lambda request, key: None)
        client = TestClient(app, raise_server_exceptions=False)
        assert client.get("/boom").status_code == 500
        assert events == ["open", "close"]

    def test_generator_dep_teardown_after_endpoint(self):
        events = []

        async def gen_dep():
            yield 1
            events.append("close")

        for label, app in _apps():
            app.router.route_class = (
                ConcurrentRoute if label == "concurrent" else app.router.route_class
            )

            @app.get("/")
            async def root(v=FastAPIDepends(gen_dep)):
                events.append("endpoint")
                return {"v": v}

            events.clear()
            assert TestClient(app).get("/").json() == {"v": 1}
            assert events == ["endpoint", "close"], label

    @pytest.mark.skipif(
        not compat.HAS_DEPENDANT_SCOPE,
        reason="per-dependency scope requires FastAPI >=0.116",
    )
    def test_function_scoped_deps_use_the_function_stack(self):
        """
        scope="function" deps are torn down *before* the response leaves the
        app; scope="request" deps after. Both route classes must agree.
        """

        def build(app, events):
            async def fn_dep():
                yield 1
                events.append("fn_close")

            async def req_dep():
                yield 2
                events.append("req_close")

            @app.middleware("http")
            async def mw(request, call_next):
                response = await call_next(request)
                events.append("after_call_next")
                return response

            @app.get("/")
            async def root(
                a=FastAPIDepends(fn_dep, scope="function"),
                b=FastAPIDepends(req_dep),
            ):
                events.append("endpoint")
                return {"a": a, "b": b}

            return app

        results = {}
        for label, app in _apps():
            events: list[str] = []
            assert TestClient(build(app, events)).get("/").json() == {"a": 1, "b": 2}
            results[label] = list(events)

        assert results["concurrent"] == results["vanilla"]
        assert results["concurrent"].index("fn_close") < results["concurrent"].index(
            "after_call_next"
        )
        assert results["concurrent"].index("after_call_next") < results["concurrent"].index(
            "req_close"
        )


# ──────────────────────────────────────────────────────────
# Endpoint context / error paths
# ──────────────────────────────────────────────────────────


class _Item(BaseModel):
    a: int


def _capture_app(concurrent: bool):
    """App whose validation errors are captured instead of formatted."""
    app = FastAPI()
    if concurrent:
        app.router.route_class = ConcurrentRoute
    captured: list = []

    @app.exception_handler(fastapi.exceptions.RequestValidationError)
    async def handler(request, exc):
        captured.append(exc)
        return fastapi.responses.JSONResponse({"detail": exc.errors()}, status_code=422)

    @app.get("/items/{item_id}")
    async def read_item(item_id: int, q: int = Query()):
        return {"item_id": item_id, "q": q}

    @app.post("/items")
    async def create_item(item: _Item):
        return {"a": item.a}

    return app, captured


async def _ctx_endpoint():
    """Module-level endpoint for endpoint-context unit tests."""
    return 1


@pytest.fixture
def clear_endpoint_context_cache():
    """
    FastAPI memoizes EndpointContext by ``id(func)`` in a module-level dict,
    so a recycled id can hand back another function's context.  Clear it to
    keep these unit tests deterministic.
    """
    cache = getattr(fastapi.routing, "_endpoint_context_cache", None)
    if cache is not None:
        cache.clear()
    yield
    if cache is not None:
        cache.clear()


class TestEndpointContext:
    def test_build_endpoint_context_matches_capability(self, clear_endpoint_context_cache):
        endpoint = _ctx_endpoint
        ctx = compat.build_endpoint_context(endpoint, "/x", "GET", "")
        if compat.HAS_ENDPOINT_CONTEXT:
            assert ctx is not None
            assert ctx["path"] == "GET /x"
            assert ctx["function"] == "_ctx_endpoint"
        else:
            assert ctx is None

    @pytest.mark.skipif(
        not compat.HAS_ENDPOINT_CONTEXT,
        reason="EndpointContext requires FastAPI >=0.116",
    )
    def test_build_endpoint_context_includes_root_path(self, clear_endpoint_context_cache):
        ctx = compat.build_endpoint_context(_ctx_endpoint, "/x", "POST", "/mounted/")
        assert ctx["path"] == "POST /mounted/x"

    @pytest.mark.skipif(
        not compat.HAS_ENDPOINT_CONTEXT,
        reason="EndpointContext requires FastAPI >=0.116",
    )
    def test_build_endpoint_context_without_path(self, clear_endpoint_context_cache):
        ctx = compat.build_endpoint_context(_ctx_endpoint, None, "GET", "")
        assert "path" not in ctx

    @pytest.mark.skipif(
        not compat.HAS_ENDPOINT_CONTEXT,
        reason="EndpointContext requires FastAPI >=0.116",
    )
    def test_build_endpoint_context_does_not_mutate_fastapi_cache(
        self, clear_endpoint_context_cache
    ):
        """FastAPI's cached context is shared; we must never write into it."""
        cached = fastapi.routing._extract_endpoint_context(_ctx_endpoint)
        before = dict(cached)
        first = compat.build_endpoint_context(_ctx_endpoint, "/a", "GET", "")
        second = compat.build_endpoint_context(_ctx_endpoint, "/b", "POST", "")
        assert dict(cached) == before
        assert first["path"] == "GET /a"
        assert second["path"] == "POST /b"

    def test_error_kwargs_shape(self):
        ctx = {"path": "GET /x"} if compat.HAS_ENDPOINT_CONTEXT else None
        kwargs = compat.error_kwargs({"body": 1}, ctx)
        assert kwargs["body"] == {"body": 1}
        assert ("endpoint_ctx" in kwargs) is compat.RVE_ACCEPTS_ENDPOINT_CTX

    def test_error_kwargs_omits_missing_context(self):
        assert compat.error_kwargs(None, None) == {"body": None}

    def test_param_validation_error_parity(self):
        bodies = {}
        for concurrent in (False, True):
            app, captured = _capture_app(concurrent)
            response = TestClient(app).get("/items/abc?q=nope")
            assert response.status_code == 422
            bodies["concurrent" if concurrent else "vanilla"] = response.json()
            if compat.RVE_ACCEPTS_ENDPOINT_CTX:
                assert captured[0].endpoint_path == "GET /items/{item_id}"
                assert captured[0].endpoint_function == "read_item"
        assert bodies["concurrent"] == bodies["vanilla"]

    def test_json_decode_error_parity(self):
        bodies = {}
        for concurrent in (False, True):
            app, captured = _capture_app(concurrent)
            response = TestClient(app).post(
                "/items",
                content=b"{not json",
                headers={"content-type": "application/json"},
            )
            assert response.status_code == 422
            bodies["concurrent" if concurrent else "vanilla"] = response.json()
            assert captured[0].errors()[0]["type"] == "json_invalid"
            if compat.RVE_ACCEPTS_ENDPOINT_CTX:
                assert captured[0].endpoint_path == "POST /items"
                assert captured[0].endpoint_function == "create_item"
        assert bodies["concurrent"] == bodies["vanilla"]

    def test_body_validation_error_parity(self):
        bodies = {}
        for concurrent in (False, True):
            app, captured = _capture_app(concurrent)
            response = TestClient(app).post("/items", json={"a": "not-an-int"})
            assert response.status_code == 422
            bodies["concurrent" if concurrent else "vanilla"] = response.json()
            if compat.RVE_ACCEPTS_ENDPOINT_CTX:
                assert captured[0].endpoint_path == "POST /items"
        assert bodies["concurrent"] == bodies["vanilla"]

    def test_dependency_validation_error_parity(self):
        """Errors raised inside a dependency keep the endpoint context too."""

        async def needs_query(q: int = Query()):
            return q

        def build(app, captured):
            @app.exception_handler(fastapi.exceptions.RequestValidationError)
            async def handler(request, exc):
                captured.append(exc)
                return fastapi.responses.JSONResponse({"detail": exc.errors()}, status_code=422)

            @app.get("/dep")
            async def dep_route(v=FastAPIDepends(needs_query)):
                return {"v": v}

            return app

        results = {}
        for label, app in _apps():
            captured: list = []
            response = TestClient(build(app, captured)).get("/dep")
            assert response.status_code == 422
            results[label] = response.json()
            if compat.RVE_ACCEPTS_ENDPOINT_CTX:
                assert captured[0].endpoint_path == "GET /dep"
        assert results["concurrent"] == results["vanilla"]

    def test_response_validation_error_carries_context(self):
        """serialize_response gets endpoint_ctx where FastAPI accepts it."""
        if not compat.SERIALIZE_ACCEPTS_ENDPOINT_CTX:
            pytest.skip("serialize_response has no endpoint_ctx keyword")

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        async def dep():
            return 1

        @app.get("/bad", response_model=_Item)
        async def bad(_=Depends(dep)):
            return {"a": "not-an-int"}

        with pytest.raises(fastapi.exceptions.ResponseValidationError) as exc_info:
            TestClient(app).get("/bad")
        assert exc_info.value.endpoint_path == "GET /bad"
        assert exc_info.value.endpoint_function == "bad"

    def test_normalize_errors_is_a_passthrough_shape(self):
        assert compat.normalize_errors([]) == []
        errors = [{"type": "missing", "loc": ("query", "q"), "msg": "Field required"}]
        assert len(compat.normalize_errors(errors)) == 1


# ──────────────────────────────────────────────────────────
# Body parsing / strict content type
# ──────────────────────────────────────────────────────────


class TestStrictContentType:
    @pytest.mark.skipif(
        not compat.HAS_STRICT_CONTENT_TYPE,
        reason="strict_content_type requires FastAPI >=0.135",
    )
    def test_missing_content_type_is_rejected_like_vanilla(self):
        statuses = {}
        for label, app in _apps():

            @app.post("/items")
            async def create(item: _Item):
                return {"a": item.a}

            statuses[label] = (
                TestClient(app)
                .post("/items", content=json.dumps({"a": 1}), headers={"content-type": ""})
                .status_code
            )
        assert statuses["concurrent"] == statuses["vanilla"] == 422

    @pytest.mark.skipif(
        not compat.HAS_STRICT_CONTENT_TYPE,
        reason="strict_content_type requires FastAPI >=0.135",
    )
    def test_app_level_opt_out_is_honoured(self):
        """strict_content_type=False must reach our handler, not just FastAPI's."""
        statuses = {}
        for label, app in _apps(strict_content_type=False):

            @app.post("/items")
            async def create(item: _Item):
                return {"a": item.a}

            statuses[label] = (
                TestClient(app)
                .post("/items", content=json.dumps({"a": 1}), headers={"content-type": ""})
                .status_code
            )
        assert statuses["concurrent"] == statuses["vanilla"] == 200

    def test_json_body_still_parses(self):
        for label, app in _apps():

            @app.post("/items")
            async def create(item: _Item, dep=FastAPIDepends(lambda: 7)):
                return {"a": item.a, "dep": dep}

            assert TestClient(app).post("/items", json={"a": 3}).json() == {
                "a": 3,
                "dep": 7,
            }, label


# ──────────────────────────────────────────────────────────
# Streaming endpoints
# ──────────────────────────────────────────────────────────


class TestStreamingRoutes:
    @pytest.mark.skipif(
        not compat.HAS_STREAMING_ROUTES,
        reason="streaming endpoints require FastAPI >=0.135",
    )
    def test_generator_endpoint_matches_vanilla(self):
        payloads = {}
        for label, app in _apps():

            async def dep():
                return "d"

            @app.get("/stream")
            async def stream(d=FastAPIDepends(dep)):
                for i in range(3):
                    yield {"i": i, "d": d}

            payloads[label] = TestClient(app).get("/stream").text
        assert payloads["concurrent"] == payloads["vanilla"]
        assert payloads["concurrent"].count("\n") == 3

    @pytest.mark.skipif(
        not compat.HAS_STREAMING_ROUTES,
        reason="streaming endpoints require FastAPI >=0.135",
    )
    def test_streaming_routes_delegate_to_fastapi(self):
        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/stream")
        async def stream():
            yield {"i": 1}

        @app.get("/plain")
        async def plain():
            return {"i": 1}

        routes = {r.path: r for r in app.routes if isinstance(r, ConcurrentRoute)}
        assert routes["/stream"]._is_streaming_route() is True
        assert routes["/plain"]._is_streaming_route() is False

    def test_non_streaming_routes_are_never_delegated(self):
        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/plain")
        async def plain(v=Depends(lambda: 1)):
            return {"v": v}

        route = next(r for r in app.routes if isinstance(r, ConcurrentRoute))
        assert route._is_streaming_route() is False


# ──────────────────────────────────────────────────────────
# Security scopes
# ──────────────────────────────────────────────────────────


class TestSecurityScopes:
    def test_dep_oauth_scopes_reads_whatever_field_exists(self):
        def scoped(scopes: SecurityScopes):
            return scopes.scopes

        async def endpoint(v=Security(scoped, scopes=["read"])):
            return v

        dependant = get_dependant(path="/", call=endpoint)
        sub = dependant.dependencies[0]
        assert compat.dep_oauth_scopes(sub) == ["read"]

    def test_security_scopes_injection_parity(self):
        oauth = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

        def scoped(scopes: SecurityScopes, token=FastAPIDepends(oauth)):
            return list(scopes.scopes)

        bodies = {}
        for label, app in _apps():

            @app.get("/scoped")
            async def scoped_route(v=Security(scoped, scopes=["read", "write"])):
                return {"scopes": v}

            bodies[label] = TestClient(app).get("/scoped").json()
        assert bodies["concurrent"] == bodies["vanilla"] == {"scopes": ["read", "write"]}

    def test_nested_scopes_parity(self):
        def inner(scopes: SecurityScopes):
            return list(scopes.scopes)

        def outer(v=Security(inner, scopes=["inner"])):
            return v

        bodies = {}
        for label, app in _apps():

            @app.get("/nested")
            async def nested(v=Security(outer, scopes=["outer"])):
                return {"scopes": v}

            bodies[label] = TestClient(app).get("/nested").json()
        assert bodies["concurrent"] == bodies["vanilla"]
        assert "inner" in bodies["concurrent"]["scopes"]


# ──────────────────────────────────────────────────────────
# dependency_overrides → get_dependant() re-analysis
# ──────────────────────────────────────────────────────────


class TestOverrideDependant:
    def test_build_override_dependant_keeps_scopes(self):
        def scoped(scopes: SecurityScopes):
            return scopes.scopes

        async def endpoint(v=Security(scoped, scopes=["read"])):
            return v

        original = get_dependant(path="/", call=endpoint).dependencies[0]

        def replacement(scopes: SecurityScopes):
            return scopes.scopes

        rebuilt = compat.build_override_dependant(original, replacement)
        assert rebuilt.call is replacement
        assert rebuilt.name == original.name
        assert compat.dep_oauth_scopes(rebuilt) == ["read"]

    def test_override_of_sub_dependency_parity(self):
        async def real_db():
            return "real"

        async def fake_db():
            return "fake"

        async def service(db=FastAPIDepends(real_db)):
            return f"service:{db}"

        bodies = {}
        for label, app in _apps():

            @app.get("/")
            async def root(s=FastAPIDepends(service)):
                return {"s": s}

            app.dependency_overrides[real_db] = fake_db
            bodies[label] = TestClient(app).get("/").json()
        assert bodies["concurrent"] == bodies["vanilla"] == {"s": "service:fake"}

    def test_override_of_generator_dependency(self):
        events = []

        async def real_res():
            yield "real"

        async def fake_res():
            yield "fake"
            events.append("closed")

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(r=Depends(real_res)):
            return {"r": r}

        app.dependency_overrides[real_res] = fake_res
        assert TestClient(app).get("/").json() == {"r": "fake"}
        assert events == ["closed"]

    def test_override_with_scoped_security_dependency(self):
        def scoped(scopes: SecurityScopes):
            return list(scopes.scopes)

        def fake_scoped(scopes: SecurityScopes):
            return ["overridden", *scopes.scopes]

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(v=Security(scoped, scopes=["read"])):
            return {"v": v}

        app.dependency_overrides[scoped] = fake_scoped
        assert TestClient(app).get("/").json() == {"v": ["overridden", "read"]}


# ──────────────────────────────────────────────────────────
# Endpoint dispatch
# ──────────────────────────────────────────────────────────


class TestRunEndpointFunction:
    def test_fastapi_still_exposes_run_endpoint_function(self):
        assert compat.HAS_RUN_ENDPOINT_FUNCTION
        assert compat.run_endpoint_function is fastapi.routing.run_endpoint_function

    def test_local_dispatch_fallback_handles_async_and_sync(self, monkeypatch):
        """A future rename must degrade to the local dispatcher, not explode."""
        monkeypatch.setattr(compat, "run_endpoint_function", compat._run_endpoint_function_fallback)

        app = FastAPI()
        app.router.route_class = ConcurrentRoute

        async def dep():
            return "dep"

        @app.get("/async")
        async def async_endpoint(v=Depends(dep)):
            return {"v": v, "kind": "async"}

        @app.get("/sync")
        def sync_endpoint(v=Depends(dep)):
            return {"v": v, "kind": "sync"}

        client = TestClient(app)
        assert client.get("/async").json() == {"v": "dep", "kind": "async"}
        assert client.get("/sync").json() == {"v": "dep", "kind": "sync"}
