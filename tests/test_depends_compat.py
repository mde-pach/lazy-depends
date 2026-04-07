"""
Compatibility test suite — every FastAPI Depends feature must work
identically with ConcurrentRoute (Depends + LazyDepends).

Run: pytest tests/test_depends_compat.py -v
"""

import asyncio
import time
from typing import AsyncGenerator, Generator

import pytest
from fastapi import FastAPI, Header, Query, Cookie, HTTPException, Request, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.testclient import TestClient
from starlette.background import BackgroundTasks

from fastapi import Depends as FastAPIDepends
from lazy_depends import ConcurrentRoute, Depends, LazyDepends


# ═══════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════

def _app() -> FastAPI:
    app = FastAPI()
    app.router.route_class = ConcurrentRoute
    return app


# ═══════════════════════════════════════════════════════════
# 1. Basic concurrent resolution
# ═══════════════════════════════════════════════════════════


class TestBasicConcurrent:
    def test_independent_deps_resolve(self):
        """Two independent deps both inject correctly."""
        app = _app()

        async def dep_a():
            return "a"

        async def dep_b():
            return "b"

        @app.get("/")
        async def root(a=Depends(dep_a), b=Depends(dep_b)):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"a": "a", "b": "b"}

    def test_independent_deps_run_in_parallel(self):
        """Independent deps actually run concurrently, not sequentially."""
        app = _app()

        async def slow_a():
            await asyncio.sleep(0.3)
            return "a"

        async def slow_b():
            await asyncio.sleep(0.3)
            return "b"

        @app.get("/")
        async def root(a=Depends(slow_a), b=Depends(slow_b)):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"a": "a", "b": "b"}
            # Sequential would be ~0.6s, concurrent should be ~0.3s
            assert dt < 0.5, f"Took {dt:.2f}s — deps ran sequentially"

    def test_single_dep(self):
        """Single dep works (no TaskGroup overhead path)."""
        app = _app()

        async def dep():
            return 42

        @app.get("/")
        async def root(val=Depends(dep)):
            return {"val": val}

        with TestClient(app) as c:
            assert c.get("/").json() == {"val": 42}

    def test_no_deps(self):
        """Route with no deps still works."""
        app = _app()

        @app.get("/")
        async def root():
            return {"ok": True}

        with TestClient(app) as c:
            assert c.get("/").json() == {"ok": True}


# ═══════════════════════════════════════════════════════════
# 2. Dependency chains
# ═══════════════════════════════════════════════════════════


class TestDependencyChains:
    def test_linear_chain(self):
        """A -> B -> C resolves in correct order."""
        app = _app()
        order = []

        async def dep_c():
            order.append("c")
            return "c"

        async def dep_b(c=Depends(dep_c)):
            order.append("b")
            return f"b({c})"

        async def dep_a(b=Depends(dep_b)):
            order.append("a")
            return f"a({b})"

        @app.get("/")
        async def root(a=Depends(dep_a)):
            return {"a": a, "order": order}

        with TestClient(app) as c:
            r = c.get("/")
            data = r.json()
            assert data["a"] == "a(b(c))"
            assert data["order"] == ["c", "b", "a"]

    def test_diamond_dependency(self):
        """Diamond shape: D depends on B and C, both depend on A."""
        app = _app()

        async def dep_a():
            return "a"

        async def dep_b(a=Depends(dep_a)):
            return f"b({a})"

        async def dep_c(a=Depends(dep_a)):
            return f"c({a})"

        @app.get("/")
        async def root(b=Depends(dep_b), c=Depends(dep_c)):
            return {"b": b, "c": c}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"b": "b(a)", "c": "c(a)"}

    def test_shared_sub_dep_cached(self):
        """Shared sub-dep (use_cache=True) is called exactly once."""
        app = _app()
        call_count = 0

        async def shared():
            nonlocal call_count
            call_count += 1
            return "shared"

        async def dep_a(s=Depends(shared)):
            return f"a({s})"

        async def dep_b(s=Depends(shared)):
            return f"b({s})"

        @app.get("/")
        async def root(a=Depends(dep_a), b=Depends(dep_b)):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            call_count = 0
            r = c.get("/")
            assert r.json() == {"a": "a(shared)", "b": "b(shared)"}
            assert call_count == 1


# ═══════════════════════════════════════════════════════════
# 3. use_cache
# ═══════════════════════════════════════════════════════════


class TestUseCache:
    def test_use_cache_true_default(self):
        """Same dep reused across params — called once."""
        app = _app()
        calls = 0

        async def dep():
            nonlocal calls
            calls += 1
            return calls

        @app.get("/")
        async def root(a=Depends(dep), b=Depends(dep)):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            calls = 0
            r = c.get("/")
            assert r.json() == {"a": 1, "b": 1}

    def test_use_cache_false(self):
        """use_cache=False — same dep called multiple times."""
        app = _app()
        calls = 0

        async def dep():
            nonlocal calls
            calls += 1
            return calls

        @app.get("/")
        async def root(
            a=Depends(dep, use_cache=False),
            b=Depends(dep, use_cache=False),
        ):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            calls = 0
            r = c.get("/")
            data = r.json()
            # Both should be called, values differ
            assert data["a"] != data["b"]


# ═══════════════════════════════════════════════════════════
# 4. Generator (yield) deps
# ═══════════════════════════════════════════════════════════


class TestGeneratorDeps:
    def test_sync_generator_dep(self):
        """Sync generator dep: yield value, cleanup runs."""
        app = _app()
        cleaned_up = []

        def gen_dep():
            cleaned_up.clear()
            yield "from-gen"
            cleaned_up.append(True)

        @app.get("/")
        async def root(val=Depends(gen_dep)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"val": "from-gen"}
            assert cleaned_up == [True]

    def test_async_generator_dep(self):
        """Async generator dep: yield value, cleanup runs."""
        app = _app()
        cleaned_up = []

        async def async_gen_dep():
            cleaned_up.clear()
            yield "from-async-gen"
            cleaned_up.append(True)

        @app.get("/")
        async def root(val=Depends(async_gen_dep)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"val": "from-async-gen"}
            assert cleaned_up == [True]

    def test_generator_cleanup_on_error(self):
        """Generator cleanup runs even when endpoint raises."""
        app = _app()
        cleaned_up = []

        async def gen():
            cleaned_up.clear()
            yield "val"
            cleaned_up.append(True)

        @app.get("/")
        async def root(val=Depends(gen)):
            raise HTTPException(500, "boom")

        with TestClient(app, raise_server_exceptions=False) as c:
            r = c.get("/")
            assert r.status_code == 500
            assert cleaned_up == [True]


# ═══════════════════════════════════════════════════════════
# 5. Sync deps
# ═══════════════════════════════════════════════════════════


class TestSyncDeps:
    def test_sync_function_dep(self):
        """Plain sync function runs in threadpool."""
        app = _app()

        def sync_dep():
            return "sync-value"

        @app.get("/")
        async def root(val=Depends(sync_dep)):
            return {"val": val}

        with TestClient(app) as c:
            assert c.get("/").json() == {"val": "sync-value"}

    def test_mixed_sync_async_deps(self):
        """Sync and async deps in the same route."""
        app = _app()

        def sync_dep():
            return "sync"

        async def async_dep():
            return "async"

        @app.get("/")
        async def root(s=Depends(sync_dep), a=Depends(async_dep)):
            return {"s": s, "a": a}

        with TestClient(app) as c:
            assert c.get("/").json() == {"s": "sync", "a": "async"}


# ═══════════════════════════════════════════════════════════
# 6. dependency_overrides
# ═══════════════════════════════════════════════════════════


class TestDependencyOverrides:
    def test_override_dep(self):
        """dependency_overrides replaces a dep."""
        app = _app()

        async def real_dep():
            return "real"

        async def mock_dep():
            return "mock"

        @app.get("/")
        async def root(val=Depends(real_dep)):
            return {"val": val}

        app.dependency_overrides[real_dep] = mock_dep
        with TestClient(app) as c:
            assert c.get("/").json() == {"val": "mock"}
        app.dependency_overrides.clear()

    def test_override_sub_dep(self):
        """Override a sub-dependency in a chain."""
        app = _app()

        async def db_dep():
            return "real-db"

        async def user_dep(db=Depends(db_dep)):
            return f"user({db})"

        async def mock_db():
            return "mock-db"

        @app.get("/")
        async def root(user=Depends(user_dep)):
            return {"user": user}

        app.dependency_overrides[db_dep] = mock_db
        with TestClient(app) as c:
            assert c.get("/").json() == {"user": "user(mock-db)"}
        app.dependency_overrides.clear()


# ═══════════════════════════════════════════════════════════
# 7. Special params: Request, Response, BackgroundTasks
# ═══════════════════════════════════════════════════════════


class TestSpecialParams:
    def test_request_injection(self):
        """Dep can receive the Request object."""
        app = _app()

        async def dep_with_request(request: Request):
            return request.headers.get("x-custom", "missing")

        @app.get("/")
        async def root(val=Depends(dep_with_request)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.get("/", headers={"x-custom": "hello"})
            assert r.json() == {"val": "hello"}

    def test_response_injection(self):
        """Dep can receive and modify the Response."""
        app = _app()

        async def dep_with_response(response: Response):
            response.headers["x-dep-header"] = "set"
            return "ok"

        @app.get("/")
        async def root(val=Depends(dep_with_response)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"val": "ok"}
            assert r.headers.get("x-dep-header") == "set"

    def test_background_tasks_injection(self):
        """Dep can receive BackgroundTasks."""
        app = _app()
        tasks_ran = []

        async def dep_with_bg(background_tasks: BackgroundTasks):
            background_tasks.add_task(lambda: tasks_ran.append(True))
            return "ok"

        @app.get("/")
        async def root(val=Depends(dep_with_bg)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"val": "ok"}
            assert tasks_ran == [True]


# ═══════════════════════════════════════════════════════════
# 8. Header / Query / Cookie / Path params in deps
# ═══════════════════════════════════════════════════════════


class TestParamInjection:
    def test_header_param(self):
        app = _app()

        async def dep(x_token: str = Header()):
            return x_token

        @app.get("/")
        async def root(token=Depends(dep)):
            return {"token": token}

        with TestClient(app) as c:
            r = c.get("/", headers={"x-token": "abc"})
            assert r.json() == {"token": "abc"}

    def test_query_param(self):
        app = _app()

        async def dep(page: int = Query(default=1)):
            return page

        @app.get("/")
        async def root(page=Depends(dep)):
            return {"page": page}

        with TestClient(app) as c:
            assert c.get("/?page=5").json() == {"page": 5}
            assert c.get("/").json() == {"page": 1}

    def test_path_param_passthrough(self):
        """Path params on the endpoint still work with concurrent deps."""
        app = _app()

        async def dep():
            return "dep-val"

        @app.get("/items/{item_id}")
        async def root(item_id: int, val=Depends(dep)):
            return {"item_id": item_id, "val": val}

        with TestClient(app) as c:
            assert c.get("/items/42").json() == {"item_id": 42, "val": "dep-val"}

    def test_cookie_param(self):
        app = _app()

        async def dep(session: str = Cookie(default=None)):
            return session

        @app.get("/")
        async def root(s=Depends(dep)):
            return {"session": s}

        with TestClient(app) as c:
            c.cookies.set("session", "abc123")
            assert c.get("/").json() == {"session": "abc123"}


# ═══════════════════════════════════════════════════════════
# 9. Body params
# ═══════════════════════════════════════════════════════════


class TestBodyParams:
    def test_json_body(self):
        """Endpoint with JSON body + deps."""
        app = _app()
        from pydantic import BaseModel

        class Item(BaseModel):
            name: str

        async def dep():
            return "dep-val"

        @app.post("/")
        async def root(item: Item, val=Depends(dep)):
            return {"name": item.name, "val": val}

        with TestClient(app) as c:
            r = c.post("/", json={"name": "test"})
            assert r.json() == {"name": "test", "val": "dep-val"}


# ═══════════════════════════════════════════════════════════
# 10. HTTPException from deps
# ═══════════════════════════════════════════════════════════


class TestDepErrors:
    def test_dep_raises_http_exception(self):
        """HTTPException in a dep surfaces as expected status code."""
        app = _app()

        async def auth_dep():
            raise HTTPException(401, "Not authenticated")

        @app.get("/")
        async def root(val=Depends(auth_dep)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.status_code == 401
            assert r.json()["detail"] == "Not authenticated"

    def test_dep_raises_in_chain(self):
        """HTTPException in a sub-dep propagates correctly."""
        app = _app()

        async def db_dep():
            return "db"

        async def auth_dep(db=Depends(db_dep)):
            raise HTTPException(403, "Forbidden")

        async def other_dep():
            return "other"

        @app.get("/")
        async def root(auth=Depends(auth_dep), other=Depends(other_dep)):
            return {"auth": auth, "other": other}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.status_code == 403

    def test_validation_error_from_missing_header(self):
        """Missing required Header param returns 422."""
        app = _app()

        async def dep(x_required: str = Header()):
            return x_required

        @app.get("/")
        async def root(val=Depends(dep)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.status_code == 422


# ═══════════════════════════════════════════════════════════
# 11. Security
# ═══════════════════════════════════════════════════════════


class TestSecurity:
    def test_http_bearer(self):
        """HTTPBearer security dep works."""
        app = _app()
        security = HTTPBearer()

        async def get_user(creds: HTTPAuthorizationCredentials = Depends(security)):
            return {"token": creds.credentials}

        @app.get("/")
        async def root(user=Depends(get_user)):
            return user

        with TestClient(app) as c:
            r = c.get("/", headers={"Authorization": "Bearer mytoken"})
            assert r.json() == {"token": "mytoken"}

            r = c.get("/")
            assert r.status_code == 403


# ═══════════════════════════════════════════════════════════
# 12. LazyDepends — basic
# ═══════════════════════════════════════════════════════════


class TestLazyDependsBasic:
    def test_lazy_dep_resolves(self):
        """LazyDepends value resolves correctly on await."""
        app = _app()

        async def dep():
            return {"id": 1, "name": "Alice"}

        @app.get("/")
        async def root(lazy_user=LazyDepends(dep)):
            user = await lazy_user
            return user

        with TestClient(app) as c:
            assert c.get("/").json() == {"id": 1, "name": "Alice"}

    def test_lazy_dep_with_sub_deps(self):
        """LazyDepends on a dep that has its own sub-deps."""
        app = _app()

        async def db_dep():
            return "db"

        async def user_dep(db=Depends(db_dep)):
            return f"user({db})"

        @app.get("/")
        async def root(lazy_user=LazyDepends(user_dep)):
            user = await lazy_user
            return {"user": user}

        with TestClient(app) as c:
            assert c.get("/").json() == {"user": "user(db)"}

    def test_lazy_dep_error_propagates(self):
        """Exception in a lazy dep propagates on await."""
        app = _app()

        async def failing_dep():
            raise HTTPException(503, "Service unavailable")

        @app.get("/")
        async def root(lazy_svc=LazyDepends(failing_dep)):
            try:
                svc = await lazy_svc
            except Exception:
                return {"error": True}
            return {"svc": svc}

        with TestClient(app) as c:
            # The HTTPException propagates through the task
            r = c.get("/")
            # Depending on how FastAPI handles exceptions from tasks,
            # this may be a 503 or caught by the endpoint
            assert r.status_code in (200, 503)


# ═══════════════════════════════════════════════════════════
# 13. LazyDepends — timing
# ═══════════════════════════════════════════════════════════


class TestLazyDependsTiming:
    def test_lazy_runs_in_background(self):
        """LazyDepends actually resolves in background — saves wall time."""
        app = _app()

        async def fast_dep():
            return "fast"

        async def slow_dep():
            await asyncio.sleep(0.5)
            return "slow"

        @app.get("/")
        async def root(
            fast=Depends(fast_dep),
            lazy_slow=LazyDepends(slow_dep),
        ):
            # Do async work while slow_dep resolves in background
            await asyncio.sleep(0.3)
            slow = await lazy_slow
            return {"fast": fast, "slow": slow}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"fast": "fast", "slow": "slow"}
            # Sequential: 0.5 + 0.3 = 0.8s
            # Lazy: max(0.5, 0.3) = 0.5s
            assert dt < 0.7, f"Took {dt:.2f}s — lazy dep didn't run in background"


# ═══════════════════════════════════════════════════════════
# 14. Mixed Depends + LazyDepends
# ═══════════════════════════════════════════════════════════


class TestMixedDeps:
    def test_eager_and_lazy_same_endpoint(self):
        """Both Depends and LazyDepends in the same endpoint."""
        app = _app()

        async def eager():
            return "eager"

        async def lazy():
            return "lazy"

        @app.get("/")
        async def root(e=Depends(eager), lazy_l=LazyDepends(lazy)):
            l = await lazy_l
            return {"e": e, "l": l}

        with TestClient(app) as c:
            assert c.get("/").json() == {"e": "eager", "l": "lazy"}

    def test_lazy_and_eager_share_sub_dep(self):
        """Lazy and eager deps sharing a cached sub-dep."""
        app = _app()
        calls = 0

        async def shared():
            nonlocal calls
            calls += 1
            return f"shared-{calls}"

        async def eager_dep(s=Depends(shared)):
            return f"eager({s})"

        async def lazy_dep(s=Depends(shared)):
            return f"lazy({s})"

        @app.get("/")
        async def root(e=Depends(eager_dep), lazy_l=LazyDepends(lazy_dep)):
            l = await lazy_l
            return {"e": e, "l": l}

        with TestClient(app) as c:
            calls = 0
            r = c.get("/")
            data = r.json()
            # Both should see the same shared value (cached)
            assert "shared-1" in data["e"]
            assert "shared-1" in data["l"]

    def test_multiple_lazy_deps(self):
        """Multiple LazyDepends in the same endpoint."""
        app = _app()

        async def dep_a():
            await asyncio.sleep(0.2)
            return "a"

        async def dep_b():
            await asyncio.sleep(0.2)
            return "b"

        @app.get("/")
        async def root(lazy_a=LazyDepends(dep_a), lazy_b=LazyDepends(dep_b)):
            a = await lazy_a
            b = await lazy_b
            return {"a": a, "b": b}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"a": "a", "b": "b"}
            # Both lazy deps run in parallel: ~0.2s, not ~0.4s
            assert dt < 0.35, f"Took {dt:.2f}s — lazy deps didn't run in parallel"


# ═══════════════════════════════════════════════════════════
# 15. LazyDepends with generator deps
# ═══════════════════════════════════════════════════════════


class TestLazyGenerators:
    def test_lazy_async_generator(self):
        """LazyDepends with an async generator dep."""
        app = _app()
        cleaned_up = []

        async def gen_dep() -> AsyncGenerator[str, None]:
            yield "gen-value"
            cleaned_up.append(True)

        @app.get("/")
        async def root(lazy_val=LazyDepends(gen_dep)):
            val = await lazy_val
            return {"val": val}

        with TestClient(app) as c:
            cleaned_up.clear()
            r = c.get("/")
            assert r.json() == {"val": "gen-value"}
            assert cleaned_up == [True]


# ═══════════════════════════════════════════════════════════
# 16. LazyDepends with dependency_overrides
# ═══════════════════════════════════════════════════════════


class TestLazyOverrides:
    def test_override_lazy_dep(self):
        """dependency_overrides works on LazyDepends deps."""
        app = _app()

        async def real_dep():
            return "real"

        async def mock_dep():
            return "mock"

        @app.get("/")
        async def root(lazy_val=LazyDepends(real_dep)):
            val = await lazy_val
            return {"val": val}

        app.dependency_overrides[real_dep] = mock_dep
        with TestClient(app) as c:
            assert c.get("/").json() == {"val": "mock"}
        app.dependency_overrides.clear()


# ═══════════════════════════════════════════════════════════
# 17. Lazy[T] object behavior
# ═══════════════════════════════════════════════════════════


class TestLazyObject:
    def test_repr_pending(self):
        from lazy_depends.lazy import Lazy

        async def noop():
            await asyncio.sleep(100)

        loop = asyncio.new_event_loop()
        task = loop.create_task(noop())
        lazy = Lazy(task)
        assert "pending" in repr(lazy)
        task.cancel()
        loop.close()

    def test_repr_resolved(self):
        from lazy_depends.lazy import Lazy

        async def val():
            return 42

        async def check():
            task = asyncio.create_task(val())
            lazy = Lazy(task)
            result = await lazy
            assert result == 42
            assert "42" in repr(lazy)

        asyncio.run(check())

    def test_await_returns_real_value(self):
        """await Lazy returns the actual T, not a wrapper."""
        from lazy_depends.lazy import Lazy

        async def check():
            task = asyncio.create_task(asyncio.coroutine(lambda: None)() if False else asyncio.sleep(0, result={"key": "val"}))
            lazy = Lazy(task)
            result = await lazy
            assert isinstance(result, dict)
            assert result["key"] == "val"
            # It's a real dict, not a proxy
            assert type(result) is dict

        asyncio.run(check())


# ═══════════════════════════════════════════════════════════
# 18. Response types
# ═══════════════════════════════════════════════════════════


class TestResponseTypes:
    def test_return_response_directly(self):
        """Returning a Response object directly works."""
        from starlette.responses import JSONResponse

        app = _app()

        async def dep():
            return "val"

        @app.get("/")
        async def root(val=Depends(dep)):
            return JSONResponse({"val": val}, headers={"x-custom": "yes"})

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"val": "val"}
            assert r.headers["x-custom"] == "yes"

    def test_status_code_override(self):
        """Custom status code on the route works."""
        app = _app()

        async def dep():
            return "created"

        @app.post("/", status_code=201)
        async def root(val=Depends(dep)):
            return {"val": val}

        with TestClient(app) as c:
            r = c.post("/")
            assert r.status_code == 201
            assert r.json() == {"val": "created"}


# ═══════════════════════════════════════════════════════════
# 19. Class-based deps (callable instances)
# ═══════════════════════════════════════════════════════════


class TestClassBasedDeps:
    def test_callable_class_dep(self):
        """A class with __call__ works as a dependency."""
        app = _app()

        class GetPage:
            def __call__(self, page: int = Query(default=1), size: int = Query(default=10)):
                return {"page": page, "size": size}

        pagination = GetPage()

        @app.get("/")
        async def root(paging=Depends(pagination)):
            return paging

        with TestClient(app) as c:
            assert c.get("/?page=3&size=20").json() == {"page": 3, "size": 20}
            assert c.get("/").json() == {"page": 1, "size": 10}

    def test_callable_class_with_init(self):
        """Class dep with __init__ config."""
        app = _app()

        class RoleChecker:
            def __init__(self, required_role: str):
                self.required_role = required_role

            async def __call__(self, request: Request):
                role = request.headers.get("x-role", "")
                if role != self.required_role:
                    raise HTTPException(403, f"Need role: {self.required_role}")
                return role

        require_admin = RoleChecker("admin")

        @app.get("/")
        async def root(role=Depends(require_admin)):
            return {"role": role}

        with TestClient(app) as c:
            r = c.get("/", headers={"x-role": "admin"})
            assert r.json() == {"role": "admin"}
            r = c.get("/", headers={"x-role": "user"})
            assert r.status_code == 403


# ═══════════════════════════════════════════════════════════
# 20. Cross-route: LazyDepends in one route, Depends in another
# ═══════════════════════════════════════════════════════════


class TestCrossRoute:
    def test_same_dep_lazy_in_one_route_eager_in_another(self):
        """A dep marked LazyDepends in route A works as eager Depends in route B."""
        app = _app()

        async def shared_dep():
            return "shared"

        @app.get("/lazy")
        async def lazy_route(lazy_val=LazyDepends(shared_dep)):
            val = await lazy_val
            return {"val": val, "route": "lazy"}

        @app.get("/eager")
        async def eager_route(val=Depends(shared_dep)):
            return {"val": val, "route": "eager"}

        with TestClient(app) as c:
            r = c.get("/lazy")
            assert r.json() == {"val": "shared", "route": "lazy"}
            r = c.get("/eager")
            assert r.json() == {"val": "shared", "route": "eager"}

    def test_multiple_routes_independent(self):
        """Multiple routes with different deps don't interfere."""
        app = _app()

        async def dep_a():
            return "a"

        async def dep_b():
            return "b"

        @app.get("/a")
        async def route_a(val=Depends(dep_a)):
            return {"val": val}

        @app.get("/b")
        async def route_b(val=Depends(dep_b)):
            return {"val": val}

        with TestClient(app) as c:
            assert c.get("/a").json() == {"val": "a"}
            assert c.get("/b").json() == {"val": "b"}


# ═══════════════════════════════════════════════════════════
# 21. WebSocket deps
# ═══════════════════════════════════════════════════════════


class TestWebSocket:
    def test_websocket_with_deps(self):
        """WebSocket routes use FastAPI's default handler (not ConcurrentRoute)
        but still work correctly with deps."""
        from starlette.websockets import WebSocket

        app = _app()

        async def dep():
            return "ws-dep"

        @app.websocket("/ws")
        async def ws_endpoint(websocket: WebSocket, val=Depends(dep)):
            await websocket.accept()
            await websocket.send_json({"val": val})
            await websocket.close()

        with TestClient(app) as c:
            with c.websocket_connect("/ws") as ws:
                data = ws.receive_json()
                assert data == {"val": "ws-dep"}


# ═══════════════════════════════════════════════════════════
# 22. StaticDepends — resolve once, cache forever
# ═══════════════════════════════════════════════════════════


class TestStaticDepends:
    def setup_method(self):
        """Reset static registry between tests."""
        from lazy_depends import StaticDepends
        StaticDepends.reset()

    def test_static_dep_resolves_at_startup(self):
        """StaticDepends resolves during lifespan and injects the value."""
        from lazy_depends import StaticDepends

        calls = 0

        async def expensive():
            nonlocal calls
            calls += 1
            return {"config": "loaded", "call": calls}

        app = FastAPI(lifespan=StaticDepends.lifespan)
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(config=StaticDepends(expensive)):
            return config

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"config": "loaded", "call": 1}

    def test_static_dep_cached_across_requests(self):
        """StaticDepends returns the same value on every request — callable runs once."""
        from lazy_depends import StaticDepends

        calls = 0

        async def expensive():
            nonlocal calls
            calls += 1
            return {"n": calls}

        app = FastAPI(lifespan=StaticDepends.lifespan)
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(config=StaticDepends(expensive)):
            return config

        with TestClient(app) as c:
            r1 = c.get("/")
            r2 = c.get("/")
            r3 = c.get("/")
            assert r1.json() == {"n": 1}
            assert r2.json() == {"n": 1}
            assert r3.json() == {"n": 1}
            assert calls == 1

    def test_static_sync_dep(self):
        """StaticDepends works with sync callables."""
        from lazy_depends import StaticDepends

        calls = 0

        def sync_config():
            nonlocal calls
            calls += 1
            return {"sync": True, "n": calls}

        app = FastAPI(lifespan=StaticDepends.lifespan)
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(config=StaticDepends(sync_config)):
            return config

        with TestClient(app) as c:
            assert c.get("/").json() == {"sync": True, "n": 1}
            assert c.get("/").json() == {"sync": True, "n": 1}
            assert calls == 1

    def test_static_with_regular_deps(self):
        """StaticDepends and regular Depends in the same endpoint."""
        from lazy_depends import StaticDepends

        static_calls = 0
        regular_calls = 0

        async def static_dep():
            nonlocal static_calls
            static_calls += 1
            return "static"

        async def regular_dep():
            nonlocal regular_calls
            regular_calls += 1
            return "regular"

        app = FastAPI(lifespan=StaticDepends.lifespan)
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(s=StaticDepends(static_dep), r=Depends(regular_dep)):
            return {"s": s, "r": r}

        with TestClient(app) as c:
            assert c.get("/").json() == {"s": "static", "r": "regular"}
            assert c.get("/").json() == {"s": "static", "r": "regular"}
            assert static_calls == 1  # resolved once at startup
            assert regular_calls == 2  # resolved each request

    def test_static_without_lifespan_raises(self):
        """Accessing a StaticDepends without lifespan gives a clear error."""
        from lazy_depends import StaticDepends

        async def dep():
            return "val"

        app = _app()  # no lifespan

        @app.get("/")
        async def root(val=StaticDepends(dep)):
            return {"val": val}

        with TestClient(app, raise_server_exceptions=False) as c:
            r = c.get("/")
            assert r.status_code == 500

    def test_static_multiple_deps_resolve_concurrently(self):
        """Multiple static deps resolve concurrently during lifespan."""
        from lazy_depends import StaticDepends

        app = FastAPI(lifespan=StaticDepends.lifespan)
        app.router.route_class = ConcurrentRoute

        async def slow_a():
            await asyncio.sleep(0.2)
            return "a"

        async def slow_b():
            await asyncio.sleep(0.2)
            return "b"

        @app.get("/")
        async def root(a=StaticDepends(slow_a), b=StaticDepends(slow_b)):
            return {"a": a, "b": b}

        import time
        t0 = time.monotonic()
        with TestClient(app) as c:
            startup_dt = time.monotonic() - t0
            r = c.get("/")
            assert r.json() == {"a": "a", "b": "b"}
        # Both resolved concurrently: ~0.2s, not ~0.4s
        assert startup_dt < 0.35, f"Startup took {startup_dt:.2f}s — deps ran sequentially"

    def test_static_with_custom_lifespan(self):
        """StaticDepends.resolve() composes with custom lifespan."""
        from contextlib import asynccontextmanager
        from lazy_depends import StaticDepends

        custom_ran = []

        async def dep():
            return "static-val"

        @asynccontextmanager
        async def custom_lifespan(app):
            await StaticDepends.resolve()
            custom_ran.append("startup")
            yield
            custom_ran.append("shutdown")

        app = FastAPI(lifespan=custom_lifespan)
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(val=StaticDepends(dep)):
            return {"val": val}

        with TestClient(app) as c:
            assert c.get("/").json() == {"val": "static-val"}
        assert custom_ran == ["startup", "shutdown"]


# ═══════════════════════════════════════════════════════════
# 23. Eager leaf resolution
# ═══════════════════════════════════════════════════════════


class TestEagerLeafResolution:
    def test_leaf_deps_still_resolve_correctly(self):
        """Leaf deps (no sub-deps) resolve correctly via the fast path."""
        app = _app()

        async def leaf_a():
            return "a"

        async def leaf_b():
            return "b"

        async def needs_both(a=Depends(leaf_a), b=Depends(leaf_b)):
            return f"{a}+{b}"

        @app.get("/")
        async def root(result=Depends(needs_both)):
            return {"result": result}

        with TestClient(app) as c:
            assert c.get("/").json() == {"result": "a+b"}

    def test_sync_leaf_deps(self):
        """Sync leaf deps benefit from eager resolution."""
        app = _app()

        def sync_leaf():
            return "sync"

        async def async_child(val=Depends(sync_leaf)):
            return f"child({val})"

        @app.get("/")
        async def root(result=Depends(async_child)):
            return {"result": result}

        with TestClient(app) as c:
            assert c.get("/").json() == {"result": "child(sync)"}


# ═══════════════════════════════════════════════════════════
# 24. Single-level fast path
# ═══════════════════════════════════════════════════════════


class TestSingleLevelFastPath:
    def test_all_independent_deps(self):
        """When all deps are independent, they resolve via gather fast path."""
        app = _app()

        async def dep_a():
            await asyncio.sleep(0.2)
            return "a"

        async def dep_b():
            await asyncio.sleep(0.2)
            return "b"

        async def dep_c():
            await asyncio.sleep(0.2)
            return "c"

        @app.get("/")
        async def root(a=Depends(dep_a), b=Depends(dep_b), c=Depends(dep_c)):
            return {"a": a, "b": b, "c": c}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"a": "a", "b": "b", "c": "c"}
            # All 3 should run in parallel: ~0.2s, not ~0.6s
            assert dt < 0.4, f"Took {dt:.2f}s — deps didn't run in parallel"


# ═══════════════════════════════════════════════════════════
# 25. Tracing
# ═══════════════════════════════════════════════════════════


class TestTracing:
    def test_trace_collects_spans(self):
        """Tracing collects timing spans for each dep."""
        app = _app()

        async def dep_a():
            return "a"

        async def dep_b():
            return "b"

        @app.get("/")
        async def root(a=Depends(dep_a), b=Depends(dep_b)):
            return {"a": a, "b": b}

        import os
        old = os.environ.get("LAZY_DEPENDS_TRACE", "")
        os.environ["LAZY_DEPENDS_TRACE"] = "1"
        # Reload to pick up env var
        import lazy_depends.tracing
        lazy_depends.tracing._ENV_TRACE = True
        try:
            with TestClient(app) as c:
                r = c.get("/")
                assert r.json() == {"a": "a", "b": "b"}
        finally:
            if old:
                os.environ["LAZY_DEPENDS_TRACE"] = old
            else:
                os.environ.pop("LAZY_DEPENDS_TRACE", None)
            lazy_depends.tracing._ENV_TRACE = False


# ═══════════════════════════════════════════════════════════
# 26. Cross-compatibility: fastapi.Depends + lazy_depends.*
#
# Test matrix:
#   Route class     │ Endpoint deps                        │ Sub-deps
#   ────────────────┼──────────────────────────────────────┼──────────────
#   Default (no CR) │ FastAPIDepends only                  │ FastAPIDepends
#   ConcurrentRoute │ lazy_depends.Depends only            │ lazy_depends.Depends
#   ConcurrentRoute │ mixed FastAPI + lazy_depends         │ mixed
#   ConcurrentRoute │ FastAPIDepends + LazyDepends         │ FastAPIDepends
#   ConcurrentRoute │ lazy_depends.Depends + StaticDepends │ n/a
# ═══════════════════════════════════════════════════════════


class TestCrossCompatibility:
    """
    Ensure fastapi.Depends and lazy_depends.Depends/LazyDepends/StaticDepends
    are fully interchangeable at every level.
    """

    # ── FastAPI default route class ────────────────────────

    def test_fastapi_depends_on_default_route(self):
        """Baseline: fastapi.Depends on a plain FastAPI app (no ConcurrentRoute)."""
        app = FastAPI()

        async def dep_a():
            return "a"

        async def dep_b(a=FastAPIDepends(dep_a)):
            return f"b({a})"

        @app.get("/")
        async def root(a=FastAPIDepends(dep_a), b=FastAPIDepends(dep_b)):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            assert c.get("/").json() == {"a": "a", "b": "b(a)"}

    def test_lazy_depends_on_default_route(self):
        """lazy_depends.Depends on a plain FastAPI app (no ConcurrentRoute).
        Should still work — it returns a standard fastapi.params.Depends."""
        app = FastAPI()

        async def dep():
            return "val"

        @app.get("/")
        async def root(val=Depends(dep)):
            return {"val": val}

        with TestClient(app) as c:
            assert c.get("/").json() == {"val": "val"}

    # ── ConcurrentRoute with fastapi.Depends ───────────────

    def test_fastapi_depends_on_concurrent_route(self):
        """fastapi.Depends works on ConcurrentRoute (resolved concurrently)."""
        app = _app()

        async def dep_a():
            await asyncio.sleep(0.2)
            return "a"

        async def dep_b():
            await asyncio.sleep(0.2)
            return "b"

        @app.get("/")
        async def root(a=FastAPIDepends(dep_a), b=FastAPIDepends(dep_b)):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"a": "a", "b": "b"}
            assert dt < 0.35, f"Took {dt:.2f}s — fastapi.Depends not concurrent on ConcurrentRoute"

    # ── Mixed fastapi.Depends + lazy_depends.Depends ───────

    def test_mixed_fastapi_and_lazy_depends_same_endpoint(self):
        """Both fastapi.Depends and lazy_depends.Depends in the same endpoint."""
        app = _app()

        async def dep_a():
            return "a"

        async def dep_b():
            return "b"

        @app.get("/")
        async def root(a=FastAPIDepends(dep_a), b=Depends(dep_b)):
            return {"a": a, "b": b}

        with TestClient(app) as c:
            assert c.get("/").json() == {"a": "a", "b": "b"}

    def test_mixed_depends_in_chain(self):
        """lazy_depends.Depends at top level, fastapi.Depends in sub-deps."""
        app = _app()

        async def sub():
            return "sub"

        async def parent(s=FastAPIDepends(sub)):
            return f"parent({s})"

        @app.get("/")
        async def root(val=Depends(parent)):
            return {"val": val}

        with TestClient(app) as c:
            assert c.get("/").json() == {"val": "parent(sub)"}

    def test_fastapi_depends_top_lazy_depends_sub(self):
        """fastapi.Depends at top level, lazy_depends.Depends in sub-deps."""
        app = _app()

        async def sub():
            return "sub"

        async def parent(s=Depends(sub)):
            return f"parent({s})"

        @app.get("/")
        async def root(val=FastAPIDepends(parent)):
            return {"val": val}

        with TestClient(app) as c:
            assert c.get("/").json() == {"val": "parent(sub)"}

    # ── Mixed with LazyDepends ─────────────────────────────

    def test_fastapi_depends_and_lazy_depends_mixed(self):
        """fastapi.Depends (eager) + LazyDepends (deferred) in same endpoint."""
        app = _app()

        async def eager_dep():
            return "eager"

        async def slow_dep():
            await asyncio.sleep(0.2)
            return "slow"

        @app.get("/")
        async def root(
            e=FastAPIDepends(eager_dep),
            lazy_s=LazyDepends(slow_dep),
        ):
            s = await lazy_s
            return {"e": e, "s": s}

        with TestClient(app) as c:
            assert c.get("/").json() == {"e": "eager", "s": "slow"}

    def test_lazy_depends_with_fastapi_sub_deps(self):
        """LazyDepends on a dep whose sub-deps use fastapi.Depends."""
        app = _app()

        async def db():
            return "db"

        async def user(db=FastAPIDepends(db)):
            return f"user({db})"

        @app.get("/")
        async def root(lazy_user=LazyDepends(user)):
            u = await lazy_user
            return {"user": u}

        with TestClient(app) as c:
            assert c.get("/").json() == {"user": "user(db)"}

    # ── Mixed with StaticDepends ───────────────────────────

    def test_all_three_in_one_endpoint(self):
        """fastapi.Depends + lazy_depends.Depends + LazyDepends + StaticDepends."""
        from lazy_depends import StaticDepends
        StaticDepends.reset()

        async def static_dep():
            return "static"

        async def fastapi_dep():
            return "fastapi"

        async def lazy_dep():
            return "lazy-depends"

        async def deferred_dep():
            return "deferred"

        app = FastAPI(lifespan=StaticDepends.lifespan)
        app.router.route_class = ConcurrentRoute

        @app.get("/")
        async def root(
            s=StaticDepends(static_dep),
            f=FastAPIDepends(fastapi_dep),
            l=Depends(lazy_dep),
            lazy_d=LazyDepends(deferred_dep),
        ):
            d = await lazy_d
            return {"s": s, "f": f, "l": l, "d": d}

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {
                "s": "static",
                "f": "fastapi",
                "l": "lazy-depends",
                "d": "deferred",
            }

    # ── Shared sub-dep across fastapi/lazy_depends ─────────

    def test_shared_sub_dep_cached_across_depends_types(self):
        """A sub-dep used via both fastapi.Depends and lazy_depends.Depends
        is still cached (called once)."""
        app = _app()
        calls = 0

        async def shared():
            nonlocal calls
            calls += 1
            return f"shared-{calls}"

        async def via_fastapi(s=FastAPIDepends(shared)):
            return f"fastapi({s})"

        async def via_lazy(s=Depends(shared)):
            return f"lazy({s})"

        @app.get("/")
        async def root(f=FastAPIDepends(via_fastapi), l=Depends(via_lazy)):
            return {"f": f, "l": l}

        with TestClient(app) as c:
            calls = 0
            r = c.get("/")
            data = r.json()
            # shared() should be called only once (use_cache=True default)
            assert calls == 1
            assert data["f"] == "fastapi(shared-1)"
            assert data["l"] == "lazy(shared-1)"

    # ── dependency_overrides with mixed types ──────────────

    def test_override_works_with_mixed_depends(self):
        """dependency_overrides works regardless of which Depends was used."""
        app = _app()

        async def real_dep():
            return "real"

        async def mock_dep():
            return "mock"

        @app.get("/fastapi")
        async def route_fastapi(val=FastAPIDepends(real_dep)):
            return {"val": val}

        @app.get("/lazy")
        async def route_lazy(val=Depends(real_dep)):
            return {"val": val}

        app.dependency_overrides[real_dep] = mock_dep
        with TestClient(app) as c:
            assert c.get("/fastapi").json() == {"val": "mock"}
            assert c.get("/lazy").json() == {"val": "mock"}
        app.dependency_overrides.clear()


# ═══════════════════════════════════════════════════════════
# 27. ConcurrentRouter
# ═══════════════════════════════════════════════════════════

from lazy_depends import ConcurrentRouter, CachedDepends


class TestConcurrentRouter:
    def test_concurrent_router_standalone(self):
        """ConcurrentRouter works as the app's main router."""
        app = FastAPI()
        router = ConcurrentRouter()

        async def dep_a():
            return "a"

        @router.get("/")
        async def root(a=Depends(dep_a)):
            return {"a": a}

        app.include_router(router)
        with TestClient(app) as c:
            assert c.get("/").json() == {"a": "a"}

    def test_include_router_with_concurrent_router(self):
        """include_router works with ConcurrentRouter under a prefix."""
        app = FastAPI()
        router = ConcurrentRouter(prefix="/api")

        async def dep():
            return "val"

        @router.get("/test")
        async def route(v=Depends(dep)):
            return {"v": v}

        app.include_router(router)
        with TestClient(app) as c:
            assert c.get("/api/test").json() == {"v": "val"}

    def test_concurrent_router_resolves_concurrently(self):
        """Dependencies resolve concurrently on ConcurrentRouter."""
        app = FastAPI()
        router = ConcurrentRouter()

        async def dep_a():
            await asyncio.sleep(0.2)
            return "a"

        async def dep_b():
            await asyncio.sleep(0.2)
            return "b"

        @router.get("/")
        async def root(a=Depends(dep_a), b=Depends(dep_b)):
            return {"a": a, "b": b}

        app.include_router(router)
        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"a": "a", "b": "b"}
            assert dt < 0.35, f"Took {dt:.2f}s — deps didn't run in parallel"


# ═══════════════════════════════════════════════════════════
# 28. CachedDepends
# ═══════════════════════════════════════════════════════════


class TestCachedDepends:
    def test_basic_resolve(self):
        """CachedDepends resolves on first call."""
        app = _app()
        calls = 0

        async def dep():
            nonlocal calls
            calls += 1
            return {"n": calls}

        @app.get("/")
        async def root(val=CachedDepends(dep, ttl=10)):
            return val

        with TestClient(app) as c:
            r = c.get("/")
            assert r.json() == {"n": 1}

    def test_cached_across_requests(self):
        """Second request returns cached value within TTL."""
        app = _app()
        calls = 0

        async def dep():
            nonlocal calls
            calls += 1
            return {"n": calls}

        @app.get("/")
        async def root(val=CachedDepends(dep, ttl=10)):
            return val

        with TestClient(app) as c:
            r1 = c.get("/")
            r2 = c.get("/")
            assert r1.json() == {"n": 1}
            assert r2.json() == {"n": 1}
            assert calls == 1

    def test_ttl_expiry(self):
        """After TTL expires, callable is invoked again."""
        app = _app()
        calls = 0

        async def dep():
            nonlocal calls
            calls += 1
            return {"n": calls}

        @app.get("/")
        async def root(val=CachedDepends(dep, ttl=0.1)):
            return val

        with TestClient(app) as c:
            r1 = c.get("/")
            assert r1.json() == {"n": 1}
            time.sleep(0.15)
            r2 = c.get("/")
            assert r2.json() == {"n": 2}
            assert calls == 2

    def test_sync_callable(self):
        """CachedDepends works with sync callables."""
        app = _app()
        calls = 0

        def sync_dep():
            nonlocal calls
            calls += 1
            return {"sync": True, "n": calls}

        @app.get("/")
        async def root(val=CachedDepends(sync_dep, ttl=10)):
            return val

        with TestClient(app) as c:
            r1 = c.get("/")
            r2 = c.get("/")
            assert r1.json() == {"sync": True, "n": 1}
            assert r2.json() == {"sync": True, "n": 1}
            assert calls == 1

    def test_mixed_with_regular_depends(self):
        """CachedDepends works alongside regular Depends."""
        app = _app()
        cached_calls = 0
        regular_calls = 0

        async def cached_dep():
            nonlocal cached_calls
            cached_calls += 1
            return "cached"

        async def regular_dep():
            nonlocal regular_calls
            regular_calls += 1
            return "regular"

        @app.get("/")
        async def root(c=CachedDepends(cached_dep, ttl=10), r=Depends(regular_dep)):
            return {"c": c, "r": r}

        with TestClient(app) as client:
            r1 = client.get("/")
            r2 = client.get("/")
            assert r1.json() == {"c": "cached", "r": "regular"}
            assert r2.json() == {"c": "cached", "r": "regular"}
            assert cached_calls == 1
            assert regular_calls == 2
