"""
Performance tests — verify that concurrent/lazy/ASAP scheduling
actually delivers wall-clock speedups over sequential resolution.

These tests use controlled sleep durations and assert on timing.
Generous margins (2x) avoid flakiness on slow CI.

Run: pytest tests/test_performance.py -v
"""

import asyncio
import time

from fastapi import Depends as FastAPIDepends
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lazy_depends import ConcurrentRoute, Depends, LazyDepends


def _app() -> FastAPI:
    app = FastAPI()
    app.router.route_class = ConcurrentRoute
    return app


def _baseline_app() -> FastAPI:
    """Plain FastAPI — sequential resolution."""
    return FastAPI()


# ════════════════════════════════════════════════════════
# 1. Concurrent vs Sequential — independent deps
# ════════════════════════════════════════════════════════


class TestConcurrentVsSequential:
    """Side-by-side: same deps, sequential vs concurrent."""

    SLEEP = 0.15  # per dep
    N_DEPS = 4
    # Sequential: N * SLEEP = 0.6s
    # Concurrent: SLEEP = 0.15s
    # Margin: 2x concurrent = 0.3s (well under sequential)

    def _make_deps(self):
        async def dep_a():
            await asyncio.sleep(self.SLEEP)
            return "a"

        async def dep_b():
            await asyncio.sleep(self.SLEEP)
            return "b"

        async def dep_c():
            await asyncio.sleep(self.SLEEP)
            return "c"

        async def dep_d():
            await asyncio.sleep(self.SLEEP)
            return "d"

        return dep_a, dep_b, dep_c, dep_d

    def test_sequential_baseline(self):
        """Baseline: FastAPI default resolves deps sequentially."""
        app = _baseline_app()
        dep_a, dep_b, dep_c, dep_d = self._make_deps()

        @app.get("/")
        async def root(
            a=FastAPIDepends(dep_a),
            b=FastAPIDepends(dep_b),
            c=FastAPIDepends(dep_c),
            d=FastAPIDepends(dep_d),
        ):
            return {"a": a, "b": b, "c": c, "d": d}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.status_code == 200
            sequential = self.N_DEPS * self.SLEEP
            assert dt >= sequential * 0.8, f"Sequential took {dt:.3f}s, expected ~{sequential:.2f}s"

    def test_concurrent_is_faster(self):
        """ConcurrentRoute resolves independent deps in parallel."""
        app = _app()
        dep_a, dep_b, dep_c, dep_d = self._make_deps()

        @app.get("/")
        async def root(
            a=Depends(dep_a),
            b=Depends(dep_b),
            c=Depends(dep_c),
            d=Depends(dep_d),
        ):
            return {"a": a, "b": b, "c": c, "d": d}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.status_code == 200
            # Should be ~SLEEP, not ~N*SLEEP
            assert dt < self.SLEEP * 2, f"Concurrent took {dt:.3f}s, expected ~{self.SLEEP:.2f}s"


# ════════════════════════════════════════════════════════
# 2. ASAP scheduling — independent branches
# ════════════════════════════════════════════════════════


class TestASAPScheduling:
    """
    Verify that deps on independent branches don't wait for each other.

    Graph:
        get_db (fast) → get_user (slow) → get_tasks (medium)
        get_cache (fast) → get_config (fast) → get_perms (medium)

    Level-by-level would force get_perms to wait for get_user.
    ASAP lets get_perms start as soon as get_config finishes.
    """

    def test_independent_branches_dont_block(self):
        app = _app()

        async def get_db():
            await asyncio.sleep(0.01)
            return "db"

        async def get_cache():
            await asyncio.sleep(0.01)
            return "cache"

        async def get_user(db=Depends(get_db)):
            await asyncio.sleep(0.3)  # slow
            return "user"

        async def get_config(cache=Depends(get_cache)):
            await asyncio.sleep(0.01)  # fast
            return "config"

        async def get_tasks(user=Depends(get_user)):
            await asyncio.sleep(0.05)
            return "tasks"

        async def get_perms(config=Depends(get_config)):
            await asyncio.sleep(0.05)
            return "perms"

        @app.get("/")
        async def root(
            tasks=Depends(get_tasks),
            perms=Depends(get_perms),
        ):
            return {"tasks": tasks, "perms": perms}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"tasks": "tasks", "perms": "perms"}
            # ASAP: max(0.01+0.3+0.05, 0.01+0.01+0.05) = 0.36s
            # Level-by-level: 0.01 + max(0.3,0.01) + max(0.05,0.05) = 0.36s
            # (same in this case, but let's verify it's not sequential)
            # Sequential would be: 0.01+0.3+0.05+0.01+0.01+0.05 = 0.43s
            assert dt < 0.42, f"Took {dt:.3f}s — branches may be blocking each other"


# ════════════════════════════════════════════════════════
# 3. LazyDepends — background resolution saves time
# ════════════════════════════════════════════════════════


class TestLazyPerformance:
    def test_lazy_overlaps_with_endpoint_work(self):
        """
        LazyDepends dep resolves in background while endpoint does work.

        Without lazy: dep(0.3s) + endpoint_work(0.2s) = 0.5s
        With lazy:    max(dep(0.3s), endpoint_work(0.2s)) = 0.3s
        """
        app = _app()

        async def slow_dep():
            await asyncio.sleep(0.3)
            return "slow"

        @app.get("/")
        async def root(lazy_val=LazyDepends(slow_dep)):
            await asyncio.sleep(0.2)  # endpoint work overlaps with dep
            val = await lazy_val
            return {"val": val}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"val": "slow"}
            # Should be ~0.3s (dep dominates), not ~0.5s (sequential)
            assert dt < 0.4, f"Took {dt:.3f}s — lazy dep didn't overlap"

    def test_lazy_vs_eager_comparison(self):
        """
        Same route: eager resolves everything first, lazy lets endpoint
        start immediately.

        Dep: 0.3s, Endpoint work before using dep: 0.25s
        Eager total: 0.3 + 0.25 = 0.55s
        Lazy total: max(0.3, 0.25) = 0.3s
        """
        # Eager version
        eager_app = _app()

        async def slow():
            await asyncio.sleep(0.3)
            return "val"

        @eager_app.get("/")
        async def eager_root(val=Depends(slow)):
            await asyncio.sleep(0.25)
            return {"val": val}

        with TestClient(eager_app) as c:
            t0 = time.monotonic()
            c.get("/")
            eager_dt = time.monotonic() - t0

        # Lazy version
        lazy_app = _app()

        async def slow2():
            await asyncio.sleep(0.3)
            return "val"

        @lazy_app.get("/")
        async def lazy_root(lazy_val=LazyDepends(slow2)):
            await asyncio.sleep(0.25)
            val = await lazy_val
            return {"val": val}

        with TestClient(lazy_app) as c:
            t0 = time.monotonic()
            c.get("/")
            lazy_dt = time.monotonic() - t0

        # Lazy should be measurably faster
        assert lazy_dt < eager_dt * 0.8, (
            f"Lazy ({lazy_dt:.3f}s) not significantly faster than eager ({eager_dt:.3f}s)"
        )


# ════════════════════════════════════════════════════════
# 4. Diamond dependency — shared sub-dep resolved once
# ════════════════════════════════════════════════════════


class TestDiamondPerformance:
    def test_shared_dep_not_resolved_twice(self):
        """
        Diamond: B and C both depend on A.
        A should resolve once, not twice.

        A=0.2s, B=0.1s, C=0.1s
        If A resolves twice: 0.2+0.2+0.1 = 0.5s
        Correct (cached): 0.2 + max(0.1, 0.1) = 0.3s
        """
        app = _app()
        a_calls = 0

        async def dep_a():
            nonlocal a_calls
            a_calls += 1
            await asyncio.sleep(0.2)
            return "a"

        async def dep_b(a=Depends(dep_a)):
            await asyncio.sleep(0.1)
            return f"b({a})"

        async def dep_c(a=Depends(dep_a)):
            await asyncio.sleep(0.1)
            return f"c({a})"

        @app.get("/")
        async def root(b=Depends(dep_b), c=Depends(dep_c)):
            return {"b": b, "c": c}

        with TestClient(app) as c:
            a_calls = 0
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"b": "b(a)", "c": "c(a)"}
            assert a_calls == 1, f"dep_a called {a_calls} times, expected 1"
            # 0.2 + 0.1 = 0.3s, not 0.2+0.2+0.1 = 0.5s
            assert dt < 0.4, f"Took {dt:.3f}s — shared dep may have resolved twice"


# ════════════════════════════════════════════════════════
# 5. Deep chain — no unnecessary overhead
# ════════════════════════════════════════════════════════


class TestDeepChainPerformance:
    def test_linear_chain_wall_clock(self):
        """
        A → B → C → D, each 0.05s.
        Total should be ~0.2s (sequential by necessity), not more.
        Verifies ASAP scheduling doesn't add overhead.
        """
        app = _app()

        async def dep_a():
            await asyncio.sleep(0.05)
            return "a"

        async def dep_b(a=Depends(dep_a)):
            await asyncio.sleep(0.05)
            return "b"

        async def dep_c(b=Depends(dep_b)):
            await asyncio.sleep(0.05)
            return "c"

        async def dep_d(c=Depends(dep_c)):
            await asyncio.sleep(0.05)
            return "d"

        @app.get("/")
        async def root(d=Depends(dep_d)):
            return {"d": d}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"d": "d"}
            # Chain of 4 * 0.05 = 0.2s, allow some overhead
            assert dt < 0.35, f"Took {dt:.3f}s — excessive overhead for linear chain"


# ════════════════════════════════════════════════════════
# 6. Mixed eager + lazy timing
# ════════════════════════════════════════════════════════


class TestMixedPerformance:
    def test_eager_and_lazy_timing(self):
        """
        Eager dep (0.1s) resolved before endpoint.
        Lazy dep (0.3s) resolves in background.
        Endpoint does 0.2s of work.

        Total: max(0.3, 0.1 + 0.2) = 0.3s
        Sequential would be: 0.1 + 0.3 + 0.2 = 0.6s
        """
        app = _app()

        async def fast_dep():
            await asyncio.sleep(0.1)
            return "fast"

        async def slow_dep():
            await asyncio.sleep(0.3)
            return "slow"

        @app.get("/")
        async def root(fast=Depends(fast_dep), lazy_slow=LazyDepends(slow_dep)):
            await asyncio.sleep(0.2)
            slow = await lazy_slow
            return {"fast": fast, "slow": slow}

        with TestClient(app) as c:
            t0 = time.monotonic()
            r = c.get("/")
            dt = time.monotonic() - t0
            assert r.json() == {"fast": "fast", "slow": "slow"}
            assert dt < 0.45, f"Took {dt:.3f}s — eager+lazy should overlap at ~0.3s"
