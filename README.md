# lazy-depends

Concurrent async dependency injection for FastAPI — drop-in replacement for `Depends()`.

## Quick Start (Drop-in)

Two changes to your existing FastAPI app:

```python
# 1. Change the import
from lazy_depends import Depends, ConcurrentRoute  # instead of: from fastapi import Depends

# 2. Set the route class
app.router.route_class = ConcurrentRoute
```

That's it. All your existing dep functions and routes stay exactly the same. Dependencies at the same level of the graph now resolve concurrently instead of sequentially.

## Install

```
pip install lazy-depends
```

## How it works

FastAPI resolves dependencies depth-first, one after another. If a route has 4 independent deps that each take ~5ms, FastAPI does `5+5+5+5 = 20ms`. lazy-depends does `max(5,5,5,5) = 5ms`.

```
Traditional (sequential):      Concurrent (lazy-depends):
  get_db ──────── 0.1ms          get_db ──────── 0.1ms
  get_user ────── 5ms            ┌─ get_user ─── 5ms  ─┐
  get_tasks ───── 5ms            │  get_tasks ── 5ms   │ all parallel
  get_perms ───── 5ms            │  get_perms ── 5ms   │
  get_activity ── 5ms            └─ get_activity 5ms  ─┘
  Total: ~20ms                   Total: ~5ms
```

## Two APIs

### 1. Drop-in `Depends` (recommended)

Same interface as `fastapi.Depends`. Concurrency is automatic via `ConcurrentRoute`.

```python
from lazy_depends import Depends, ConcurrentRoute
from fastapi import FastAPI

app = FastAPI()
app.router.route_class = ConcurrentRoute

async def get_db(): ...
async def get_user(request: Request, db=Depends(get_db)): ...
async def get_tasks(user=Depends(get_user), db=Depends(get_db)): ...

@app.get("/dashboard")
async def dashboard(
    user=Depends(get_user),
    tasks=Depends(get_tasks),
):
    return {"user": user, "tasks": tasks}
```

Fully compatible with:
- `dependency_overrides` (testing)
- Generator (`yield`) deps
- `use_cache=True/False`
- `Header()`, `Query()`, `Cookie()`, `Security()` params
- `BackgroundTasks`, `Response` injection

### 2. Container-based

Explicit registration with middleware-driven concurrency. Different paradigm — all request deps start resolving the moment the request arrives.

```python
from lazy_depends import Container
from fastapi import Depends, FastAPI

container = Container()

@container.request_lazy("current_user")
async def resolve_user(ctx, ctr):
    db = await ctr.get("db")
    return await fetch_user(db, ctx["token"])

app = FastAPI(lifespan=container.lifespan())
app.middleware("http")(container.middleware(
    lambda req: {"token": req.headers.get("authorization", "").removeprefix("Bearer ")}
))

@app.get("/")
async def root(user=Depends(container.request_depends("current_user"))):
    return {"user": user}
```

## Examples

See the `examples/` directory for complete working apps:

```
examples/
  domain.py                — shared domain logic (aiosqlite queries)
  example_traditional.py   — standard FastAPI Depends() (sequential)
  example_concurrent.py    — drop-in lazy_depends (concurrent)
  example_lazy.py          — container-based (concurrent)
  benchmark.py             — performance comparison
```

### Benchmark results (5ms network latency per query)

```
Dashboard route (5 deps):     ~24ms → ~12ms   (49% faster)
Total (7 requests):           ~145ms → ~98ms  (32% faster)
```
