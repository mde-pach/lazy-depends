# lazy-depends

Concurrent and lazy async dependency injection for FastAPI.

`lazy-depends` is a drop-in enhancement for FastAPI's dependency system. It resolves independent dependencies in parallel, supports deferred background resolution, startup-resolved constants, and TTL-based caching — all while staying fully compatible with `fastapi.Depends`.

## Install

```
pip install lazy-depends
```

## Quick start

```python
from fastapi import FastAPI
from lazy_depends import Depends, ConcurrentRoute

app = FastAPI()
app.router.route_class = ConcurrentRoute
```

Two changes. Your existing dep functions and route handlers stay exactly the same. Dependencies at the same level of the graph now resolve concurrently instead of sequentially.

## When to use what

| You need... | Use | Resolved | Overhead |
|---|---|---|---|
| Standard dep, but concurrent | `Depends` | Before endpoint, in parallel | Graph lookup (cached) |
| Dep that can wait | `LazyDepends` | In background while endpoint runs | Task creation |
| Value that never changes | `StaticDepends` | Once at startup | Dict lookup |
| Value that changes slowly | `CachedDepends` | Once per TTL window | Timestamp check |

All four return standard `fastapi.params.Depends` instances and are fully interchangeable with `fastapi.Depends` — you can mix them freely in the same endpoint.

## Concurrent resolution with `Depends`

FastAPI resolves dependencies depth-first, one after another. If a route has 4 independent deps that each take 5ms, FastAPI does `5+5+5+5 = 20ms`. lazy-depends does `max(5,5,5,5) = 5ms`.

```
Sequential (FastAPI default):     Concurrent (lazy-depends):

  get_user ────── 5ms               get_user ─── 5ms  ─┐
  get_tasks ───── 5ms               get_tasks ── 5ms   │ parallel
  get_perms ───── 5ms               get_perms ── 5ms   │
  get_activity ── 5ms               get_activity 5ms  ─┘
  Total: ~20ms                      Total: ~5ms
```

Only the import changes:

```python
from lazy_depends import Depends, ConcurrentRoute   # instead of: from fastapi import Depends

app.router.route_class = ConcurrentRoute             # one extra line

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

Dependencies that depend on each other still run in the correct order — only independent branches run in parallel.

### ASAP scheduling

lazy-depends uses ASAP (As-Soon-As-Possible) scheduling instead of level-by-level resolution. Each dependency starts the moment its own sub-dependencies are ready, without waiting for unrelated deps at the same depth:

```
Level-by-level (slower):              ASAP (lazy-depends):

  get_db ─┐                            get_db ─┐
  get_cache┘ wait for both              get_cache┘
  get_user ──────┐ wait for both        get_user ──────┐
  get_config ────┘                      get_config ─┐  │
                                        get_perms ──┘  │ doesn't wait for get_user
  get_tasks ─┐ wait for both            get_tasks ─────┘
  get_perms ─┘
```

`get_perms` depends on `get_config`, not `get_user`. ASAP lets it start as soon as `get_config` finishes — it doesn't wait for the slower `get_user` to complete.

### Compatibility

Fully compatible with all FastAPI features:

- `dependency_overrides` for testing
- Generator (`yield`) dependencies with proper cleanup
- `use_cache=True/False`
- `Header()`, `Query()`, `Cookie()`, `Security()` params
- `BackgroundTasks`, `Response` injection
- Sync dependencies (run in threadpool)
- Class-based dependencies (`__call__`)
- WebSocket routes (use FastAPI's default resolver)

### Interop with `fastapi.Depends`

`lazy_depends.Depends` and `fastapi.Depends` are fully interchangeable. You can mix them in the same endpoint, in sub-dependency chains, and across routes:

```python
from fastapi import Depends as FastAPIDepends
from lazy_depends import Depends

async def sub_dep():
    return "sub"

async def parent(s=FastAPIDepends(sub_dep)):   # fastapi.Depends in sub-dep
    return f"parent({s})"

@app.get("/")
async def root(val=Depends(parent)):            # lazy_depends.Depends at top
    return {"val": val}
```

Shared sub-dependencies are cached correctly regardless of which `Depends` variant was used.

## Lazy resolution with `LazyDepends`

`Depends` resolves everything before the endpoint starts. `LazyDepends` lets you start the endpoint immediately while slow dependencies resolve in the background.

```python
from lazy_depends import Depends, LazyDepends

@app.get("/")
async def root(
    db=Depends(get_db),                  # eager: resolved before endpoint starts
    lazy_user=LazyDepends(get_user),     # lazy: resolves in background
):
    await db.execute("SELECT 1")         # db is ready
    await asyncio.sleep(0.5)             # get_user keeps resolving in parallel

    user = await lazy_user               # blocks only for remaining time
    print(user["name"])                  # real dict, no proxy
```

Without `LazyDepends`: `get_user(1s) + db.execute + sleep(0.5s) = ~1.5s`
With `LazyDepends`: `get_user` runs in background during the 0.5s sleep = **~1.0s**

### No proxy, real values

`await lazy_user` returns the actual resolved value — a real `dict`, not a wrapper. `isinstance`, `json.dumps`, Pydantic serialization all work normally.

### Convention

Prefix lazy parameters with `lazy_` to make it obvious which deps need an `await`:

```python
@app.get("/dashboard")
async def dashboard(
    user=Depends(get_current_user),              # ready to use
    tasks=Depends(get_my_tasks),                 # ready to use
    lazy_analytics=LazyDepends(get_analytics),   # needs await
):
    summary = build_summary(user, tasks)
    analytics = await lazy_analytics
    return {**summary, "analytics": analytics}
```

### Typing

Type checkers infer `Lazy[T]` from the dependency's return type:

```python
async def get_user() -> dict: ...

@app.get("/")
async def root(lazy_user=LazyDepends(get_user)):
    # lazy_user: Lazy[dict]
    user = await lazy_user
    # user: dict
```

## Static dependencies with `StaticDepends`

For values that never change after startup — config, feature flags, ML models, connection pools:

```python
from lazy_depends import StaticDepends

async def load_config():
    return await fetch_from_db()

app = FastAPI(lifespan=StaticDepends.lifespan)
app.router.route_class = ConcurrentRoute

@app.get("/")
async def root(config=StaticDepends(load_config)):
    return {"max_tasks": config["max_tasks"]}
```

All static deps resolve concurrently during startup. At request time, the injected value is the real `T` — no wrapper, no `await`, zero overhead.

To compose with your own lifespan:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    await StaticDepends.resolve()
    yield

app = FastAPI(lifespan=lifespan)
```

Not suitable for generator (yield) deps.

## Cached dependencies with `CachedDepends`

For data that changes but not on every request — feature flags, permissions, rate limits:

```python
from lazy_depends import CachedDepends

@app.get("/")
async def root(flags=CachedDepends(get_feature_flags, ttl=30)):
    ...  # refreshes every 30 seconds, shared across requests
```

After the TTL expires, the next request re-invokes the callable. Within the TTL, all requests get the cached value instantly. Works with both sync and async callables.

## `ConcurrentRouter`

Drop-in `APIRouter` subclass — no need to set `route_class` manually:

```python
from lazy_depends import ConcurrentRouter

router = ConcurrentRouter(prefix="/api")

@router.get("/dashboard")
async def dashboard(user=Depends(get_user), tasks=Depends(get_tasks)):
    ...

app.include_router(router)
```

## Tracing

Enable dependency resolution tracing to see what resolved, how long it took, and what ran in parallel:

```bash
LAZY_DEPENDS_TRACE=1 uvicorn myapp:app
```

Or via Python logging:

```python
import logging
logging.getLogger("lazy_depends").setLevel(logging.DEBUG)
```

Output:

```
GET /dashboard deps resolved in 14.2ms
  ├─ get_db ·····················    0.1ms
  ├─ get_user ···················    5.1ms  (after: get_db)
  ├─ get_tasks ··················    4.8ms ─┐  (after: get_user, get_db)
  ├─ get_activity ···············    4.5ms  │  (after: get_user, get_db)
  └─ get_perms ··················    5.0ms ─┘  (after: get_user, get_db)
```

Deps with `─┐`/`─┘` markers ran concurrently. Use this to identify slow deps that are candidates for `LazyDepends`.

## Testing

lazy-depends works with `dependency_overrides` exactly like FastAPI:

```python
from fastapi.testclient import TestClient

app.dependency_overrides[get_db] = lambda: mock_db
with TestClient(app) as client:
    response = client.get("/")
```

For `StaticDepends`, call `reset()` between tests to clear cached values:

```python
from lazy_depends import StaticDepends

def setup_function():
    StaticDepends.reset()
```

For `CachedDepends`, use short TTLs in tests or create separate instances per test to avoid cross-test pollution.

## Migration from FastAPI

1. Change the import:
   ```python
   # Before
   from fastapi import Depends
   # After
   from lazy_depends import Depends
   ```

2. Set the route class (pick one):
   ```python
   # Option A: global
   app.router.route_class = ConcurrentRoute

   # Option B: per-router
   router = ConcurrentRouter(prefix="/api")
   ```

3. That's it. All existing code works unchanged with concurrent resolution.

4. Optionally, add `LazyDepends` / `StaticDepends` / `CachedDepends` where it makes sense.

## Examples

Working apps in the `examples/` directory:

```
examples/
  domain.py                  -- shared async domain logic (aiosqlite)
  example_traditional.py     -- standard FastAPI Depends (sequential)
  example_concurrent.py      -- lazy-depends drop-in (concurrent)
  example_lazy_depends.py    -- LazyDepends demo
  benchmark.py               -- performance comparison
```

Run the benchmark:

```bash
cd examples
pip install aiosqlite httpx asgi-lifespan
python benchmark.py
```

## API reference

### `Depends(dependency, *, use_cache=True)`

Drop-in replacement for `fastapi.Depends`. Concurrency handled by `ConcurrentRoute`.

### `LazyDepends(dependency, *, use_cache=True)`

Background resolution. Returns `Lazy[T]` that must be awaited to get the real value.

### `StaticDepends(dependency)`

Resolved once at startup via lifespan. Returns real `T` at request time.

| Method | Purpose |
|---|---|
| `StaticDepends.lifespan` | Ready-to-use lifespan for `FastAPI(lifespan=...)` |
| `await StaticDepends.resolve()` | Resolve all static deps (for custom lifespans) |
| `StaticDepends.reset()` | Clear all values (for testing) |

### `CachedDepends(dependency, *, ttl: float)`

TTL-based caching across requests. Re-invokes after expiry.

### `ConcurrentRoute`

Route class enabling concurrent + lazy resolution.

### `ConcurrentRouter(**kwargs)`

`APIRouter` subclass with `ConcurrentRoute` as default.

### `Lazy[T]`

Thin awaitable wrapping a background task. No proxy.

| Operation | Result |
|---|---|
| `result = await lazy` | Blocks until resolved, returns `T` |
| `repr(lazy)` | `<Lazy pending>` or `<Lazy resolved=...>` |

## Requirements

- Python >= 3.11
- FastAPI >= 0.100.0

## License

MIT
