# lazy-depends

Concurrent and lazy async dependency injection for FastAPI.

**`Depends`** resolves independent dependencies in parallel instead of one-by-one.
**`LazyDepends`** goes further: the endpoint starts immediately while dependencies resolve in the background.

## Install

```
pip install lazy-depends
```

## Quick start

```python
from fastapi import FastAPI
from lazy_depends import Depends, LazyDepends, ConcurrentRoute

app = FastAPI()
app.router.route_class = ConcurrentRoute
```

That's it. All your existing `Depends()` chains now resolve concurrently, and you can opt individual deps into lazy background resolution with `LazyDepends`.

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

Only the import changes. Your dependency functions and route handlers stay exactly the same:

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

Dependencies that depend on each other still run in the correct order. Only independent branches run in parallel.

### Compatibility

`Depends` is fully compatible with all FastAPI features:

- `dependency_overrides` for testing
- Generator (`yield`) dependencies
- `use_cache=True/False`
- `Header()`, `Query()`, `Cookie()`, `Security()` params
- `BackgroundTasks`, `Response` injection

## Lazy resolution with `LazyDepends`

`Depends` resolves everything before the endpoint starts. `LazyDepends` lets you start the endpoint immediately while slow dependencies resolve in the background.

```python
from lazy_depends import Depends, LazyDepends, ConcurrentRoute

app.router.route_class = ConcurrentRoute

async def get_db():
    return await connect()

async def get_user():
    await asyncio.sleep(1)  # slow external call
    return {"id": 1, "name": "Alice"}

@app.get("/")
async def root(
    db=Depends(get_db),                  # eager: resolved before endpoint starts
    lazy_user=LazyDepends(get_user),     # lazy: resolves in background
):
    await db.execute("SELECT 1")         # db is ready
    await asyncio.sleep(0.5)             # get_user keeps resolving in parallel

    user = await lazy_user               # blocks only for remaining ~0.5s
    print(user["name"])                  # real dict, no proxy
```

Without `LazyDepends`: `get_user(1s) + db.execute + sleep(0.5s) = ~1.5s`
With `LazyDepends`: `get_user` runs in background during the 0.5s sleep = **~1.0s**

### How it works

`LazyDepends` injects a `Lazy[T]` — a thin awaitable wrapping the background task. `await` it to get the real value:

```python
user = await lazy_user          # user is a real dict
print(user["name"])             # no proxy, no magic
```

There is no proxy layer. `await` returns the actual resolved value. This means `isinstance`, `json.dumps`, Pydantic serialization — everything works exactly as with a regular `Depends`.

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

    analytics = await lazy_analytics             # resolve when needed
    return {**summary, "analytics": analytics}
```

### Typing

`LazyDepends` is typed with overloads so type checkers infer `Lazy[T]` from the dependency's return type:

```python
async def get_user() -> dict: ...

@app.get("/")
async def root(lazy_user=LazyDepends(get_user)):
    # type checker infers: lazy_user: Lazy[dict]
    user = await lazy_user
    # type checker infers: user: dict
```

## Static dependencies with `StaticDepends`

For values that never change after startup — config, feature flags, ML models, connection pools — use `StaticDepends`. Dependencies are resolved once during the app lifespan (at startup) and injected directly at request time with zero overhead.

```python
from lazy_depends import StaticDepends, ConcurrentRoute

async def load_config():
    return await fetch_from_db()

async def load_model():
    return await download_ml_model()

app = FastAPI(lifespan=StaticDepends.lifespan)
app.router.route_class = ConcurrentRoute

@app.get("/")
async def root(
    config=StaticDepends(load_config),    # resolved at startup
    model=StaticDepends(load_model),      # resolved at startup (concurrently)
):
    return {"max_tasks": config["max_tasks"]}
```

All static deps resolve concurrently during startup. At request time, the injected value is the real `T` — no wrapper, no `await`, no overhead.

To compose with your own lifespan:

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    await StaticDepends.resolve()   # resolve all static deps
    db = await connect_db()         # your own startup logic
    yield
    await db.close()                # your own shutdown logic

app = FastAPI(lifespan=lifespan)
```

Not suitable for generator (yield) deps — use `Depends` for those.

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

### Benchmark results (5ms simulated network latency per query)

```
Dashboard route (5 deps):   ~24ms -> ~12ms   (49% faster)
Total (7 requests):         ~145ms -> ~98ms  (32% faster)
```

Run it yourself:

```bash
cd examples
pip install aiosqlite httpx asgi-lifespan
python benchmark.py
```

## Tracing

Enable dependency resolution tracing to see a waterfall of what resolved, how long it took, and what ran in parallel:

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

Deps on the same level with `─┐`/`─┘` markers ran concurrently. The `(after: ...)` shows which deps they waited on. Use this to identify slow deps that are candidates for `LazyDepends`.

## Cached dependencies with `CachedDepends`

For data that changes but not on every request — feature flags, permissions, rate limits — use `CachedDepends` with a TTL:

```python
from lazy_depends import CachedDepends

@app.get("/")
async def root(flags=CachedDepends(get_feature_flags, ttl=30)):
    ...  # refreshes every 30 seconds, shared across requests
```

After the TTL expires, the next request re-invokes the callable. Within the TTL, all requests get the cached value instantly.

## `ConcurrentRouter`

Drop-in `APIRouter` subclass — no need to set `route_class` manually. Works naturally with `include_router`:

```python
from lazy_depends import ConcurrentRouter

router = ConcurrentRouter(prefix="/api")

@router.get("/dashboard")
async def dashboard(user=Depends(get_user), tasks=Depends(get_tasks)):
    ...

app.include_router(router)
```

## API reference

### `Depends(dependency, *, use_cache=True)`

Drop-in replacement for `fastapi.Depends`. Returns a standard `fastapi.params.Depends` instance. The concurrency is handled entirely by `ConcurrentRoute`.

### `LazyDepends(dependency, *, use_cache=True)`

Marks a dependency for lazy background resolution. The endpoint starts immediately and receives a `Lazy[T]` that must be awaited to get the real value.

### `StaticDepends(dependency)`

Dependency resolved once at startup via the app lifespan. The injected value is the real `T` — zero overhead at request time.

| Class method | Purpose |
|---|---|
| `StaticDepends.lifespan` | Ready-to-use lifespan for `FastAPI(lifespan=...)` |
| `await StaticDepends.resolve()` | Resolve all static deps (for custom lifespans) |
| `StaticDepends.reset()` | Clear all values (for testing) |

### `CachedDepends(dependency, *, ttl: float)`

Caches the dependency result for `ttl` seconds across requests. After expiry, the next request re-invokes the callable.

### `ConcurrentRoute`

Route class that enables concurrent and lazy resolution:

```python
app.router.route_class = ConcurrentRoute
```

### `ConcurrentRouter`

`APIRouter` subclass with `ConcurrentRoute` as default. Use with `include_router`:

```python
router = ConcurrentRouter(prefix="/api")
app.include_router(router)
```

### `Lazy[T]`

Thin awaitable wrapping a background `asyncio.Task`. No proxy, no magic.

| Operation | Result |
|---|---|
| `result = await lazy` | Blocks until resolved, returns `T` |
| `repr(lazy)` | `<Lazy pending>` or `<Lazy resolved=...>` |

## Requirements

- Python >= 3.11
- FastAPI >= 0.100.0

## License

MIT
