# lazy-depends

Drop-in dependency injection for FastAPI that resolves dependencies concurrently.

```
pip install lazy-depends
```

## Setup

```python
from fastapi import FastAPI
from lazy_depends import Depends, ConcurrentRoute

app = FastAPI()
app.router.route_class = ConcurrentRoute
```

Change the import, set the route class. Everything else stays the same.

## What it does

FastAPI resolves dependencies one after another. If your route has 4 independent deps that each take 5ms, that's `5+5+5+5 = 20ms`. lazy-depends runs them in parallel: `max(5,5,5,5) = 5ms`.

```
Sequential (FastAPI):             Concurrent (lazy-depends):

  get_user ────── 5ms               get_user ─── 5ms  ─┐
  get_tasks ───── 5ms               get_tasks ── 5ms   │ parallel
  get_perms ───── 5ms               get_perms ── 5ms   │
  get_activity ── 5ms               get_activity 5ms  ─┘
  Total: ~20ms                      Total: ~5ms
```

Dependencies that depend on each other still run in the correct order. Independent branches run in parallel, and each dep starts the moment its own sub-deps are ready (ASAP scheduling) — it never waits for unrelated deps at the same depth.

## Four ways to declare dependencies

### `Depends` — concurrent, resolved before the endpoint

```python
from lazy_depends import Depends

@app.get("/")
async def root(user=Depends(get_user), tasks=Depends(get_tasks)):
    return {"user": user, "tasks": tasks}
```

Same as `fastapi.Depends`, but independent deps resolve in parallel. Fully compatible with `dependency_overrides`, generators, `use_cache`, `Header()`, `Query()`, `Security()`, `BackgroundTasks`, sync callables, and class-based deps.

`lazy_depends.Depends` and `fastapi.Depends` are interchangeable — you can mix them freely in the same endpoint and sub-dependency chains.

### `LazyDepends` — resolved in the background while the endpoint runs

```python
from lazy_depends import Depends, LazyDepends

@app.get("/")
async def root(
    db=Depends(get_db),                  # ready before endpoint starts
    lazy_user=LazyDepends(get_user),     # starts resolving immediately, in background
):
    await db.execute("SELECT 1")
    await asyncio.sleep(0.5)             # get_user keeps resolving in parallel

    user = await lazy_user               # blocks only for remaining time
    return {"user": user}
```

`await lazy_user` returns the real value — a plain `dict`, not a wrapper. `isinstance`, `json.dumps`, Pydantic serialization all work normally.

Prefix lazy params with `lazy_` to make it clear which deps need `await`.

`LazyDepends` works at any level — endpoints and sub-dependencies:

```python
async def get_dashboard(
    db=Depends(get_db),
    lazy_user=LazyDepends(get_user),     # lazy inside a sub-dep
):
    result = await db.execute("SELECT ...")  # user resolves in background
    user = await lazy_user                   # ready by now
    return {"data": result, "user": user}

@app.get("/")
async def root(dashboard=Depends(get_dashboard)):
    return dashboard
```

All tasks start immediately via ASAP scheduling. When a sub-dep declares `LazyDepends`, the child task is already running — the sub-dep receives a `Lazy[T]` wrapper and can `await` it after doing other work.

### `StaticDepends` — resolved once at startup

```python
from lazy_depends import StaticDepends

async def load_config():
    return await fetch_from_db()

app = FastAPI(lifespan=StaticDepends.lifespan)
app.router.route_class = ConcurrentRoute

@app.get("/")
async def root(config=StaticDepends(load_config)):
    return config["max_tasks"]
```

All static deps resolve concurrently during startup. At request time, it's a dict lookup — zero overhead. To compose with your own lifespan, call `await StaticDepends.resolve()` during startup instead.

### `CachedDepends` — resolved once per TTL window

```python
from lazy_depends import CachedDepends

@app.get("/")
async def root(flags=CachedDepends(get_feature_flags, ttl=30)):
    ...
```

The callable runs once, then the result is reused for `ttl` seconds. After expiry, the next request re-invokes it.

### Choosing the right one

| Situation | Use | When it resolves |
|---|---|---|
| Standard dependency | `Depends` | Before endpoint, concurrently |
| Slow dep you don't need right away | `LazyDepends` | Background, `await` when needed |
| Config, ML models, connection pools | `StaticDepends` | Once at startup |
| Feature flags, permissions | `CachedDepends` | Once per TTL window |

## `ConcurrentRouter`

`APIRouter` subclass with concurrent resolution built in. Works with `include_router`:

```python
from lazy_depends import ConcurrentRouter

router = ConcurrentRouter(prefix="/api")

@router.get("/dashboard")
async def dashboard(user=Depends(get_user)):
    ...

app.include_router(router)
```

## Tracing

See how your deps resolve:

```bash
LAZY_DEPENDS_TRACE=1 uvicorn myapp:app
```

```
GET /dashboard deps resolved in 14.2ms
  ├─ get_db ·····················    0.1ms
  ├─ get_user ···················    5.1ms  (after: get_db)
  ├─ get_tasks ··················    4.8ms ─┐  (after: get_user, get_db)
  ├─ get_activity ···············    4.5ms  │  (after: get_user, get_db)
  └─ get_perms ··················    5.0ms ─┘  (after: get_user, get_db)
```

Also available via `logging.getLogger("lazy_depends").setLevel(logging.DEBUG)`.

## Testing

Works with `dependency_overrides` exactly like FastAPI:

```python
app.dependency_overrides[get_db] = lambda: mock_db

with TestClient(app) as client:
    response = client.get("/")
```

For `StaticDepends`, call `StaticDepends.reset()` between tests.

## Migration from FastAPI

1. `from lazy_depends import Depends` instead of `from fastapi import Depends`
2. `app.router.route_class = ConcurrentRoute` (or use `ConcurrentRouter`)
3. Done. Add `LazyDepends` / `StaticDepends` / `CachedDepends` where it helps.

## Requirements

- Python >= 3.11
- FastAPI >= 0.100.0

## License

MIT
