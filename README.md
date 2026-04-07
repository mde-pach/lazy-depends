# lazy-depends

FastAPI resolves your dependencies one by one. If you have 4 independent deps that each take 5ms, you're waiting 20ms. lazy-depends runs them in parallel.

```
pip install lazy-depends
```

## Get started

Two lines:

```python
from lazy_depends import Depends, ConcurrentRoute   # swap the import

app = FastAPI()
app.router.route_class = ConcurrentRoute             # add this
```

Your dep functions don't change. Your routes don't change. You just get concurrent resolution.

```
Before:                               After:

  get_user ────── 5ms                   get_user ─── 5ms  ─┐
  get_tasks ───── 5ms                   get_tasks ── 5ms   │ parallel
  get_perms ───── 5ms                   get_perms ── 5ms   │
  get_activity ── 5ms                   get_activity 5ms  ─┘
  Total: ~20ms                          Total: ~5ms
```

Deps that depend on each other still wait for their parents. Only independent branches run in parallel, and each dep kicks off the moment its own parents are done — it doesn't hang around waiting for unrelated deps at the same depth.

`lazy_depends.Depends` and `fastapi.Depends` are the same thing underneath. Mix them in the same endpoint, same sub-dep chain -- doesn't matter.

## `LazyDepends` -- don't wait for what you don't need yet

Sometimes a dep is slow and you don't need its result right away. `LazyDepends` starts the dep in the background and hands you a `Lazy[T]`. Await it when you're ready:

```python
from lazy_depends import Depends, LazyDepends

@app.get("/")
async def root(
    db=Depends(get_db),                  # resolved before endpoint starts
    lazy_user=LazyDepends(get_user),     # starts immediately, in background
):
    await db.execute("SELECT 1")         # db is ready
    await asyncio.sleep(0.5)             # get_user keeps running during this

    user = await lazy_user               # blocks only for whatever time is left
    return {"user": user}
```

`await lazy_user` gives you the real value -- a plain dict, not a proxy. Pydantic, `isinstance`, `json.dumps` all work fine.

Convention: prefix lazy params with `lazy_` so it's obvious which ones need an `await`.

This works inside sub-dependencies too, not just at the endpoint level:

```python
async def get_dashboard(
    db=Depends(get_db),
    lazy_user=LazyDepends(get_user),
):
    result = await db.execute("SELECT ...")   # user resolves in background
    user = await lazy_user                    # done by now
    return {"data": result, "user": user}

@app.get("/")
async def root(dashboard=Depends(get_dashboard)):
    return dashboard
```

## `StaticDepends` -- resolve once at startup, inject forever

For things that never change: config, ML models, connection pools. Resolved during the app lifespan, injected as a plain value with zero per-request cost.

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

Multiple static deps resolve concurrently during startup. At request time it's just a dict lookup.

If you have your own lifespan, call `await StaticDepends.resolve()` in it instead of using `StaticDepends.lifespan`.

## `CachedDepends` -- like static, but expires

For data that changes slowly: feature flags, permission sets, rate limit config. Runs the callable once, then serves the cached result for `ttl` seconds.

```python
from lazy_depends import CachedDepends

@app.get("/")
async def root(flags=CachedDepends(get_feature_flags, ttl=30)):
    ...
```

After 30 seconds, the next request runs the callable again.

## Which one do I use?

Quick rule of thumb: start with `Depends`. Reach for `LazyDepends` when you've got a slow dep you don't immediately need, `StaticDepends` for stuff that never changes, and `CachedDepends` when it changes but not often.

| I need... | Use | It resolves... |
|---|---|---|
| Normal dep, but faster | `Depends` | Before the endpoint, in parallel |
| Slow dep I don't need right away | `LazyDepends` | In the background, await when ready |
| Something that never changes | `StaticDepends` | Once at startup |
| Something that changes slowly | `CachedDepends` | Once per TTL window |

## `ConcurrentRouter`

If you use `include_router`, this is cleaner than setting `route_class` everywhere:

```python
from lazy_depends import ConcurrentRouter

router = ConcurrentRouter(prefix="/api")

@router.get("/dashboard")
async def dashboard(user=Depends(get_user)):
    ...

app.include_router(router)
```

## Tracing

Want to see what's happening? Set `LAZY_DEPENDS_TRACE=1`:

```
GET /dashboard deps resolved in 14.2ms
  ├─ get_db ·····················    0.1ms
  ├─ get_user ···················    5.1ms  (after: get_db)
  ├─ get_tasks ··················    4.8ms ─┐  (after: get_user, get_db)
  ├─ get_activity ···············    4.5ms  │  (after: get_user, get_db)
  └─ get_perms ··················    5.0ms ─┘  (after: get_user, get_db)
```

The `─┐`/`─┘` markers mean those deps ran in parallel. You can also enable it via `logging.getLogger("lazy_depends").setLevel(logging.DEBUG)`.

## Testing

`dependency_overrides` works the same as with plain FastAPI:

```python
app.dependency_overrides[get_db] = lambda: mock_db

with TestClient(app) as client:
    response = client.get("/")
```

Call `StaticDepends.reset()` between tests to clear startup-resolved values.

## Migrating

1. Change `from fastapi import Depends` to `from lazy_depends import Depends`
2. Add `app.router.route_class = ConcurrentRoute` (or use `ConcurrentRouter`)
3. That's it. You can add `LazyDepends` / `StaticDepends` / `CachedDepends` later when you need them.

## Requirements

Python >= 3.11, FastAPI >= 0.114.0

## License

MIT
