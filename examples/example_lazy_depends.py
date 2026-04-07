import asyncio
import time

from fastapi import FastAPI

from lazy_depends import ConcurrentRoute, Depends, LazyDepends

app = FastAPI()
app.router.route_class = ConcurrentRoute


async def get_db():
    """Fast dependency — resolved eagerly."""
    return {"db": "connected"}


async def get_user():
    """Slow dependency (1s) — resolved lazily in background."""
    await asyncio.sleep(1)
    return {"id": 1, "name": "John Doe"}


@app.get("/")
async def root(
    db: dict = Depends(get_db),
    lazy_user=LazyDepends(get_user),
):
    t0 = time.monotonic()
    print(f"[{0:.3f}s] endpoint starts immediately")

    # db is eager — already resolved, use normally
    print(f"[{time.monotonic() - t0:.3f}s] db ready: {db}")

    # do async work — meanwhile get_user resolves in background
    await asyncio.sleep(0.5)
    print(f"[{time.monotonic() - t0:.3f}s] after 0.5s sleep")

    # resolve user — blocks only for remaining ~0.5s, not the full 1s
    user = await lazy_user
    print(f"[{time.monotonic() - t0:.3f}s] user resolved: {user['name']}")

    return {"db": db, "user": user}
