"""
Lazy-deps FastAPI app — concurrent per-request resolution.

Same domain logic as example_traditional.py, only the DI wiring differs.
DB is created at startup via container.lazy (same timing as traditional).

Run:  uvicorn example_lazy:app
"""

import aiosqlite
from fastapi import FastAPI, Depends, HTTPException, Request
from pydantic import BaseModel

from lazy_depends import Container
from domain import (
    create_db,
    fetch_user_by_token,
    fetch_tasks_for_user,
    fetch_team_activity,
    fetch_permissions,
    load_config,
    insert_task,
    build_dashboard,
    build_focus,
    compute_workload,
    check_permission,
)


# ═══════════════════════════════════════════════════════════
# Container — DB created at startup (same as traditional)
# ═══════════════════════════════════════════════════════════

container = Container()

container.register("db", create_db, teardown=lambda db: db.close())


@container.lazy("config")
async def _load_config():
    db = await container.get("db")
    return await load_config(db)


# ═══════════════════════════════════════════════════════════
# Request-scoped deps — all start concurrently in middleware
#
# For /dashboard, middleware fires all 5 at once:
#   current_user   → 1 DB query
#   my_tasks       → 1 DB query (needs own user lookup)
#   permissions    → 1 DB query (needs own user lookup)
#   team_activity  → 1 DB join query (needs own user lookup)
#   feed_context   → 2 DB queries
#
# All run in parallel. Total time ≈ slowest single one.
# ═══════════════════════════════════════════════════════════

@container.request_lazy("current_user")
async def resolve_user(ctx, ctr):
    db = await ctr.get("db")
    user = await fetch_user_by_token(db, ctx["token"])
    if user is None:
        raise HTTPException(401, "Invalid token")
    return user


@container.request_lazy("my_tasks")
async def resolve_tasks(ctx, ctr):
    db = await ctr.get("db")
    user = await fetch_user_by_token(db, ctx["token"])
    if not user:
        return []
    return await fetch_tasks_for_user(db, user["id"])


@container.request_lazy("permissions")
async def resolve_permissions(ctx, ctr):
    db = await ctr.get("db")
    user = await fetch_user_by_token(db, ctx["token"])
    if not user:
        return set()
    return await fetch_permissions(db, user["role"])


@container.request_lazy("team_activity")
async def resolve_activity(ctx, ctr):
    db = await ctr.get("db")
    user = await fetch_user_by_token(db, ctx["token"])
    if not user:
        return []
    return await fetch_team_activity(db, user["team"])


@container.request_lazy("feed_context")
async def resolve_feed_context(ctx, ctr):
    db = await ctr.get("db")
    cursor = await db.execute("SELECT id, name FROM users")
    user_names = {r["id"]: r["name"] for r in await cursor.fetchall()}
    cursor = await db.execute("SELECT id, title FROM tasks")
    task_titles = {r["id"]: r["title"] for r in await cursor.fetchall()}
    return user_names, task_titles


# ═══════════════════════════════════════════════════════════
# App + middleware
# ═══════════════════════════════════════════════════════════

app = FastAPI(title="Task Manager (lazy-deps)", lifespan=container.lifespan())

app.middleware("http")(container.middleware(
    lambda req: {"token": req.headers.get("authorization", "").removeprefix("Bearer ")}
))


# ═══════════════════════════════════════════════════════════
# Routes — same business logic as traditional version
# ═══════════════════════════════════════════════════════════

@app.get("/dashboard")
async def dashboard(
    user=Depends(container.request_depends("current_user")),
    tasks=Depends(container.request_depends("my_tasks")),
    permissions=Depends(container.request_depends("permissions")),
    activity=Depends(container.request_depends("team_activity")),
    feed_ctx=Depends(container.request_depends("feed_context")),
):
    user_names, task_titles = feed_ctx
    return build_dashboard(user, tasks, permissions, activity, user_names, task_titles)


class TaskCreateBody(BaseModel):
    title: str
    priority: str = "medium"
    estimate_hours: int = 1


@app.post("/tasks")
async def create_task(
    body: TaskCreateBody,
    user=Depends(container.request_depends("current_user")),
    tasks=Depends(container.request_depends("my_tasks")),
    permissions=Depends(container.request_depends("permissions")),
    config=Depends(container.depends("config")),
    db=Depends(container.depends("db")),
):
    err = check_permission(permissions, "task.create")
    if err:
        raise HTTPException(403, err)

    active = [t for t in tasks if t["status"] != "done"]
    if len(active) >= config["max_tasks_per_user"]:
        raise HTTPException(400, f"Too many active tasks ({len(active)}/{config['max_tasks_per_user']})")

    if body.priority not in ("high", "medium", "low"):
        raise HTTPException(422, f"Invalid priority: {body.priority}")

    new_task = await insert_task(db, body.title, user["id"], body.priority, body.estimate_hours)

    updated = await fetch_tasks_for_user(db, user["id"])
    workload = compute_workload(updated)

    return {
        "created": new_task,
        "workload_after": {
            "remaining_hours": workload["remaining_hours"],
            "is_overloaded": workload["is_overloaded"],
        },
    }


@app.get("/focus")
async def focus_mode(
    user=Depends(container.request_depends("current_user")),
    tasks=Depends(container.request_depends("my_tasks")),
):
    return build_focus(user, tasks)
