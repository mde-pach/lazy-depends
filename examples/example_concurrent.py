"""
Concurrent-deps FastAPI app — drop-in Depends replacement.

COMPARE WITH example_traditional.py — the only differences are:
  1. Import: `from lazy_depends import Depends, ConcurrentRoute`
  2. One line: `app.router.route_class = ConcurrentRoute`

Everything else — dep functions, route handlers, signatures — is IDENTICAL.
Dependencies at the same level run in parallel instead of sequentially.

Run:  uvicorn example_concurrent:app
"""

from contextlib import asynccontextmanager

import aiosqlite

# instead of: from fastapi import Depends
# ────────────────────────────────────────────────────────────
from domain import (
    build_dashboard,
    build_focus,
    check_permission,
    compute_workload,
    create_db,
    fetch_permissions,
    fetch_tasks_for_user,
    fetch_team_activity,
    fetch_user_by_token,
    insert_task,
    load_config,
)
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

# ── THE ONLY IMPORT CHANGE ──────────────────────────────────────
from lazy_depends import ConcurrentRoute, Depends

# ════════════════════════════════════════════════════════
# Lifespan — IDENTICAL to traditional
# ════════════════════════════════════════════════════════

state: dict = {}


@asynccontextmanager
async def lifespan(app):
    state["db"] = await create_db()
    state["config"] = await load_config(state["db"])
    yield
    await state["db"].close()


app = FastAPI(title="Task Manager (concurrent)", lifespan=lifespan)

# ── THE ONLY EXTRA LINE ────────────────────────────────────────
app.router.route_class = ConcurrentRoute
# ────────────────────────────────────────────────────────────


# ════════════════════════════════════════════════════════
# Dependencies — IDENTICAL to traditional (copy-paste)
# ════════════════════════════════════════════════════════


async def get_db() -> aiosqlite.Connection:
    return state["db"]


async def get_config() -> dict:
    return state["config"]


async def get_current_user(request: Request, db=Depends(get_db)):
    token = request.headers.get("authorization", "").removeprefix("Bearer ")
    user = await fetch_user_by_token(db, token)
    if user is None:
        raise HTTPException(401, "Invalid token")
    return user


async def get_my_tasks(user=Depends(get_current_user), db=Depends(get_db)):
    return await fetch_tasks_for_user(db, user["id"])


async def get_permissions(user=Depends(get_current_user), db=Depends(get_db)):
    return await fetch_permissions(db, user["role"])


async def get_team_activity(user=Depends(get_current_user), db=Depends(get_db)):
    return await fetch_team_activity(db, user["team"])


async def get_feed_context(db=Depends(get_db)):
    cursor = await db.execute("SELECT id, name FROM users")
    user_names = {r["id"]: r["name"] for r in await cursor.fetchall()}
    cursor = await db.execute("SELECT id, title FROM tasks")
    task_titles = {r["id"]: r["title"] for r in await cursor.fetchall()}
    return user_names, task_titles


# ════════════════════════════════════════════════════════
# Routes — IDENTICAL to traditional (copy-paste)
# ════════════════════════════════════════════════════════


@app.get("/dashboard")
async def dashboard(
    user=Depends(get_current_user),
    tasks=Depends(get_my_tasks),
    permissions=Depends(get_permissions),
    activity=Depends(get_team_activity),
    feed_ctx=Depends(get_feed_context),
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
    user=Depends(get_current_user),
    tasks=Depends(get_my_tasks),
    permissions=Depends(get_permissions),
    config=Depends(get_config),
    db=Depends(get_db),
):
    err = check_permission(permissions, "task.create")
    if err:
        raise HTTPException(403, err)

    active = [t for t in tasks if t["status"] != "done"]
    if len(active) >= config["max_tasks_per_user"]:
        raise HTTPException(
            400, f"Too many active tasks ({len(active)}/{config['max_tasks_per_user']})"
        )

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
    user=Depends(get_current_user),
    tasks=Depends(get_my_tasks),
):
    return build_focus(user, tasks)
