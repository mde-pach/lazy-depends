"""
Shared domain: database setup, async services, and sync business logic.

Both example apps (traditional and lazy) import from here.
Nothing in this file knows about FastAPI or lazy_deps.

All async services perform real aiosqlite queries — no asyncio.sleep.
"""

import asyncio
import os
import time
import tempfile

import aiosqlite

DB_PATH = os.path.join(tempfile.gettempdir(), "lazy_deps_tasks.db")

# Simulate network round-trip latency for each query.
# Local SQLite completes in ~0.05ms, but a real PostgreSQL/MySQL query
# over the network takes 1-5ms minimum. Set to 0 for raw local speed.
NETWORK_LATENCY_MS = 5

ROLE_PERMISSIONS = {
    "admin": {"task.create", "task.edit", "task.delete", "task.assign", "task.view", "team.manage"},
    "viewer": {"task.view"},
}


# ═══════════════════════════════════════════════════════════
# Database setup
# ═══════════════════════════════════════════════════════════

async def create_db() -> aiosqlite.Connection:
    """Open (or create) the SQLite database, seed if empty."""
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row

    await db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT NOT NULL,
            team TEXT NOT NULL,
            token TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            assignee_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'todo',
            priority TEXT NOT NULL DEFAULT 'medium',
            estimate_hours INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (assignee_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS activity (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            task_id INTEGER NOT NULL,
            ts REAL NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        );
        CREATE TABLE IF NOT EXISTS permissions (
            id INTEGER PRIMARY KEY,
            role TEXT NOT NULL,
            permission TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_users_token ON users(token);
        CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee_id);
        CREATE INDEX IF NOT EXISTS idx_activity_user ON activity(user_id);
        CREATE INDEX IF NOT EXISTS idx_permissions_role ON permissions(role);
    """)

    cursor = await db.execute("SELECT COUNT(*) FROM users")
    count = (await cursor.fetchone())[0]
    if count == 0:
        await _seed(db)

    return db


async def _seed(db: aiosqlite.Connection):
    """Seed database with initial data."""
    users = [
        (1, "Alice", "admin", "backend", "token-alice"),
        (2, "Bob", "viewer", "frontend", "token-bob"),
    ]
    await db.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", users)

    tasks = [
        (1, "Fix auth bug", 1, "in_progress", "high", 3),
        (2, "Write API docs", 2, "todo", "medium", 5),
        (3, "Deploy v2.1", 1, "done", "high", 1),
        (4, "Review PR #42", 2, "in_progress", "low", 2),
        (5, "DB migration", 1, "todo", "high", 4),
    ]
    await db.executemany("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?)", tasks)

    now = time.time()
    activity = [
        (1, "completed", 3, now - 3600),
        (2, "commented", 1, now - 1800),
        (1, "assigned", 5, now - 900),
    ]
    await db.executemany(
        "INSERT INTO activity (user_id, action, task_id, ts) VALUES (?, ?, ?, ?)",
        activity,
    )

    for role, perms in ROLE_PERMISSIONS.items():
        for perm in perms:
            await db.execute(
                "INSERT INTO permissions (role, permission) VALUES (?, ?)",
                (role, perm),
            )

    config = [
        ("max_tasks_per_user", "20"),
        ("hmac_secret", "secret-key"),
    ]
    await db.executemany("INSERT INTO config VALUES (?, ?)", config)

    await db.commit()


def reset_db():
    """Delete the database file so next create_db() starts fresh."""
    if os.path.exists(DB_PATH):
        os.unlink(DB_PATH)


# ═══════════════════════════════════════════════════════════
# Async services — real aiosqlite queries
# ═══════════════════════════════════════════════════════════

async def _latency():
    """Simulate network round-trip latency for a DB query."""
    if NETWORK_LATENCY_MS > 0:
        await asyncio.sleep(NETWORK_LATENCY_MS / 1000)


async def fetch_user_by_token(db: aiosqlite.Connection, token: str) -> dict | None:
    """Validate token and load user from DB."""
    await _latency()
    cursor = await db.execute(
        "SELECT id, name, role, team FROM users WHERE token = ?", (token,)
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def fetch_tasks_for_user(db: aiosqlite.Connection, user_id: int) -> list[dict]:
    """Load all tasks assigned to a user."""
    await _latency()
    cursor = await db.execute(
        "SELECT id, title, assignee_id, status, priority, estimate_hours "
        "FROM tasks WHERE assignee_id = ?",
        (user_id,),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def fetch_team_activity(db: aiosqlite.Connection, team: str) -> list[dict]:
    """Load recent activity for all users in a team (join query)."""
    await _latency()
    cursor = await db.execute(
        "SELECT a.user_id, a.action, a.task_id, a.ts "
        "FROM activity a "
        "JOIN users u ON a.user_id = u.id "
        "WHERE u.team = ? "
        "ORDER BY a.ts DESC",
        (team,),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def fetch_permissions(db: aiosqlite.Connection, role: str) -> set[str]:
    """Load permissions for a role."""
    await _latency()
    cursor = await db.execute(
        "SELECT permission FROM permissions WHERE role = ?", (role,)
    )
    rows = await cursor.fetchall()
    return {r["permission"] for r in rows}


async def load_config(db: aiosqlite.Connection) -> dict:
    """Load config key-value pairs from DB."""
    await _latency()
    cursor = await db.execute("SELECT key, value FROM config")
    rows = await cursor.fetchall()
    config = {}
    for r in rows:
        try:
            config[r["key"]] = int(r["value"])
        except ValueError:
            config[r["key"]] = r["value"]
    return config


async def insert_task(
    db: aiosqlite.Connection, title: str, assignee_id: int, priority: str, estimate_hours: int,
) -> dict:
    """Insert a new task and return it."""
    await _latency()
    cursor = await db.execute(
        "INSERT INTO tasks (title, assignee_id, status, priority, estimate_hours) "
        "VALUES (?, ?, 'todo', ?, ?)",
        (title, assignee_id, priority, estimate_hours),
    )
    await db.commit()
    task_id = cursor.lastrowid
    return {
        "id": task_id,
        "title": title,
        "assignee_id": assignee_id,
        "status": "todo",
        "priority": priority,
        "estimate_hours": estimate_hours,
    }


# ═══════════════════════════════════════════════════════════
# Sync business logic (unchanged — operates on dicts)
# ═══════════════════════════════════════════════════════════

def compute_workload(tasks: list[dict]) -> dict:
    by_status = {}
    for t in tasks:
        by_status.setdefault(t["status"], []).append(t)

    total_hours = sum(t["estimate_hours"] for t in tasks)
    remaining_hours = sum(t["estimate_hours"] for t in tasks if t["status"] != "done")
    high_priority_open = [t for t in tasks if t["priority"] == "high" and t["status"] != "done"]

    return {
        "total_tasks": len(tasks),
        "by_status": {s: len(ts) for s, ts in by_status.items()},
        "total_estimate_hours": total_hours,
        "remaining_hours": remaining_hours,
        "completion_pct": round((total_hours - remaining_hours) / total_hours * 100, 1) if total_hours else 0,
        "high_priority_open": [t["title"] for t in high_priority_open],
        "is_overloaded": remaining_hours > 10,
    }


def format_activity_feed(activity: list[dict], user_names: dict[int, str], task_titles: dict[int, str]) -> list[dict]:
    feed = []
    for a in sorted(activity, key=lambda x: x["ts"], reverse=True):
        ago = int(time.time() - a["ts"])
        if ago < 60:
            ago_str = f"{ago}s ago"
        elif ago < 3600:
            ago_str = f"{ago // 60}m ago"
        else:
            ago_str = f"{ago // 3600}h ago"

        feed.append({
            "who": user_names.get(a["user_id"], "Unknown"),
            "did": a["action"],
            "what": task_titles.get(a["task_id"], "Unknown task"),
            "when": ago_str,
        })
    return feed


def prioritize_tasks(tasks: list[dict]) -> list[dict]:
    priority_order = {"high": 0, "medium": 1, "low": 2}
    status_order = {"in_progress": 0, "todo": 1, "done": 2}
    return sorted(tasks, key=lambda t: (
        status_order.get(t["status"], 9),
        priority_order.get(t["priority"], 9),
    ))


def check_permission(permissions: set[str], required: str) -> str | None:
    if required not in permissions:
        return f"Missing permission: {required}"
    return None


def build_dashboard(user, tasks, permissions, activity, user_names, task_titles):
    workload = compute_workload(tasks)
    feed = format_activity_feed(activity, user_names, task_titles)
    active = [t for t in tasks if t["status"] != "done"]
    prioritized = prioritize_tasks(active)
    next_up = prioritized[0]["title"] if prioritized else None

    return {
        "greeting": f"Welcome back, {user['name']}",
        "role": user["role"],
        "workload": workload,
        "next_task": next_up,
        "can_manage_team": "team.manage" in permissions,
        "recent_activity": feed[:5],
    }


def build_focus(user, tasks):
    active = [t for t in tasks if t["status"] != "done"]
    prioritized = prioritize_tasks(active)

    if not prioritized:
        return {"message": "All done! Nothing to focus on."}

    top = prioritized[0]
    total_remaining = sum(t["estimate_hours"] for t in active)
    task_pct = round(top["estimate_hours"] / total_remaining * 100, 1) if total_remaining else 0

    return {
        "focus_on": top["title"],
        "priority": top["priority"],
        "estimate": f"{top['estimate_hours']}h",
        "represents": f"{task_pct}% of your remaining workload",
        "backlog_size": len(active),
        "tip": "Finish this first!" if top["priority"] == "high" else "Take your time.",
    }
