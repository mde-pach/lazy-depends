"""
Benchmark: Traditional vs Concurrent vs Lazy — same domain, same routes.

All queries are real aiosqlite calls.
Runs at two latency levels to show when concurrency pays off:
  1. Local DB (0ms latency) — raw SQLite speed
  2. Network DB (5ms latency) — simulates PostgreSQL/MySQL over TCP

Run: python benchmark.py
"""

import asyncio
import importlib
import statistics
import time

from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient


HEADERS = {"Authorization": "Bearer token-alice"}
HEADERS_BOB = {"Authorization": "Bearer token-bob"}

SCENARIOS = [
    ("GET",  "/dashboard", HEADERS,     None,                                                      "Dashboard (5 deps)"),
    ("GET",  "/dashboard", HEADERS,     None,                                                      "Dashboard (5 deps, 2nd)"),
    ("GET",  "/focus",     HEADERS,     None,                                                      "Focus (2 deps)"),
    ("GET",  "/focus",     HEADERS_BOB, None,                                                      "Focus as Bob (2 deps)"),
    ("POST", "/tasks",     HEADERS,     {"title": "Benchmark task", "priority": "high", "estimate_hours": 2}, "Create task (4 deps)"),
    ("POST", "/tasks",     HEADERS_BOB, {"title": "Nope"},                                         "Create denied (4 deps)"),
    ("GET",  "/dashboard", HEADERS,     None,                                                      "Dashboard after mutation"),
]


async def run_scenarios(client: AsyncClient) -> list[dict]:
    results = []
    for method, path, headers, body, desc in SCENARIOS:
        t0 = time.monotonic()
        if method == "GET":
            r = await client.get(path, headers=headers)
        else:
            r = await client.post(path, headers=headers, json=body)
        dt = (time.monotonic() - t0) * 1000
        results.append({"desc": desc, "status": r.status_code, "time_ms": dt})
    return results


async def run_burst(client: AsyncClient, n: int = 30) -> float:
    t0 = time.monotonic()
    responses = await asyncio.gather(*[
        client.get("/dashboard", headers=HEADERS) for _ in range(n)
    ])
    total = (time.monotonic() - t0) * 1000
    ok = sum(1 for r in responses if r.status_code == 200)
    return total, ok


async def benchmark_app(module_name: str, label: str) -> dict:
    import domain
    domain.reset_db()
    importlib.reload(domain)
    mod = importlib.import_module(module_name)
    importlib.reload(mod)

    t0 = time.monotonic()
    async with LifespanManager(mod.app) as mgr:
        startup_ms = (time.monotonic() - t0) * 1000

        async with AsyncClient(
            transport=ASGITransport(app=mgr.app),
            base_url="http://test",
        ) as client:
            results = await run_scenarios(client)
            burst_ms, burst_ok = await run_burst(client, 30)

    return {
        "startup_ms": startup_ms,
        "scenarios": results,
        "total_ms": sum(r["time_ms"] for r in results),
        "burst_ms": burst_ms,
        "burst_ok": burst_ok,
    }


def print_comparison(labels, all_results, runs):
    n_apps = len(labels)

    header = f"  {'Scenario':<35s}"
    for label in labels:
        header += f" {label:>10s}"
    header += f"  {'Speedup':>10s}"
    print(header)
    print(f"  {'─'*35}" + f" {'─'*10}" * n_apps + f"  {'─'*10}")

    for i, (_, _, _, _, desc) in enumerate(SCENARIOS):
        means = [statistics.mean(r["scenarios"][i]["time_ms"] for r in results) for results in all_results]
        line = f"  {desc:<35s}"
        for m in means:
            line += f" {m:9.2f}ms"
        # Speedup: traditional vs best alternative
        if means[0] > 0:
            best_alt = min(means[1:])
            pct = (means[0] - best_alt) / means[0] * 100
            line += f"  {pct:+8.1f}%"
        print(line)

    # Totals
    totals = [statistics.mean(r["total_ms"] for r in results) for results in all_results]
    bursts = [statistics.mean(r["burst_ms"] for r in results) for results in all_results]

    print()
    line = f"  {'Total (7 requests)':<35s}"
    for t in totals:
        line += f" {t:9.2f}ms"
    if totals[0] > 0:
        best = min(totals[1:])
        line += f"  {(totals[0] - best) / totals[0] * 100:+8.1f}%"
    print(line)

    line = f"  {'Burst (30x /dashboard)':<35s}"
    for b in bursts:
        line += f" {b:9.2f}ms"
    if bursts[0] > 0:
        best = min(bursts[1:])
        line += f"  {(bursts[0] - best) / bursts[0] * 100:+8.1f}%"
    print(line)


async def run_suite(latency_ms: int, label: str, runs: int = 5):
    import domain
    domain.NETWORK_LATENCY_MS = latency_ms

    print(f"\n{'=' * 80}")
    print(f"  {label} (NETWORK_LATENCY_MS = {latency_ms})")
    print(f"{'=' * 80}")

    apps = [
        ("example_traditional", "Traditional"),
        ("example_concurrent",  "Concurrent"),
    ]

    all_results = [[] for _ in apps]

    for run in range(runs):
        print(f"  Run {run + 1}/{runs}...", end="", flush=True)
        for idx, (mod_name, lbl) in enumerate(apps):
            result = await benchmark_app(mod_name, lbl)
            all_results[idx].append(result)
        totals = " ".join(
            f"{lbl}={all_results[idx][-1]['total_ms']:.1f}ms"
            for idx, (_, lbl) in enumerate(apps)
        )
        print(f" {totals}")

    labels = [lbl for _, lbl in apps]
    print_comparison(labels, all_results, runs)
    return all_results


async def main():
    print("=" * 80)
    print("  BENCHMARK: Traditional vs Concurrent (drop-in)")
    print("=" * 80)
    print()
    print("  Same domain.py, same routes, same business logic.")
    print("  All queries are real aiosqlite calls.")
    print()
    print("  Traditional:  standard FastAPI Depends (sequential)")
    print("  Concurrent:   lazy_depends.Depends drop-in (concurrent, same code!)")

    await run_suite(0, "LOCAL SQLite (no network latency)")
    await run_suite(5, "NETWORK-LIKE (5ms latency per query)")

    print()
    print("─" * 80)
    print("  TAKEAWAY")
    print("─" * 80)
    print()
    print("  The Concurrent version uses the EXACT same dep functions as Traditional.")
    print("  Only two changes: import Depends from lazy_depends + set ConcurrentRoute.")
    print()


if __name__ == "__main__":
    asyncio.run(main())
