"""
lazy_depends — Concurrent async dependency injection for FastAPI.

Two APIs:
  1. Container-based: explicit registration, middleware-driven concurrency
  2. Drop-in Depends: same FastAPI Depends() interface, automatic concurrency

Quick start (drop-in):
    from lazy_depends import Depends, ConcurrentRoute

    app = FastAPI()
    app.router.route_class = ConcurrentRoute

    # Everything else is standard FastAPI — deps resolve concurrently.
"""

from lazy_depends.container import (
    AsyncLazyDep,
    Container,
    LazyDepNotReady,
    RequestScope,
    install,
)

from lazy_depends.concurrent import Depends, ConcurrentRoute

__all__ = [
    "AsyncLazyDep",
    "Container",
    "ConcurrentRoute",
    "Depends",
    "LazyDepNotReady",
    "RequestScope",
    "install",
]
