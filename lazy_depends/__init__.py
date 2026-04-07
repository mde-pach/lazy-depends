"""
lazy_depends — Concurrent async dependency injection for FastAPI.

Drop-in ``Depends`` with concurrent resolution, plus ``LazyDepends``
for dependencies that resolve in the background while the endpoint runs.

Quick start:
    from lazy_depends import Depends, LazyDepends, ConcurrentRoute

    app = FastAPI()
    app.router.route_class = ConcurrentRoute

    @app.get("/")
    async def root(
        db=Depends(get_db),              # eager — resolved before endpoint
        user=LazyDepends(get_user),      # lazy — resolves in background
    ):
        await user                       # resolve when needed
        ...
"""

from lazy_depends.concurrent import Depends
from lazy_depends.lazy import Lazy, LazyDepends, StaticDepends, CachedDepends

try:
    from lazy_depends.concurrent import ConcurrentRoute, ConcurrentRouter
except ImportError:
    ConcurrentRoute = None  # type: ignore[assignment,misc]
    ConcurrentRouter = None  # type: ignore[assignment,misc]

__all__ = [
    "CachedDepends",
    "ConcurrentRoute",
    "ConcurrentRouter",
    "Depends",
    "Lazy",
    "LazyDepends",
    "StaticDepends",
]
