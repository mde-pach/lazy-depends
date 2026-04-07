"""
Container-based lazy dependency injection.

Provides AsyncLazyDep, Container, and RequestScope for explicit
registration and middleware-driven concurrent resolution.
"""

from __future__ import annotations

import asyncio
import functools
import threading
from typing import Any, Callable, Awaitable, TypeVar, Generic, overload
from contextlib import asynccontextmanager

T = TypeVar("T")


class _UNSET:
    """Sentinel for unresolved state."""


class LazyDepNotReady(Exception):
    pass


class AsyncLazyDep(Generic[T]):
    """
    An awaitable + attribute-proxy hybrid.

    - `await dep` → waits for resolution, returns the real object
    - `dep.some_attr` → if resolved, returns attr; if not, raises with helpful message
    - Used inside async code where you can naturally await
    """

    __slots__ = ("_name", "_factory", "_future", "_result", "_teardown")

    def __init__(
        self,
        name: str,
        factory: Callable[[], Awaitable[T]],
        teardown: Callable[[T], Awaitable[None]] | None = None,
    ):
        self._name = name
        self._factory = factory
        self._teardown = teardown
        self._result: T | _UNSET = _UNSET
        self._future: asyncio.Future[T] | None = None

    def _schedule(self):
        """Schedule the factory as a background task on the running loop."""
        if self._future is None:
            self._future = asyncio.ensure_future(self._do_resolve())

    async def _do_resolve(self) -> T:
        result = await self._factory()
        self._result = result
        return result

    async def _shutdown(self):
        """Teardown the resolved dependency if a teardown was registered."""
        if self._result is not _UNSET and self._teardown is not None:
            await self._teardown(self._result)

    @property
    def is_resolved(self) -> bool:
        if self._result is not _UNSET:
            return True
        if self._future is not None and self._future.done():
            self._result = self._future.result()
            return True
        return False

    def __await__(self):
        if self._result is not _UNSET:
            return self._result
            yield  # noqa — makes this a generator
        if self._future is None:
            self._schedule()
        return (yield from self._future.__await__())

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if self._result is _UNSET:
            if self._future is not None and self._future.done():
                self._result = self._future.result()
            else:
                raise LazyDepNotReady(
                    f"Dependency '{self._name}' not yet resolved. "
                    f"Await it first: `obj = await dep`"
                )
        return getattr(self._result, name)

    def __repr__(self) -> str:
        if self._result is not _UNSET:
            return f"<AsyncLazyDep '{self._name}' resolved={self._result!r}>"
        if self._future and self._future.done():
            return f"<AsyncLazyDep '{self._name}' done (not yet unwrapped)>"
        return f"<AsyncLazyDep '{self._name}' pending>"


class Container:
    """
    Dependency injection container with async lazy loading.

    Integrates with:
      - FastAPI via .lifespan() and .depends()
      - fast-depends via .inject_into() and .provider()
      - Standalone async code via .start() / .get()
    """

    def __init__(self):
        self._factories: dict[str, Callable[[], Awaitable[Any]]] = {}
        self._teardowns: dict[str, Callable[[Any], Awaitable[None]]] = {}
        self._deps: dict[str, AsyncLazyDep] = {}
        self._started = False

    def lazy(
        self,
        name: str,
        teardown: Callable[[Any], Awaitable[None]] | None = None,
    ):
        """Decorator to register a lazy async dependency factory."""
        def decorator(factory: Callable[[], Awaitable[T]]) -> Callable[[], Awaitable[T]]:
            self._factories[name] = factory
            if teardown is not None:
                self._teardowns[name] = teardown
            return factory
        return decorator

    def register(
        self,
        name: str,
        factory: Callable[[], Awaitable[Any]],
        teardown: Callable[[Any], Awaitable[None]] | None = None,
    ):
        """Register a factory function for a named dependency."""
        self._factories[name] = factory
        if teardown is not None:
            self._teardowns[name] = teardown

    def start(self):
        """
        Kick off all dependency resolutions as concurrent background tasks.
        Call this early in your async context — all factories start resolving
        immediately. By the time you access them, they're (hopefully) done.
        """
        for name, factory in self._factories.items():
            if name not in self._deps:
                dep = AsyncLazyDep(name, factory, self._teardowns.get(name))
                dep._schedule()
                self._deps[name] = dep
        self._started = True

    async def shutdown(self):
        """Teardown all resolved dependencies (in reverse registration order)."""
        for name in reversed(list(self._deps)):
            await self._deps[name]._shutdown()

    async def get(self, name: str) -> Any:
        """Get a dependency, awaiting its resolution if needed."""
        if name not in self._deps:
            if name in self._factories:
                dep = AsyncLazyDep(
                    name, self._factories[name], self._teardowns.get(name)
                )
                dep._schedule()
                self._deps[name] = dep
            else:
                raise KeyError(f"Unknown dependency: {name}")
        return await self._deps[name]

    async def ensure(self, *names: str):
        """Await multiple dependencies concurrently."""
        deps = []
        for name in names:
            if name not in self._deps:
                if name not in self._factories:
                    raise KeyError(f"Unknown dependency: {name}")
                dep = AsyncLazyDep(
                    name, self._factories[name], self._teardowns.get(name)
                )
                dep._schedule()
                self._deps[name] = dep
            deps.append(self._deps[name])
        return await asyncio.gather(*deps)

    def __getattr__(self, name: str) -> AsyncLazyDep:
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._deps:
            return self._deps[name]
        if name in self._factories:
            dep = AsyncLazyDep(
                name, self._factories[name], self._teardowns.get(name)
            )
            dep._schedule()
            self._deps[name] = dep
            return dep
        raise AttributeError(f"No dependency registered as '{name}'")

    # ──────────────────────────────────────────────────────────
    # FastAPI integration
    # ──────────────────────────────────────────────────────────

    def lifespan(self, app_lifespan=None):
        """
        Returns an async context manager usable as FastAPI's lifespan.

        Starts all lazy deps at startup, shuts them down on exit.
        Optionally wraps an existing lifespan.

        Usage:
            container = Container()

            @container.lazy("db", teardown=lambda db: db.close())
            async def create_db():
                return await asyncpg.create_pool(...)

            app = FastAPI(lifespan=container.lifespan())
        """
        container = self

        @asynccontextmanager
        async def _lifespan(app):
            container.start()
            # Ensure all deps are resolved before the app starts serving
            await container.ensure(*container._factories.keys())
            try:
                if app_lifespan is not None:
                    async with app_lifespan(app) as state:
                        yield state
                else:
                    yield
            finally:
                await container.shutdown()

        return _lifespan

    def depends(self, name: str) -> Any:
        """
        Returns a callable suitable for FastAPI's Depends().

        The callable is an async function that awaits the lazy dep —
        since resolution started at lifespan, this is essentially free.

        Usage:
            from fastapi import Depends, FastAPI

            app = FastAPI(lifespan=container.lifespan())

            @app.get("/")
            async def root(db=Depends(container.depends("db"))):
                return await db.fetch("SELECT 1")
        """
        container = self

        async def _dep_getter() -> Any:
            return await container.get(name)

        _dep_getter.__name__ = f"get_{name}"
        _dep_getter.__qualname__ = f"Container.depends.<get_{name}>"
        return _dep_getter

    # ──────────────────────────────────────────────────────────
    # fast-depends integration
    # ──────────────────────────────────────────────────────────

    def as_fast_depends(self, name: str):
        """
        Returns a Depends()-compatible callable for use with fast-depends' @inject.

        Usage:
            from fast_depends import inject, Depends

            @inject
            async def handler(db=Depends(container.as_fast_depends("db"))):
                ...
        """
        return self.depends(name)  # Same callable works for both

    def inject_into(self, func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        """
        Decorator that auto-injects lazy deps by matching parameter names
        to registered dependency names — no Depends() needed.

        Usage:
            @container.inject_into
            async def handler(db, cache, request_id: str):
                # db and cache are auto-injected (matched by name)
                # request_id is passed through normally
                ...
        """
        import inspect

        sig = inspect.signature(func)
        dep_params = [
            name for name, param in sig.parameters.items()
            if name in self._factories
            and param.default is inspect.Parameter.empty
        ]

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Resolve any matching deps not already provided
            for pname in dep_params:
                if pname not in kwargs:
                    kwargs[pname] = await self.get(pname)
            return await func(*args, **kwargs)

        return wrapper


    # ──────────────────────────────────────────────────────────
    # Per-request scoped dependencies
    # ──────────────────────────────────────────────────────────

    _request_factories: dict[str, Callable] = {}

    def request_lazy(self, name: str):
        """
        Register a per-request dependency factory.

        The factory receives a `context` dict (built from the request)
        and can also reference app-level deps via the container.

        All request-scoped deps start resolving concurrently when the
        middleware fires, BEFORE FastAPI's sequential Depends() chain.

        Usage:
            @container.request_lazy("current_user")
            async def get_current_user(context, container):
                db = await container.get("db")
                token = context["token"]
                row = await db.execute("SELECT * FROM users WHERE token = ?", (token,))
                return await row.fetchone()
        """
        def decorator(factory):
            self._request_factories[name] = factory
            return factory
        return decorator

    def request_depends(self, name: str):
        """
        Returns a FastAPI Depends()-compatible callable for a request-scoped dep.

        The resolved value comes from the RequestScope stored on request.state.
        """
        async def _getter(request: "Request"):
            scope: RequestScope = request.state.lazy_scope
            return await scope.get(name)

        _getter.__name__ = f"get_{name}"
        _getter.__qualname__ = f"Container.request_depends.<get_{name}>"

        # We need to annotate the `request` param so FastAPI injects it
        import inspect
        from starlette.requests import Request

        sig = inspect.signature(_getter)
        _getter.__signature__ = sig.replace(parameters=[
            inspect.Parameter("request", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Request),
        ])

        return _getter

    def middleware(self, extract_context: Callable[["Request"], dict] | None = None):
        """
        Returns a FastAPI middleware that creates a RequestScope for each request.

        All request-scoped factories start resolving concurrently the moment
        the request arrives — before any route handler or Depends() runs.

        Usage:
            def extract(request):
                return {
                    "token": request.headers.get("authorization", "").removeprefix("Bearer "),
                    "ip": request.client.host,
                }

            app.middleware("http")(container.middleware(extract))
        """
        container = self

        async def _middleware(request: "Request", call_next):
            # Build context from the request
            if extract_context is not None:
                ctx = extract_context(request)
            else:
                ctx = {}

            # Create a per-request scope and start ALL request deps concurrently
            scope = RequestScope(container, ctx)
            scope.start()
            request.state.lazy_scope = scope

            response = await call_next(request)
            return response

        return _middleware


class RequestScope:
    """
    Per-request lazy dependency scope.

    Created by the middleware for each incoming request.
    All registered request-scoped factories start resolving concurrently
    the moment the scope is created — before the route handler runs.
    """

    __slots__ = ("_container", "_context", "_deps")

    def __init__(self, container: Container, context: dict):
        self._container = container
        self._context = context
        self._deps: dict[str, AsyncLazyDep] = {}

    def start(self):
        """Kick off all request-scoped factories concurrently."""
        for name, factory in self._container._request_factories.items():
            if name not in self._deps:
                # Bind context and container into the factory call
                bound = functools.partial(factory, self._context, self._container)
                dep = AsyncLazyDep(name, bound)
                dep._schedule()
                self._deps[name] = dep

    async def get(self, name: str) -> Any:
        """Get a request-scoped dep, awaiting resolution if needed."""
        if name in self._deps:
            return await self._deps[name]
        # Fall back to app-level container
        return await self._container.get(name)


# ──────────────────────────────────────────────────────────
# Module-level lazy injection using __getattr__ (PEP 562)
# ──────────────────────────────────────────────────────────

def install(container: Container, module_dict: dict):
    """
    Install lazy deps into a module's namespace via PEP 562 __getattr__.

    After calling this, `await module.db` gives you the resolved object.
    """
    import sys

    caller_module = module_dict.get("__name__")
    if caller_module is None:
        return

    mod = sys.modules.get(caller_module)
    if mod is None:
        return

    original_getattr = getattr(mod, "__getattr__", None)

    def __getattr__(name: str):
        try:
            return container.__getattr__(name)
        except AttributeError:
            if original_getattr:
                return original_getattr(name)
            raise

    mod.__getattr__ = __getattr__
