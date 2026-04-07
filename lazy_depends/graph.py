"""
Graph construction from FastAPI's Dependant tree.

Pure functions with no side effects — used by the solver to build
and sort the dependency DAG.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import get_dependant


def _node_key(dep: Dependant) -> Any:
    """
    Unique key for a graph node.

    use_cache=True  → shared by cache_key (same fn + scopes = same node)
    use_cache=False → unique per call-site (id of the Dependant object)
    """
    if dep.use_cache:
        return dep.cache_key
    return id(dep)


def _build_graph(
    dependant: Dependant,
    dependency_overrides_provider: Any,
) -> tuple[
    dict,  # graph: {node_key: (Dependant, call, {param_name: child_node_key})}
    dict,  # key_to_dependant: {node_key: Dependant}  (for use_cache dedup)
]:
    """
    Walk the Dependant tree and build a DAG for concurrent resolution.

    Handles dependency_overrides by re-analyzing overridden callables.
    """
    graph: dict = {}
    key_to_dependant: dict = {}
    _visited: set = set()

    def walk(dep: Dependant) -> Any:
        """Process a Dependant node, returns its node_key."""
        dep.call = cast(Callable[..., Any], dep.call)
        dep.cache_key = cast(tuple[Callable[..., Any], tuple[str]], dep.cache_key)

        # Apply dependency overrides (same logic as FastAPI)
        call = dep.call
        use_dep = dep
        if dependency_overrides_provider and dependency_overrides_provider.dependency_overrides:
            call = getattr(dependency_overrides_provider, "dependency_overrides", {}).get(
                dep.call, dep.call
            )
            if call is not dep.call:
                use_dep = get_dependant(
                    path=dep.path,  # type: ignore
                    call=call,
                    name=dep.name,
                    security_scopes=dep.security_scopes,
                )

        key = _node_key(dep)

        # For use_cache=True, skip if already processed
        if dep.use_cache and key in graph:
            return key

        # Cycle detection for use_cache=False
        visit_id = id(dep)
        if visit_id in _visited:
            return key
        _visited.add(visit_id)

        # Recurse into sub-dependencies
        child_keys: dict[str, Any] = {}
        for sub_dep in use_dep.dependencies:
            child_key = walk(sub_dep)
            if sub_dep.name is not None:
                child_keys[sub_dep.name] = child_key

        graph[key] = (use_dep, call, child_keys)
        key_to_dependant[key] = dep
        return key

    # Walk all top-level deps of the endpoint (not the endpoint itself)
    for sub_dep in dependant.dependencies:
        walk(sub_dep)

    return graph, key_to_dependant


def _topo_levels(graph: dict) -> list[list]:
    """Topological sort into levels for concurrent execution."""
    deps_of = {key: set(child_keys.values()) for key, (_, _, child_keys) in graph.items()}

    levels: list[list] = []
    resolved: set = set()
    remaining = set(deps_of.keys())

    while remaining:
        level = [key for key in remaining if deps_of[key].issubset(resolved)]
        if not level:
            break  # circular — bail, FastAPI will handle the error
        levels.append(level)
        resolved.update(level)
        remaining -= set(level)

    return levels
