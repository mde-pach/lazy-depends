"""
Dependency resolution tracing.

Enable by setting the ``lazy_depends`` logger to DEBUG::

    import logging
    logging.getLogger("lazy_depends").setLevel(logging.DEBUG)

Or set the environment variable::

    LAZY_DEPENDS_TRACE=1
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

logger = logging.getLogger("lazy_depends")

# Allow env-var activation without touching logging config
_ENV_TRACE = os.environ.get("LAZY_DEPENDS_TRACE", "").lower() in (
    "1",
    "true",
    "yes",
)


def is_trace_enabled() -> bool:
    return _ENV_TRACE or logger.isEnabledFor(logging.DEBUG)


@dataclass
class DepSpan:
    """Timing data for a single dependency resolution."""

    name: str
    level: int
    start: float = 0.0
    end: float = 0.0
    is_lazy: bool = False
    parent_names: list[str] = field(default_factory=list)

    @property
    def duration_ms(self) -> float:
        return (self.end - self.start) * 1000


@dataclass
class RequestTrace:
    """Collected trace for one request's dependency resolution."""

    method: str = ""
    path: str = ""
    spans: list[DepSpan] = field(default_factory=list)
    resolve_start: float = 0.0
    resolve_end: float = 0.0

    @property
    def total_ms(self) -> float:
        return (self.resolve_end - self.resolve_start) * 1000

    def format_waterfall(self) -> str:
        if not self.spans:
            return f"{self.method} {self.path} — no deps"

        lines: list[str] = []
        lines.append(f"{self.method} {self.path} deps resolved in {self.total_ms:.1f}ms")

        # Group spans by level
        levels: dict[int, list[DepSpan]] = {}
        for span in self.spans:
            levels.setdefault(span.level, []).append(span)

        max_levels = max(levels.keys()) if levels else 0

        for lvl in sorted(levels.keys()):
            spans = levels[lvl]
            is_concurrent = len(spans) > 1

            for i, span in enumerate(spans):
                # Tree connector
                is_last_span = lvl == max_levels and i == len(spans) - 1
                connector = "└─" if is_last_span else "├─"

                # Name, padded
                name = span.name
                if span.is_lazy:
                    name = f"{name} (lazy)"
                padded = f"{name} ".ljust(28, "·")

                # Duration
                duration = f" {span.duration_ms:>6.1f}ms"

                # Concurrent marker
                if is_concurrent:
                    if i == 0:
                        marker = " ─┐"
                    elif i == len(spans) - 1:
                        marker = " ─┘"
                    else:
                        marker = "  │"
                else:
                    marker = ""

                # After info
                after = ""
                if span.parent_names:
                    after = f"  (after: {', '.join(span.parent_names)})"

                lines.append(f"  {connector} {padded}{duration}{marker}{after}")

        return "\n".join(lines)


def log_trace(trace: RequestTrace) -> None:
    """Log a completed trace at DEBUG level."""
    if _ENV_TRACE and not logger.isEnabledFor(logging.DEBUG):
        # Env var set but no handler configured — print directly
        print(trace.format_waterfall())
    else:
        logger.debug("\n%s", trace.format_waterfall())
