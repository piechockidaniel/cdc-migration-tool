"""Adaptive polling interval (exponential back-off while idle, fast while busy).

Used by connectors that have no true push channel. The goal is the one the
architect stated: *do not hammer the source*. An idle source is asked once
every ``max_s`` seconds with a single, index-only watermark query; a busy
source is drained at ``min_s`` cadence.
"""
from __future__ import annotations


class AdaptiveInterval:
    def __init__(self, min_s: float = 0.25, max_s: float = 30.0, factor: float = 2.0, error_s: float = 5.0):
        if min_s <= 0 or max_s < min_s:
            raise ValueError("need 0 < min_s <= max_s")
        self.min_s = min_s
        self.max_s = max_s
        self.factor = factor
        self.error_s = error_s
        self.current = min_s
        self.idle_cycles = 0

    def idle(self) -> float:
        """Source had nothing new: back off."""
        self.idle_cycles += 1
        wait = self.current
        self.current = min(self.max_s, self.current * self.factor)
        return wait

    def busy(self) -> float:
        """Source produced changes: reset to the fastest cadence."""
        self.idle_cycles = 0
        self.current = self.min_s
        return self.min_s

    def error(self) -> float:
        self.current = min(self.max_s, max(self.current, self.error_s))
        return self.current

    def __repr__(self) -> str:  # pragma: no cover
        return f"AdaptiveInterval(current={self.current:.2f}s, idle_cycles={self.idle_cycles})"
