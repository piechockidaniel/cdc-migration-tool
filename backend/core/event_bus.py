"""In-process publish/subscribe bus with optional external transport.

Why a bus instead of the original ``asyncio.Queue`` between monitor and engine:

* **Decoupling** - readers (connectors), appliers, the audit logger, the alert
  engine and the WebSocket broadcaster all subscribe to the same topics without
  knowing each other.
* **Back-pressure** - every subscription has a bounded queue; a slow consumer
  blocks the publisher instead of growing memory without limit (a real risk
  with a 16 TB source that can emit millions of changes per hour).
* **Transport abstraction** - ``ExternalTransport`` lets the same events be
  mirrored to Kafka/Confluent (``KafkaTransport``) so downstream consumers can
  be strangled-in one by one, which is the whole point of the pattern.

Topics are plain strings; wildcard ``*`` suffix is supported for subscribers
(``"changes.*"``).
"""
from __future__ import annotations

import fnmatch
import json
import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from backend.core import metrics

logger = logging.getLogger(__name__)


@dataclass
class Message:
    topic: str
    payload: Any
    key: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    published_at: float = field(default_factory=time.time)


class ExternalTransport:
    """Interface for mirroring messages to an external broker."""

    name = "none"

    def publish(self, message: Message) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover
        pass


class KafkaTransport(ExternalTransport):
    """Mirror messages to Kafka (Confluent Platform). Requires ``confluent-kafka``.

    Payloads are serialised as JSON; the message key is the event's table key
    so all changes of one table land in the same partition (ordering).
    """

    name = "kafka"

    def __init__(self, bootstrap_servers: str, topic_prefix: str = "cdcsync.", extra_config: Optional[Dict[str, Any]] = None,
                 producer=None):
        self.topic_prefix = topic_prefix
        if producer is not None:
            self._producer = producer
        else:  # pragma: no cover - needs the native library
            from confluent_kafka import Producer  # type: ignore[import-not-found]  # optional dependency
            conf = {"bootstrap.servers": bootstrap_servers, "enable.idempotence": True, "acks": "all"}
            conf.update(extra_config or {})
            self._producer = Producer(conf)

    def publish(self, message: Message) -> None:
        payload = message.payload
        if hasattr(payload, "to_json"):
            payload = payload.to_json()
        self._producer.produce(
            self.topic_prefix + message.topic.replace("*", "all"),
            key=(message.key or "").encode("utf-8"),
            value=json.dumps(payload, default=str).encode("utf-8"),
            headers=list(message.headers.items()) or None,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        try:
            self.flush()
        except Exception:  # pragma: no cover
            pass


class Subscription:
    def __init__(self, owner: "EventBus", pattern: str, maxsize: int, name: str):
        self.bus = owner
        self.pattern = pattern
        self.name = name
        self.queue: "queue.Queue[Optional[Message]]" = queue.Queue(maxsize=maxsize)
        self.delivered = 0
        self.dropped = 0
        self.active = True

    def matches(self, topic: str) -> bool:
        return fnmatch.fnmatchcase(topic, self.pattern)

    def get(self, timeout: Optional[float] = None) -> Optional[Message]:
        try:
            msg = self.queue.get(timeout=timeout)
        except queue.Empty:
            return None
        return msg

    def get_batch(self, max_items: int, timeout: float) -> List[Message]:
        """Collect up to ``max_items`` messages, waiting at most ``timeout`` for the first."""
        out: List[Message] = []
        first = self.get(timeout=timeout)
        if first is None:
            return out
        out.append(first)
        while len(out) < max_items:
            try:
                out.append(self.queue.get_nowait())
            except queue.Empty:
                break
        return [m for m in out if m is not None]

    def close(self) -> None:
        self.active = False
        self.bus.unsubscribe(self)
        try:
            self.queue.put_nowait(None)  # wake up blocked consumer
        except queue.Full:
            pass

    @property
    def depth(self) -> int:
        return self.queue.qsize()


class EventBus:
    def __init__(self, default_maxsize: int = 10_000, transports: Optional[List[ExternalTransport]] = None,
                 mirror_patterns: Optional[List[str]] = None):
        self._subs: List[Subscription] = []
        self._lock = threading.RLock()
        self.default_maxsize = default_maxsize
        self.transports = list(transports or [])
        self.mirror_patterns = list(mirror_patterns or ["changes.*"])
        self.published = 0
        self._callbacks: List[tuple] = []  # (pattern, fn) - synchronous observers (cheap, non-blocking)

    # -- subscriptions -----------------------------------------------------
    def subscribe(self, pattern: str, maxsize: Optional[int] = None, name: str = "") -> Subscription:
        sub = Subscription(self, pattern, maxsize or self.default_maxsize, name or pattern)
        with self._lock:
            self._subs.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        with self._lock:
            if sub in self._subs:
                self._subs.remove(sub)

    def on(self, pattern: str, fn: Callable[[Message], None]) -> None:
        """Register a lightweight synchronous observer (must not block)."""
        with self._lock:
            self._callbacks.append((pattern, fn))

    # -- publishing --------------------------------------------------------
    def publish(self, topic: str, payload: Any, key: Optional[str] = None, headers: Optional[Dict[str, str]] = None,
                block: bool = True, timeout: Optional[float] = None) -> int:
        msg = Message(topic, payload, key, headers or {})
        with self._lock:
            subs = [s for s in self._subs if s.active and s.matches(topic)]
            callbacks = [fn for pat, fn in self._callbacks if fnmatch.fnmatchcase(topic, pat)]
        delivered = 0
        for sub in subs:
            try:
                sub.queue.put(msg, block=block, timeout=timeout)
                sub.delivered += 1
                delivered += 1
            except queue.Full:
                sub.dropped += 1
                logger.warning("subscriber %s is full; dropping message on %s", sub.name, topic)
        for fn in callbacks:
            try:
                fn(msg)
            except Exception as exc:  # observers must never break publishing
                logger.error("bus observer failed on %s: %s", topic, exc)
        for transport in self.transports:
            if any(fnmatch.fnmatchcase(topic, p) for p in self.mirror_patterns):
                try:
                    transport.publish(msg)
                except Exception as exc:
                    logger.error("transport %s failed: %s", transport.name, exc)
        self.published += 1
        metrics.QUEUE_DEPTH.labels(pipeline="bus:" + topic.split(".")[0]).set(max((s.depth for s in subs), default=0))
        return delivered

    # -- ops ---------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            subs = list(self._subs)
        return {
            "published": self.published,
            "subscriptions": [
                {"name": s.name, "pattern": s.pattern, "depth": s.depth, "delivered": s.delivered, "dropped": s.dropped}
                for s in subs
            ],
            "transports": [t.name for t in self.transports],
        }

    def close(self) -> None:
        with self._lock:
            subs = list(self._subs)
        for s in subs:
            s.close()
        for t in self.transports:
            t.close()


# process-wide bus
bus = EventBus()
