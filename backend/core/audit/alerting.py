"""Alert manager: evaluates rules against a health context and notifies channels.

Life-cycle of an alert (per rule + scope instance):

    pending (for_seconds not reached) -> firing -> (cooldown suppresses repeats) -> resolved

State survives restarts through the config store so a restart does not
re-fire every active alert.
"""
from __future__ import annotations

import json
import logging
import smtplib
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Callable, Dict, List, Optional

from backend.core import metrics
from backend.core.audit.rules import BUILTIN_RULES, ExpressionError, Rule, SafeEvaluator

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    rule_id: str
    scope_key: str
    severity: str
    name: str
    message: str
    status: str = "firing"            # pending | firing | resolved
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    last_notified: float = 0.0
    notifications: int = 0
    labels: Dict[str, str] = field(default_factory=dict)
    facts: Dict[str, Any] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.rule_id}|{self.scope_key}"

    def to_json(self) -> Dict[str, Any]:
        d = asdict(self)
        d["first_seen_iso"] = datetime.fromtimestamp(self.first_seen, timezone.utc).isoformat()
        d["last_seen_iso"] = datetime.fromtimestamp(self.last_seen, timezone.utc).isoformat()
        return d


# ---------------------------------------------------------------------------
# channels
# ---------------------------------------------------------------------------
class AlertChannel:
    name = "base"

    def send(self, alert: Alert, resolved: bool = False) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class LogChannel(AlertChannel):
    name = "log"

    def send(self, alert: Alert, resolved: bool = False) -> None:
        level = {"critical": logging.CRITICAL, "error": logging.ERROR, "warning": logging.WARNING}.get(alert.severity, logging.INFO)
        logger.log(level, "ALERT %s [%s] %s (%s): %s", "RESOLVED" if resolved else "FIRING", alert.severity, alert.name,
                   alert.scope_key, alert.message)


class WebhookChannel(AlertChannel):
    """Generic JSON webhook (works with Slack/Teams incoming webhooks, Alertmanager-style receivers, n8n...)."""

    name = "webhook"

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None, timeout: float = 5.0, opener: Optional[Callable] = None):
        self.url = url
        self.headers = headers or {}
        self.timeout = timeout
        self._opener = opener or urllib.request.urlopen

    def send(self, alert: Alert, resolved: bool = False) -> None:
        body = {"status": "resolved" if resolved else "firing", "alert": alert.to_json(),
                "text": f"[{alert.severity.upper()}] {'RESOLVED: ' if resolved else ''}{alert.name} ({alert.scope_key}) - {alert.message}"}
        req = urllib.request.Request(self.url, data=json.dumps(body, default=str).encode("utf-8"),
                                     headers={"Content-Type": "application/json", **self.headers}, method="POST")
        self._opener(req, timeout=self.timeout)


class EmailChannel(AlertChannel):
    name = "email"

    def __init__(self, host: str, port: int, sender: str, recipients: List[str], username: Optional[str] = None,
                 password: Optional[str] = None, use_tls: bool = True, smtp_factory: Optional[Callable] = None):
        self.host, self.port, self.sender, self.recipients = host, port, sender, recipients
        self.username, self.password, self.use_tls = username, password, use_tls
        self._smtp_factory = smtp_factory or (lambda: smtplib.SMTP(self.host, self.port, timeout=10))

    def send(self, alert: Alert, resolved: bool = False) -> None:
        msg = EmailMessage()
        msg["Subject"] = f"[cdcsync][{alert.severity.upper()}] {'RESOLVED ' if resolved else ''}{alert.name} - {alert.scope_key}"
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg.set_content(f"{alert.message}\n\nfacts:\n{json.dumps(alert.facts, indent=2, default=str)}")
        with self._smtp_factory() as smtp:
            if self.use_tls:
                try:
                    smtp.starttls()
                except Exception:  # pragma: no cover
                    pass
            if self.username:
                smtp.login(self.username, self.password or "")
            smtp.send_message(msg)


class BusChannel(AlertChannel):
    """Publish to the in-process event bus (WebSocket UI, Kafka mirror)."""

    name = "bus"

    def __init__(self, bus, topic: str = "alerts"):
        self.bus = bus
        self.topic = topic

    def send(self, alert: Alert, resolved: bool = False) -> None:
        payload = alert.to_json()
        payload["status"] = "resolved" if resolved else alert.status
        self.bus.publish(self.topic, payload, key=alert.key, block=False)


class AuditChannel(AlertChannel):
    name = "audit"

    def __init__(self, audit_store):
        self.audit = audit_store

    def send(self, alert: Alert, resolved: bool = False) -> None:
        self.audit.record("alert", "alerting", f"{'RESOLVED' if resolved else 'FIRING'}: {alert.name} - {alert.message}",
                          severity="info" if resolved else alert.severity, table=alert.labels.get("table"),
                          status="resolved" if resolved else "firing", payload=alert.to_json())


# ---------------------------------------------------------------------------
# manager
# ---------------------------------------------------------------------------
class AlertManager:
    def __init__(self, rules: Optional[List[Rule]] = None, channels: Optional[List[AlertChannel]] = None,
                 state_store=None, enabled_rule_sets: Optional[List[str]] = None, notify_on_resolve: bool = True):
        self.rules: Dict[str, Rule] = {r.id: r for r in (rules if rules is not None else BUILTIN_RULES)}
        self.channels: List[AlertChannel] = list(channels or [LogChannel()])
        self.state_store = state_store  # ConfigStore (ALERT_STATE collection) - optional
        self.enabled_rule_sets = set(enabled_rule_sets) if enabled_rule_sets is not None else None
        self.notify_on_resolve = notify_on_resolve
        self.active: Dict[str, Alert] = {}
        self.history: List[Alert] = []
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.last_evaluation: Optional[float] = None
        self.evaluation_errors: Dict[str, str] = {}
        self._load_state()

    # -- persistence -------------------------------------------------------
    def _load_state(self) -> None:
        if not self.state_store:
            return
        try:
            from backend.storage.base import ALERT_STATE
            for doc in self.state_store.get_all(ALERT_STATE):
                b = doc.body
                if b.get("status") in ("firing", "pending"):
                    known = {f.name for f in fields(Alert)}
                    alert = Alert(**{k: v for k, v in b.items() if k in known})
                    self.active[alert.key] = alert
        except Exception as exc:  # pragma: no cover
            logger.warning("cannot load alert state: %s", exc)

    def _persist(self, alert: Alert) -> None:
        if not self.state_store:
            return
        try:
            from backend.storage.base import ALERT_STATE
            body = {k: v for k, v in asdict(alert).items()}
            self.state_store.put(ALERT_STATE, alert.key, body)
        except Exception as exc:  # pragma: no cover
            logger.warning("cannot persist alert state: %s", exc)

    # -- rules management --------------------------------------------------
    def upsert_rule(self, rule: Rule) -> None:
        with self._lock:
            self.rules[rule.id] = rule

    def remove_rule(self, rule_id: str) -> bool:
        with self._lock:
            return self.rules.pop(rule_id, None) is not None

    def rule_enabled(self, rule: Rule) -> bool:
        if not rule.enabled:
            return False
        if self.enabled_rule_sets is not None and rule.rule_set not in self.enabled_rule_sets:
            return False
        return True

    # -- evaluation --------------------------------------------------------
    def evaluate(self, contexts: Dict[str, Dict[str, Any]]) -> List[Alert]:
        """Evaluate all rules.

        ``contexts`` maps a scope key (``"global"``, ``"pipeline:<id>"``,
        ``"snapshot:<id>"``) to the facts dictionary for that scope. Rules
        with ``scope == "global"`` see the global context; per-* rules are
        evaluated once per matching scope key.
        """
        now = time.time()
        fired: List[Alert] = []
        seen_keys = set()
        with self._lock:
            for rule in list(self.rules.values()):
                if not self.rule_enabled(rule):
                    continue
                targets = self._targets(rule, contexts)
                for scope_key, ctx in targets:
                    key = f"{rule.id}|{scope_key}"
                    try:
                        hit = bool(SafeEvaluator(ctx).evaluate(rule.condition))
                        self.evaluation_errors.pop(rule.id, None)
                    except ExpressionError as exc:
                        self.evaluation_errors[rule.id] = str(exc)
                        continue
                    except Exception as exc:  # pragma: no cover
                        self.evaluation_errors[rule.id] = f"{type(exc).__name__}: {exc}"
                        continue
                    if hit:
                        seen_keys.add(key)
                        alert = self.active.get(key)
                        message = self._render(rule, ctx)
                        if alert is None:
                            alert = Alert(rule.id, scope_key, rule.severity, rule.name, message,
                                          status="pending" if rule.for_seconds else "firing", labels=dict(rule.labels),
                                          facts=self._facts(ctx))
                            self.active[key] = alert
                        alert.last_seen = now
                        alert.message = message
                        alert.facts = self._facts(ctx)
                        if alert.status == "pending" and now - alert.first_seen >= rule.for_seconds:
                            alert.status = "firing"
                        if alert.status == "firing" and (alert.notifications == 0 or now - alert.last_notified >= rule.cooldown_seconds):
                            self._notify(alert, resolved=False)
                            alert.last_notified = now
                            alert.notifications += 1
                            metrics.ALERTS_FIRED.labels(rule=rule.id, severity=rule.severity).inc()
                            fired.append(alert)
                        self._persist(alert)
            # resolve alerts whose condition cleared (only for scopes that were evaluated)
            evaluated_scopes = set(contexts.keys())
            for key, alert in list(self.active.items()):
                if key in seen_keys:
                    continue
                rule = self.rules.get(alert.rule_id)
                if rule is None or (alert.scope_key not in evaluated_scopes and alert.scope_key != "global"):
                    continue
                was_firing = alert.status == "firing"
                alert.status = "resolved"
                alert.last_seen = now
                del self.active[key]
                self.history.append(alert)
                self._persist(alert)
                if was_firing and self.notify_on_resolve:
                    self._notify(alert, resolved=True)
            self.history = self.history[-500:]
            self.last_evaluation = now
        return fired

    @staticmethod
    def _targets(rule: Rule, contexts: Dict[str, Dict[str, Any]]):
        if rule.scope == "global":
            return [("global", contexts.get("global", {}))]
        prefix = {"per_pipeline": "pipeline:", "per_table": "table:", "per_snapshot": "snapshot:"}.get(rule.scope, rule.scope + ":")
        out = []
        for key, ctx in contexts.items():
            if key.startswith(prefix):
                merged = dict(contexts.get("global", {}))
                merged.update(ctx)
                out.append((key, merged))
        return out

    @staticmethod
    def _render(rule: Rule, ctx: Dict[str, Any]) -> str:
        template = rule.message or rule.description or rule.name
        try:
            return template.format(**ctx)
        except Exception:
            return template

    @staticmethod
    def _facts(ctx: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        for k, v in ctx.items():
            if isinstance(v, dict):
                out[k] = {kk: vv for kk, vv in v.items() if isinstance(vv, (int, float, str, bool, type(None)))}
            elif isinstance(v, (int, float, str, bool, type(None))):
                out[k] = v
        return out

    def _notify(self, alert: Alert, resolved: bool) -> None:
        for ch in self.channels:
            try:
                ch.send(alert, resolved=resolved)
            except Exception as exc:
                logger.error("alert channel %s failed: %s", ch.name, exc)

    # -- background loop ---------------------------------------------------
    def start(self, context_provider: Callable[[], Dict[str, Dict[str, Any]]], interval_s: float = 30.0) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()

        def loop():
            while not self._stop.is_set():
                try:
                    self.evaluate(context_provider())
                except Exception as exc:  # pragma: no cover
                    logger.error("alert evaluation failed: %s", exc)
                self._stop.wait(interval_s)

        self._thread = threading.Thread(target=loop, name="alert-manager", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "active": [a.to_json() for a in self.active.values()],
                "recent_resolved": [a.to_json() for a in self.history[-50:]],
                "rules": [r.to_json() for r in self.rules.values()],
                "enabled_rule_sets": sorted(self.enabled_rule_sets) if self.enabled_rule_sets is not None else "all",
                "evaluation_errors": dict(self.evaluation_errors),
                "last_evaluation": self.last_evaluation,
                "channels": [c.name for c in self.channels],
            }
