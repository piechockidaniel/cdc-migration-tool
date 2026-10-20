"""Secret reference resolution.

Connection documents should not carry plaintext passwords. Instead a value can
be a *reference* that is resolved at connect time:

* ``${env:DB_PASSWORD}``      - environment variable
* ``${file:/run/secrets/pw}`` - file content (Docker/Kubernetes secrets)
* ``${vault:path#key}``       - placeholder for an external vault (hook below)

Anything else is returned unchanged, so existing configs keep working.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional

_REF = re.compile(r"^\$\{(?P<scheme>env|file|vault):(?P<ref>[^}]+)\}$")

SENSITIVE_KEYS = ("password", "pwd", "secret", "token", "api_key", "connection_string", "uri")

_vault_resolver: Optional[Callable[[str], str]] = None


def register_vault_resolver(fn: Callable[[str], str]) -> None:
    """Plug in an external secret manager (HashiCorp Vault, Azure Key Vault...)."""
    global _vault_resolver
    _vault_resolver = fn


def resolve_secret(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    m = _REF.match(value.strip())
    if not m:
        return value
    scheme, ref = m.group("scheme"), m.group("ref")
    if scheme == "env":
        resolved = os.environ.get(ref)
        if resolved is None:
            raise KeyError(f"environment variable {ref!r} referenced by a secret is not set")
        return resolved
    if scheme == "file":
        return Path(ref).read_text(encoding="utf-8").strip()
    if scheme == "vault":
        resolver = _vault_resolver
        if resolver is None:
            raise RuntimeError("vault secret referenced but no vault resolver registered")
        return resolver(ref)
    return value  # pragma: no cover


def resolve_secrets(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``doc`` with all secret references resolved."""
    out = {}
    for k, v in doc.items():
        if isinstance(v, dict):
            out[k] = resolve_secrets(v)
        else:
            out[k] = resolve_secret(v)
    return out


def redact(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Copy of ``doc`` safe for logging / API responses."""
    out = {}
    for k, v in doc.items():
        if isinstance(v, dict):
            out[k] = redact(v)
        elif any(s in k.lower() for s in SENSITIVE_KEYS) and v:
            out[k] = "***" if not (isinstance(v, str) and _REF.match(v)) else v
        else:
            out[k] = v
    return out
