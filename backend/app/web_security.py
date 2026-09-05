"""Small, deployment-safe owner gate for the first ProTreBot web preview.

This is intentionally separate from the commercial customer/session system.
It protects the entire API while the project is being deployed and tested by
its owner.  A later multi-tenant release can replace it with per-user scopes.
"""

from __future__ import annotations

import hmac
import json
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit


MIN_ACCESS_TOKEN_LENGTH = 24
PUBLIC_PATHS = frozenset({"/healthz", "/api/health"})
OWNER_REQUIRED_PATHS = frozenset({"/api/web/access/check"})
CUSTOMER_SESSION_PATH_PREFIXES = ("/api/v22", "/api/v24", "/api/v25")
LOCAL_BOOTSTRAP_HOSTS = frozenset({"127.0.0.1", "::1", "localhost", "testclient"})
VERCEL_PREVIEW_ORIGIN_RE = re.compile(r"^https://frontend-nu-two-18(?:-[a-z0-9]+)*\.vercel\.app$")


def env_flag(name: str, *, default: bool = False) -> bool:
    value = str(os.getenv(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on", "enabled"}


def cors_origins(value: str | None, *, fallback: list[str] | None = None) -> list[str]:
    raw_value = str(value or "").strip()
    if raw_value.startswith("[") and raw_value.endswith("]"):
        try:
            parsed_value = json.loads(raw_value)
            raw_items = parsed_value if isinstance(parsed_value, list) else [raw_value]
        except json.JSONDecodeError:
            raw_items = [raw_value]
    else:
        raw_items = raw_value.split(",")
    configured = []
    for item in raw_items:
        origin = str(item).strip().strip('"\'').rstrip("/")
        if not origin or origin == "*":
            continue
        try:
            parsed = urlsplit(origin)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.path
                or parsed.query
                or parsed.fragment
            ):
                continue
            parsed.port
        except ValueError:
            continue
        configured.append(origin)
    configured = list(dict.fromkeys(configured))
    return configured or (fallback if fallback is not None else ["http://localhost:5173", "http://127.0.0.1:5173"])


def is_allowed_cors_origin(origin: str | None, configured_origins: set[str] | list[str] | tuple[str, ...]) -> bool:
    normalized = str(origin or "").strip().rstrip("/")
    return normalized in configured_origins or bool(VERCEL_PREVIEW_ORIGIN_RE.fullmatch(normalized))


def bearer_token(value: str | None) -> str:
    header = str(value or "").strip()
    if not header.lower().startswith("bearer "):
        return ""
    return header.split(" ", 1)[1].strip()


def bootstrap_access_allowed(client_host: str | None, *, web_owner_authenticated: bool) -> bool:
    """Allow first-owner creation locally or behind the authenticated web gate.

    A public deployment may bootstrap exactly once, but only after the global
    owner preview token has already been verified by the API middleware.
    """
    host = str(client_host or "").strip().lower()
    return host in LOCAL_BOOTSTRAP_HOSTS or bool(web_owner_authenticated)


@dataclass(frozen=True)
class AccessDecision:
    allowed: bool
    status_code: int = 200
    detail: str = "OK"


def evaluate_access(
    *,
    required: bool,
    configured_token: str,
    authorization: str | None,
    path: str,
    method: str,
    owner_access: str | None = None,
) -> AccessDecision:
    """Return a deterministic decision without logging either secret."""
    if not required or method.upper() == "OPTIONS" or path in PUBLIC_PATHS:
        return AccessDecision(True)

    if any(path.startswith(prefix) for prefix in CUSTOMER_SESSION_PATH_PREFIXES):
        return AccessDecision(True)

    if len(configured_token) < MIN_ACCESS_TOKEN_LENGTH:
        return AccessDecision(False, 503, "Web erişim kilidi sunucuda tamamlanmamış.")

    provided = str(owner_access or "").strip() or bearer_token(authorization)
    if not provided:
        return AccessDecision(False, 401, "Yönetici erişim kodu gerekli.")
    if not hmac.compare_digest(provided, configured_token):
        return AccessDecision(False, 401, "Yönetici erişim kodu geçersiz.")
    return AccessDecision(True)
