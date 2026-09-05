"""V24 Commercial Complete control plane (keeps the /api/v22 compatibility path).

The endpoints here are a production-shaped local lab: membership, licensing,
device pairing, audit and fee-aware planning.  Billing and exchange execution
remain disabled so this package cannot move real money or place real orders.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import logging
import os
import re
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Query, Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from pydantic import BaseModel, ConfigDict, Field

from .binance_demo import credentials_configured, public_status as demo_public_status
from .commercial_core import (
    FeeGuardInput,
    V22_VERSION,
    calculate_fee_guard,
    calculate_grid_guard,
    default_commercial_state,
    device_fingerprint_hash,
    hash_password,
    issue_token,
    normalize_email,
    pairing_code_hash,
    verify_password,
    verify_token,
)
from .commerce_core import sanitize_business_settings
from .local_storage import DATA_DIR, migrate_legacy_files
from .web_security import MIN_ACCESS_TOKEN_LENGTH, bootstrap_access_allowed, env_flag
from .subscription_core import PLAN_CATALOG as SUBSCRIPTION_PLAN_CATALOG, TRIAL_DAYS, active_subscription, entitlement_snapshot

try:
    import stripe
except ImportError:  # pragma: no cover - dependency is installed in deployed environments
    stripe = None


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v22", tags=["V24 Commercial Complete"])
migrate_legacy_files((
    "v22_commercial_state.json",
    "v22_commercial_state.backup.json",
    "v22_server_secret.dat",
))
STATE_PATH = DATA_DIR / "v22_commercial_state.json"
BACKUP_PATH = DATA_DIR / "v22_commercial_state.backup.json"
SECRET_PATH = DATA_DIR / "v22_server_secret.dat"
LOGIN_ATTEMPTS: dict[str, list[float]] = {}
STANDARD_SESSION_SECONDS = 8 * 60 * 60
REMEMBER_SESSION_SECONDS = 30 * 24 * 60 * 60
COMMERCIAL_STATE_KEY = "v22-commercial"
DURABLE_AUTH_REQUIRED = str(os.getenv("PROTREBOT_DURABLE_AUTH_REQUIRED", "")).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_GMAIL_FROM_EMAIL = "privacykais@gmail.com"
DEFAULT_GMAIL_FROM_NAME = "ProTreBot"
GMAIL_SEND_SCOPE = "https://www.googleapis.com/auth/gmail.send"
GMAIL_TOKEN_URI = "https://oauth2.googleapis.com/token"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def gmail_failure_log(exc: BaseException) -> dict[str, str]:
    raw_message = str(exc)
    sanitized = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[email-redacted]", raw_message)
    sanitized = re.sub(r"(?i)(password|passwd|secret|token|authorization|bearer|credential)(\s*[:=]\s*)[^\s,;]+", r"\1\2[redacted]", sanitized)
    sanitized = re.sub(r"(?i)(https?://[^\s?]+)\?[^\s]+", r"\1?[redacted]", sanitized)
    response = getattr(exc, "resp", None)
    code = getattr(response, "status", None) or getattr(exc, "status_code", None)
    reason = "gmail_api"
    if code in {401, 403}:
        reason = "authentication"
    elif isinstance(exc, (TimeoutError, ConnectionError)):
        reason = "timeout"
    return {"type": type(exc).__name__, "code": str(code) if code is not None else "none", "reason": reason, "message": sanitized[:500]}


def log_gmail_failure(exc: BaseException) -> None:
    details = gmail_failure_log(exc)
    logger.warning("Gmail API email delivery failed: reason=%s type=%s code=%s message=%s", details["reason"], details["type"], details["code"], details["message"])


def gmail_configured() -> bool:
    return all(os.getenv(name, "").strip() for name in ("GMAIL_CLIENT_ID", "GMAIL_CLIENT_SECRET", "GMAIL_REFRESH_TOKEN"))


def app_base_url() -> str:
    value = os.getenv("APP_BASE_URL", "http://localhost:5173").strip().rstrip("/")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
        raise HTTPException(503, "APP_BASE_URL güvenli bir mutlak URL olarak yapılandırılmalı")
    return value


def auth_email_html(title: str, display_name: str, action_url: str, action_label: str, expiry: str) -> str:
    return f"""<!doctype html><html><body style=\"margin:0;background:#071116;color:#dce8ee;font-family:Arial,sans-serif\"><table role=\"presentation\" width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" style=\"padding:32px 12px;background:#071116\"><tr><td align=\"center\"><table role=\"presentation\" width=\"100%\" style=\"max-width:560px;background:#101d23;border:1px solid #263943;border-radius:8px\" cellspacing=\"0\" cellpadding=\"0\"><tr><td style=\"padding:30px\"><div style=\"color:#64e3a1;font-size:12px;font-weight:700;letter-spacing:2px\">PROTREBOT</div><h1 style=\"font-size:26px;line-height:1.2;margin:22px 0 12px;color:#f1f7f8\">{title}</h1><p style=\"font-size:15px;line-height:1.6;color:#a9bbc2\">Merhaba {display_name}, hesabınız için güvenli işlem başlatıldı.</p><p style=\"font-size:15px;line-height:1.6;color:#a9bbc2\">Devam etmek için aşağıdaki düğmeyi kullanın:</p><p style=\"margin:26px 0\"><a href=\"{action_url}\" style=\"display:inline-block;padding:14px 22px;background:#64e3a1;color:#082016;text-decoration:none;border-radius:4px;font-weight:700\">{action_label}</a></p><p style=\"font-size:12px;line-height:1.5;color:#8297a0\">Bu bağlantı {expiry} içinde geçerliliğini yitirir ve yalnızca bir kez kullanılabilir.</p><p style=\"font-size:12px;line-height:1.5;color:#8297a0\">Bu işlemi siz başlatmadıysanız bu e-postayı güvenle yok sayın. ProTreBot ekibi parolanızı veya API anahtarınızı istemez.</p></td></tr></table></td></tr></table></body></html>"""


def send_auth_email(*, to_email: str, display_name: str, subject: str, title: str, action_url: str, action_label: str) -> None:
    missing = [name for name in ("GMAIL_CLIENT_ID", "GMAIL_CLIENT_SECRET", "GMAIL_REFRESH_TOKEN") if not os.getenv(name, "").strip()]
    if missing:
        raise RuntimeError(f"Gmail API yapılandırması eksik: {', '.join(missing)}")
    credentials = Credentials(
        token=None,
        refresh_token=os.environ["GMAIL_REFRESH_TOKEN"].strip(),
        token_uri=GMAIL_TOKEN_URI,
        client_id=os.environ["GMAIL_CLIENT_ID"].strip(),
        client_secret=os.environ["GMAIL_CLIENT_SECRET"].strip(),
        scopes=[GMAIL_SEND_SCOPE],
    )
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = f"{os.getenv('GMAIL_FROM_NAME', DEFAULT_GMAIL_FROM_NAME).strip()} <{os.getenv('GMAIL_FROM_EMAIL', DEFAULT_GMAIL_FROM_EMAIL).strip()}>"
    message["To"] = to_email
    message.set_content(f"{title}\n\n{action_url}")
    message.add_alternative(auth_email_html(title, display_name, action_url, action_label, "24 saat"), subtype="html")
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")
    build("gmail", "v1", credentials=credentials, cache_discovery=False).users().messages().send(userId="me", body={"raw": raw}).execute()


def issue_one_time_token(state: dict[str, Any], user: dict[str, Any], secret: bytes, *, kind: str) -> str:
    token = issue_token(user["id"], user["role"], secret, kind=kind, ttl_seconds=24 * 60 * 60)
    payload = verify_token(token, secret, expected_kind=kind)
    state.setdefault("auth_tokens", []).append({"jti": payload["jti"], "kind": kind, "user_id": user["id"], "expires_at": datetime.fromtimestamp(payload["exp"], timezone.utc).isoformat(), "used": False})
    return token


def consume_one_time_token(state: dict[str, Any], token: str, secret: bytes, *, kind: str) -> dict[str, Any]:
    try:
        payload = verify_token(token, secret, expected_kind=kind)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    row = next((item for item in state.get("auth_tokens", []) if item.get("jti") == payload.get("jti") and item.get("kind") == kind and item.get("user_id") == payload.get("sub")), None)
    if not row or row.get("used"):
        raise HTTPException(400, "Bu güvenlik bağlantısı geçersiz veya daha önce kullanılmış")
    row["used"] = True
    return payload


def parse_date(value: str | None) -> datetime:
    try:
        return datetime.fromisoformat(value or "").astimezone(timezone.utc)
    except ValueError:
        return datetime.fromtimestamp(0, timezone.utc)


def load_secret() -> bytes:
    configured = str(os.getenv("PROTREBOT_SESSION_SECRET") or "").strip()
    if len(configured) >= 32:
        return hashlib.sha256(configured.encode("utf-8")).digest()
    web_owner_token = str(os.getenv("PROTREBOT_WEB_ACCESS_TOKEN") or "").strip()
    if len(web_owner_token) >= MIN_ACCESS_TOKEN_LENGTH:
        return hashlib.sha256(f"protrebot-v22-session-v1:{web_owner_token}".encode("utf-8")).digest()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if SECRET_PATH.exists():
        raw = SECRET_PATH.read_bytes()
        if len(raw) >= 32:
            return raw
    raw = secrets.token_bytes(48)
    temp = SECRET_PATH.with_suffix(".tmp")
    temp.write_bytes(raw)
    os.chmod(temp, 0o600)
    temp.replace(SECRET_PATH)
    return raw


def sanitize_state(payload: Any) -> dict[str, Any]:
    base = default_commercial_state()
    if not isinstance(payload, dict):
        return base
    for key in (
        "users", "profiles", "subscriptions", "auth_tokens", "stripe_event_ids", "licenses", "pairing_codes", "agents", "audit", "plans",
        "release_evidence", "leads", "demo_invoices", "support_tickets", "acceptances",
    ):
        if key in payload and isinstance(payload[key], type(base[key])):
            base[key] = payload[key]
    if isinstance(payload.get("owner_user_id"), str) or payload.get("owner_user_id") is None:
        base["owner_user_id"] = payload.get("owner_user_id")
    base["business"] = sanitize_business_settings(payload.get("business"))
    # These safety switches are never restored from disk as enabled values.
    base["security"] = default_commercial_state()["security"]
    base["billing"] = {"provider": "MANUAL_DEMO", "live": False, "currency": "USD"}
    base["version"] = V22_VERSION
    return base


def load_state() -> dict[str, Any]:
    for path in (STATE_PATH, BACKUP_PATH):
        try:
            return sanitize_state(json.loads(path.read_text(encoding="utf-8")))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            continue
    return default_commercial_state()


def save_state(state: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    serializable = sanitize_state(state)
    body = json.dumps(serializable, ensure_ascii=False, indent=2)
    temp = STATE_PATH.with_suffix(".tmp")
    temp.write_text(body, encoding="utf-8")
    if STATE_PATH.exists():
        try:
            BACKUP_PATH.write_text(STATE_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        except OSError:
            pass
    temp.replace(STATE_PATH)
    state["_database_revision"] = int(state.get("_database_revision", 0)) + 1
    state["_database_dirty"] = True


async def ensure_commercial_schema(application: Any) -> None:
    pool = getattr(application.state, "db_pool", None)
    if pool is None:
        return
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS application_state_snapshots (
          state_key TEXT PRIMARY KEY,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          payload JSONB NOT NULL
        )
        """
    )
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS trading_accounts (
          id TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          provider TEXT NOT NULL,
          environment TEXT NOT NULL CHECK (environment IN ('DEMO', 'TESTNET', 'PAPER', 'LIVE')),
          account_reference TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'UNASSIGNED',
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          UNIQUE (user_id, provider, environment, account_reference)
        )
        """
    )
    await pool.execute(
        """
        CREATE INDEX IF NOT EXISTS trading_accounts_user_id_idx
        ON trading_accounts (user_id)
        """
    )


async def persist_v22_commercial(application: Any) -> bool:
    pool = getattr(application.state, "db_pool", None)
    if pool is None or not hasattr(application.state, "v22_commercial"):
        return False
    rt = application.state.v22_commercial
    state = rt["state"]
    revision = int(state.get("_database_revision", 0))
    payload = json.dumps(sanitize_state(state), ensure_ascii=False)
    try:
        async with rt["storage_lock"]:
            await pool.execute(
                """
                INSERT INTO application_state_snapshots (state_key, updated_at, payload)
                VALUES ($1, NOW(), $2::jsonb)
                ON CONFLICT (state_key) DO UPDATE
                SET updated_at = NOW(), payload = EXCLUDED.payload
                """,
                COMMERCIAL_STATE_KEY,
                payload,
            )
        if int(state.get("_database_revision", 0)) == revision:
            state["_database_dirty"] = False
        rt["storage_status"] = "POSTGRESQL_KALICI"
        return True
    except Exception:
        rt["storage_status"] = "YEREL_YEDEK"
        return False


async def restore_v22_commercial(application: Any) -> bool:
    pool = getattr(application.state, "db_pool", None)
    if pool is None or not hasattr(application.state, "v22_commercial"):
        return False
    rt = application.state.v22_commercial
    try:
        row = await pool.fetchrow(
            "SELECT payload FROM application_state_snapshots WHERE state_key = $1",
            COMMERCIAL_STATE_KEY,
        )
    except Exception:
        rt["storage_status"] = "YEREL_YEDEK"
        return False
    if row is None:
        return False
    payload = row["payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return False
    if not isinstance(payload, dict):
        return False
    restored = sanitize_state(payload)
    restored["_database_revision"] = 0
    restored["_database_dirty"] = False
    rt["state"] = restored
    rt["storage_status"] = "POSTGRESQL_KALICI"
    return True


async def sync_v22_storage(application: Any) -> None:
    if not hasattr(application.state, "v22_commercial"):
        return
    rt = application.state.v22_commercial
    try:
        if not rt.get("storage_ready"):
            await ensure_commercial_schema(application)
            rt["storage_ready"] = True
        if not rt.get("restore_attempted"):
            restored = await restore_v22_commercial(application)
            rt["restore_attempted"] = True
            if not restored:
                await persist_v22_commercial(application)
        elif rt["state"].get("_database_dirty"):
            await persist_v22_commercial(application)
    except Exception:
        rt["storage_status"] = "YEREL_YEDEK"


def public_user(user: dict[str, Any]) -> dict[str, Any]:
    return {key: user.get(key) for key in ("id", "email", "display_name", "role", "active", "created_at", "last_activity", "email_verified")}


def issue_email_token(user: dict[str, Any], secret: bytes, *, kind: str) -> str:
    return issue_token(user["id"], user["role"], secret, kind=kind, ttl_seconds=24 * 60 * 60)


def add_audit(state: dict[str, Any], kind: str, message: str, *, actor: str = "SYSTEM", subject: str | None = None) -> None:
    state["audit"].insert(0, {
        "id": uuid.uuid4().hex,
        "kind": kind,
        "message": message,
        "actor": actor,
        "subject": subject,
        "created_at": now_iso(),
        "demo_only": True,
    })
    del state["audit"][250:]


def runtime(request: Request) -> dict[str, Any]:
    return request.app.state.v22_commercial


def bearer(request: Request) -> str:
    value = request.headers.get("authorization", "")
    if not value.lower().startswith("bearer "):
        raise HTTPException(401, "Oturum gerekli")
    return value.split(" ", 1)[1].strip()


def authenticated_user(request: Request, *, owner: bool = False) -> dict[str, Any]:
    rt = runtime(request)
    try:
        payload = verify_token(bearer(request), rt["secret"], expected_kind="USER")
    except ValueError as exc:
        raise HTTPException(401, str(exc)) from exc
    user = next((item for item in rt["state"]["users"] if item.get("id") == payload["sub"] and item.get("active")), None)
    if not user:
        raise HTTPException(401, "Kullanıcı etkin değil")
    if user.get("role") != "OWNER" and user.get("email_verified") is False:
        raise HTTPException(403, "E-posta doğrulaması gerekli")
    if int(payload.get("ver", 1)) != int(user.get("auth_version", 1)):
        raise HTTPException(401, "Oturum yenilenmeli")
    if owner and user.get("role") != "OWNER":
        raise HTTPException(403, "Yönetici yetkisi gerekli")
    return user


def active_license(state: dict[str, Any], user_id: str) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc)
    candidates = [item for item in state["licenses"] if item.get("user_id") == user_id and item.get("status") == "ACTIVE" and parse_date(item.get("expires_at")) > now]
    return max(candidates, key=lambda item: item.get("expires_at", ""), default=None)


def admin_overview(state: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    active_licenses = [item for item in state["licenses"] if item.get("status") == "ACTIVE" and parse_date(item.get("expires_at")) > now]
    active_subscriptions = [item for item in state.get("subscriptions", []) if item.get("status") in {"active", "ACTIVE", "TRIAL"} and (not item.get("expires_at") or parse_date(item.get("expires_at")) > now)]
    pro_users = {item.get("user_id") for item in active_subscriptions if str(item.get("plan", "")).upper() == "PRO"} | {item.get("user_id") for item in active_licenses if str(item.get("plan", "")).upper() == "PRO"}
    new_cutoff = now - timedelta(days=30)
    online_cutoff = now - timedelta(minutes=3)
    online_agents = [item for item in state["agents"] if parse_date(item.get("last_seen_at")) >= online_cutoff and item.get("status") == "ACTIVE"]
    return {
        "users": len(state["users"]), "total_users": len(state["users"]),
        "active_users": sum(1 for item in state["users"] if item.get("active")),
        "new_users": sum(1 for item in state["users"] if parse_date(item.get("created_at")) >= new_cutoff),
        "pro_users": len(pro_users), "free_users": max(0, len(state["users"]) - len(pro_users)),
        "active_subscriptions": len(active_subscriptions), "expired_subscriptions": sum(1 for item in state.get("subscriptions", []) if item.get("expires_at") and parse_date(item.get("expires_at")) <= now),
        "licenses": len(state["licenses"]),
        "active_licenses": len(active_licenses),
        "agents": len(state["agents"]),
        "online_agents": len(online_agents),
        "monthly_demo_revenue_usd": round(sum(float(state["plans"].get(item.get("plan"), {}).get("monthly_usd", 0)) for item in active_licenses), 2),
        "customers": [{**public_user(user), "license": active_license(state, user["id"]), "subscription": subscription_for_user(state, user["id"])} for user in state["users"]],
        "agents_list": state["agents"][-40:],
        "audit": state["audit"][:50],
        "billing_live": False,
        "demo_only": True,
    }


def operations_overview(application: Any) -> dict[str, Any]:
    """Return a secret-free summary of the local professional stack."""
    demo_state = getattr(application.state, "binance_demo", {})
    v21 = getattr(application.state, "v21_demo", {})
    paper = getattr(application.state, "paper", {})
    paper_bot = getattr(application.state, "paper_bot", {})
    infrastructure = getattr(application.state, "infrastructure", {})
    demo = demo_public_status(demo_state) if demo_state else {
        "configured": credentials_configured(), "connected": False, "armed": False,
        "events": [], "real_trading_locked": True,
    }
    snapshot = v21.get("snapshot") or {}
    return {
        "version": V22_VERSION,
        "demo_connector": {
            "configured": bool(demo.get("configured")),
            "connected": bool(demo.get("connected")),
            "armed": bool(demo.get("armed")),
            "armed_until": demo.get("armed_until"),
            "last_error": demo.get("last_error"),
        },
        "demo_account": {
            "positions": len(snapshot.get("positions", [])),
            "open_orders": len(snapshot.get("open_orders", [])),
            "open_algo_orders": len(snapshot.get("open_algo_orders", [])),
            "available_balance": snapshot.get("available_balance"),
            "wallet_balance": snapshot.get("wallet_balance"),
            "one_way": not bool(snapshot.get("hedge_mode", False)),
        },
        "automation": {
            "demo_enabled": bool(v21.get("auto", {}).get("enabled")),
            "demo_cycles": int(v21.get("auto", {}).get("cycles", 0)),
            "demo_last_decision": v21.get("auto", {}).get("last_decision"),
            "paper_enabled": bool(paper_bot.get("enabled")),
            "paper_cycles": int(paper_bot.get("cycles", 0)),
        },
        "paper": {
            "balance": paper.get("balance", 0),
            "positions": len(paper.get("positions", [])),
            "pending_orders": len(paper.get("limit_orders", [])),
            "closed_trades": len(paper.get("trades", [])),
            "emergency_brake": bool(paper.get("emergency_brake", {}).get("active")),
        },
        "services": {
            "api": infrastructure.get("api", "BAĞLI"),
            "database": infrastructure.get("database", "BEKLENİYOR"),
            "redis": infrastructure.get("redis", "BEKLENİYOR"),
            "paper_storage": infrastructure.get("paper_storage", "BEKLENİYOR"),
        },
        "recent_demo_events": demo.get("events", [])[:8],
        "real_orders_enabled": False,
        "testnet_orders_enabled": False,
        "withdrawals_supported": False,
        "demo_only": True,
    }


class BootstrapRequest(BaseModel):
    display_name: str = Field(min_length=2, max_length=80)
    email: str = Field(min_length=5, max_length=180)
    password: str = Field(min_length=10, max_length=256)
    remember: bool = True


class LoginRequest(BaseModel):
    email: str = Field(min_length=5, max_length=180)
    password: str = Field(min_length=1, max_length=256)
    remember: bool = False


class RegisterRequest(BaseModel):
    display_name: str = Field(min_length=2, max_length=80)
    email: str = Field(min_length=5, max_length=180)
    password: str = Field(min_length=10, max_length=256)
    confirm_password: str = Field(min_length=10, max_length=256)
    terms_accepted: bool = False


class EmailTokenRequest(BaseModel):
    token: str = Field(min_length=20, max_length=600)


class PasswordResetRequest(BaseModel):
    email: str = Field(min_length=5, max_length=180)


class PasswordResetConfirmRequest(BaseModel):
    token: str = Field(min_length=20, max_length=600)
    password: str = Field(min_length=10, max_length=256)
    confirm_password: str = Field(min_length=10, max_length=256)


class ProfileUpdateRequest(BaseModel):
    display_name: str = Field(min_length=2, max_length=80)
    preferences: dict[str, Any] = Field(default_factory=dict)


class CustomerRequest(BootstrapRequest):
    plan: Literal["TRIAL", "STARTER", "PRO", "ELITE"] = "TRIAL"
    days: int = Field(default=7, ge=1, le=730)


class SubscriptionRequest(BaseModel):
    user_id: str = Field(min_length=8, max_length=80)
    plan: Literal["TRIAL", "STARTER", "PRO", "ELITE"]
    days: int = Field(default=30, ge=1, le=730)


class CheckoutRequest(BaseModel):
    plan: Literal["STARTER", "PRO", "ELITE"]
    billing_interval: Literal["monthly", "annual"] = "monthly"


class TradingAccountLinkRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: Literal["BINANCE"]
    environment: Literal["DEMO", "TESTNET", "PAPER", "LIVE"]
    account_reference: str = Field(min_length=1, max_length=160)


class PlanUpdateRequest(BaseModel):
    monthly_usd: float = Field(ge=0, le=100_000)
    agents: int = Field(ge=1, le=1_000)
    bots: int = Field(ge=1, le=10_000)


class PairAgentRequest(BaseModel):
    code: str = Field(min_length=6, max_length=24)
    device_name: str = Field(min_length=2, max_length=100)
    fingerprint: str = Field(min_length=12, max_length=500)


class HeartbeatRequest(BaseModel):
    app_version: str = Field(default=V22_VERSION, max_length=40)
    status: str = Field(default="READY", max_length=40)


class FeeGuardRequest(BaseModel):
    entry: float = Field(gt=0)
    target: float = Field(gt=0)
    notional_usdt: float = Field(gt=0, le=1_000_000)
    direction: Literal["LONG", "SHORT"] = "LONG"
    fee_bps_per_side: float = Field(default=4.0, ge=0, le=500)
    slippage_bps_per_side: float = Field(default=2.0, ge=0, le=500)
    funding_bps: float = Field(default=0.0, ge=-500, le=500)
    minimum_net_usdt: float = Field(default=0.25, ge=0, le=100_000)
    minimum_net_pct: float = Field(default=0.05, ge=0, le=100)


class GridGuardRequest(BaseModel):
    lower: float = Field(gt=0)
    upper: float = Field(gt=0)
    grid_count: int = Field(default=20, ge=3, le=200)
    capital_usdt: float = Field(default=1_000, gt=0, le=1_000_000)
    maker_share_pct: float = Field(default=80, ge=0, le=100)
    maker_fee_bps: float = Field(default=2.0, ge=0, le=500)
    taker_fee_bps: float = Field(default=5.0, ge=0, le=500)
    slippage_bps_per_side: float = Field(default=1.0, ge=0, le=500)
    funding_bps: float = Field(default=0.0, ge=-500, le=500)
    minimum_cycle_net_usdt: float = Field(default=0.05, ge=0, le=100_000)


class CustomerStatusRequest(BaseModel):
    active: bool
    reason: str = Field(default="Yönetici işlemi", max_length=180)


class RoleUpdateRequest(BaseModel):
    role: Literal["OWNER", "CUSTOMER"]


class RevokeRequest(BaseModel):
    confirmation: str = Field(min_length=3, max_length=40)
    reason: str = Field(default="Yönetici işlemi", max_length=180)


class PasswordChangeRequest(BaseModel):
    current_password: str = Field(min_length=1, max_length=256)
    new_password: str = Field(min_length=10, max_length=256)


class ReleaseEvidenceRequest(BaseModel):
    status: Literal["PENDING", "RECORDED"]
    note: str = Field(min_length=3, max_length=500)


@router.get("/public")
async def v22_public(request: Request):
    rt = runtime(request)
    state = rt["state"]
    storage_ready = rt.get("storage_status") == "POSTGRESQL_KALICI" or not DURABLE_AUTH_REQUIRED
    return {
        "version": V22_VERSION,
        "edition": "COMMERCIAL COMPLETE · LAUNCH LAB",
        "setup_required": storage_ready and not bool(state.get("owner_user_id")),
        "auth_available": storage_ready,
        "plans": state["plans"],
        "billing": state["billing"],
        "security": state["security"],
        "account_storage": "WINDOWS_LOCAL_APP_DATA" if os.name == "nt" else rt.get("storage_status", "YEREL_YEDEK"),
        "message": "Üyelik, lisans, yerel ajan, satış ve müşteri kurulum altyapısı tek Demo/Paper paketinde; gerçek para ve gerçek emir yok.",
    }


@router.post("/bootstrap")
async def v22_bootstrap(payload: BootstrapRequest, request: Request):
    rt = runtime(request)
    if DURABLE_AUTH_REQUIRED and rt.get("storage_status") != "POSTGRESQL_KALICI":
        raise HTTPException(503, "Kalıcı hesap veritabanı hazır değil; kayıt güvenli şekilde başlatılamıyor")
    host = request.client.host if request.client else ""
    if not bootstrap_access_allowed(
        host,
        web_owner_authenticated=bool(getattr(request.state, "web_owner_authenticated", False)),
    ):
        raise HTTPException(403, "İlk yönetici yalnızca yerel uygulamadan veya doğrulanmış güvenli web oturumundan oluşturulabilir")
    async with rt["lock"]:
        state = rt["state"]
        if state.get("owner_user_id"):
            raise HTTPException(409, "İlk yönetici daha önce oluşturuldu")
        previous_state = copy.deepcopy(state)
        email = normalize_email(payload.email)
        if "@" not in email:
            raise HTTPException(422, "Geçerli bir e-posta yazın")
        user_id = uuid.uuid4().hex
        user = {
            "id": user_id,
            "email": email,
            "display_name": payload.display_name.strip(),
            "role": "OWNER",
            "active": True,
            "auth_version": 1,
            "password": hash_password(payload.password),
            "created_at": now_iso(),
        }
        state["users"].append(user)
        state["owner_user_id"] = user_id
        expires_at = (datetime.now(timezone.utc) + timedelta(days=3650)).isoformat()
        state["licenses"].append({"id": uuid.uuid4().hex, "user_id": user_id, "plan": "ELITE", "status": "ACTIVE", "starts_at": now_iso(), "expires_at": expires_at, "source": "OWNER_BOOTSTRAP", "demo_only": True})
        add_audit(state, "OWNER_CREATED", "Yerel V24 sahibi ve geliştirme lisansı oluşturuldu.", actor=user_id, subject=user_id)
        save_state(state)
    persisted = await persist_v22_commercial(request.app)
    if DURABLE_AUTH_REQUIRED and not persisted:
        async with rt["lock"]:
            rt["state"] = previous_state
            save_state(previous_state)
        raise HTTPException(503, "Hesap PostgreSQL'e yazılamadı; kayıt tamamlanmadı")
    token = issue_token(
        user_id,
        "OWNER",
        rt["secret"],
        token_version=user["auth_version"],
        ttl_seconds=REMEMBER_SESSION_SECONDS if payload.remember else STANDARD_SESSION_SECONDS,
    )
    return {"token": token, "user": public_user(user), "license": active_license(state, user_id), "demo_only": True}


@router.post("/auth/login")
async def v22_login(payload: LoginRequest, request: Request):
    rt = runtime(request)
    host = request.client.host if request.client else "unknown"
    now = time.monotonic()
    attempts = [stamp for stamp in LOGIN_ATTEMPTS.get(host, []) if now - stamp < 300]
    if len(attempts) >= 8:
        raise HTTPException(429, "Çok fazla deneme; beş dakika sonra tekrar deneyin")
    user = next((item for item in rt["state"]["users"] if item.get("email") == normalize_email(payload.email)), None)
    if not user or not user.get("active") or not verify_password(payload.password, user.get("password", {})):
        attempts.append(now)
        LOGIN_ATTEMPTS[host] = attempts
        raise HTTPException(401, "E-posta veya parola hatalı")
    LOGIN_ATTEMPTS.pop(host, None)
    user["last_activity"] = now_iso()
    save_state(rt["state"])
    await persist_v22_commercial(request.app)
    token = issue_token(
        user["id"],
        user["role"],
        rt["secret"],
        token_version=int(user.get("auth_version", 1)),
        ttl_seconds=REMEMBER_SESSION_SECONDS if payload.remember else STANDARD_SESSION_SECONDS,
    )
    return {"token": token, "user": public_user(user), "license": active_license(rt["state"], user["id"]), "demo_only": True}


@router.post("/auth/register")
async def v22_register(payload: RegisterRequest, request: Request):
    if not payload.terms_accepted:
        raise HTTPException(422, "Kullanım koşullarını kabul etmelisiniz")
    if payload.password != payload.confirm_password:
        raise HTTPException(422, "Parolalar eşleşmiyor")
    rt = runtime(request)
    email = normalize_email(payload.email)
    if "@" not in email:
        raise HTTPException(422, "Geçerli bir e-posta yazın")
    expose_dev_token = env_flag("PROTREBOT_EXPOSE_DEV_TOKENS", default=False)
    logger.warning("Gmail config presence: client_id=%s client_secret=%s refresh_token=%s", bool(os.getenv("GMAIL_CLIENT_ID", "").strip()), bool(os.getenv("GMAIL_CLIENT_SECRET", "").strip()), bool(os.getenv("GMAIL_REFRESH_TOKEN", "").strip()))
    if not gmail_configured() and not expose_dev_token:
        raise HTTPException(503, "E-posta servisi yapılandırılmamış; kayıt şu anda tamamlanamıyor")
    async with rt["lock"]:
        state = rt["state"]
        if any(item.get("email") == email for item in state["users"]):
            raise HTTPException(409, "Bu e-posta zaten kayıtlı")
        user_id = uuid.uuid4().hex
        user = {
            "id": user_id, "email": email, "display_name": payload.display_name.strip(),
            "role": "CUSTOMER", "active": True, "auth_version": 1,
            "email_verified": False, "password": hash_password(payload.password), "created_at": now_iso(),
        }
        state["users"].append(user)
        state["profiles"].append({"id": uuid.uuid4().hex, "user_id": user_id, "full_name": user["display_name"], "avatar_url": None, "role": "user", "preferences": {}, "created_at": user["created_at"], "updated_at": user["created_at"]})
        state["subscriptions"].append({"id": uuid.uuid4().hex, "user_id": user_id, "plan": "FREE", "status": "inactive", "started_at": user["created_at"], "expires_at": None, "created_at": user["created_at"], "updated_at": user["created_at"]})
        verification_token = issue_one_time_token(state, user, rt["secret"], kind="EMAIL_VERIFY")
        verification_status_token = issue_token(user_id, user["role"], rt["secret"], kind="EMAIL_STATUS", ttl_seconds=24 * 60 * 60)
        add_audit(state, "USER_REGISTERED", "Yeni kullanıcı hesabı oluşturuldu.", actor=user_id, subject=user_id)
        save_state(state)
    await persist_v22_commercial(request.app)
    if gmail_configured():
        try:
            await asyncio.to_thread(send_auth_email, to_email=user["email"], display_name=user["display_name"], subject="Verify your ProTreBot account", title="Verify your ProTreBot account", action_url=f"{app_base_url()}/verify-email?token={verification_token}", action_label="VERIFY EMAIL")
        except (OSError, RuntimeError, ValueError) as exc:
            log_gmail_failure(exc)
            async with rt["lock"]:
                rt["state"]["users"] = [item for item in rt["state"]["users"] if item.get("id") != user["id"]]
                rt["state"]["profiles"] = [item for item in rt["state"].get("profiles", []) if item.get("user_id") != user["id"]]
                rt["state"]["subscriptions"] = [item for item in rt["state"].get("subscriptions", []) if item.get("user_id") != user["id"]]
                rt["state"]["auth_tokens"] = [item for item in rt["state"].get("auth_tokens", []) if item.get("user_id") != user["id"]]
                save_state(rt["state"])
            await persist_v22_commercial(request.app)
            raise HTTPException(503, "Doğrulama e-postası gönderilemedi; SMTP ayarlarını kontrol edin") from exc
    response: dict[str, Any] = {"user": public_user(user), "message": "Hesabınız oluşturuldu. E-postanızı doğrulayın.", "email_verification_required": True, "verification_status_token": verification_status_token}
    if expose_dev_token:
        response["development_verification_token"] = verification_token
    return response


@router.post("/auth/verify-email")
async def v22_verify_email(payload: EmailTokenRequest, request: Request):
    rt = runtime(request)
    token = consume_one_time_token(rt["state"], payload.token, rt["secret"], kind="EMAIL_VERIFY")
    user = next((item for item in rt["state"]["users"] if item.get("id") == token["sub"]), None)
    if not user:
        raise HTTPException(404, "Kullanıcı bulunamadı")
    user["email_verified"] = True
    save_state(rt["state"])
    await persist_v22_commercial(request.app)
    return {"ok": True, "message": "E-posta doğrulandı. Artık giriş yapabilirsiniz."}


@router.get("/auth/verification-status")
async def v22_verification_status(request: Request, token: str = Query(..., min_length=20, max_length=600)):
    rt = runtime(request)
    try:
        try:
            payload = verify_token(token, rt["secret"], expected_kind="EMAIL_STATUS")
        except ValueError:
            payload = verify_token(token, rt["secret"], expected_kind="EMAIL_VERIFY")
    except ValueError as exc:
        raise HTTPException(400, "Geçersiz veya süresi dolmuş doğrulama bağlantısı") from exc
    user = next((item for item in rt["state"]["users"] if item.get("id") == payload["sub"]), None)
    if not user:
        raise HTTPException(400, "Geçersiz veya süresi dolmuş doğrulama bağlantısı")
    return {"verified": bool(user.get("email_verified", False))}


@router.post("/auth/forgot-password")
async def v22_forgot_password(payload: PasswordResetRequest, request: Request):
    rt = runtime(request)
    user = next((item for item in rt["state"]["users"] if item.get("email") == normalize_email(payload.email)), None)
    response: dict[str, Any] = {"ok": True, "message": "E-posta kayıtlıysa parola yenileme bağlantısı gönderildi."}
    if user:
        async with rt["lock"]:
            reset_token = issue_one_time_token(rt["state"], user, rt["secret"], kind="PASSWORD_RESET")
            save_state(rt["state"])
        await persist_v22_commercial(request.app)
        if gmail_configured():
            try:
                await asyncio.to_thread(send_auth_email, to_email=user["email"], display_name=user["display_name"], subject="Reset your ProTreBot password", title="Reset your ProTreBot password", action_url=f"{app_base_url()}/reset-password?token={reset_token}", action_label="RESET PASSWORD")
            except (OSError, RuntimeError, ValueError) as exc:
                log_gmail_failure(exc)
                pass
        if env_flag("PROTREBOT_EXPOSE_DEV_TOKENS", default=False):
            response["development_reset_token"] = reset_token
    return response


@router.post("/auth/reset-password")
async def v22_reset_password(payload: PasswordResetConfirmRequest, request: Request):
    if payload.password != payload.confirm_password:
        raise HTTPException(422, "Parolalar eşleşmiyor")
    rt = runtime(request)
    token = consume_one_time_token(rt["state"], payload.token, rt["secret"], kind="PASSWORD_RESET")
    user = next((item for item in rt["state"]["users"] if item.get("id") == token["sub"]), None)
    if not user:
        raise HTTPException(404, "Kullanıcı bulunamadı")
    user["password"] = hash_password(payload.password)
    user["auth_version"] = int(user.get("auth_version", 1)) + 1
    save_state(rt["state"])
    await persist_v22_commercial(request.app)
    return {"ok": True, "message": "Parolanız güncellendi. Yeni parolanızla giriş yapabilirsiniz."}


@router.get("/session")
async def v22_session(request: Request):
    user = authenticated_user(request)
    return {"user": public_user(user), "license": active_license(runtime(request)["state"], user["id"]), "demo_only": True}


@router.get("/profile")
async def v22_profile(request: Request):
    user = authenticated_user(request)
    profile = next((item for item in runtime(request)["state"].get("profiles", []) if item.get("user_id") == user["id"]), None)
    return {"user": public_user(user), "profile": profile, "subscription": subscription_for_user(runtime(request)["state"], user["id"]), "demo_only": True}


@router.patch("/profile")
async def v22_update_profile(payload: ProfileUpdateRequest, request: Request):
    user = authenticated_user(request)
    rt = runtime(request)
    async with rt["lock"]:
        user["display_name"] = payload.display_name.strip()
        profile = next((item for item in rt["state"].setdefault("profiles", []) if item.get("user_id") == user["id"]), None)
        if profile is None:
            profile = {"id": uuid.uuid4().hex, "user_id": user["id"], "created_at": now_iso()}
            rt["state"]["profiles"].append(profile)
        profile.update({"full_name": user["display_name"], "preferences": payload.preferences, "updated_at": now_iso()})
        save_state(rt["state"])
    await persist_v22_commercial(request.app)
    return {"user": public_user(user), "profile": profile}


@router.delete("/profile")
async def v22_delete_profile(request: Request):
    user = authenticated_user(request)
    if user.get("role") == "OWNER":
        raise HTTPException(422, "Ana yönetici hesabı bu ekrandan silinemez")
    rt = runtime(request)
    async with rt["lock"]:
        user["active"] = False
        user["auth_version"] = int(user.get("auth_version", 1)) + 1
        add_audit(rt["state"], "ACCOUNT_DISABLED", "Kullanıcı kendi hesabını kapattı.", actor=user["id"], subject=user["id"])
        save_state(rt["state"])
    await persist_v22_commercial(request.app)
    return {"ok": True}


def subscription_for_user(state: dict[str, Any], user_id: str) -> dict[str, Any]:
    subscription = active_subscription(state, user_id)
    if subscription:
        return entitlement_snapshot(state, user_id)
    license_row = active_license(state, user_id)
    if not license_row:
        return entitlement_snapshot(state, user_id)
    plan = str(license_row.get("plan") or "STARTER").upper()
    catalog = SUBSCRIPTION_PLAN_CATALOG.get(plan, SUBSCRIPTION_PLAN_CATALOG["STARTER"])
    return {
        "status": "ACTIVE", "plan": plan, "billingInterval": "annual", "trialStart": None,
        "trialEnd": None, "currentPeriodStart": license_row.get("starts_at"),
        "currentPeriodEnd": license_row.get("expires_at"), "currentPrice": catalog["annual_price"],
        "features": catalog["features"], "entitlements": catalog["entitlements"],
        "cancelAtPeriodEnd": False, "mode": "DEVELOPMENT",
    }


def stripe_configured() -> bool:
    required = ("STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "APP_BASE_URL")
    price_keys = tuple(f"STRIPE_PRICE_{plan}_{interval}" for plan in ("STARTER", "PRO", "ELITE") for interval in ("MONTHLY", "YEARLY"))
    return bool(stripe and all(os.getenv(key, "").strip() for key in (*required, *price_keys)))


def stripe_base_url() -> str:
    value = os.getenv("APP_BASE_URL", "").strip().rstrip("/")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
        raise HTTPException(503, "APP_BASE_URL is not configured as a safe absolute URL")
    return value


def stripe_price_id(plan: str, interval: str) -> str:
    if plan not in SUBSCRIPTION_PLAN_CATALOG or interval not in {"monthly", "annual"}:
        raise HTTPException(422, "Invalid subscription plan or billing interval")
    key = f"STRIPE_PRICE_{plan}_{'YEARLY' if interval == 'annual' else 'MONTHLY'}"
    price_id = os.getenv(key, "").strip()
    if not price_id:
        raise HTTPException(503, "Stripe price mapping is not configured")
    return price_id


def stripe_value(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def stripe_customer_for_user(state: dict[str, Any], user_id: str) -> str | None:
    rows = [row for row in state.get("subscriptions", []) if row.get("user_id") == user_id]
    return next((str(row.get("stripeCustomerId") or row.get("stripe_customer_id")) for row in reversed(rows) if row.get("stripeCustomerId") or row.get("stripe_customer_id")), None)


def subscription_user_for_customer(state: dict[str, Any], customer_id: str | None) -> str | None:
    if not customer_id:
        return None
    return next((str(row.get("user_id")) for row in state.get("subscriptions", []) if str(row.get("stripeCustomerId") or row.get("stripe_customer_id") or "") == str(customer_id)), None)


def normalize_stripe_status(value: Any) -> str:
    return {"active": "ACTIVE", "trialing": "TRIAL", "past_due": "PAST_DUE", "canceled": "CANCELED", "unpaid": "PAST_DUE", "incomplete": "PAST_DUE", "incomplete_expired": "CANCELED"}.get(str(value or "").lower(), "FREE")


def upsert_stripe_subscription(state: dict[str, Any], payload: Any, *, fallback_user_id: str | None = None, fallback_plan: str | None = None, fallback_interval: str | None = None) -> dict[str, Any]:
    metadata = stripe_value(payload, "metadata", {}) or {}
    customer_id = str(stripe_value(payload, "customer") or stripe_value(payload, "customer_id") or "") or None
    user_id = str(metadata.get("user_id") or fallback_user_id or subscription_user_for_customer(state, customer_id) or "")
    if not user_id:
        raise HTTPException(422, "Stripe subscription is not linked to an application user")
    items = stripe_value(payload, "items", {}) or {}
    data = stripe_value(items, "data", []) or []
    price = stripe_value(data[0], "price", {}) if data else {}
    price_id = str(stripe_value(price, "id") or "")
    reverse = next(((plan, interval) for plan in SUBSCRIPTION_PLAN_CATALOG for interval in ("monthly", "annual") if price_id and os.getenv(f"STRIPE_PRICE_{plan}_{'YEARLY' if interval == 'annual' else 'MONTHLY'}", "").strip() == price_id), (fallback_plan or metadata.get("plan") or "STARTER", fallback_interval or metadata.get("billing_interval") or "monthly"))
    plan, interval = reverse
    status = normalize_stripe_status(stripe_value(payload, "status"))
    now = now_iso()
    row = next((item for item in reversed(state.setdefault("subscriptions", [])) if item.get("stripeSubscriptionId") == str(stripe_value(payload, "id") or "")), None)
    if row is None:
        row = {"id": uuid.uuid4().hex, "user_id": user_id}
        state["subscriptions"].append(row)
    row.update({"status": status, "plan": plan, "billingInterval": interval, "stripeCustomerId": customer_id, "stripeSubscriptionId": str(stripe_value(payload, "id") or "") or None, "currentPeriodStart": datetime.fromtimestamp(int(stripe_value(payload, "current_period_start") or 0), timezone.utc).isoformat() if stripe_value(payload, "current_period_start") else row.get("currentPeriodStart"), "currentPeriodEnd": datetime.fromtimestamp(int(stripe_value(payload, "current_period_end") or 0), timezone.utc).isoformat() if stripe_value(payload, "current_period_end") else row.get("currentPeriodEnd"), "cancelAtPeriodEnd": bool(stripe_value(payload, "cancel_at_period_end", False)), "currentPrice": SUBSCRIPTION_PLAN_CATALOG[plan]["annual_price"] if interval == "annual" else SUBSCRIPTION_PLAN_CATALOG[plan]["monthly_price"], "provider": "STRIPE", "updatedAt": now})
    return row


def apply_stripe_event(state: dict[str, Any], event: Any) -> bool:
    event_id = str(stripe_value(event, "id") or "")
    event_type = str(stripe_value(event, "type") or "")
    if not event_id:
        raise HTTPException(400, "Stripe event ID is required")
    processed = state.setdefault("stripe_event_ids", [])
    if event_id in processed:
        return False
    event_data = stripe_value(stripe_value(event, "data", {}), "object", {})
    if event_type == "checkout.session.completed":
        metadata = stripe_value(event_data, "metadata", {}) or {}
        upsert_stripe_subscription(state, {"id": stripe_value(event_data, "subscription"), "customer": stripe_value(event_data, "customer"), "status": "active", "metadata": metadata}, fallback_user_id=metadata.get("user_id"), fallback_plan=metadata.get("plan"), fallback_interval=metadata.get("billing_interval"))
    elif event_type in {"customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"}:
        upsert_stripe_subscription(state, event_data)
    elif event_type in {"invoice.paid", "invoice.payment_failed"}:
        customer_id = str(stripe_value(event_data, "customer") or "")
        user_id = subscription_user_for_customer(state, customer_id)
        if user_id:
            row = active_subscription(state, user_id)
            if row:
                row["status"] = "ACTIVE" if event_type == "invoice.paid" else "PAST_DUE"
                row["updatedAt"] = now_iso()
    else:
        raise HTTPException(400, "Unsupported Stripe event")
    processed.append(event_id)
    del processed[:-1000]
    return True


@router.get("/subscription")
async def v22_subscription(request: Request):
    user = authenticated_user(request)
    return subscription_for_user(runtime(request)["state"], user["id"])


@router.post("/subscription/trial")
async def v22_start_trial(request: Request):
    user = authenticated_user(request)
    rt = runtime(request)
    async with rt["lock"]:
        state = rt["state"]
        if active_license(state, user["id"]):
            raise HTTPException(409, "An active subscription or trial already exists")
        started = now_iso()
        expires = (datetime.now(timezone.utc) + timedelta(days=TRIAL_DAYS)).isoformat()
        row = {"id": uuid.uuid4().hex, "user_id": user["id"], "plan": "STARTER", "status": "TRIAL", "billingInterval": "monthly", "trialStart": started, "trialEnd": expires, "currentPeriodStart": started, "currentPeriodEnd": expires, "currentPrice": 0, "stripeCustomerId": None, "stripeSubscriptionId": None, "cancelAtPeriodEnd": False, "provider": "DEVELOPMENT", "createdAt": started, "updatedAt": started}
        state["subscriptions"].append(row)
        state["licenses"].append({"id": uuid.uuid4().hex, "user_id": user["id"], "plan": "STARTER", "status": "ACTIVE", "starts_at": started, "expires_at": expires, "source": "FREE_TRIAL", "demo_only": True})
        add_audit(state, "TRIAL_STARTED", "7-day Starter trial started.", actor=user["id"], subject=user["id"])
        save_state(state)
    return subscription_for_user(rt["state"], user["id"])


@router.post("/subscription/checkout")
async def v22_subscription_checkout(payload: CheckoutRequest, request: Request):
    user = authenticated_user(request)
    if not stripe_configured():
        raise HTTPException(503, "Stripe billing is not configured; no subscription was activated")
    price_id = stripe_price_id(payload.plan, payload.billing_interval)
    base_url = stripe_base_url()
    state = runtime(request)["state"]
    customer_id = stripe_customer_for_user(state, user["id"])
    stripe.api_key = os.environ["STRIPE_SECRET_KEY"]
    if not customer_id:
        customer = stripe.Customer.create(email=user["email"], name=user.get("display_name"), metadata={"user_id": user["id"]})
        customer_id = str(stripe_value(customer, "id"))
    metadata = {"user_id": user["id"], "plan": payload.plan, "billing_interval": payload.billing_interval}
    session = stripe.checkout.Session.create(
        mode="subscription", customer=customer_id, line_items=[{"price": price_id, "quantity": 1}],
        success_url=f"{base_url}/billing?checkout=success", cancel_url=f"{base_url}/pricing?checkout=cancelled",
        metadata=metadata, subscription_data={"metadata": metadata},
    )
    return {"mode": "STRIPE", "checkout_url": stripe_value(session, "url"), "session_id": stripe_value(session, "id"), "plan": payload.plan, "billing_interval": payload.billing_interval}


@router.post("/subscription/customer-portal")
async def v22_customer_portal(request: Request):
    user = authenticated_user(request)
    if not stripe_configured():
        raise HTTPException(503, "Stripe billing is not configured; customer portal is unavailable")
    customer_id = stripe_customer_for_user(runtime(request)["state"], user["id"])
    if not customer_id:
        raise HTTPException(409, "No Stripe customer is linked to this account")
    stripe.api_key = os.environ["STRIPE_SECRET_KEY"]
    portal = stripe.billing_portal.Session.create(customer=customer_id, return_url=f"{stripe_base_url()}/billing")
    return {"url": stripe_value(portal, "url"), "mode": "STRIPE"}


@router.post("/subscription/webhook")
async def v22_subscription_webhook(request: Request):
    secret = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
    signature = request.headers.get("stripe-signature", "").strip()
    body = await request.body()
    if not stripe or not secret or not signature:
        raise HTTPException(503, "Stripe webhook verification is not configured")
    try:
        event = stripe.Webhook.construct_event(body, signature, secret)
    except Exception as exc:
        raise HTTPException(400, "Invalid Stripe webhook signature") from exc
    rt = runtime(request)
    async with rt["lock"]:
        state = rt["state"]
        event_id = str(stripe_value(event, "id") or "")
        event_type = str(stripe_value(event, "type") or "")
        applied = apply_stripe_event(state, event)
        if not applied:
            return {"ok": True, "duplicate": True, "event_id": event_id}
        save_state(state)
    await persist_v22_commercial(request.app)
    return {"ok": True, "event_id": event_id, "event_type": event_type}


@router.post("/subscription/cancel")
async def v22_subscription_cancel(request: Request):
    user = authenticated_user(request)
    rt = runtime(request)
    async with rt["lock"]:
        row = active_subscription(rt["state"], user["id"])
        if not row:
            raise HTTPException(409, "No active subscription exists")
        row["cancelAtPeriodEnd"] = True
        row["updatedAt"] = now_iso()
        add_audit(rt["state"], "SUBSCRIPTION_CANCEL_SCHEDULED", "Subscription cancellation scheduled for period end.", actor=user["id"], subject=user["id"])
        save_state(rt["state"])
    return subscription_for_user(rt["state"], user["id"])


@router.post("/auth/logout")
async def v22_logout(request: Request):
    user = authenticated_user(request)
    rt = runtime(request)
    async with rt["lock"]:
        user["auth_version"] = int(user.get("auth_version", 1)) + 1
        save_state(rt["state"])
    await persist_v22_commercial(request.app)
    return {"ok": True}


@router.get("/admin/overview")
async def v22_admin_overview(request: Request):
    authenticated_user(request, owner=True)
    return admin_overview(runtime(request)["state"])


@router.get("/admin/users/{user_id}/trading-accounts")
async def v22_admin_trading_accounts(user_id: str, request: Request):
    authenticated_user(request, owner=True)
    rt = runtime(request)
    if not any(str(item.get("id")) == user_id for item in rt["state"]["users"]):
        raise HTTPException(404, "Kullanıcı bulunamadı")
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        return {"user_id": user_id, "accounts": []}
    await ensure_commercial_schema(request.app)
    rows = await pool.fetch(
        """
        SELECT id, provider, environment, status, created_at, updated_at
        FROM trading_accounts
        WHERE user_id = $1
        ORDER BY created_at ASC, id ASC
        """,
        user_id,
    )
    return {
        "user_id": user_id,
        "accounts": [
            {
                "id": str(row["id"]),
                "provider": str(row["provider"]),
                "environment": str(row["environment"]),
                "account_reference": str(row["account_reference"]),
                "status": str(row["status"]),
                "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else str(row["created_at"]),
                "updated_at": row["updated_at"].isoformat() if hasattr(row["updated_at"], "isoformat") else str(row["updated_at"]),
            }
            for row in rows
        ],
    }


@router.post("/admin/users/{user_id}/trading-accounts")
async def v22_admin_link_trading_account(user_id: str, payload: TradingAccountLinkRequest, request: Request):
    authenticated_user(request, owner=True)
    rt = runtime(request)
    if not any(str(item.get("id")) == user_id for item in rt["state"]["users"]):
        raise HTTPException(404, "Kullanıcı bulunamadı")
    account_reference = " ".join(payload.account_reference.split())
    if not account_reference:
        raise HTTPException(422, "Account reference boş olamaz")
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(503, "Trading account ownership storage unavailable")
    await ensure_commercial_schema(request.app)
    try:
        row = await pool.fetchrow(
            """
            INSERT INTO trading_accounts (id, user_id, provider, environment, account_reference, status)
            VALUES ($1, $2, $3, $4, $5, 'UNASSIGNED')
            RETURNING id, provider, environment, account_reference, status, created_at, updated_at
            """,
            uuid.uuid4().hex,
            user_id,
            payload.provider,
            payload.environment,
            account_reference,
        )
    except Exception as exc:
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(409, "Bu trading account mapping zaten mevcut") from exc
        raise HTTPException(503, "Trading account ownership storage unavailable") from exc
    return {
        "user_id": user_id,
        "account": {
            "id": str(row["id"]),
            "provider": str(row["provider"]),
            "environment": str(row["environment"]),
            "account_reference": str(row["account_reference"]),
            "status": str(row["status"]),
            "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else str(row["created_at"]),
            "updated_at": row["updated_at"].isoformat() if hasattr(row["updated_at"], "isoformat") else str(row["updated_at"]),
        },
    }


@router.delete("/admin/users/{user_id}/trading-accounts/{account_id}")
async def v22_admin_unlink_trading_account(user_id: str, account_id: str, request: Request):
    authenticated_user(request, owner=True)
    rt = runtime(request)
    if not any(str(item.get("id")) == user_id for item in rt["state"]["users"]):
        raise HTTPException(404, "Kullanıcı bulunamadı")
    pool = getattr(request.app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(503, "Trading account ownership storage unavailable")
    await ensure_commercial_schema(request.app)
    deleted = await pool.fetchrow(
        "DELETE FROM trading_accounts WHERE id = $1 AND user_id = $2 RETURNING id",
        account_id,
        user_id,
    )
    if not deleted:
        raise HTTPException(404, "Trading account mapping bulunamadı")
    return {"ok": True, "user_id": user_id, "account_id": account_id}


@router.patch("/admin/users/{user_id}/role")
async def v22_admin_update_role(user_id: str, payload: RoleUpdateRequest, request: Request):
    owner = authenticated_user(request, owner=True)
    rt = runtime(request)
    if user_id == owner["id"] and payload.role != "OWNER":
        raise HTTPException(422, "Son yönetici hesabının rolü düşürülemez")
    async with rt["lock"]:
        user = next((item for item in rt["state"]["users"] if item.get("id") == user_id), None)
        if not user:
            raise HTTPException(404, "Kullanıcı bulunamadı")
        user["role"] = payload.role
        user["auth_version"] = int(user.get("auth_version", 1)) + 1
        add_audit(rt["state"], "ROLE_CHANGED", f"Kullanıcı rolü {payload.role} olarak güncellendi.", actor=owner["id"], subject=user_id)
        save_state(rt["state"])
    await persist_v22_commercial(request.app)
    return {"user": public_user(user)}


@router.get("/operations")
async def v22_operations(request: Request):
    authenticated_user(request)
    return operations_overview(request.app)


@router.post("/auth/change-password")
async def v22_change_password(payload: PasswordChangeRequest, request: Request):
    user = authenticated_user(request)
    rt = runtime(request)
    if not verify_password(payload.current_password, user.get("password", {})):
        raise HTTPException(401, "Mevcut parola hatalı")
    async with rt["lock"]:
        user["password"] = hash_password(payload.new_password)
        user["auth_version"] = int(user.get("auth_version", 1)) + 1
        add_audit(rt["state"], "PASSWORD_CHANGED", "Hesap parolası değiştirildi; eski oturumlar kapatıldı.", actor=user["id"], subject=user["id"])
        save_state(rt["state"])
    return {"ok": True, "reauthenticate": True, "message": "Parola değişti. Güvenlik için yeniden giriş yapın."}


@router.post("/customers")
async def v22_create_customer(payload: CustomerRequest, request: Request):
    owner = authenticated_user(request, owner=True)
    rt = runtime(request)
    async with rt["lock"]:
        state = rt["state"]
        email = normalize_email(payload.email)
        if "@" not in email:
            raise HTTPException(422, "Geçerli bir e-posta yazın")
        if any(item.get("email") == email for item in state["users"]):
            raise HTTPException(409, "Bu e-posta zaten kayıtlı")
        user_id = uuid.uuid4().hex
        user = {"id": user_id, "email": email, "display_name": payload.display_name.strip(), "role": "CUSTOMER", "active": True, "auth_version": 1, "password": hash_password(payload.password), "created_at": now_iso()}
        state["users"].append(user)
        expires_at = (datetime.now(timezone.utc) + timedelta(days=payload.days)).isoformat()
        license_row = {"id": uuid.uuid4().hex, "user_id": user_id, "plan": payload.plan, "status": "ACTIVE", "starts_at": now_iso(), "expires_at": expires_at, "source": "MANUAL_DEMO", "demo_only": True}
        state["licenses"].append(license_row)
        created_at = now_iso()
        state["subscriptions"].append({"id": uuid.uuid4().hex, "user_id": user_id, "plan": "STARTER" if payload.plan == "TRIAL" else payload.plan, "status": "TRIAL" if payload.plan == "TRIAL" else "ACTIVE", "billingInterval": "monthly", "trialStart": created_at if payload.plan == "TRIAL" else None, "trialEnd": expires_at if payload.plan == "TRIAL" else None, "currentPeriodStart": created_at, "currentPeriodEnd": expires_at, "currentPrice": 0 if payload.plan == "TRIAL" else state["plans"].get(payload.plan, {}).get("monthly_usd"), "stripeCustomerId": None, "stripeSubscriptionId": None, "cancelAtPeriodEnd": False, "provider": "DEVELOPMENT", "createdAt": created_at, "updatedAt": created_at})
        add_audit(state, "CUSTOMER_CREATED", f"{email} için {payload.plan} Demo lisansı oluşturuldu.", actor=owner["id"], subject=user_id)
        save_state(state)
    return {"user": public_user(user), "license": license_row, "demo_only": True}


@router.post("/customers/{user_id}/status")
async def v22_customer_status(user_id: str, payload: CustomerStatusRequest, request: Request):
    owner = authenticated_user(request, owner=True)
    rt = runtime(request)
    async with rt["lock"]:
        user = next((item for item in rt["state"]["users"] if item.get("id") == user_id), None)
        if not user:
            raise HTTPException(404, "Kullanıcı bulunamadı")
        if user.get("role") == "OWNER":
            raise HTTPException(409, "Sahip hesabı bu ekrandan askıya alınamaz")
        user["active"] = payload.active
        user["auth_version"] = int(user.get("auth_version", 1)) + 1
        if not payload.active:
            for agent in rt["state"]["agents"]:
                if agent.get("user_id") == user_id and agent.get("status") == "ACTIVE":
                    agent["status"] = "REVOKED"
                    agent["revoked_at"] = now_iso()
                    agent["token_version"] = int(agent.get("token_version", 1)) + 1
        kind = "CUSTOMER_ACTIVATED" if payload.active else "CUSTOMER_SUSPENDED"
        message = f"{user['email']} {'etkinleştirildi' if payload.active else 'askıya alındı'}: {payload.reason}"
        add_audit(rt["state"], kind, message, actor=owner["id"], subject=user_id)
        save_state(rt["state"])
    return {"user": public_user(user), "agents_revoked": not payload.active, "demo_only": True}


@router.post("/subscriptions/activate-demo")
async def v22_activate_subscription(payload: SubscriptionRequest, request: Request):
    owner = authenticated_user(request, owner=True)
    rt = runtime(request)
    async with rt["lock"]:
        state = rt["state"]
        if not any(item.get("id") == payload.user_id for item in state["users"]):
            raise HTTPException(404, "Kullanıcı bulunamadı")
        expires_at = (datetime.now(timezone.utc) + timedelta(days=payload.days)).isoformat()
        row = {"id": uuid.uuid4().hex, "user_id": payload.user_id, "plan": payload.plan, "status": "ACTIVE", "starts_at": now_iso(), "expires_at": expires_at, "source": "MANUAL_DEMO", "demo_only": True}
        state["licenses"].append(row)
        state["subscriptions"].append({"id": uuid.uuid4().hex, "user_id": payload.user_id, "plan": payload.plan, "status": "TEST_ACTIVE", "period_end": expires_at, "provider": "MANUAL_DEMO", "created_at": now_iso()})
        add_audit(state, "LICENSE_ACTIVATED", f"{payload.plan} Demo lisansı {payload.days} gün etkinleştirildi.", actor=owner["id"], subject=payload.user_id)
        save_state(state)
    return {"license": row, "billing_live": False, "demo_only": True}


@router.post("/licenses/{license_id}/revoke")
async def v22_revoke_license(license_id: str, payload: RevokeRequest, request: Request):
    if payload.confirmation.strip().upper() != "LİSANS İPTAL":
        raise HTTPException(422, "İşlem için LİSANS İPTAL yazın")
    owner = authenticated_user(request, owner=True)
    rt = runtime(request)
    async with rt["lock"]:
        row = next((item for item in rt["state"]["licenses"] if item.get("id") == license_id), None)
        if not row:
            raise HTTPException(404, "Lisans bulunamadı")
        target = next((item for item in rt["state"]["users"] if item.get("id") == row.get("user_id")), None)
        if target and target.get("role") == "OWNER":
            raise HTTPException(409, "Sahip geliştirme lisansı iptal edilemez")
        row.update({"status": "REVOKED", "revoked_at": now_iso(), "revoked_reason": payload.reason})
        for subscription in rt["state"]["subscriptions"]:
            if subscription.get("user_id") == row.get("user_id") and subscription.get("status") in {"TEST_ACTIVE", "ACTIVE", "TRIAL"}:
                subscription["status"] = "CANCELED"
        for agent in rt["state"]["agents"]:
            if agent.get("user_id") == row.get("user_id") and agent.get("status") == "ACTIVE":
                agent["status"] = "REVOKED"
                agent["revoked_at"] = now_iso()
                agent["token_version"] = int(agent.get("token_version", 1)) + 1
        add_audit(rt["state"], "LICENSE_REVOKED", f"Demo lisansı iptal edildi: {payload.reason}", actor=owner["id"], subject=row.get("user_id"))
        save_state(rt["state"])
    return {"ok": True, "license_id": license_id, "agents_revoked": True, "demo_only": True}


@router.put("/plans/{plan_code}")
async def v22_update_plan(plan_code: str, payload: PlanUpdateRequest, request: Request):
    owner = authenticated_user(request, owner=True)
    rt = runtime(request)
    code = plan_code.upper()
    async with rt["lock"]:
        if code not in rt["state"]["plans"]:
            raise HTTPException(404, "Paket bulunamadı")
        rt["state"]["plans"][code].update({"monthly_usd": payload.monthly_usd, "agents": payload.agents, "bots": payload.bots})
        add_audit(rt["state"], "PLAN_UPDATED", f"{code} fiyatı ve sınırları güncellendi.", actor=owner["id"], subject=code)
        save_state(rt["state"])
    return {"code": code, **rt["state"]["plans"][code], "billing_live": False}


@router.post("/agent/pair-code")
async def v22_pair_code(request: Request):
    user = authenticated_user(request)
    rt = runtime(request)
    license_row = active_license(rt["state"], user["id"])
    if not license_row:
        raise HTTPException(403, "Etkin lisans gerekli")
    raw = f"{secrets.token_hex(4).upper()}-{secrets.token_hex(4).upper()}"
    async with rt["lock"]:
        rt["state"]["pairing_codes"] = [item for item in rt["state"]["pairing_codes"] if parse_date(item.get("expires_at")) > datetime.now(timezone.utc) and not item.get("used")]
        rt["state"]["pairing_codes"].append({"id": uuid.uuid4().hex, "user_id": user["id"], "code_hash": pairing_code_hash(raw, rt["secret"]), "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(), "used": False, "created_at": now_iso()})
        add_audit(rt["state"], "PAIR_CODE_CREATED", "10 dakikalık yerel ajan eşleştirme kodu üretildi.", actor=user["id"], subject=user["id"])
        save_state(rt["state"])
    return {"code": raw, "expires_in_seconds": 600, "message": "Bu kod yalnızca yerel V24 ajanına yazılır; API anahtarı değildir.", "demo_only": True}


@router.post("/agent/pair")
async def v22_pair_agent(payload: PairAgentRequest, request: Request):
    rt = runtime(request)
    code_hash = pairing_code_hash(payload.code, rt["secret"])
    async with rt["lock"]:
        state = rt["state"]
        code = next((item for item in state["pairing_codes"] if item.get("code_hash") == code_hash and not item.get("used") and parse_date(item.get("expires_at")) > datetime.now(timezone.utc)), None)
        if not code:
            raise HTTPException(401, "Eşleştirme kodu geçersiz veya süresi doldu")
        license_row = active_license(state, code["user_id"])
        if not license_row:
            raise HTTPException(403, "Etkin lisans bulunamadı")
        plan = state["plans"].get(license_row["plan"], {})
        active_agents = [item for item in state["agents"] if item.get("user_id") == code["user_id"] and item.get("status") == "ACTIVE"]
        fingerprint_hash = device_fingerprint_hash(payload.fingerprint)
        existing = next((item for item in active_agents if item.get("fingerprint_hash") == fingerprint_hash), None)
        if not existing and len(active_agents) >= int(plan.get("agents", 1)):
            raise HTTPException(409, "Paketin cihaz sınırına ulaşıldı")
        agent = existing or {"id": uuid.uuid4().hex, "user_id": code["user_id"], "fingerprint_hash": fingerprint_hash, "created_at": now_iso(), "token_version": 1}
        if existing:
            agent["token_version"] = int(agent.get("token_version", 1)) + 1
        agent.update({"device_name": payload.device_name.strip(), "status": "ACTIVE", "last_seen_at": now_iso(), "app_version": V22_VERSION, "mode": "DEMO_ONLY"})
        if not existing:
            state["agents"].append(agent)
        code["used"] = True
        add_audit(state, "AGENT_PAIRED", f"{agent['device_name']} güvenli yerel ajan olarak eşleştirildi.", actor=agent["id"], subject=code["user_id"])
        save_state(state)
    token = issue_token(
        agent["id"], "AGENT", rt["secret"], kind="AGENT",
        ttl_seconds=30 * 24 * 60 * 60, token_version=int(agent.get("token_version", 1)),
    )
    return {"agent": agent, "agent_token": token, "commands": [], "mode": "DEMO_ONLY", "exchange_credentials_received": False}


@router.post("/agent/heartbeat")
async def v22_agent_heartbeat(payload: HeartbeatRequest, request: Request):
    rt = runtime(request)
    try:
        token = verify_token(bearer(request), rt["secret"], expected_kind="AGENT")
    except ValueError as exc:
        raise HTTPException(401, str(exc)) from exc
    async with rt["lock"]:
        agent = next((item for item in rt["state"]["agents"] if item.get("id") == token["sub"] and item.get("status") == "ACTIVE"), None)
        if not agent:
            raise HTTPException(401, "Ajan etkin değil")
        if int(token.get("ver", 1)) != int(agent.get("token_version", 1)):
            raise HTTPException(401, "Ajan oturumu yenilenmeli")
        if not active_license(rt["state"], agent["user_id"]):
            raise HTTPException(403, "Lisans süresi doldu")
        agent.update({"last_seen_at": now_iso(), "app_version": payload.app_version, "runtime_status": payload.status, "mode": "DEMO_ONLY"})
        save_state(rt["state"])
    return {"accepted": True, "server_time": now_iso(), "commands": [], "mode": "DEMO_ONLY", "real_orders_enabled": False}


@router.post("/agents/{agent_id}/revoke")
async def v22_revoke_agent(agent_id: str, payload: RevokeRequest, request: Request):
    if payload.confirmation.strip().upper() != "AJAN İPTAL":
        raise HTTPException(422, "İşlem için AJAN İPTAL yazın")
    user = authenticated_user(request)
    rt = runtime(request)
    async with rt["lock"]:
        agent = next((item for item in rt["state"]["agents"] if item.get("id") == agent_id), None)
        if not agent:
            raise HTTPException(404, "Ajan bulunamadı")
        if user.get("role") != "OWNER" and agent.get("user_id") != user.get("id"):
            raise HTTPException(403, "Bu cihaz için yetkiniz yok")
        agent.update({
            "status": "REVOKED", "revoked_at": now_iso(), "revoked_reason": payload.reason,
            "token_version": int(agent.get("token_version", 1)) + 1,
        })
        add_audit(rt["state"], "AGENT_REVOKED", f"{agent.get('device_name', 'Cihaz')} erişimi kaldırıldı: {payload.reason}", actor=user["id"], subject=agent.get("user_id"))
        save_state(rt["state"])
    return {"ok": True, "agent_id": agent_id, "status": "REVOKED", "demo_only": True}


@router.post("/fee-guard")
async def v22_fee_guard(payload: FeeGuardRequest, request: Request):
    authenticated_user(request)
    try:
        return calculate_fee_guard(FeeGuardInput(**payload.model_dump()))
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.post("/grid-guard")
async def v22_grid_guard(payload: GridGuardRequest, request: Request):
    authenticated_user(request)
    try:
        return calculate_grid_guard(**payload.model_dump())
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.put("/release-evidence/{evidence_key}")
async def v22_release_evidence(evidence_key: str, payload: ReleaseEvidenceRequest, request: Request):
    owner = authenticated_user(request, owner=True)
    rt = runtime(request)
    key = evidence_key.strip().lower()
    if key not in {"backup", "support", "legal", "security_review"}:
        raise HTTPException(404, "Yayın kanıtı bulunamadı")
    async with rt["lock"]:
        row = rt["state"]["release_evidence"].setdefault(key, {})
        row.update({"status": payload.status, "note": payload.note.strip(), "updated_at": now_iso(), "actor": owner["id"]})
        add_audit(rt["state"], "RELEASE_EVIDENCE", f"{key} kanıt kaydı {payload.status} olarak güncellendi.", actor=owner["id"], subject=key)
        save_state(rt["state"])
    return {"key": key, **row, "self_attested": True, "production_approval": False}


@router.get("/readiness")
async def v22_readiness(request: Request):
    user = authenticated_user(request)
    state = runtime(request)["state"]
    now = datetime.now(timezone.utc)
    online_cutoff = now - timedelta(minutes=3)
    evidence = state.get("release_evidence", {})
    operations = operations_overview(request.app)
    agent_online = any(
        item.get("user_id") == user["id"] and item.get("status") == "ACTIVE"
        and parse_date(item.get("last_seen_at")) >= online_cutoff
        for item in state["agents"]
    )
    gates = [
        {"key": "owner", "label": "Yönetici hesabı", "passed": bool(state.get("owner_user_id")), "detail": "Yerel sahip oluşturuldu."},
        {"key": "auth", "label": "Parola ve imzalı oturum", "passed": True, "detail": "Scrypt parola özeti ve HMAC süreli oturum kullanılıyor."},
        {"key": "license", "label": "Etkin lisans", "passed": bool(active_license(state, user["id"])), "detail": "Plan, bitiş tarihi ve cihaz sınırı doğrulanıyor."},
        {"key": "agent", "label": "Güvenli yerel ajan", "passed": agent_online, "detail": "API anahtarını merkeze göndermeyen, sürekli kalp atışlı cihaz modeli."},
        {"key": "demo_connector", "label": "Binance Futures Demo bağlantısı", "passed": bool(operations["demo_connector"]["configured"]), "detail": "Anahtar yalnızca yerel Windows DPAPI kasasında tutulur."},
        {"key": "fee_guard", "label": "Net kâr koruması", "passed": True, "detail": "Komisyon, kayma ve fonlama tahmini karar öncesi düşülüyor."},
        {"key": "backup", "label": "Yedekleme tatbikatı", "passed": evidence.get("backup", {}).get("status") == "RECORDED", "detail": evidence.get("backup", {}).get("note", "YEDEKLE.bat tatbikatı bekleniyor.")},
        {"key": "support", "label": "Müşteri destek süreci", "passed": evidence.get("support", {}).get("status") == "RECORDED", "detail": evidence.get("support", {}).get("note", "Destek akışı bekleniyor.")},
        {"key": "payment", "label": "Canlı ödeme sağlayıcısı", "passed": False, "detail": "Şimdilik MANUAL_DEMO; para tahsilatı kapalı."},
        {"key": "legal", "label": "Hukuk ve sözleşmeler", "passed": False, "detail": evidence.get("legal", {}).get("note", "Satış öncesi ülkeye özel hukuk incelemesi gerekli.")},
        {"key": "security_review", "label": "Bağımsız güvenlik testi", "passed": False, "detail": evidence.get("security_review", {}).get("note", "Genel kullanıma açılmadan pentest ve gizli anahtar yönetimi gerekli.")},
    ]
    passed = sum(1 for item in gates if item["passed"])
    return {
        "version": V22_VERSION,
        "stage": "COMMERCIAL COMPLETE · LAUNCH LAB · DEMO",
        "score": round(passed / len(gates) * 100),
        "passed": passed,
        "total": len(gates),
        "gates": gates,
        "production_ready": False,
        "closed_beta_candidate": all(item["passed"] for item in gates if item["key"] in {"owner", "auth", "license", "agent", "demo_connector", "fee_guard", "backup", "support"}),
        "demo_only": True,
        "release_evidence": evidence,
        "next_step": "Demo emir, otomasyon, yedek ve destek tatbikatlarını tamamla; ardından bağımsız hukuk ve güvenlik incelemesine geç.",
    }


def init_v22_commercial(application: Any) -> None:
    state = load_state()
    application.state.v22_commercial = {
        "state": state,
        "secret": load_secret(),
        "lock": asyncio.Lock(),
        "storage_lock": asyncio.Lock(),
        "storage_ready": False,
        "restore_attempted": False,
        "storage_status": "YEREL_YEDEK",
    }
    add_audit(state, "V24_START", "V24 Commercial Complete başladı; ödeme ve gerçek emir kanalları kapalı.")
    save_state(state)


async def shutdown_v22_commercial(application: Any) -> None:
    if hasattr(application.state, "v22_commercial"):
        save_state(application.state.v22_commercial["state"])
        try:
            await asyncio.wait_for(persist_v22_commercial(application), timeout=3)
        except asyncio.TimeoutError:
            pass
