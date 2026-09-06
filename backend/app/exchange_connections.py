"""V28 in-application Binance connection vault.

Secrets enter the owner-only API over HTTPS, are tested against the selected
Binance USD-M Futures host, and are persisted only as Fernet ciphertext in
PostgreSQL.  Public responses contain metadata and account summaries, never
API or secret key material.  Activating a connection does not arm an order
channel; V25's independent consent, evidence, policy and time-limited locks
remain authoritative for real orders.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, SecretStr, model_validator


VERSION = "28.0.0"
Mode = Literal["TESTNET", "LIVE"]
HOSTS: dict[str, str] = {
    "TESTNET": "https://demo-fapi.binance.com",
    "LIVE": "https://fapi.binance.com",
}
SAVE_CONFIRMATIONS = {
    "TESTNET": "TESTNET KASAYA KAYDET",
    "LIVE": "CANLI KASAYA KAYDET",
}
ACTIVATE_CONFIRMATIONS = {
    "TESTNET": "TESTNET BAĞLANTIYI AÇ",
    "LIVE": "CANLI SALT OKUNUR BAĞLANTIYI AÇ",
}
DELETE_CONFIRMATION = "ANAHTARI KALICI SİL"

router = APIRouter(prefix="/api/exchange-connections", tags=["V28 Exchange Connections"])

# One Render web process is used by the supplied deployment.  Every process
# reloads the encrypted rows at startup; plaintext never enters app.state or a
# response/log payload.
_CACHE: dict[str, tuple[str, str]] = {}
_META: dict[str, dict[str, Any]] = {}


class VaultError(RuntimeError):
    pass


class SaveCredentialsRequest(BaseModel):
    mode: Mode
    api_key: SecretStr
    secret_key: SecretStr
    confirmation: str = Field(min_length=1, max_length=80)


class TestCredentialsRequest(BaseModel):
    mode: Mode
    api_key: SecretStr | None = None
    secret_key: SecretStr | None = None

    @model_validator(mode="after")
    def keys_are_a_pair(self):
        if (self.api_key is None) != (self.secret_key is None):
            raise ValueError("API Key ve Secret Key birlikte girilmelidir.")
        return self


class ConnectionActionRequest(BaseModel):
    mode: Mode
    confirmation: str = Field(min_length=1, max_length=100)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_mode(mode: str) -> str:
    value = str(mode or "").strip().upper()
    if value not in HOSTS:
        raise VaultError("Bilinmeyen borsa bağlantı modu.")
    return value


def key_fingerprint(api_key: str) -> str:
    """Return a non-reversible identifier; never reveal key suffixes."""
    if len(api_key.strip()) < 10:
        return ""
    digest = hashlib.sha256(api_key.strip().encode("utf-8")).hexdigest().upper()
    return f"SHA256:{digest[:12]}"


def _master_secret() -> str:
    # A dedicated key is optional.  The existing owner access token is already
    # required in the hosted build and lets the user configure the vault fully
    # inside the program without returning to Render.
    return (
        os.getenv("PROTREBOT_VAULT_MASTER_KEY", "").strip()
        or os.getenv("PROTREBOT_WEB_ACCESS_TOKEN", "").strip()
    )


def vault_cipher(master_secret: str | None = None) -> Fernet:
    secret = (master_secret if master_secret is not None else _master_secret()).strip()
    if len(secret) < 24:
        raise VaultError("Sunucu yönetici erişim kodu en az 24 karakter olmalıdır; şifreli kasa hazır değil.")
    digest = hashlib.sha256(b"ProTreBot:ExchangeVault:V28\0" + secret.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def encrypt_credentials(api_key: str, secret_key: str, *, mode: str, master_secret: str | None = None) -> bytes:
    payload = {
        "scope": f"BINANCE_USDM_FUTURES_{normalize_mode(mode)}",
        "api_key": api_key.strip(),
        "secret_key": secret_key.strip(),
        "saved_at": now_iso(),
    }
    return vault_cipher(master_secret).encrypt(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def decrypt_credentials(ciphertext: bytes, *, mode: str, master_secret: str | None = None) -> tuple[str, str]:
    try:
        payload = json.loads(vault_cipher(master_secret).decrypt(bytes(ciphertext)).decode("utf-8"))
    except (InvalidToken, ValueError, TypeError, json.JSONDecodeError, UnicodeError) as exc:
        raise VaultError("Şifreli anahtar kaydı açılamadı; kasa anahtarı değişmiş olabilir.") from exc
    expected_scope = f"BINANCE_USDM_FUTURES_{normalize_mode(mode)}"
    if not isinstance(payload, dict) or payload.get("scope") != expected_scope:
        raise VaultError("Şifreli anahtar kaydının kapsamı geçersiz.")
    api_key = str(payload.get("api_key") or "").strip()
    secret_key = str(payload.get("secret_key") or "").strip()
    validate_key_pair(api_key, secret_key)
    return api_key, secret_key


def validate_key_pair(api_key: str, secret_key: str) -> None:
    if len(api_key.strip()) < 10 or len(secret_key.strip()) < 10:
        raise VaultError("API Key veya Secret Key eksik ya da çok kısa.")
    if any(char.isspace() for char in api_key.strip()) or any(char.isspace() for char in secret_key.strip()):
        raise VaultError("API anahtarlarında boşluk veya satır sonu bulunamaz.")


def vault_managed(mode: str) -> bool:
    return bool(_META.get(normalize_mode(mode), {}).get("configured"))


def cached_credentials(mode: str, *, active_only: bool = True) -> tuple[str, str]:
    normalized = normalize_mode(mode)
    meta = _META.get(normalized, {})
    if active_only and not bool(meta.get("active")):
        return "", ""
    return _CACHE.get(normalized, ("", ""))


def credential_source(mode: str) -> str:
    normalized = normalize_mode(mode)
    if not vault_managed(normalized):
        return "YAPILANDIRILMADI"
    return "UYGULAMA_KASASI" if _META.get(normalized, {}).get("active") else "UYGULAMA_KASASI_KAPALI"


def clear_vault_cache() -> None:
    _CACHE.clear()
    _META.clear()


async def ensure_schema(pool: Any) -> None:
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS protrebot_exchange_vault (
          mode TEXT PRIMARY KEY CHECK (mode IN ('TESTNET', 'LIVE')),
          encrypted_payload BYTEA NOT NULL,
          fingerprint TEXT NOT NULL,
          active BOOLEAN NOT NULL DEFAULT FALSE,
          last_test_ok BOOLEAN NOT NULL DEFAULT FALSE,
          last_test_at TIMESTAMPTZ,
          last_error TEXT,
          account_summary JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


def _row_meta(row: Any) -> dict[str, Any]:
    summary = row.get("account_summary") if hasattr(row, "get") else None
    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except json.JSONDecodeError:
            summary = None
    tested_at = row.get("last_test_at") if hasattr(row, "get") else None
    updated_at = row.get("updated_at") if hasattr(row, "get") else None
    return {
        "configured": True,
        "active": bool(row.get("active")),
        "fingerprint": str(row.get("fingerprint") or ""),
        "last_test_ok": bool(row.get("last_test_ok")),
        "last_test_at": tested_at.isoformat() if hasattr(tested_at, "isoformat") else tested_at,
        "last_error": str(row.get("last_error") or "")[:240] or None,
        "account": summary if isinstance(summary, dict) else None,
        "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else updated_at,
    }


async def ensure_exchange_vault(application: Any, *, force: bool = False) -> bool:
    state = getattr(application.state, "exchange_vault", None)
    if state is None:
        state = {
            "ready": False,
            "storage": "POSTGRESQL + FERNET",
            "reason": "Kasa başlatılıyor.",
            "pool_id": None,
            "loaded_at": None,
        }
        application.state.exchange_vault = state
    pool = getattr(application.state, "db_pool", None)
    if pool is None:
        clear_vault_cache()
        state.update({"ready": False, "reason": "PostgreSQL bağlantısı bekleniyor.", "pool_id": None})
        return False
    if state.get("ready") and state.get("pool_id") == id(pool) and not force:
        return True
    try:
        vault_cipher()
        await ensure_schema(pool)
        rows = await pool.fetch(
            "SELECT mode, encrypted_payload, fingerprint, active, last_test_ok, last_test_at, last_error, account_summary, updated_at FROM protrebot_exchange_vault"
        )
        clear_vault_cache()
        for row in rows:
            mode = normalize_mode(str(row["mode"]))
            try:
                _CACHE[mode] = decrypt_credentials(bytes(row["encrypted_payload"]), mode=mode)
                _META[mode] = _row_meta(row)
            except VaultError as exc:
                _META[mode] = {**_row_meta(row), "active": False, "last_test_ok": False, "last_error": str(exc)}
        state.update({"ready": True, "reason": None, "pool_id": id(pool), "loaded_at": now_iso()})
        return True
    except Exception as exc:
        clear_vault_cache()
        state.update({"ready": False, "reason": str(exc)[:240], "pool_id": id(pool)})
        return False


async def init_exchange_connections(application: Any) -> None:
    application.state.exchange_vault = {
        "ready": False,
        "storage": "POSTGRESQL + FERNET",
        "reason": "Kasa başlatılıyor.",
        "pool_id": None,
        "loaded_at": None,
    }
    await ensure_exchange_vault(application, force=True)


def _signed_query(secret_key: str, params: dict[str, Any]) -> str:
    query = urlencode(params)
    signature = hmac.new(secret_key.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def _safe_exchange_message(payload: Any, api_key: str) -> str:
    if isinstance(payload, dict):
        raw = str(payload.get("msg") or "Binance bağlantıyı reddetti.")
    else:
        raw = "Binance bağlantıyı reddetti."
    return raw.replace(api_key, "[gizli]")[:240]


async def _signed_get(http: httpx.AsyncClient, host: str, path: str, api_key: str, secret_key: str, timestamp: int) -> Any:
    query = _signed_query(secret_key, {"timestamp": timestamp, "recvWindow": 5000})
    try:
        response = await http.get(f"{host}{path}?{query}", headers={"X-MBX-APIKEY": api_key})
    except httpx.RequestError as exc:
        raise VaultError("Binance sunucusuna ulaşılamadı.") from exc
    if response.status_code >= 400:
        try:
            payload = response.json()
        except ValueError:
            payload = None
        raise VaultError(_safe_exchange_message(payload, api_key))
    try:
        return response.json()
    except ValueError as exc:
        raise VaultError("Binance beklenmeyen bir yanıt döndürdü.") from exc


async def test_binance_credentials(http: httpx.AsyncClient, mode: str, api_key: str, secret_key: str) -> dict[str, Any]:
    normalized = normalize_mode(mode)
    validate_key_pair(api_key, secret_key)
    host = HOSTS[normalized]
    before = int(time.time() * 1000)
    try:
        time_response = await http.get(f"{host}/fapi/v1/time")
        time_response.raise_for_status()
    except httpx.TimeoutException as exc:
        raise VaultError("Binance saat servisine ulaşırken zaman aşımı oluştu.") from exc
    except httpx.ConnectError as exc:
        raise VaultError("Binance saat servisine bağlantı veya DNS kurulamadı.") from exc
    except httpx.NetworkError as exc:
        raise VaultError("Binance saat servisine ağ bağlantısı kurulamadı.") from exc
    except httpx.HTTPStatusError as exc:
        raise VaultError(f"Binance saat servisi HTTP {exc.response.status_code} döndürdü.") from exc
    except httpx.HTTPError as exc:
        raise VaultError("Binance saat servisi HTTP isteği başarısız oldu.") from exc

    try:
        time_payload = time_response.json()
    except ValueError as exc:
        raise VaultError("Binance saat servisi geçerli JSON döndürmedi.") from exc
    if not isinstance(time_payload, dict) or "serverTime" not in time_payload:
        raise VaultError("Binance saat servisi yanıtında serverTime alanı bulunamadı.")
    try:
        server_time = int(time_payload["serverTime"])
    except (TypeError, ValueError) as exc:
        raise VaultError("Binance saat servisi serverTime alanını geçerli bir sayıya dönüştüremedi.") from exc
    after = int(time.time() * 1000)
    offset = server_time - ((before + after) // 2)
    timestamp = int(time.time() * 1000) + offset
    account = await _signed_get(http, host, "/fapi/v3/account", api_key, secret_key, timestamp)
    position_mode = await _signed_get(http, host, "/fapi/v1/positionSide/dual", api_key, secret_key, timestamp)
    if not isinstance(account, dict):
        raise VaultError("Binance hesap yanıtı geçersiz.")
    positions = account.get("positions") if isinstance(account.get("positions"), list) else []
    active_positions = sum(
        1 for row in positions
        if isinstance(row, dict) and abs(float(row.get("positionAmt") or 0)) > 0
    )
    return {
        "mode": normalized,
        "host": host,
        "wallet_balance": float(account.get("totalWalletBalance") or 0),
        "available_balance": float(account.get("availableBalance") or 0),
        "unrealized_pnl": float(account.get("totalUnrealizedProfit") or 0),
        "active_positions": active_positions,
        "hedge_mode": bool(position_mode.get("dualSidePosition")) if isinstance(position_mode, dict) else None,
        "clock_offset_ms": offset,
        "tested_at": now_iso(),
        "orders_created": False,
    }


def _public_connection(mode: str) -> dict[str, Any]:
    meta = _META.get(mode, {})
    return {
        "mode": mode,
        "label": "Binance Futures Demo" if mode == "TESTNET" else "Binance USD-M Futures Gerçek",
        "host": HOSTS[mode],
        "configured": bool(meta.get("configured")),
        "active": bool(meta.get("active")),
        "fingerprint": meta.get("fingerprint"),
        "last_test_ok": bool(meta.get("last_test_ok")),
        "last_test_at": meta.get("last_test_at"),
        "last_error": meta.get("last_error"),
        "account": meta.get("account"),
        "storage": credential_source(mode) if meta.get("configured") else "YAPILANDIRILMADI",
        "secrets_returned": False,
    }


def public_status(application: Any) -> dict[str, Any]:
    state = application.state.exchange_vault
    return {
        "version": VERSION,
        "vault": {
            "ready": bool(state.get("ready")),
            "storage": state.get("storage"),
            "reason": state.get("reason"),
            "loaded_at": state.get("loaded_at"),
        },
        "connections": {mode: _public_connection(mode) for mode in HOSTS},
        "safety": {
            "https_required": True,
            "secrets_returned_to_browser": False,
            "connection_test_creates_orders": False,
            "activation_arms_orders": False,
            "live_orders_require_v25_gates": True,
            "withdrawals_supported": False,
        },
    }


def _lock_runtime(application: Any, mode: str) -> None:
    if mode == "TESTNET" and hasattr(application.state, "binance_demo"):
        state = application.state.binance_demo
        state["armed_until"] = 0
        state["connected"] = False
    if mode == "LIVE" and hasattr(application.state, "v25_execution"):
        state = application.state.v25_execution
        state["armed_until"] = 0.0
        state["connected"] = False
        state["auto"]["enabled"] = False
        state["auto"]["session_until"] = 0.0
        state["web_consent"] = {"accepted_at": None, "expires_at_epoch": 0.0, "key_fingerprint": None}


async def _require_ready(application: Any) -> Any:
    if not await ensure_exchange_vault(application):
        reason = application.state.exchange_vault.get("reason") or "Şifreli kasa hazır değil."
        raise HTTPException(503, reason)
    return application.state.db_pool


def _require_owner_member(request: Request) -> dict[str, Any]:
    user = getattr(request.state, "member", None)
    if not user:
        raise HTTPException(401, "Oturum gerekli")
    if user.get("role") != "OWNER":
        raise HTTPException(403, "Yönetici yetkisi gerekli")
    return user


@router.get("/status")
async def exchange_connection_status(request: Request) -> dict[str, Any]:
    await ensure_exchange_vault(request.app)
    return public_status(request.app)


@router.post("/test")
async def exchange_connection_test(request: Request, body: TestCredentialsRequest) -> dict[str, Any]:
    _require_owner_member(request)
    await _require_ready(request.app)
    mode = normalize_mode(body.mode)
    if body.api_key is not None and body.secret_key is not None:
        api_key = body.api_key.get_secret_value().strip()
        secret_key = body.secret_key.get_secret_value().strip()
    else:
        api_key, secret_key = cached_credentials(mode, active_only=False)
    if not api_key or not secret_key:
        raise HTTPException(412, "Önce API Key ve Secret Key girin veya kasaya kaydedin.")
    try:
        account = await test_binance_credentials(request.app.state.http, mode, api_key, secret_key)
    except VaultError as exc:
        raise HTTPException(502, str(exc)) from exc
    return {"ok": True, "message": "Bağlantı ve imza doğrulandı; hiçbir emir oluşturulmadı.", "account": account}


@router.post("/save")
async def exchange_connection_save(request: Request, body: SaveCredentialsRequest) -> dict[str, Any]:
    _require_owner_member(request)
    pool = await _require_ready(request.app)
    mode = normalize_mode(body.mode)
    if body.confirmation.strip().upper() != SAVE_CONFIRMATIONS[mode]:
        raise HTTPException(422, "Güvenli kaydetme onayı eksik.")
    api_key = body.api_key.get_secret_value().strip()
    secret_key = body.secret_key.get_secret_value().strip()
    try:
        validate_key_pair(api_key, secret_key)
        account = await test_binance_credentials(request.app.state.http, mode, api_key, secret_key)
        encrypted = encrypt_credentials(api_key, secret_key, mode=mode)
    except VaultError as exc:
        raise HTTPException(422 if "kısa" in str(exc) or "boşluk" in str(exc) else 502, str(exc)) from exc
    fingerprint = key_fingerprint(api_key)
    await pool.execute(
        """
        INSERT INTO protrebot_exchange_vault
          (mode, encrypted_payload, fingerprint, active, last_test_ok, last_test_at, last_error, account_summary, updated_at)
        VALUES ($1, $2, $3, FALSE, TRUE, NOW(), NULL, $4::jsonb, NOW())
        ON CONFLICT (mode) DO UPDATE SET
          encrypted_payload = EXCLUDED.encrypted_payload,
          fingerprint = EXCLUDED.fingerprint,
          active = FALSE,
          last_test_ok = TRUE,
          last_test_at = NOW(),
          last_error = NULL,
          account_summary = EXCLUDED.account_summary,
          updated_at = NOW()
        """,
        mode,
        encrypted,
        fingerprint,
        json.dumps(account, ensure_ascii=False),
    )
    _CACHE[mode] = (api_key, secret_key)
    _META[mode] = {
        "configured": True,
        "active": False,
        "fingerprint": fingerprint,
        "last_test_ok": True,
        "last_test_at": account["tested_at"],
        "last_error": None,
        "account": account,
        "updated_at": now_iso(),
    }
    _lock_runtime(request.app, mode)
    return {**public_status(request.app), "message": "Anahtarlar şifreli kasaya kaydedildi. Kullanmak için bağlantıyı ayrıca aktifleştirin."}


@router.post("/activate")
async def exchange_connection_activate(request: Request, body: ConnectionActionRequest) -> dict[str, Any]:
    _require_owner_member(request)
    pool = await _require_ready(request.app)
    mode = normalize_mode(body.mode)
    if body.confirmation.strip().upper() != ACTIVATE_CONFIRMATIONS[mode]:
        raise HTTPException(422, "Bağlantı aktifleştirme onayı eksik.")
    api_key, secret_key = cached_credentials(mode, active_only=False)
    if not api_key or not secret_key:
        raise HTTPException(412, "Bu kanal için kasada kayıtlı anahtar yok.")
    try:
        account = await test_binance_credentials(request.app.state.http, mode, api_key, secret_key)
    except VaultError as exc:
        await pool.execute(
            "UPDATE protrebot_exchange_vault SET active = FALSE, last_test_ok = FALSE, last_test_at = NOW(), last_error = $2, updated_at = NOW() WHERE mode = $1",
            mode,
            str(exc)[:240],
        )
        _META[mode].update({"active": False, "last_test_ok": False, "last_test_at": now_iso(), "last_error": str(exc)[:240]})
        _lock_runtime(request.app, mode)
        raise HTTPException(502, str(exc)) from exc
    await pool.execute(
        "UPDATE protrebot_exchange_vault SET active = TRUE, last_test_ok = TRUE, last_test_at = NOW(), last_error = NULL, account_summary = $2::jsonb, updated_at = NOW() WHERE mode = $1",
        mode,
        json.dumps(account, ensure_ascii=False),
    )
    _META[mode].update({"active": True, "last_test_ok": True, "last_test_at": account["tested_at"], "last_error": None, "account": account, "updated_at": now_iso()})
    return {**public_status(request.app), "message": "Bağlantı aktifleştirildi. Bu işlem emir kilidini açmadı."}


@router.post("/deactivate")
async def exchange_connection_deactivate(request: Request, body: ConnectionActionRequest) -> dict[str, Any]:
    _require_owner_member(request)
    pool = await _require_ready(request.app)
    mode = normalize_mode(body.mode)
    if body.confirmation.strip().upper() != "BAĞLANTIYI KAPAT":
        raise HTTPException(422, "Bağlantıyı kapatma onayı eksik.")
    await pool.execute(
        "UPDATE protrebot_exchange_vault SET active = FALSE, updated_at = NOW() WHERE mode = $1",
        mode,
    )
    if mode in _META:
        _META[mode]["active"] = False
        _META[mode]["updated_at"] = now_iso()
    _lock_runtime(request.app, mode)
    return {**public_status(request.app), "message": "Bağlantı kapatıldı; yeni emir yetkileri sıfırlandı."}


@router.delete("/credentials")
async def exchange_connection_delete(request: Request, body: ConnectionActionRequest) -> dict[str, Any]:
    _require_owner_member(request)
    pool = await _require_ready(request.app)
    mode = normalize_mode(body.mode)
    if body.confirmation.strip().upper() != DELETE_CONFIRMATION:
        raise HTTPException(422, f"Silmek için {DELETE_CONFIRMATION} onayı gerekir.")
    if _META.get(mode, {}).get("active"):
        raise HTTPException(409, "Önce bağlantıyı devre dışı bırakın; sonra anahtarı silebilirsiniz.")
    api_key, secret_key = cached_credentials(mode, active_only=False)
    if api_key and secret_key:
        try:
            account = await test_binance_credentials(request.app.state.http, mode, api_key, secret_key)
            if int(account.get("active_positions") or 0) > 0:
                raise HTTPException(409, "Açık pozisyon varken anahtar silinemez. Önce Binance hesabındaki pozisyonları güvenle kapatın.")
        except HTTPException:
            raise
        except VaultError:
            # Invalid/expired credentials must remain removable after the
            # connection has been deactivated.
            pass
    await pool.execute("DELETE FROM protrebot_exchange_vault WHERE mode = $1", mode)
    _CACHE.pop(mode, None)
    _META.pop(mode, None)
    _lock_runtime(request.app, mode)
    return {**public_status(request.app), "message": "Şifreli anahtar kaydı kalıcı olarak silindi."}

