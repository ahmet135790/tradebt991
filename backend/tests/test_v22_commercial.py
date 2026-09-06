import asyncio
import base64
import email
import logging
import os
import sys
import time
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).parents[2]
BACKEND = ROOT / "backend"
sys.path.insert(0, str(BACKEND))

from app.commercial_core import (  # noqa: E402
    FeeGuardInput,
    calculate_fee_guard,
    calculate_grid_guard,
    default_commercial_state,
    hash_password,
    issue_token,
    verify_password,
    verify_token,
)
from app.v22_commercial import gmail_failure_log, send_auth_email, sync_v22_storage, v22_admin_link_trading_account, v22_admin_trading_accounts, v22_admin_unlink_trading_account, v22_verification_status  # noqa: E402
from app.main import health_check_redis, health_item, healthz, run_health_checks  # noqa: E402


MAIN_SOURCE = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
V22_SOURCE = (BACKEND / "app" / "v22_commercial.py").read_text(encoding="utf-8")
CORE_SOURCE = (BACKEND / "app" / "commercial_core.py").read_text(encoding="utf-8")
AGENT_SOURCE = (BACKEND / "v22_agent.py").read_text(encoding="utf-8")
FRONTEND_SOURCE = (ROOT / "frontend" / "src" / "CommercialHub.tsx").read_text(encoding="utf-8")
LOCAL_STORAGE_SOURCE = (BACKEND / "app" / "local_storage.py").read_text(encoding="utf-8")
GITIGNORE_SOURCE = (ROOT / ".gitignore").read_text(encoding="utf-8")


class V22CommercialTests(unittest.TestCase):
    def test_health_result_is_safe_and_standardized(self):
        result = health_item("Database", "ERROR", "Database connection failed.", 0.0)
        self.assertEqual(set(result), {"name", "status", "message", "checked_at", "latency_ms"})
        self.assertEqual(result["status"], "ERROR")
        self.assertNotIn("password", str(result).lower())

    def test_redis_without_url_is_disabled_not_error(self):
        application = SimpleNamespace(state=SimpleNamespace(redis_client=None))
        with patch("app.main.REDIS_URL", ""):
            result = asyncio.run(health_check_redis(application))
        self.assertEqual(result["status"], "DISABLED")
        self.assertEqual(result["message"], "Redis is not configured.")

    def test_health_snapshot_has_real_aggregate_and_persists_without_side_effects(self):
        application = SimpleNamespace(state=SimpleNamespace(
            health_lock=asyncio.Lock(),
            health_snapshot=None,
            v22_commercial={"secret": b"owner-secret", "state": {"subscriptions": []}},
        ))
        checks = [
            AsyncMock(return_value=health_item("Database", "ERROR", "Database connection failed.", 0.0)),
            AsyncMock(return_value=health_item("Redis", "ACTIVE", "Redis connection healthy.", 0.0)),
            AsyncMock(return_value=health_item("Gmail", "WARNING", "OAuth refresh timed out.", 0.0)),
            AsyncMock(return_value=health_item("Frontend", "ACTIVE", "Frontend reachable.", 0.0)),
            AsyncMock(return_value=health_item("Market data", "ACTIVE", "Read-only endpoint reachable.", 0.0)),
        ]
        with patch("app.main.health_check_database", checks[0]), patch("app.main.health_check_redis", checks[1]), patch("app.main.health_check_gmail", checks[2]), patch("app.main.health_check_frontend", checks[3]), patch("app.main.health_check_market_data", checks[4]), patch("app.main.persist_health_snapshot", new=AsyncMock(return_value=True)):
            snapshot = asyncio.run(run_health_checks(application))
        self.assertEqual(snapshot["overall_status"], "ERROR")
        self.assertEqual(snapshot["last_checked_at"], snapshot["checked_at"])
        self.assertEqual(snapshot["incident_count"], 2)
        self.assertEqual(snapshot["counts"]["ERROR"], 1)
        self.assertEqual(snapshot["counts"]["WARNING"], 1)
        self.assertEqual((asyncio.run(healthz()))["status"], "ok")

    def test_admin_panel_reads_snapshot_and_uses_post_then_get(self):
        panel_source = (ROOT / "AdminPanel.tsx").read_text(encoding="utf-8")
        self.assertIn("/admin/system-health", panel_source)
        self.assertIn("/admin/system-health/check", panel_source)
        self.assertIn("await refreshHealth()", panel_source)
        self.assertIn("45000", panel_source)
        self.assertIn("last_checked_at", panel_source)

    def test_health_routes_treat_request_as_request_context(self):
        source = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn("async def admin_health(request: Request)", source)
        self.assertIn("async def admin_health_check(request: Request)", source)

    def test_trading_account_ownership_schema_is_credential_free_and_idempotent(self):
        source = V22_SOURCE
        self.assertIn("CREATE TABLE IF NOT EXISTS trading_accounts", source)
        self.assertIn("UNIQUE (user_id, provider, environment, account_reference)", source)
        self.assertIn("CHECK (environment IN ('DEMO', 'TESTNET', 'PAPER', 'LIVE'))", source)
        self.assertIn("account_reference TEXT NOT NULL DEFAULT ''", source)
        self.assertIn("CREATE INDEX IF NOT EXISTS trading_accounts_user_id_idx", source)
        for forbidden in ("api_key", "secret", "password", "refresh_token"):
            self.assertNotIn(f"{forbidden} TEXT", source)

    def test_owner_trading_account_endpoint_contract_is_safe(self):
        source = V22_SOURCE
        self.assertIn('@router.get("/admin/users/{user_id}/trading-accounts")', source)
        self.assertIn("authenticated_user(request, owner=True)", source)
        self.assertIn('SELECT id, provider, environment, status, created_at, updated_at', source)
        self.assertNotIn('SELECT * FROM trading_accounts', source)
        self.assertIn('raise HTTPException(404, "Kullanıcı bulunamadı")', source)

    def test_trading_account_endpoint_enforces_owner_and_returns_empty_without_accounts(self):
        from fastapi import HTTPException

        secret = b"trading-account-owner-secret-long-enough"
        owner = {"id": "owner-account-test", "role": "OWNER", "active": True, "email_verified": True, "auth_version": 1}
        customer = {"id": "customer-account-test", "role": "CUSTOMER", "active": True, "email_verified": True, "auth_version": 1}

        class Pool:
            def __init__(self, rows=None):
                self.rows = rows or []
                self.executed = []

            async def execute(self, query, *args):
                self.executed.append(query)

            async def fetch(self, query, *args):
                return self.rows

        pool = Pool()
        application = SimpleNamespace(state=SimpleNamespace(
            db_pool=pool,
            v22_commercial={"secret": secret, "state": {"users": [owner, customer]}},
        ))
        owner_token = issue_token(owner["id"], owner["role"], secret, now=int(time.time()), ttl_seconds=3_600)
        customer_token = issue_token(customer["id"], customer["role"], secret, now=int(time.time()), ttl_seconds=3_600)
        owner_request = SimpleNamespace(app=application, headers={"authorization": f"Bearer {owner_token}"})
        customer_request = SimpleNamespace(app=application, headers={"authorization": f"Bearer {customer_token}"})
        missing_request = SimpleNamespace(app=application, headers={"authorization": f"Bearer {owner_token}"})

        empty = asyncio.run(v22_admin_trading_accounts(customer["id"], owner_request))
        self.assertEqual(empty, {"user_id": customer["id"], "accounts": []})
        with self.assertRaisesRegex(HTTPException, "Yönetici yetkisi gerekli"):
            asyncio.run(v22_admin_trading_accounts(customer["id"], customer_request))
        unauthenticated = SimpleNamespace(app=application, headers={})
        with self.assertRaisesRegex(HTTPException, "Oturum gerekli"):
            asyncio.run(v22_admin_trading_accounts(customer["id"], unauthenticated))
        with self.assertRaisesRegex(HTTPException, "Kullanıcı bulunamadı"):
            asyncio.run(v22_admin_trading_accounts("missing-user", missing_request))

        self.assertEqual(len(pool.executed), 3)

    def test_trading_account_endpoint_returns_only_safe_account_fields(self):
        secret = b"trading-account-response-secret-long-enough"
        owner = {"id": "owner-account-response", "role": "OWNER", "active": True, "email_verified": True, "auth_version": 1}
        created = SimpleNamespace(isoformat=lambda: "2026-09-05T00:00:00+00:00")
        row = {"id": "account-1", "provider": "BINANCE", "environment": "TESTNET", "account_reference": "ahmet-testnet", "status": "ACTIVE", "created_at": created, "updated_at": created}

        class Pool:
            async def execute(self, query, *args):
                return None

            async def fetch(self, query, *args):
                return [row]

        application = SimpleNamespace(state=SimpleNamespace(
            db_pool=Pool(),
            v22_commercial={"secret": secret, "state": {"users": [owner]}},
        ))
        token = issue_token(owner["id"], owner["role"], secret, now=int(time.time()), ttl_seconds=3_600)
        request = SimpleNamespace(app=application, headers={"authorization": f"Bearer {token}"})
        response = asyncio.run(v22_admin_trading_accounts(owner["id"], request))
        account = response["accounts"][0]
        self.assertEqual(account["provider"], "BINANCE")
        self.assertEqual(account["environment"], "TESTNET")
        self.assertNotIn("api_key", str(response).lower())
        self.assertNotIn("secret", str(response).lower())
        self.assertNotIn("password", str(response).lower())

    def test_owner_can_link_and_unlink_mapping_without_provider_calls(self):
        from fastapi import HTTPException
        secret = b"trading-account-link-secret-long-enough"
        owner = {"id": "owner-link-test", "role": "OWNER", "active": True, "email_verified": True, "auth_version": 1}

        class Pool:
            def __init__(self):
                self.rows = [{"id": "account-1", "provider": "BINANCE", "environment": "TESTNET", "account_reference": "safe ref", "status": "UNASSIGNED", "created_at": "2026-09-05T00:00:00+00:00", "updated_at": "2026-09-05T00:00:00+00:00"}]
                self.deleted = False

            async def execute(self, query, *args):
                return None

            async def fetchrow(self, query, *args):
                if query.lstrip().upper().startswith("INSERT"):
                    return self.rows[0]
                if query.lstrip().upper().startswith("DELETE"):
                    if self.deleted:
                        return None
                    self.deleted = True
                    return {"id": "account-1"}
                return None

        pool = Pool()
        application = SimpleNamespace(state=SimpleNamespace(db_pool=pool, v22_commercial={"secret": secret, "state": {"users": [owner]}}))
        token = issue_token(owner["id"], owner["role"], secret, now=int(time.time()), ttl_seconds=3_600)
        request = SimpleNamespace(app=application, headers={"authorization": f"Bearer {token}"})
        linked = asyncio.run(v22_admin_link_trading_account("owner-link-test", SimpleNamespace(provider="BINANCE", environment="TESTNET", account_reference="  safe   ref  "), request))
        self.assertEqual(linked["account"]["status"], "UNASSIGNED")
        self.assertEqual(linked["account"]["account_reference"], "safe ref")
        removed = asyncio.run(v22_admin_unlink_trading_account("owner-link-test", "account-1", request))
        self.assertTrue(removed["ok"])

    def test_link_rejects_invalid_or_duplicate_mapping_without_provider_access(self):
        from fastapi import HTTPException
        from pydantic import ValidationError
        secret = b"trading-account-validation-secret-long-enough"
        owner = {"id": "owner-validation-test", "role": "OWNER", "active": True, "email_verified": True, "auth_version": 1}

        class Pool:
            async def execute(self, query, *args):
                return None

            async def fetchrow(self, query, *args):
                raise Exception("duplicate key value violates unique constraint")

        application = SimpleNamespace(state=SimpleNamespace(db_pool=Pool(), v22_commercial={"secret": secret, "state": {"users": [owner]}}))
        token = issue_token(owner["id"], owner["role"], secret, now=int(time.time()), ttl_seconds=3_600)
        request = SimpleNamespace(app=application, headers={"authorization": f"Bearer {token}"})
        with self.assertRaisesRegex(HTTPException, "mapping zaten mevcut"):
            asyncio.run(v22_admin_link_trading_account("owner-validation-test", SimpleNamespace(provider="BINANCE", environment="TESTNET", account_reference="safe-ref"), request))
        from app.v22_commercial import TradingAccountLinkRequest
        with self.assertRaises(ValidationError):
            TradingAccountLinkRequest(provider="BINANCE", environment="INVALID", account_reference="safe-ref")
        with self.assertRaises(ValidationError):
            TradingAccountLinkRequest(provider="BINANCE", environment="TESTNET", account_reference="")
        with self.assertRaises(ValidationError):
            TradingAccountLinkRequest(provider="BINANCE", environment="TESTNET", account_reference="safe-ref", api_key="forbidden")

    def test_health_endpoints_are_owner_protected_and_render_cron_is_absent(self):
        source = (BACKEND / "app" / "main.py").read_text(encoding="utf-8")
        render_source = (ROOT / "render.yaml").read_text(encoding="utf-8")
        self.assertIn('"/api/v22/admin/system-health"', source)
        self.assertIn('"/api/v22/admin/system-health/check"', source)
        self.assertIn('"/healthz"', (BACKEND / "app" / "web_security.py").read_text(encoding="utf-8"))
        self.assertIn("authenticated_user(request, owner=True)", source)
        self.assertIn("HEALTH_CHECK_INTERVAL_SECONDS = 15 * 60", source)
        self.assertIn('"overall_status": overall_status', source)
        self.assertIn('"last_checked_at": checked_at', source)
        self.assertIn('healthCheckPath: /healthz', render_source)
        self.assertNotIn("health_monitor_loop", source)
        self.assertNotIn("type: cron", render_source)
        self.assertNotIn('schedule: "*/15 * * * *"', render_source)
    def test_verification_status_reads_without_consuming_token(self):
        secret = b"verification-status-test-secret-long-enough"
        user = {"id": "user-1", "role": "CUSTOMER", "email_verified": False}
        token = issue_token(user["id"], user["role"], secret, kind="EMAIL_STATUS", now=int(time.time()), ttl_seconds=3_600)
        application = SimpleNamespace(state=SimpleNamespace(v22_commercial={"secret": secret, "state": {"users": [user]}}))
        request = SimpleNamespace(app=application)
        self.assertEqual(asyncio.run(v22_verification_status(request, token)), {"verified": False})
        user["email_verified"] = True
        self.assertEqual(asyncio.run(v22_verification_status(request, token)), {"verified": True})

    def test_gmail_delivery_requires_oauth_configuration_without_sending(self):
        with patch.dict(os.environ, {}, clear=True), patch("app.v22_commercial.build") as gmail_build:
            with self.assertRaisesRegex(RuntimeError, "Gmail API yapılandırması eksik"):
                send_auth_email(
                    to_email="user@example.com", display_name="Test User", subject="Verify",
                    title="Verify", action_url="https://example.com/verify?token=local", action_label="VERIFY",
                )
            gmail_build.assert_not_called()

    def test_gmail_delivery_builds_rfc2822_html_message_and_sends_via_mock_api(self):
        sent = {}

        class Messages:
            def send(self, *, userId, body):
                sent.update({"user_id": userId, "body": body})
                return self

            def execute(self):
                return {"id": "mock-message"}

        class Users:
            def messages(self):
                return Messages()

        class Gmail:
            def users(self):
                return Users()

        env = {
            "GMAIL_CLIENT_ID": "client-id-for-test",
            "GMAIL_CLIENT_SECRET": "client-secret-for-test",
            "GMAIL_REFRESH_TOKEN": "refresh-token-for-test",
            "GMAIL_FROM_EMAIL": "privacykais@gmail.com",
            "GMAIL_FROM_NAME": "ProTreBot",
        }
        with patch.dict(os.environ, env, clear=True), patch("app.v22_commercial.build", return_value=Gmail()) as gmail_build:
            send_auth_email(
                to_email="user@example.com", display_name="Test User", subject="Verify",
                title="Verify", action_url="https://example.com/verify?token=local", action_label="VERIFY",
            )
        gmail_build.assert_called_once_with("gmail", "v1", credentials=gmail_build.call_args.kwargs["credentials"], cache_discovery=False)
        self.assertEqual(sent["user_id"], "me")
        decoded = base64.urlsafe_b64decode(sent["body"]["raw"] + "=" * (-len(sent["body"]["raw"]) % 4))
        parsed = email.message_from_bytes(decoded)
        self.assertEqual(parsed["From"], "ProTreBot <privacykais@gmail.com>")
        part_types = [part.get_content_type() for part in parsed.walk()]
        self.assertIn("text/plain", part_types)
        self.assertIn("text/html", part_types)
        self.assertNotIn("client-secret-for-test", decoded.decode("utf-8", errors="replace"))
        self.assertNotIn("refresh-token-for-test", decoded.decode("utf-8", errors="replace"))

    def test_gmail_diagnostic_redacts_credentials_and_classifies_api_auth(self):
        error = type("MockHttpError", (Exception,), {"resp": SimpleNamespace(status=401)})("access_token=secret-value for user@example.com")
        details = gmail_failure_log(error)
        self.assertEqual(details["reason"], "authentication")
        self.assertEqual(details["code"], "401")
        self.assertNotIn("secret-value", details["message"])
        self.assertNotIn("user@example.com", details["message"])

    def test_postgres_snapshot_restores_owner_after_runtime_restart(self):
        restored_state = default_commercial_state()
        restored_state["owner_user_id"] = "owner-from-postgres"
        restored_state["users"] = [{"id": "owner-from-postgres", "email": "owner@example.com", "role": "OWNER", "active": True}]

        class SnapshotPool:
            async def execute(self, query, *args):
                return "OK"

            async def fetchrow(self, query, *args):
                return {"payload": restored_state}

        application = SimpleNamespace()
        application.state = SimpleNamespace(
            db_pool=SnapshotPool(),
            v22_commercial={
                "state": default_commercial_state(),
                "storage_lock": asyncio.Lock(),
                "storage_ready": False,
                "restore_attempted": False,
                "storage_status": "YEREL_YEDEK",
            },
        )

        asyncio.run(sync_v22_storage(application))

        runtime = application.state.v22_commercial
        self.assertEqual(runtime["state"]["owner_user_id"], "owner-from-postgres")
        self.assertEqual(runtime["storage_status"], "POSTGRESQL_KALICI")

    def test_passwords_are_scrypt_hashed_and_verified(self):
        record = hash_password("CokGuvenli-Parola-22")
        self.assertTrue(verify_password("CokGuvenli-Parola-22", record))
        self.assertFalse(verify_password("yanlis-parola", record))
        self.assertNotIn("CokGuvenli", str(record))

    def test_tokens_are_signed_typed_and_expiring(self):
        secret = b"v22-test-secret-that-is-long-enough-for-hmac"
        token = issue_token("owner-1", "OWNER", secret, now=1_000, ttl_seconds=120)
        payload = verify_token(token, secret, expected_kind="USER", now=1_050)
        self.assertEqual(payload["sub"], "owner-1")
        with self.assertRaises(ValueError):
            verify_token(token, secret, expected_kind="AGENT", now=1_050)
        with self.assertRaises(ValueError):
            verify_token(token, secret, now=1_121)

    def test_fee_guard_rejects_target_that_does_not_cover_costs(self):
        blocked = calculate_fee_guard(FeeGuardInput(entry=100, target=100.05, notional_usdt=1_000, fee_bps_per_side=4, slippage_bps_per_side=2, minimum_net_usdt=0.25))
        self.assertFalse(blocked["approved"])
        approved = calculate_fee_guard(FeeGuardInput(entry=100, target=101, notional_usdt=1_000, fee_bps_per_side=4, slippage_bps_per_side=2, minimum_net_usdt=0.25))
        self.assertTrue(approved["approved"])
        self.assertGreater(approved["net_usdt"], approved["minimum_required_usdt"])

    def test_short_direction_and_grid_costs_are_supported(self):
        short = calculate_fee_guard(FeeGuardInput(entry=100, target=98, notional_usdt=500, direction="SHORT"))
        self.assertTrue(short["approved"])
        grid = calculate_grid_guard(lower=90, upper=110, grid_count=11, capital_usdt=1_000, maker_share_pct=80)
        self.assertTrue(grid["approved"])
        self.assertGreater(grid["net_cycle_usdt"], 0)

    def test_default_state_can_never_enable_money_or_real_orders(self):
        state = default_commercial_state()
        self.assertTrue(state["security"]["demo_only"])
        self.assertFalse(state["security"]["real_orders_enabled"])
        self.assertFalse(state["security"]["testnet_orders_enabled"])
        self.assertFalse(state["security"]["withdrawals_supported"])
        self.assertFalse(state["security"]["central_exchange_credentials"])
        self.assertFalse(state["billing"]["live"])

    def test_v24_is_wired_into_api_and_interface(self):
        self.assertIn('version="25.0.0"', MAIN_SOURCE)
        self.assertIn("init_v22_commercial(app)", MAIN_SOURCE)
        self.assertIn("v22_commercial_router", MAIN_SOURCE)
        for label in ("Business & Robot Control Center", "Satış Merkezi", "Müşteriler", "Lisans & Ajan", "Net Kâr Koruması", "Yayın Kapısı"):
            self.assertIn(label, FRONTEND_SOURCE)

    def test_v23_tokens_can_be_revoked_with_a_version_bump(self):
        secret = b"v23-test-secret-that-is-long-enough-for-hmac"
        token = issue_token("owner-1", "OWNER", secret, token_version=4, now=2_000)
        payload = verify_token(token, secret, expected_kind="USER", now=2_010)
        self.assertEqual(payload["ver"], 4)

    def test_v23_release_evidence_starts_unverified(self):
        state = default_commercial_state()
        self.assertEqual(state["version"], "25.0.0")
        for key in ("backup", "support", "legal", "security_review"):
            self.assertEqual(state["release_evidence"][key]["status"], "PENDING")

    def test_v23_management_and_operations_endpoints_are_present(self):
        for route in (
            '"/operations"',
            '"/auth/change-password"',
            '"/customers/{user_id}/status"',
            '"/licenses/{license_id}/revoke"',
            '"/agents/{agent_id}/revoke"',
            '"/release-evidence/{evidence_key}"',
        ):
            self.assertIn(route, V22_SOURCE)
        self.assertIn("def monitor", AGENT_SOURCE)
        self.assertIn("HEARTBEAT_SECONDS = 45", AGENT_SOURCE)

    def test_logout_invalidates_existing_sessions_and_persistence_is_configured(self):
        self.assertIn('@router.post("/auth/logout")', V22_SOURCE)
        self.assertIn('user["auth_version"] = int(user.get("auth_version", 1)) + 1', V22_SOURCE)
        self.assertIn("PROTREBOT_DURABLE_AUTH_REQUIRED", V22_SOURCE)
        self.assertIn("PROTREBOT_DURABLE_AUTH_REQUIRED", (ROOT / "render.yaml").read_text(encoding="utf-8"))
        self.assertIn("/auth/logout", FRONTEND_SOURCE)

    def test_owner_registration_is_one_time_and_later_opens_login(self):
        self.assertIn("info.setup_required", FRONTEND_SOURCE)
        self.assertIn("TEK SEFERLİK KAYIT", FRONTEND_SOURCE)
        self.assertIn("HESAP HAZIR", FRONTEND_SOURCE)
        self.assertIn("Bu bilgisayarda beni hatırla", FRONTEND_SOURCE)
        self.assertIn("localStorage", FRONTEND_SOURCE)
        self.assertIn("sessionStorage", FRONTEND_SOURCE)
        self.assertIn("REMEMBER_SESSION_SECONDS", V22_SOURCE)
        self.assertIn("payload.remember", V22_SOURCE)

    def test_owner_and_encrypted_demo_state_use_update_safe_windows_storage(self):
        self.assertIn("LOCALAPPDATA", LOCAL_STORAGE_SOURCE)
        self.assertIn("ProTreBotEliteX", LOCAL_STORAGE_SOURCE)
        self.assertIn("PROTREBOT_DATA_DIR", LOCAL_STORAGE_SOURCE)
        self.assertIn("migrate_legacy_files", V22_SOURCE)

    def test_agent_never_requests_or_transmits_exchange_credentials(self):
        self.assertNotIn("api_key", AGENT_SOURCE.casefold())
        self.assertNotIn("secret_key", AGENT_SOURCE.casefold())
        self.assertIn("fingerprint", AGENT_SOURCE)
        self.assertIn("save_agent_token", AGENT_SOURCE)
        self.assertIn("central_exchange_credentials", CORE_SOURCE)

    def test_v22_secrets_and_runtime_state_are_excluded(self):
        for path in ("v22_commercial_state.json", "v22_server_secret.dat", "v22_agent_token.dat"):
            self.assertIn(path, GITIGNORE_SOURCE)


if __name__ == "__main__":
    unittest.main(verbosity=2)
