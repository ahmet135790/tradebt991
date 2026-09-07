import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from app import v22_commercial
from app.subscription_core import PLAN_CATALOG, TRIAL_DAYS, active_subscription, entitlement_snapshot


class SubscriptionCoreTests(unittest.TestCase):
    def test_plan_prices_and_entitlements_are_centralized(self):
        self.assertEqual(PLAN_CATALOG["STARTER"]["monthly_price"], 19)
        self.assertEqual(PLAN_CATALOG["STARTER"]["annual_price"], 190)
        self.assertEqual(PLAN_CATALOG["PRO"]["monthly_price"], 39)
        self.assertEqual(PLAN_CATALOG["PRO"]["annual_price"], 390)
        self.assertEqual(PLAN_CATALOG["ELITE"]["monthly_price"], 79)
        self.assertEqual(PLAN_CATALOG["ELITE"]["annual_price"], 790)
        self.assertFalse(PLAN_CATALOG["STARTER"]["entitlements"]["canUseLiveTrading"])
        self.assertTrue(PLAN_CATALOG["PRO"]["entitlements"]["canUseLiveTrading"])
        self.assertTrue(PLAN_CATALOG["ELITE"]["entitlements"]["canUseAdvancedAI"])

    def test_trial_is_seven_days_and_expired_subscription_is_free(self):
        now = datetime.now(timezone.utc)
        state = {"subscriptions": [{"user_id": "user-1", "plan": "STARTER", "status": "TRIAL", "trialEnd": (now + timedelta(days=TRIAL_DAYS)).isoformat()}], "licenses": []}
        snapshot = entitlement_snapshot(state, "user-1")
        self.assertEqual(snapshot["status"], "TRIAL")
        self.assertEqual(snapshot["plan"], "STARTER")
        expired = {"subscriptions": [{"user_id": "user-1", "plan": "PRO", "status": "ACTIVE", "currentPeriodEnd": (now - timedelta(days=1)).isoformat()}], "licenses": []}
        self.assertEqual(entitlement_snapshot(expired, "user-1")["status"], "FREE")

    def test_license_fallback_is_not_an_implicit_frontend_unlock(self):
        state = {"subscriptions": [], "licenses": [{"user_id": "user-1", "plan": "PRO", "status": "ACTIVE", "expires_at": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()}]}
        self.assertIsNone(active_subscription(state, "user-1"))
        self.assertEqual(entitlement_snapshot(state, "user-1")["status"], "FREE")

    def test_stripe_price_mapping_is_server_side_and_rejects_missing_config(self):
        env = {
            "STRIPE_PRICE_STARTER_MONTHLY": "price_starter_monthly",
            "STRIPE_PRICE_STARTER_YEARLY": "price_starter_yearly",
        }
        with patch.dict("os.environ", env, clear=False):
            self.assertEqual(v22_commercial.stripe_price_id("STARTER", "monthly"), "price_starter_monthly")
            with self.assertRaises(v22_commercial.HTTPException):
                v22_commercial.stripe_price_id("PRO", "annual")
        with patch.dict("os.environ", {}, clear=True):
            self.assertFalse(v22_commercial.stripe_configured())

    def test_stripe_status_and_return_url_are_fail_closed(self):
        self.assertEqual(v22_commercial.normalize_stripe_status("active"), "ACTIVE")
        self.assertEqual(v22_commercial.normalize_stripe_status("trialing"), "TRIAL")
        self.assertEqual(v22_commercial.normalize_stripe_status("past_due"), "PAST_DUE")
        self.assertEqual(v22_commercial.normalize_stripe_status("canceled"), "CANCELED")
        with patch.dict("os.environ", {"APP_BASE_URL": "javascript:alert(1)"}, clear=False):
            with self.assertRaises(v22_commercial.HTTPException):
                v22_commercial.stripe_base_url()

    def test_production_verification_link_uses_frontend_route(self):
        with patch.dict("os.environ", {"APP_BASE_URL": "https://frontend-nu-two-18.vercel.app"}, clear=False):
            verification_link = f"{v22_commercial.app_base_url()}/verify-email?token=opaque-test-token"
        self.assertEqual(verification_link, "https://frontend-nu-two-18.vercel.app/verify-email?token=opaque-test-token")
        self.assertNotIn("localhost:5173", verification_link)
        with patch.dict("os.environ", {"APP_BASE_URL": "https://example.com/?redirect=https://evil.example"}, clear=False):
            with self.assertRaises(v22_commercial.HTTPException):
                v22_commercial.stripe_base_url()

    def test_customer_id_lookup_is_scoped_to_the_authenticated_user(self):
        state = {"subscriptions": [
            {"user_id": "user-1", "stripeCustomerId": "cus_one"},
            {"user_id": "user-2", "stripeCustomerId": "cus_two"},
        ]}
        self.assertEqual(v22_commercial.stripe_customer_for_user(state, "user-1"), "cus_one")
        self.assertEqual(v22_commercial.subscription_user_for_customer(state, "cus_two"), "user-2")
        self.assertIsNone(v22_commercial.stripe_customer_for_user(state, "user-3"))

    def test_phase_routes_require_existing_auth_and_no_frontend_price_input(self):
        source = (v22_commercial.Path(__file__).parents[1] / "app" / "v22_commercial.py").read_text(encoding="utf-8")
        for route in ("/subscription/checkout", "/subscription/customer-portal", "/subscription/webhook"):
            self.assertIn(route, source)
        self.assertIn("authenticated_user(request)", source)
        self.assertIn("stripe_price_id(payload.plan, payload.billing_interval)", source)
        self.assertNotIn("payload.price_id", source)

    def test_stripe_webhook_events_update_state_once(self):
        future = int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
        state = {"subscriptions": [], "licenses": [], "stripe_event_ids": []}
        completed = {"id":"evt_checkout", "type":"checkout.session.completed", "data":{"object":{"id":"cs_1", "customer":"cus_1", "subscription":"sub_1", "metadata":{"user_id":"user-1", "plan":"PRO", "billing_interval":"monthly"}}}}
        self.assertTrue(v22_commercial.apply_stripe_event(state, completed))
        subscription = {"id":"evt_sub", "type":"customer.subscription.created", "data":{"object":{"id":"sub_1", "customer":"cus_1", "status":"active", "current_period_start":future - 100, "current_period_end":future, "cancel_at_period_end":False, "metadata":{"user_id":"user-1", "plan":"PRO", "billing_interval":"monthly"}, "items":{"data":[]}}}}
        with patch.dict("os.environ", {"STRIPE_PRICE_PRO_MONTHLY":"price_pro_monthly"}, clear=False):
            self.assertTrue(v22_commercial.apply_stripe_event(state, subscription))
            row = state["subscriptions"][-1]
            self.assertEqual(row["status"], "ACTIVE")
            self.assertEqual(row["plan"], "PRO")
            self.assertEqual(row["stripeCustomerId"], "cus_1")
            self.assertFalse(v22_commercial.apply_stripe_event(state, subscription))
            self.assertEqual(state["stripe_event_ids"].count("evt_sub"), 1)

    def test_payment_failed_and_deleted_map_to_safe_statuses(self):
        future = int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
        state = {"subscriptions":[{"user_id":"user-1","plan":"PRO","status":"ACTIVE","stripeCustomerId":"cus_1","stripeSubscriptionId":"sub_1","currentPeriodEnd":datetime.fromtimestamp(future, timezone.utc).isoformat()}], "licenses":[], "stripe_event_ids":[]}
        failed = {"id":"evt_failed", "type":"invoice.payment_failed", "data":{"object":{"customer":"cus_1"}}}
        self.assertTrue(v22_commercial.apply_stripe_event(state, failed))
        self.assertEqual(state["subscriptions"][0]["status"], "PAST_DUE")
        deleted = {"id":"evt_deleted", "type":"customer.subscription.deleted", "data":{"object":{"id":"sub_1","customer":"cus_1","status":"canceled","current_period_end":future,"metadata":{"user_id":"user-1","plan":"PRO","billing_interval":"monthly"},"items":{"data":[]}}}}
        self.assertTrue(v22_commercial.apply_stripe_event(state, deleted))
        self.assertEqual(state["subscriptions"][0]["status"], "CANCELED")


if __name__ == "__main__":
    unittest.main()
