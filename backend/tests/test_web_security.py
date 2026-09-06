import os
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.web_security import bootstrap_access_allowed, bearer_token, cors_origins, env_flag, evaluate_access, is_allowed_cors_origin


class WebSecurityTests(unittest.TestCase):
    def test_local_mode_can_be_disabled(self):
        decision = evaluate_access(
            required=False, configured_token="", authorization=None,
            path="/api/markets", method="GET",
        )
        self.assertTrue(decision.allowed)

    def test_health_is_public_for_host_monitoring(self):
        decision = evaluate_access(
            required=True, configured_token="x" * 32, authorization=None,
            path="/api/health", method="GET",
        )
        self.assertTrue(decision.allowed)

    def test_missing_server_secret_fails_closed(self):
        decision = evaluate_access(
            required=True, configured_token="short", authorization="Bearer anything",
            path="/api/markets", method="GET",
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, 503)

    def test_wrong_owner_token_is_rejected(self):
        decision = evaluate_access(
            required=True, configured_token="a" * 32, authorization=f"Bearer {'b' * 32}",
            path="/api/exchange-connections/save", method="POST",
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, 401)

    def test_customer_bearer_reaches_normal_application_api(self):
        decision = evaluate_access(
            required=True, configured_token="a" * 32, authorization="Bearer customer-session",
            path="/api/markets", method="GET",
        )
        self.assertTrue(decision.allowed)

    def test_correct_owner_token_is_accepted(self):
        token = "owner-preview-token-1234567890"
        decision = evaluate_access(
            required=True, configured_token=token, authorization=f"Bearer {token}",
            path="/api/markets", method="GET",
        )
        self.assertTrue(decision.allowed)

    def test_dedicated_owner_header_does_not_conflict_with_customer_session(self):
        token = "owner-preview-token-1234567890"
        decision = evaluate_access(
            required=True, configured_token=token, authorization="Bearer customer-session",
            owner_access=token, path="/api/v22/me", method="GET",
        )
        self.assertTrue(decision.allowed)

    def test_customer_jwt_is_not_treated_as_owner_token_on_user_routes(self):
        token = "owner-preview-token-1234567890"
        decision = evaluate_access(
            required=True, configured_token=token, authorization="Bearer customer-session",
            owner_access=None, path="/api/v22/me", method="GET",
        )
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.status_code, 200)

    def test_owner_gate_is_only_required_on_owner_paths(self):
        token = "owner-preview-token-1234567890"
        decision = evaluate_access(
            required=True, configured_token=token, authorization="Bearer customer-session",
            owner_access=None, path="/api/web/access/check", method="GET",
        )
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.status_code, 401)

    def test_first_owner_bootstrap_requires_local_or_verified_web_owner(self):
        self.assertTrue(bootstrap_access_allowed("127.0.0.1", web_owner_authenticated=False))
        self.assertTrue(bootstrap_access_allowed("10.0.0.12", web_owner_authenticated=True))
        self.assertFalse(bootstrap_access_allowed("10.0.0.12", web_owner_authenticated=False))

    def test_helpers_normalize_inputs(self):
        self.assertEqual(bearer_token("Bearer abc"), "abc")
        self.assertEqual(cors_origins("https://app.example.com/, https://preview.example.com"), [
            "https://app.example.com", "https://preview.example.com",
        ])
        self.assertEqual(
            cors_origins("https://pro-tre-bot-web.vercel.app"),
            ["https://pro-tre-bot-web.vercel.app"],
        )
        self.assertEqual(
            cors_origins(" https://frontend-nu-two-18.vercel.app/ "),
            ["https://frontend-nu-two-18.vercel.app"],
        )
        self.assertEqual(
            cors_origins("*, https://app.example.com/api, https://app.example.com, https://app.example.com"),
            ["https://app.example.com"],
        )
        self.assertEqual(
            cors_origins('["https://frontend-nu-two-18.vercel.app/"]', fallback=[]),
            ["https://frontend-nu-two-18.vercel.app"],
        )
        self.assertEqual(cors_origins("", fallback=[]), [])
        self.assertTrue(is_allowed_cors_origin("https://frontend-nu-two-18.vercel.app", {"https://frontend-nu-two-18.vercel.app"}))
        self.assertTrue(is_allowed_cors_origin("https://frontend-nu-two-18-git-main-ahmtt9871-dot.vercel.app", {"https://frontend-nu-two-18.vercel.app"}))
        self.assertTrue(is_allowed_cors_origin("https://frontend-gh7asjvqj-ahmet-f11.vercel.app", {"https://frontend-nu-two-18.vercel.app"}))
        self.assertFalse(is_allowed_cors_origin("https://other-project.vercel.app", {"https://frontend-nu-two-18.vercel.app"}))
        self.assertFalse(is_allowed_cors_origin("http://frontend-nu-two-18.vercel.app", {"https://frontend-nu-two-18.vercel.app"}))
        old = os.environ.get("PROTREBOT_TEST_FLAG")
        try:
            os.environ["PROTREBOT_TEST_FLAG"] = "yes"
            self.assertTrue(env_flag("PROTREBOT_TEST_FLAG"))
        finally:
            if old is None:
                os.environ.pop("PROTREBOT_TEST_FLAG", None)
            else:
                os.environ["PROTREBOT_TEST_FLAG"] = old


if __name__ == "__main__":
    unittest.main()
