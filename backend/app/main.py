import asyncio
import json
import math
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Literal

import httpx
import asyncpg
import websockets
from redis import asyncio as redis_async
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from pydantic import BaseModel, Field
from .analysis import analyze
from .exchange_connections import (
    clear_vault_cache,
    ensure_exchange_vault,
    init_exchange_connections,
    router as exchange_connections_router,
)
from .binance_demo import (
    DEMO_REST_BASE,
    armed as demo_armed,
    credentials_configured as demo_credentials_configured,
    init_binance_demo,
    router as binance_demo_router,
    shutdown_binance_demo,
)
from .v21_demo import init_v21_demo, router as v21_demo_router, shutdown_v21_demo
from .v22_commercial import (
    authenticated_user,
    ensure_commercial_schema,
    gmail_configured,
    init_v22_commercial,
    router as v22_commercial_router,
    shutdown_v22_commercial,
    sync_v22_storage,
)
from .v24_commerce import router as v24_commerce_router
from .v25_execution import init_v25_execution, router as v25_execution_router, shutdown_v25_execution
from .v27_cloud_ops import (
    init_v27_cloud,
    router as v27_cloud_router,
    shutdown_v27_cloud,
)
from .paper_autonomy import (
    PAPER_AUTONOMY_VERSION,
    autonomy_policy,
    daily_reference_progress,
    dynamic_paper_allocation,
    rank_paper_candidates,
)
from .web_security import PUBLIC_PATHS, bearer_token, cors_origins, env_flag, evaluate_access, is_allowed_cors_origin

BINANCE_API = "https://api.binance.com"
FUTURES_MARKET_DATA_API = DEMO_REST_BASE
LEGACY_PAPER_CONTRACT = 'version="20.2.0"'
LEGACY_V25_API_CONTRACT = 'version="25.0.0"'
DEPLOYMENT_PATCH = "28.0.0-in-app-encrypted-exchange-vault"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://protrebot:protrebot_local_change_me@127.0.0.1:5432/protrebot",
).strip()
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0").strip()
WEB_REQUIRE_AUTH = env_flag("PROTREBOT_WEB_REQUIRE_AUTH", default=False)
WEB_ACCESS_TOKEN = os.getenv("PROTREBOT_WEB_ACCESS_TOKEN", "").strip()
PRODUCTION_WEB_ORIGIN = "https://frontend-nu-two-18.vercel.app"
WEB_CORS_ORIGINS = list(dict.fromkeys([
    *cors_origins(os.getenv("PROTREBOT_CORS_ORIGINS"), fallback=[]),
    PRODUCTION_WEB_ORIGIN,
]))
WEB_CORS_ORIGIN_REGEX = r"https://(?:frontend-nu-two-18|frontend-gh7asjvqj-ahmet-f11)(?:-[a-z0-9-]+)*\.vercel\.app"
PAPER_ENABLED = env_flag("PROTREBOT_PAPER_ENABLED", default=True)
RISK_PER_TRADE = 0.01
SHORT_MTF_ALIGNMENT_MAX = 80.0
LIVE_CHANNEL_ENABLED = env_flag("PROTREBOT_LIVE_CHANNEL_ENABLED", default=True)
EXECUTION_MODE = "TESTNET_FIRST"
ALLOWED_INTERVALS = {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}
SCAN_CACHE: dict[tuple[int, str], tuple[float, list]] = {}
ANALYSIS_UNIVERSE_CACHE: dict[str, tuple[float, list[dict]]] = {}
CONSENSUS_CACHE: dict[str, tuple[float, dict]] = {}
GUARD_CACHE: dict[str, tuple[float, dict]] = {}
LAB_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
GATE_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
FRESHNESS_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
REGIME_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
PORTFOLIO_CACHE: dict[tuple[str, str, tuple[tuple[str, str], ...]], tuple[float, dict]] = {}
REGIME_STABILITY_HISTORY: dict[tuple[str, str], list[dict]] = {}
LIQUIDITY_CACHE: dict[str, tuple[float, dict]] = {}
WALK_FORWARD_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
STRESS_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
EXPLANATION_CACHE: dict[tuple[str, str], tuple[float, dict]] = {}
GRID_PLAN_CACHE: dict[tuple[str, str, int], tuple[float, dict]] = {}
GRID_LAB_CACHE: dict[tuple[str, str, int], tuple[float, dict]] = {}
GRID_TWIN_CACHE: dict[tuple[str, str, int], tuple[float, dict]] = {}
ORDERBOOK_INTELLIGENCE_CACHE: dict[str, tuple[float, dict]] = {}
ORDERBOOK_WALL_HISTORY: dict[str, list[dict]] = {}
V7_REPLAY_CACHE: dict[tuple[str, str, str, int], tuple[float, dict]] = {}
V8_FUTURE_CACHE: dict[tuple[str, str, int, int], tuple[float, dict]] = {}
V10_EVOLUTION_CACHE: dict[tuple[str, str, int], tuple[float, dict]] = {}
V11_RISK_CACHE: dict[tuple[tuple[str, ...], str, int, int, int], tuple[float, dict]] = {}
V9_DEFAULT_UNIVERSE = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT")
MTF_BACKTEST_SYMBOLS = frozenset({"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"})
V9_STREAM_STALE_SECONDS = 8
V9_EVENT_LIMIT = 100
V9_FILL_LIMIT = 160
DECISION_REVIEW_MINUTES = (15, 30, 60)
DECISION_MEMORY_LIMIT = 160
GRID_PLAN_LIMIT = 12
GRID_FEE_SIDE_PCT = 0.10
GRID_ENGINE_EVENT_LIMIT = 80
GRID_ENGINE_FILL_LIMIT = 120
GRID_ENGINE_TICK_SECONDS = 4
GRID_RECENTER_SECONDS = 60
V7_ORCHESTRATOR_TICK_SECONDS = 12
V7_ORCHESTRATOR_EVENT_LIMIT = 100
V7_STRATEGIES = ("GRID", "TREND", "KIRILIM")
V8_FORECAST_HORIZONS = (12, 24)
V10_EVOLUTION_TICK_SECONDS = 45
V10_EVENT_LIMIT = 100


def shared_mtf_decision(
    symbol: str,
    entry_direction: str,
    confidence_15m: float,
    timeframe_results: dict[str, dict],
    short_filter: bool = True,
    short_alignment_max: float = SHORT_MTF_ALIGNMENT_MAX,
) -> dict:
    """Pure MTF gate that evaluates already-computed timeframe opinions without fetching data.

    This helper intentionally does not touch network, analyze(), or any caller logic.
    It only inspects the current 15m direction and the already-computed 1h/4h payloads.
    """
    default_timeframe = {"direction": "BEKLE", "confidence": 0.0}
    timeframe_map = {
        "15m": {
            "direction": entry_direction if entry_direction in {"LONG", "SHORT"} else "BEKLE",
            "confidence": float(confidence_15m or 0.0),
            "weight": 0.25,
        },
        "1h": {
            "direction": (timeframe_results.get("1h") or {}).get("direction", default_timeframe["direction"]),
            "confidence": float((timeframe_results.get("1h") or {}).get("confidence", 0.0) or 0.0),
            "weight": 0.35,
        },
        "4h": {
            "direction": (timeframe_results.get("4h") or {}).get("direction", default_timeframe["direction"]),
            "confidence": float((timeframe_results.get("4h") or {}).get("confidence", 0.0) or 0.0),
            "weight": 0.40,
        },
    }

    if entry_direction not in {"LONG", "SHORT"}:
        return {
            "symbol": symbol,
            "direction": "BEKLE",
            "alignment": 0,
            "verdict": "UYUMSUZ",
            "entry_permission": False,
            "reason": f"15m giriş yönü geçersiz: {entry_direction}; LONG/SHORT bekleniyordu.",
            "timeframes": timeframe_map,
            "long_permission": False,
            "short_permission": False,
            "blocked_by_short_filter": False,
            "higher_timeframe_confirmation": False,
        }

    higher_timeframe_confirmation = (
        timeframe_map["1h"]["direction"] == entry_direction
        and timeframe_map["4h"]["direction"] == entry_direction
    )
    alignment = round(
        float(timeframe_map["15m"]["confidence"]) * 0.25
        + float(timeframe_map["1h"]["confidence"]) * 0.35
        + float(timeframe_map["4h"]["confidence"]) * 0.40
    )

    if not higher_timeframe_confirmation:
        reason_bits = []
        if timeframe_map["1h"]["direction"] != entry_direction:
            reason_bits.append(f"1h={timeframe_map['1h']['direction']}")
        if timeframe_map["4h"]["direction"] != entry_direction:
            reason_bits.append(f"4h={timeframe_map['4h']['direction']}")
        return {
            "symbol": symbol,
            "direction": "BEKLE",
            "alignment": alignment,
            "verdict": "UYUMSUZ",
            "entry_permission": False,
            "reason": (
                f"15m {entry_direction} yönü, yüksek zaman dilimlerinde onaylanmadı: "
                + ", ".join(reason_bits) if reason_bits else "yüksek zaman dilimleri aynı yönde değil."
            ),
            "timeframes": timeframe_map,
            "long_permission": False,
            "short_permission": False,
            "blocked_by_short_filter": False,
            "higher_timeframe_confirmation": False,
        }

    if entry_direction == "LONG":
        verdict = "GÜÇLÜ ONAY"
        reason = "15m, 1h ve 4h aynı LONG yönünü doğruluyor."
        long_permission = True
        short_permission = False
        blocked_by_short_filter = False
        entry_permission = True
        direction = "LONG"
    else:
        if short_filter and alignment >= short_alignment_max:
            verdict = "SHORT_ALIGNMENT_FILTRE"
            reason = (
                f"SHORT yönü 15m/1h/4h tarafından doğrulandı, ancak alignment {alignment} >= "
                f"{short_alignment_max}; kısa pozisyon için kısa filtre devreye girdi."
            )
            long_permission = False
            short_permission = False
            blocked_by_short_filter = True
            entry_permission = False
            direction = "SHORT"
        else:
            verdict = "GÜÇLÜ ONAY"
            reason = "15m, 1h ve 4h aynı SHORT yönünü doğruluyor."
            long_permission = False
            short_permission = True
            blocked_by_short_filter = False
            entry_permission = True
            direction = "SHORT"

    return {
        "symbol": symbol,
        "direction": direction,
        "alignment": alignment,
        "verdict": verdict,
        "entry_permission": entry_permission,
        "reason": reason,
        "timeframes": timeframe_map,
        "long_permission": long_permission,
        "short_permission": short_permission,
        "blocked_by_short_filter": blocked_by_short_filter,
        "higher_timeframe_confirmation": higher_timeframe_confirmation,
    }


def normalize_analysis_signal(direction: str) -> str:
    """Expose analysis directions as BUY/SELL/HOLD without changing legacy values."""
    return {"LONG": "BUY", "SHORT": "SELL", "BEKLE": "HOLD"}.get(direction, "HOLD")


def json_safe_payload(value):
    """API yanıtındaki NaN/Infinity değerlerini JSON için güvenli hale getirir.

    Canlı veri eksikliği veya çok kısa istatistik örneği bazı risk hesaplarında
    sonlu olmayan kayan nokta değerleri üretebilir. Starlette bu değerleri
    serileştirirken 500 üretir. Bu yardımcı yalnızca yanıt kopyasını temizler;
    Paper durumunu veya borsa bağlantısını değiştirmez.
    """
    if isinstance(value, bool) or value is None or isinstance(value, (str, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): json_safe_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe_payload(item) for item in value]
    return value
V11_RISK_TICK_SECONDS = 60
V11_EVENT_LIMIT = 100
HEALTH_CHECK_INTERVAL_SECONDS = 15 * 60
HEALTH_STATE_KEY = "system-health"


def safe_json_object(value: object) -> dict:
    """Eski veya bozuk JSONB kayıtları uygulama başlangıcını durdurmadan okur."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(value) if isinstance(value, dict) else {}
    try:
        return dict(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return {}


def empty_v11_risk_state() -> dict:
    """V11 yeniden başlatmada otomatik Paper müdahalesi yapmadan onay bekler."""
    return {
        "enabled": False, "busy": False, "status": "KULLANICI ONAYI BEKLİYOR",
        "interval": "15m", "capital": 5_000.0, "universe": list(V9_DEFAULT_UNIVERSE),
        "simulations": 500, "horizon_candles": 24, "cycles": 0,
        "latest_report": None, "approved_allocations": [], "events": [],
        "intervention": {
            "active": False, "status": "HAZIR", "reason": "Portföy riski henüz kritik değil.",
            "triggered_at": None, "paper_orchestrator_stopped": False,
        },
        "started_at": None, "stopped_at": None, "last_tick_at": None,
        "last_action": "V11 Otonom Risk Beyni kullanıcı onayı bekliyor.",
        "orders_enabled": False, "testnet_orders_enabled": False, "mode": "PAPER_ONLY",
    }


def empty_v10_evolution_state() -> dict:
    """V10 hiçbir yeniden başlatmada kendiliğinden çalışmaz veya emir kanalı açmaz."""
    return {
        "enabled": False, "busy": False, "status": "KULLANICI ONAYI BEKLİYOR",
        "interval": "15m", "capital": 1_000.0, "universe": list(V9_DEFAULT_UNIVERSE),
        "generation": 1, "cycle_index": 0, "cycles": 0, "candidate_genomes": [],
        "active_champion": None, "previous_champion": None, "champions": {},
        "leaderboard": [], "latest_tournament": None, "events": [],
        "started_at": None, "stopped_at": None, "last_tick_at": None,
        "last_action": "V10 Evrim Laboratuvarı kullanıcı onayı bekliyor.",
        "promotion_gate": "3/3 ZAMAN TESTİ + MALİYET STRESİ + AŞIRI ÖĞRENME KALKANI",
        "orders_enabled": False, "testnet_orders_enabled": False, "mode": "PAPER_ONLY",
    }


def empty_strategy_orchestrator_state() -> dict:
    return {
        "enabled": False, "status": "DURDU", "interval": "15m", "capital": 3_000.0,
        "universe": ["BTCUSDT", "ETHUSDT", "SOLUSDT"], "symbols": [], "allocations": [],
        "strategies": [], "events": [], "cycle_index": 0, "cycles": 0,
        "last_tick_at": None, "started_at": None, "stopped_at": None,
        "last_action": "V7 Otonom Strateji Orkestrası kullanıcı onayı bekliyor.",
        "certification_status": "KANIT BEKLİYOR", "quarantined_strategies": [],
        "return_fingerprints": {}, "orders_enabled": False,
    }


def empty_grid_engine_state() -> dict:
    return {
        "enabled": False, "status": "DURDU", "symbol": None, "interval": "15m",
        "capital": 1_000.0, "active_profile": "DENGELİ", "recommended_profile": "DENGELİ",
        "recommendation_status": "VERİ BEKLİYOR", "profiles": [], "events": [],
        "last_tick_at": None, "started_at": None, "stopped_at": None,
        "last_action": "V6 Otonom Paper Grid beklemede.", "recenter_count": 0,
        "orders_enabled": False,
    }


def paper_snapshot_data(paper: dict) -> dict:
    return {
        "balance": paper["balance"], "initial_balance": paper["initial_balance"],
        "positions": paper["positions"], "trades": paper["trades"],
        "next_id": paper["next_id"],
        "limit_orders": paper.get("limit_orders", []),
        "next_limit_id": paper.get("next_limit_id", 1),
        "risk": paper["risk"],
        "shadow": paper.get("shadow", {}),
        "emergency_brake": paper.get("emergency_brake", {}),
        "notifications": paper.get("notifications", []),
        "decision_memory": paper.get("decision_memory", []),
        "signal_history": paper.get("signal_history", []),
        "alerts": paper.get("alerts", []),
        "grid_plans": paper.get("grid_plans", []),
        "grid_engine": paper.get("grid_engine", empty_grid_engine_state()),
        "strategy_orchestrator": paper.get("strategy_orchestrator", empty_strategy_orchestrator_state()),
        "strategy_evolution": paper.get("strategy_evolution", empty_v10_evolution_state()),
        "portfolio_risk": paper.get("portfolio_risk", empty_v11_risk_state()),
    }


async def ensure_paper_schema(application: FastAPI) -> None:
    pool = application.state.db_pool
    if pool is None:
        return
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_account_snapshots (
          account_key TEXT PRIMARY KEY,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          payload JSONB NOT NULL
        )
        """
    )


async def restore_paper_snapshot(application: FastAPI) -> bool:
    pool = application.state.db_pool
    if pool is None:
        return False
    try:
        row = await pool.fetchrow("SELECT payload FROM paper_account_snapshots WHERE account_key = $1", "local")
    except Exception:
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
    paper = application.state.paper
    async with paper["lock"]:
        paper["balance"] = float(payload.get("balance", paper["balance"]))
        paper["initial_balance"] = float(payload.get("initial_balance", paper["initial_balance"]))
        paper["positions"] = payload.get("positions", paper["positions"]) if isinstance(payload.get("positions"), list) else paper["positions"]
        paper["trades"] = payload.get("trades", paper["trades"]) if isinstance(payload.get("trades"), list) else paper["trades"]
        paper["next_id"] = int(payload.get("next_id", paper["next_id"]))
        existing_limit_orders = paper.get("limit_orders", [])
        paper["limit_orders"] = payload.get("limit_orders", existing_limit_orders) if isinstance(payload.get("limit_orders"), list) else existing_limit_orders
        paper["next_limit_id"] = int(payload.get("next_limit_id", paper.get("next_limit_id", 1)))
        paper["risk"] = payload.get("risk", paper["risk"]) if isinstance(payload.get("risk"), dict) else paper["risk"]
        paper["shadow"] = payload.get("shadow", paper["shadow"]) if isinstance(payload.get("shadow"), dict) else paper["shadow"]
        paper["emergency_brake"] = payload.get("emergency_brake", paper["emergency_brake"]) if isinstance(payload.get("emergency_brake"), dict) else paper["emergency_brake"]
        paper["notifications"] = payload.get("notifications", paper["notifications"]) if isinstance(payload.get("notifications"), list) else paper["notifications"]
        paper["decision_memory"] = payload.get("decision_memory", paper["decision_memory"]) if isinstance(payload.get("decision_memory"), list) else paper["decision_memory"]
        paper["signal_history"] = payload.get("signal_history", paper.get("signal_history", [])) if isinstance(payload.get("signal_history"), list) else paper.get("signal_history", [])
        paper["alerts"] = payload.get("alerts", paper.get("alerts", [])) if isinstance(payload.get("alerts"), list) else paper.get("alerts", [])
        paper["grid_plans"] = payload.get("grid_plans", paper["grid_plans"]) if isinstance(payload.get("grid_plans"), list) else paper["grid_plans"]
        restored_engine = payload.get("grid_engine")
        if isinstance(restored_engine, dict):
            paper["grid_engine"] = {**empty_grid_engine_state(), **restored_engine}
            paper["grid_engine"]["enabled"] = False
            paper["grid_engine"]["status"] = "YENİDEN BAŞLATMA ONAYI"
            paper["grid_engine"]["last_action"] = "Bilgisayar yeniden başladı; güvenlik için V6 Paper Grid kullanıcı onayı bekliyor."
        restored_orchestrator = payload.get("strategy_orchestrator")
        if isinstance(restored_orchestrator, dict):
            paper["strategy_orchestrator"] = {**empty_strategy_orchestrator_state(), **restored_orchestrator}
            paper["strategy_orchestrator"]["enabled"] = False
            paper["strategy_orchestrator"]["status"] = "YENİDEN BAŞLATMA ONAYI"
            paper["strategy_orchestrator"]["last_action"] = "Bilgisayar yeniden başladı; güvenlik için V7 Orkestra yeniden kullanıcı onayı bekliyor."
        restored_evolution = payload.get("strategy_evolution")
        if isinstance(restored_evolution, dict):
            paper["strategy_evolution"] = {**empty_v10_evolution_state(), **restored_evolution}
            paper["strategy_evolution"]["enabled"] = False
            paper["strategy_evolution"]["busy"] = False
            paper["strategy_evolution"]["status"] = "YENİDEN BAŞLATMA ONAYI"
            paper["strategy_evolution"]["orders_enabled"] = False
            paper["strategy_evolution"]["testnet_orders_enabled"] = False
            paper["strategy_evolution"]["last_action"] = "Bilgisayar yeniden başladı; güvenlik için V10 Evrim Laboratuvarı yeniden kullanıcı onayı bekliyor."
        restored_risk = payload.get("portfolio_risk")
        if isinstance(restored_risk, dict):
            paper["portfolio_risk"] = {**empty_v11_risk_state(), **restored_risk}
            paper["portfolio_risk"]["enabled"] = False
            paper["portfolio_risk"]["busy"] = False
            paper["portfolio_risk"]["status"] = "YENİDEN BAŞLATMA ONAYI"
            paper["portfolio_risk"]["orders_enabled"] = False
            paper["portfolio_risk"]["testnet_orders_enabled"] = False
            paper["portfolio_risk"]["last_action"] = "Bilgisayar yeniden başladı; V11 Paper Risk Beyni yeniden kullanıcı onayı bekliyor."
    application.state.paper_dirty = False
    application.state.infrastructure["paper_storage"] = "KALICI"
    return True


async def persist_paper_snapshot(application: FastAPI) -> None:
    pool = application.state.db_pool
    if pool is None or not application.state.paper_schema_ready:
        application.state.paper_dirty = True
        return
    paper = application.state.paper
    async with paper["lock"]:
        payload = json.dumps(paper_snapshot_data(paper))
    try:
        async with application.state.snapshot_lock:
            await pool.execute(
                """
                INSERT INTO paper_account_snapshots (account_key, updated_at, payload)
                VALUES ($1, NOW(), $2::jsonb)
                ON CONFLICT (account_key) DO UPDATE SET updated_at = NOW(), payload = EXCLUDED.payload
                """,
                "local", payload,
            )
        application.state.paper_dirty = False
        application.state.infrastructure["paper_storage"] = "KALICI"
    except Exception:
        application.state.paper_dirty = True
        application.state.infrastructure["paper_storage"] = "BEKLENİYOR"


async def ensure_infrastructure(application: FastAPI) -> None:
    """TimescaleDB ve Redis'i uygulamayı durdurmadan yeniden bağlamayı dener."""
    infrastructure = application.state.infrastructure
    db_pool = application.state.db_pool
    if db_pool is not None:
        try:
            await db_pool.fetchval("SELECT 1")
        except Exception:
            await db_pool.close()
            application.state.db_pool = None
            application.state.paper_schema_ready = False
            application.state.market_twin_schema_ready = False
            infrastructure["paper_storage"] = "BEKLENİYOR" if PAPER_ENABLED else "DEVRE DIŞI"
    if application.state.db_pool is None:
        try:
            application.state.db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2, timeout=3)
            application.state.paper_schema_ready = False
            application.state.market_twin_schema_ready = False
        except Exception:
            application.state.db_pool = None
            infrastructure["paper_storage"] = "BEKLENİYOR" if PAPER_ENABLED else "DEVRE DIŞI"

    redis_client = application.state.redis_client
    if redis_client is not None:
        try:
            await redis_client.ping()
        except Exception:
            await redis_client.aclose()
            application.state.redis_client = None
    if application.state.redis_client is None and REDIS_URL:
        try:
            client = redis_async.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=3)
            await client.ping()
            application.state.redis_client = client
        except Exception:
            application.state.redis_client = None

    database_ok = application.state.db_pool is not None
    redis_ok = application.state.redis_client is not None
    if PAPER_ENABLED and database_ok and not application.state.paper_schema_ready:
        try:
            await ensure_paper_schema(application)
            application.state.paper_schema_ready = True
        except Exception:
            application.state.paper_schema_ready = False
            infrastructure["paper_storage"] = "BEKLENİYOR"
    if database_ok and not application.state.market_twin_schema_ready:
        try:
            await ensure_v9_schema(application)
            application.state.market_twin_schema_ready = True
        except Exception:
            application.state.market_twin_schema_ready = False
    if application.state.market_twin_schema_ready and not application.state.market_twin_restore_attempted:
        await restore_v9_history(application)
        application.state.market_twin_restore_attempted = True
    if PAPER_ENABLED and application.state.paper_schema_ready:
        if not application.state.paper_restore_attempted:
            restored = await restore_paper_snapshot(application)
            application.state.paper_restore_attempted = True
            if not restored:
                application.state.paper_dirty = True
                await persist_paper_snapshot(application)
        elif application.state.paper_dirty:
            await persist_paper_snapshot(application)
    if database_ok:
        await sync_v22_storage(application)
        if hasattr(application.state, "exchange_vault"):
            await ensure_exchange_vault(application)
    infrastructure.update({
        "api": "BAĞLI",
        "database": "BAĞLI" if database_ok else "BAĞLANIYOR",
        "redis": "BAĞLI" if redis_ok else "BAĞLANIYOR",
        "paper_storage": infrastructure.get("paper_storage", "BEKLENİYOR") if PAPER_ENABLED else "DEVRE DIŞI",
        "self_healing": "AKTİF",
        "last_checked": datetime.now(timezone.utc).isoformat(),
        "message": "Testnet-First altyapısı ve kalıcı kayıt servisi hazır." if database_ok else "Altyapı servisi bekleniyor; Sistem Sağlık Merkezi yeniden bağlanmayı deniyor.",
    })


async def infrastructure_loop(application: FastAPI) -> None:
    while True:
        try:
            await ensure_infrastructure(application)
        except Exception:
            application.state.infrastructure["message"] = "Altyapı kontrolü yeniden denenecek."
        await asyncio.sleep(15)


def health_item(name: str, status: str, message: str, started: float) -> dict:
    return {
        "name": name,
        "status": status,
        "message": message,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "latency_ms": round((time.perf_counter() - started) * 1000, 2),
    }


async def health_check_database(application: FastAPI) -> dict:
    started = time.perf_counter()
    pool = getattr(application.state, "db_pool", None)
    if pool is None:
        return health_item("Database", "ERROR", "PostgreSQL connection unavailable.", started)
    try:
        await asyncio.wait_for(pool.fetchval("SELECT 1"), timeout=3)
        return health_item("Database", "ACTIVE", "PostgreSQL connection healthy.", started)
    except Exception:
        return health_item("Database", "ERROR", "Database connection failed.", started)


async def health_check_redis(application: FastAPI) -> dict:
    started = time.perf_counter()
    if not REDIS_URL:
        return health_item("Redis", "DISABLED", "Redis is not configured.", started)
    client = getattr(application.state, "redis_client", None)
    if client is None:
        return health_item("Redis", "ERROR", "Redis connection unavailable.", started)
    try:
        await asyncio.wait_for(client.ping(), timeout=3)
        return health_item("Redis", "ACTIVE", "Redis connection healthy.", started)
    except Exception:
        return health_item("Redis", "ERROR", "Redis connection failed.", started)


async def health_check_gmail() -> dict:
    started = time.perf_counter()
    if not gmail_configured():
        return health_item("Email / Gmail API", "NOT CONFIGURED", "Gmail OAuth configuration is incomplete.", started)
    try:
        credentials = Credentials(
            token=None,
            refresh_token=os.environ["GMAIL_REFRESH_TOKEN"].strip(),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.environ["GMAIL_CLIENT_ID"].strip(),
            client_secret=os.environ["GMAIL_CLIENT_SECRET"].strip(),
            scopes=["https://www.googleapis.com/auth/gmail.send"],
        )
        await asyncio.wait_for(asyncio.to_thread(credentials.refresh, GoogleAuthRequest()), timeout=10)
        return health_item("Email / Gmail API", "ACTIVE", "OAuth connection healthy.", started)
    except asyncio.TimeoutError:
        return health_item("Email / Gmail API", "WARNING", "OAuth refresh timed out.", started)
    except Exception:
        return health_item("Email / Gmail API", "ERROR", "OAuth connection unavailable.", started)


async def health_check_frontend(application: FastAPI) -> dict:
    started = time.perf_counter()
    try:
        response = await asyncio.wait_for(application.state.http.get(PRODUCTION_WEB_ORIGIN), timeout=10)
        if response.status_code < 400:
            return health_item("Frontend / Vercel", "ACTIVE", "Vercel deployment reachable.", started)
        return health_item("Frontend / Vercel", "WARNING", f"Frontend responded with HTTP {response.status_code}.", started)
    except asyncio.TimeoutError:
        return health_item("Frontend / Vercel", "WARNING", "Frontend request timed out.", started)
    except Exception:
        return health_item("Frontend / Vercel", "ERROR", "Frontend deployment unreachable.", started)


async def health_check_market_data(application: FastAPI) -> dict:
    started = time.perf_counter()
    try:
        response = await asyncio.wait_for(application.state.http.get(f"{FUTURES_MARKET_DATA_API}/fapi/v1/ping"), timeout=10)
        if response.status_code == 200:
            return health_item("Market data", "ACTIVE", "Read-only market data endpoint reachable.", started)
        return health_item("Market data", "ERROR", f"Market data responded with HTTP {response.status_code}.", started)
    except asyncio.TimeoutError:
        return health_item("Market data", "WARNING", "Market data request timed out.", started)
    except Exception:
        return health_item("Market data", "ERROR", "Market data endpoint unreachable.", started)


async def run_health_checks(application: FastAPI) -> dict:
    started = time.perf_counter()
    async with application.state.health_lock:
        checks = await asyncio.gather(
            health_check_database(application),
            health_check_redis(application),
            health_check_gmail(),
            health_check_frontend(application),
            health_check_market_data(application),
            return_exceptions=True,
        )
        normalized = []
        for check in checks:
            if isinstance(check, dict):
                normalized.append(check)
            else:
                normalized.append(health_item("Health check", "ERROR", "Health check failed.", started))
        state = getattr(application.state, "v22_commercial", {}).get("state", {})
        normalized.extend([
            health_item("Backend API", "ACTIVE", "Backend process is serving requests.", started),
            health_item("Authentication", "ACTIVE" if getattr(application.state, "v22_commercial", {}).get("secret") else "ERROR", "Authentication state is loaded." if getattr(application.state, "v22_commercial", {}).get("secret") else "Authentication state unavailable.", started),
            health_item("Subscriptions", "ACTIVE" if state.get("subscriptions") is not None else "ERROR", "Subscription state is loaded." if state.get("subscriptions") is not None else "Subscription state unavailable.", started),
            health_item("Admin API", "ACTIVE", "OWNER-protected admin API is available.", started),
            health_item("Trading engine", "NOT CONFIGURED", "No independent trading-engine heartbeat is exposed.", started),
            health_item("Live trading", "DISABLED" if not LIVE_CHANNEL_ENABLED else "NOT CONFIGURED", "Live order channel is disabled." if not LIVE_CHANNEL_ENABLED else "Live readiness is not established.", started),
        ])
        counts = {status: sum(1 for item in normalized if item["status"] == status) for status in ("ACTIVE", "WARNING", "ERROR", "DISABLED", "NOT CONFIGURED")}
        overall_status = "ERROR" if counts["ERROR"] else "WARNING" if counts["WARNING"] else "ACTIVE"
        checked_at = datetime.now(timezone.utc).isoformat()
        snapshot = {"overall_status": overall_status, "checks": normalized, "checked_at": checked_at, "last_checked_at": checked_at, "next_check_at": (datetime.now(timezone.utc) + timedelta(seconds=HEALTH_CHECK_INTERVAL_SECONDS)).isoformat(), "counts": counts, "incident_count": counts["ERROR"] + counts["WARNING"]}
        application.state.health_snapshot = snapshot
        await persist_health_snapshot(application, snapshot)
        return snapshot


async def persist_health_snapshot(application: FastAPI, snapshot: dict) -> bool:
    pool = getattr(application.state, "db_pool", None)
    if pool is None:
        snapshot["persistence"] = "UNAVAILABLE"
        snapshot["persistence_message"] = "PostgreSQL connection unavailable."
        return False
    try:
        for attempt in range(2):
            try:
                await pool.execute(
                    """
                    INSERT INTO application_state_snapshots (state_key, updated_at, payload)
                    VALUES ($1, NOW(), $2::jsonb)
                    ON CONFLICT (state_key) DO UPDATE
                    SET updated_at = NOW(), payload = EXCLUDED.payload
                    """,
                    HEALTH_STATE_KEY,
                    json.dumps(snapshot, ensure_ascii=True),
                )
                break
            except Exception:
                if attempt == 1:
                    raise
                await ensure_commercial_schema(application)
        snapshot["persistence"] = "PERSISTED"
        snapshot.pop("persistence_message", None)
        return True
    except Exception:
        snapshot["persistence"] = "ERROR"
        snapshot["persistence_message"] = "Health snapshot persistence failed."
        return False


async def restore_health_snapshot(application: FastAPI) -> None:
    pool = getattr(application.state, "db_pool", None)
    if pool is None:
        return
    try:
        row = await pool.fetchrow("SELECT payload FROM application_state_snapshots WHERE state_key = $1", HEALTH_STATE_KEY)
        if row and isinstance(row["payload"], dict):
            application.state.health_snapshot = row["payload"]
    except Exception:
        return


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Exchange signatures and API-key traffic must not silently inherit an
    # unrelated system proxy. This also avoids optional SOCKS dependencies
    # preventing the local API from starting.
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(30, connect=10, read=30, write=10, pool=30),
        limits=httpx.Limits(max_connections=40, max_keepalive_connections=20, keepalive_expiry=30),
        trust_env=False,
    )
    app.state.db_pool = None
    app.state.redis_client = None
    app.state.paper_schema_ready = False
    app.state.paper_restore_attempted = False
    app.state.market_twin_schema_ready = False
    app.state.market_twin_restore_attempted = False
    app.state.paper_dirty = False
    app.state.snapshot_lock = asyncio.Lock()
    app.state.health_lock = asyncio.Lock()
    app.state.health_snapshot = None
    app.state.infrastructure = {"api": "BAĞLI", "database": "BAĞLANIYOR", "redis": "BAĞLANIYOR", "paper_storage": "BEKLENİYOR" if PAPER_ENABLED else "DEVRE DIŞI", "self_healing": "AKTİF", "last_checked": None, "message": "Testnet-First altyapısı kontrol ediliyor."}
    app.state.paper = {
        "balance": 10_000.0,
        "initial_balance": 10_000.0,
        "positions": [],
        "trades": [],
        "next_id": 1,
        "limit_orders": [],
        "next_limit_id": 1,
        "lock": asyncio.Lock(),
        "risk": {
            "day": datetime.now(timezone.utc).date().isoformat(),
            "daily_realized_pnl": 0.0,
            "daily_loss_limit": 250.0,
            "consecutive_losses": 0,
            "consecutive_loss_limit": 2,
            "cooldown_until": None,
            "daily_locked": False,
            "reason": "Risk Kasası aktif; Paper Bot korumalı çalışıyor.",
        },
        "shadow": {"enabled": False, "events": []},
        "emergency_brake": {"active": False, "reason": "Acil fren kapalı.", "source": None, "triggered_at": None},
        "notifications": [],
        "decision_memory": [],
        "signal_history": [],
        "alerts": [],
        "grid_plans": [],
        "grid_engine": empty_grid_engine_state(),
        "strategy_orchestrator": empty_strategy_orchestrator_state(),
        "strategy_evolution": empty_v10_evolution_state(),
        "portfolio_risk": empty_v11_risk_state(),
    }
    app.state.paper_bot = {
        "enabled": False,
        "busy": False,
        "training_mode": True,
        "profile": "DENGELI",
        "mode": "V25.1 DENGELİ OTONOM PAPER",
        "scan_interval_seconds": 20,
        "last_action": "V25.1 Otonom Paper Avcısı beklemede; Dengeli profil hazır.",
        "last_check": None,
        "last_blocker": None,
        "last_candidate_count": 0,
        "cycles": 0,
        "events": [],
        "autonomy": {
            **autonomy_policy("DENGELI"),
            "shortlist": [],
            "last_allocation": None,
            "last_scan_at": None,
            "daily_reference": daily_reference_progress(0.0),
            "note": "Otonom seçim ve sermaye tahsisi yalnızca yerel Paper cüzdanda çalışır.",
        },
        "orders_enabled": False,
        "testnet_orders_enabled": False,
    }
    app.state.market_twin = empty_v9_market_twin_state()
    app.state.market_twin_lock = asyncio.Lock()
    init_binance_demo(app)
    init_v21_demo(app)
    init_v22_commercial(app)
    init_v25_execution(app)
    await ensure_infrastructure(app)
    await init_exchange_connections(app)
    await init_v27_cloud(app)
    await restore_health_snapshot(app)
    app.state.infrastructure_task = asyncio.create_task(infrastructure_loop(app))
    app.state.runtime_tasks = [app.state.infrastructure_task]
    if PAPER_ENABLED:
        app.state.runtime_tasks.extend([
            asyncio.create_task(paper_bot_loop(app)),
            asyncio.create_task(paper_limit_loop(app)),
            asyncio.create_task(grid_engine_loop(app)),
            asyncio.create_task(strategy_orchestrator_loop(app)),
            asyncio.create_task(v10_evolution_loop(app)),
            asyncio.create_task(v11_risk_loop(app)),
        ])
    app.state.runtime_tasks.append(asyncio.create_task(v9_market_twin_loop(app)))
    yield
    for task in app.state.runtime_tasks:
        task.cancel()
    try:
        await asyncio.wait_for(
            asyncio.gather(*app.state.runtime_tasks, return_exceptions=True), timeout=5
        )
    except asyncio.TimeoutError:
        pass

    async def bounded_shutdown(operation):
        try:
            await asyncio.wait_for(operation, timeout=5)
        except asyncio.TimeoutError:
            pass

    await bounded_shutdown(shutdown_v21_demo(app))
    await bounded_shutdown(shutdown_binance_demo(app))
    await bounded_shutdown(shutdown_v25_execution(app))
    await bounded_shutdown(shutdown_v27_cloud(app))
    await bounded_shutdown(shutdown_v22_commercial(app))

    # Endpoint-triggered fire-and-forget work must not keep Uvicorn alive.
    current = asyncio.current_task()
    pending = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        try:
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=3)
        except asyncio.TimeoutError:
            pass
    clear_vault_cache()
    # Kapanışta, son işlemden hemen sonra uygulama durdurulsa bile Paper
    # bakiyesinin ve açık pozisyonların veritabanına ulaşmasını dener.
    if PAPER_ENABLED:
        try:
            await asyncio.wait_for(persist_paper_snapshot(app), timeout=3)
        except Exception:
            pass
    if app.state.db_pool is not None:
        await app.state.db_pool.close()
    if app.state.redis_client is not None:
        await app.state.redis_client.aclose()
    await app.state.http.aclose()


app = FastAPI(title="ProTreBot Elite X API", version="28.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=WEB_CORS_ORIGINS,
    allow_origin_regex=WEB_CORS_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Accept", "Authorization", "Content-Type", "Origin", "X-Requested-With", "X-Protrebot-Owner"],
)

MEMBER_PUBLIC_PATHS = frozenset({
    "/api/health", "/api/web/access/check", "/api/v22/public", "/api/v22/bootstrap",
    "/api/v22/auth/login", "/api/v22/auth/register", "/api/v22/auth/verify-email",
    "/api/v22/auth/verification-status", "/api/v22/auth/forgot-password", "/api/v22/auth/reset-password", "/api/v22/subscription/webhook",
})


def apply_cors_headers(request, response):
    origin = request.headers.get("origin")
    if is_allowed_cors_origin(origin, WEB_CORS_ORIGINS):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Vary"] = "Origin"
    return response


@app.middleware("http")
async def owner_preview_gate(request, call_next):
    decision = evaluate_access(
        required=WEB_REQUIRE_AUTH,
        configured_token=WEB_ACCESS_TOKEN,
        authorization=request.headers.get("authorization"),
        owner_access=request.headers.get("x-protrebot-owner"),
        path=request.url.path,
        method=request.method,
    )
    if not decision.allowed:
        return apply_cors_headers(request, JSONResponse({"detail": decision.detail}, status_code=decision.status_code))
    configured_owner = str(request.headers.get("x-protrebot-owner") or "").strip() or bearer_token(request.headers.get("authorization"))
    owner_access_authenticated = bool(
        WEB_REQUIRE_AUTH
        and request.method.upper() != "OPTIONS"
        and request.url.path not in PUBLIC_PATHS
        and configured_owner
        and configured_owner == WEB_ACCESS_TOKEN
    )
    if request.url.path.startswith("/api/") and request.method.upper() != "OPTIONS" and request.url.path not in MEMBER_PUBLIC_PATHS and not owner_access_authenticated:
        try:
            request.state.member = authenticated_user(request)
        except HTTPException as exc:
            return apply_cors_headers(request, JSONResponse({"detail": exc.detail}, status_code=exc.status_code))
    request.state.web_owner_authenticated = bool(
        owner_access_authenticated
    )
    paper_prefixes = ("/api/paper", "/api/v6", "/api/v7", "/api/v10", "/api/v11", "/api/v9/paper")
    if not PAPER_ENABLED and request.method.upper() in {"POST", "PUT", "DELETE", "PATCH"} and request.url.path.startswith(paper_prefixes):
        return apply_cors_headers(
            request,
            JSONResponse(
                {"detail": "Paper motoru V28 Testnet-First sürümünde devre dışıdır; Binance Futures Demo kanalını kullanın."},
                status_code=410,
            ),
        )
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    if request.url.path.startswith("/api/"):
        response.headers.setdefault("Cache-Control", "no-store")
    return response

app.include_router(binance_demo_router)
app.include_router(v21_demo_router)
app.include_router(v22_commercial_router)
app.include_router(v24_commerce_router)
app.include_router(v25_execution_router)
app.include_router(v27_cloud_router)
app.include_router(exchange_connections_router)


@app.get("/api/v22/admin/system-health")
async def admin_health(request: Request):
    authenticated_user(request, owner=True)
    if request.app.state.health_snapshot is None:
        return {
            "overall_status": "UNAVAILABLE",
            "checks": [],
            "checked_at": None,
            "last_checked_at": None,
            "next_check_at": None,
            "counts": {"ACTIVE": 0, "WARNING": 0, "ERROR": 0, "DISABLED": 0, "NOT CONFIGURED": 0},
            "incident_count": 0,
            "persistence": "UNAVAILABLE",
            "persistence_message": "No persisted health snapshot is available.",
        }
    return request.app.state.health_snapshot


@app.post("/api/v22/admin/system-health/check")
async def admin_health_check(request: Request):
    authenticated_user(request, owner=True)
    return await run_health_checks(request.app)


@app.get("/api/health")
async def health():
    return {
        "status": "ok", "version": "28.0.0", "patch": DEPLOYMENT_PATCH,
        "build_commit": os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or None,
        "mode": "TESTNET_FIRST_CLOUD_DURABLE", "execution_mode": EXECUTION_MODE, "time": datetime.now(timezone.utc),
        **app.state.infrastructure,
        "paper": "DEVRE DIŞI",
        "paper_bot": "DEVRE DIŞI",
        "grid_engine": "DEVRE DIŞI",
        "strategy_orchestrator": "DEVRE DIŞI",
        "exchange_vault": "HAZIR" if getattr(app.state, "exchange_vault", {}).get("ready") else "BEKLİYOR",
        "strategy_evolution": "DEVRE DIŞI",
        "portfolio_risk": "DEVRE DIŞI",
        "future_lab": "AKTİF",
        "market_twin": app.state.market_twin.get("stream_health", "BEKLEMEDE"),
        "testnet": testnet_readiness()["status"],
        "live_guard": "DEVRE DIŞI" if not LIVE_CHANNEL_ENABLED else "API BEKLİYOR" if not app.state.v25_execution.get("connected") else "SALT OKUNUR BAĞLI",
        "cloud_evidence": app.state.v27_cloud.get("status", "BAŞLIYOR"),
        "web_access": "YÖNETİCİ KİLİTLİ" if WEB_REQUIRE_AUTH else "YEREL MOD",
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/api/web/access/check")
async def web_access_check():
    """The owner gate middleware has already authenticated this request."""
    return {
        "authorized": True,
        "mode": "OWNER_PREVIEW",
        "real_orders_enabled": False,
        "testnet_orders_available": True,
    }


@app.get("/api/markets")
async def markets(limit: int = Query(12, ge=1, le=200)):
    try:
        response = await app.state.http.get(f"{FUTURES_MARKET_DATA_API}/fapi/v1/ticker/24hr")
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise market_data_http_exception("Binance Futures piyasa özeti alınamadı", exc) from exc
    blocked = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "USD1", "USDE", "USDS"}
    result = []
    for item in response.json():
        symbol = item.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        base = symbol[:-4]
        if base in blocked or any(word in base for word in ("UP", "DOWN", "BULL", "BEAR")):
            continue
        result.append({
            "symbol": symbol,
            "display": f"{base}/USDT",
            "price": float(item["lastPrice"]),
            "change": float(item["priceChangePercent"]),
            "volume": float(item["quoteVolume"]),
        })
    result.sort(key=lambda market: market["volume"], reverse=True)
    return result[:limit]


async def fetch_candles(symbol: str, interval: str, limit: int) -> list[dict]:
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Desteklenmeyen zaman dilimi")
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    try:
        response = await app.state.http.get(
            f"{FUTURES_MARKET_DATA_API}/fapi/v1/klines",
            params={"symbol": safe_symbol, "interval": interval, "limit": limit},
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise market_data_http_exception("Binance Futures mum verisi alınamadı", exc) from exc
    try:
        rows = response.json()
    except (ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(502, "Binance Futures geçersiz mum verisi döndürdü.") from exc
    if not isinstance(rows, list):
        raise HTTPException(502, "Binance Futures mum verisi beklenen biçimde değil.")
    return [
        {
            "time": int(row[0] / 1000), "open": float(row[1]), "high": float(row[2]),
            "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
        }
        for row in rows
    ]


def market_data_http_exception(prefix: str, error: httpx.HTTPError) -> HTTPException:
    response = error.response if isinstance(error, httpx.HTTPStatusError) else None
    status = response.status_code if response is not None else 503
    detail = ""
    if response is not None:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                detail = str(payload.get("msg") or payload.get("message") or "").strip()
        except (ValueError, json.JSONDecodeError):
            detail = ""
    if status == 418:
        message = f"{prefix}: Binance Futures erişimi geçici olarak engelledi (HTTP 418)."
    elif status == 429:
        message = f"{prefix}: Binance Futures hız sınırına ulaşıldı (HTTP 429)."
    elif status >= 500:
        message = f"{prefix}: Binance Futures sunucu hatası (HTTP {status})."
    elif response is None:
        message = f"{prefix}: Binance Futures sunucusuna ulaşılamadı."
    else:
        message = f"{prefix}: HTTP {status}."
    if detail:
        message = f"{message} {detail}"
    return HTTPException(status_code=429 if status == 429 else 502 if status >= 500 else status, detail=message)


async def historical_fetch_candles(symbol: str, interval: str, total_limit: int = 10_000) -> list[dict]:
    """Fetch a bounded historical series backward without changing live candle fetches."""
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Desteklenmeyen zaman dilimi")
    if total_limit < 1:
        raise HTTPException(400, "Historical candle limiti pozitif olmalı")
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    collected: dict[int, dict] = {}
    end_time_ms: int | None = None
    previous_oldest: int | None = None
    max_retries = 3
    while len(collected) < total_limit:
        params = {"symbol": safe_symbol, "interval": interval, "limit": min(1000, total_limit - len(collected))}
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        rows = None
        for attempt in range(max_retries):
            try:
                response = await app.state.http.get(f"{FUTURES_MARKET_DATA_API}/fapi/v1/klines", params=params)
                if response.status_code == 429:
                    if attempt == max_retries - 1:
                        raise HTTPException(502, "Binance historical candle rate limitine ulaşıldı")
                    await asyncio.sleep(0.5 * (2 ** attempt))
                    continue
                response.raise_for_status()
                rows = response.json()
                break
            except HTTPException:
                raise
            except (httpx.HTTPError, ValueError, TypeError) as exc:
                if attempt == max_retries - 1:
                    raise HTTPException(502, f"Binance historical mum verisi alınamadı: {exc}") from exc
                await asyncio.sleep(0.5 * (2 ** attempt))
        if not rows:
            raise HTTPException(502, "Binance historical candle verisi eksik döndü")
        oldest_open_time = None
        for row in rows:
            open_time_ms = int(row[0])
            oldest_open_time = open_time_ms if oldest_open_time is None else min(oldest_open_time, open_time_ms)
            collected[open_time_ms] = {
                "time": int(open_time_ms / 1000), "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]), "volume": float(row[5]),
                "timeframe": interval,
            }
        if oldest_open_time is None or oldest_open_time == previous_oldest:
            raise HTTPException(502, "Binance historical pagination ilerlemedi")
        previous_oldest = oldest_open_time
        end_time_ms = oldest_open_time - 1

    return [collected[key] for key in sorted(collected)[:total_limit]]


@app.get("/api/klines/{symbol}")
async def klines(symbol: str, interval: str = "15m", limit: int = Query(500, ge=50, le=1000)):
    return await fetch_candles(symbol, interval, limit)


@app.get("/api/analysis/{symbol}")
async def technical_analysis(symbol: str, interval: str = "15m"):
    candles = await fetch_candles(symbol, interval, 500)
    if len(candles) < 220:
        raise HTTPException(422, "Analiz için yeterli mum verisi yok")
    result = analyze(candles)
    # Keep the legacy direction for existing consumers and expose a normalized signal separately.
    result["normalized_signal"] = normalize_analysis_signal(result["direction"])
    return result


async def candle_close_gate(symbol: str, interval: str = "15m") -> dict:
    """Sadece kapanmış mumlarla iki aşamalı giriş doğrulaması üretir."""
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Desteklenmeyen zaman dilimi")
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    key = (safe_symbol, interval)
    cached = GATE_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 15:
        return cached[1]
    candles = await fetch_candles(safe_symbol, interval, 260)
    closed_candles = candles[:-1]
    if len(closed_candles) < 225:
        raise HTTPException(422, "Mum kapanış doğrulaması için yeterli veri yok")
    current = analyze(closed_candles)
    previous = analyze(closed_candles[:-1])
    same_direction = (
        current["direction"] in {"LONG", "SHORT"}
        and current["direction"] == previous["direction"]
    )
    entry_allowed = (
        same_direction
        and current["confidence"] >= 78
        and previous["confidence"] >= 70
        and current["radar"]["trap_score"] <= 35
        and current["radar"]["breakout_quality"] >= 55
    )
    if entry_allowed:
        status, reason = "KAPANIŞ ONAYLI", "İki kapanmış mum aynı yönü ve kalite filtresini doğruluyor."
    elif current["direction"] == "BEKLE":
        status, reason = "SİNYAL BEKLİYOR", "Kapanmış mumlarda net bir yön oluşmadı."
    elif not same_direction:
        status, reason = "İKİNCİ KAPANIŞ BEKLİYOR", "Son iki kapanmış mum aynı yönü doğrulamadı."
    elif current["radar"]["trap_score"] > 35:
        status, reason = "TUZAK FİLTRESİ", "Kapanış sinyali var ancak tuzak riski yüksek."
    else:
        status, reason = "KALİTE BEKLİYOR", "Kapanış yönü var fakat güven veya kırılım kalitesi yeterli değil."
    interval_seconds = INTERVAL_SECONDS[interval]
    now = time.time()
    next_close_at = int(now - now % interval_seconds + interval_seconds)
    payload = {
        "symbol": safe_symbol, "interval": interval, "direction": current["direction"],
        "status": status, "entry_allowed": entry_allowed, "reason": reason,
        "confidence": current["confidence"], "last_closed_at": closed_candles[-1]["time"],
        "next_close_at": next_close_at, "analysis": current,
    }
    GATE_CACHE[key] = (time.monotonic(), payload)
    return payload


@app.get("/api/gate/{symbol}")
async def candle_close_gate_endpoint(symbol: str, interval: str = "15m"):
    gate = await candle_close_gate(symbol, interval)
    return {key: value for key, value in gate.items() if key != "analysis"}


async def signal_freshness(symbol: str, interval: str = "15m") -> dict:
    """Kapanışta oluşan planın canlı fiyata göre hâlâ geçerli olup olmadığını denetler."""
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    key = (safe_symbol, interval)
    cached = FRESHNESS_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 3:
        return cached[1]
    gate = await candle_close_gate(safe_symbol, interval)
    analysis = gate["analysis"]
    live_price = await latest_price(safe_symbol)
    entry, stop, target, atr = analysis["entry"], analysis["stop_loss"], analysis["tp1"], max(analysis["atr"], 0.00000001)
    drift_atr = abs(live_price - entry) / atr
    drift_pct = abs((live_price - entry) / entry) * 100 if entry else 0.0
    if not gate["entry_allowed"]:
        status, auto_allowed, reason = "KAPANIŞ BEKLE", False, gate["reason"]
    elif analysis["direction"] == "LONG" and live_price <= stop:
        status, auto_allowed, reason = "PLAN BOZULDU", False, "Canlı fiyat LONG stop seviyesinin altında."
    elif analysis["direction"] == "SHORT" and live_price >= stop:
        status, auto_allowed, reason = "PLAN BOZULDU", False, "Canlı fiyat SHORT stop seviyesinin üstünde."
    elif analysis["direction"] == "LONG" and live_price >= target:
        status, auto_allowed, reason = "HEDEF YAKIN", False, "Canlı fiyat ilk hedefe ulaştı veya geçti; yeni plan bekleniyor."
    elif analysis["direction"] == "SHORT" and live_price <= target:
        status, auto_allowed, reason = "HEDEF YAKIN", False, "Canlı fiyat ilk hedefe ulaştı veya geçti; yeni plan bekleniyor."
    elif drift_atr > 0.65:
        status, auto_allowed, reason = "YENİDEN ONAYLA", False, "Fiyat kapanış planından 0,65 ATR'den fazla uzaklaştı."
    else:
        status, auto_allowed, reason = "TAZE GİRİŞ", True, "Canlı fiyat kapanış planına yeterince yakın."
    payload = {
        "symbol": safe_symbol, "interval": interval, "direction": analysis["direction"],
        "status": status, "auto_allowed": auto_allowed, "reason": reason,
        "live_price": live_price, "planned_entry": entry, "stop_loss": stop, "take_profit": target,
        "drift_atr": round(drift_atr, 2), "drift_pct": round(drift_pct, 3),
    }
    FRESHNESS_CACHE[key] = (time.monotonic(), payload)
    return payload


@app.get("/api/freshness/{symbol}")
async def signal_freshness_endpoint(symbol: str, interval: str = "15m"):
    return await signal_freshness(symbol, interval)


def simulate_strategy(
    candles: list[dict],
    start_index: int | None = None,
    end_index: int | None = None,
    max_results: int = 40,
    mtf: bool = False,
    mtf_candles: dict[str, list[dict]] | None = None,
    capital: float = 10_000.0,
    symbol: str | None = None,
    short_filter: bool = True,
) -> dict:
    """Geçmiş mumlarda mevcut giriş/stop/TP1 kurallarını konservatif olarak test eder."""
    warmup, horizon, step = 220, 16, 4
    fee_pct = 0.10  # Giriş + çıkış için örnek toplam maliyet yüzdesi.
    results: list[dict] = []
    blocked_by_mtf = 0
    short_blocked_by_alignment = 0
    mtf_results: list[dict] = []
    mtf_equity = float(capital)
    mtf_peak_equity = mtf_equity
    mtf_max_drawdown = 0.0
    mtf_indexes = {"1h": -1, "4h": -1}
    mtf_intervals = {"1h": 60 * 60, "4h": 4 * 60 * 60}
    mtf_warmup = 50
    if mtf and not mtf_candles:
        raise ValueError("MTF backtest için 1h ve 4h candle verisi gerekli")
    index = max(warmup, start_index or warmup)
    terminal = min(len(candles) - horizon, end_index if end_index is not None else len(candles) - horizon)
    while index < terminal and (mtf or len(results) < max_results):
        setup = analyze(candles[index - warmup:index + 1])
        valid = (
            setup["direction"] in {"LONG", "SHORT"}
            and setup["confidence"] >= 78
            and setup["radar"]["trap_score"] <= 35
            and setup["radar"]["breakout_quality"] >= 55
        )
        if not valid:
            index += step
            continue
        if mtf:
            signal_close_time = int(candles[index]["time"]) + 15 * 60
            mtf_direction = setup["direction"]
            mtf_confidences = {"1h": None, "4h": None}
            for timeframe, interval_seconds in mtf_intervals.items():
                series = mtf_candles.get(timeframe, [])
                cursor = mtf_indexes[timeframe]
                while (
                    cursor + 1 < len(series)
                    and int(series[cursor + 1]["time"]) + interval_seconds <= signal_close_time
                ):
                    cursor += 1
                mtf_indexes[timeframe] = cursor
                if cursor < mtf_warmup:
                    mtf_direction = "BEKLE"
                    continue
                higher = analyze(series[:cursor + 1])
                mtf_confidences[timeframe] = higher.get("confidence")
                if higher["direction"] != setup["direction"]:
                    mtf_direction = "BEKLE"
            timeframe_results = {
                "1h": {
                    "direction": analyze(mtf_candles["1h"][:mtf_indexes["1h"] + 1])["direction"] if mtf_indexes["1h"] >= mtf_warmup else "BEKLE",
                    "confidence": mtf_confidences["1h"] if mtf_confidences["1h"] is not None else 0.0,
                },
                "4h": {
                    "direction": analyze(mtf_candles["4h"][:mtf_indexes["4h"] + 1])["direction"] if mtf_indexes["4h"] >= mtf_warmup else "BEKLE",
                    "confidence": mtf_confidences["4h"] if mtf_confidences["4h"] is not None else 0.0,
                },
            }
            mtf_decision = shared_mtf_decision(
                symbol=symbol or "",
                entry_direction=setup["direction"],
                confidence_15m=float(setup["confidence"]),
                timeframe_results=timeframe_results,
                short_filter=short_filter,
                short_alignment_max=SHORT_MTF_ALIGNMENT_MAX,
            )
            mtf_direction = mtf_decision["direction"]
            mtf_alignment = mtf_decision["alignment"]
            mtf_entry_permission = mtf_decision["entry_permission"]
            if mtf_direction != setup["direction"] or not mtf_entry_permission:
                blocked_by_mtf += 1
                index += step
                continue
            if mtf_decision["blocked_by_short_filter"]:
                short_blocked_by_alignment += 1
                index += step
                continue
        entry, stop, target = setup["entry"], setup["stop_loss"], setup["tp1"]
        exit_price, outcome, exit_offset = candles[index + horizon]["close"], "SÜRE", horizon
        for offset, candle in enumerate(candles[index + 1:index + horizon + 1], start=1):
            if setup["direction"] == "LONG":
                stop_hit, target_hit = candle["low"] <= stop, candle["high"] >= target
            else:
                stop_hit, target_hit = candle["high"] >= stop, candle["low"] <= target
            # Aynı mumda iki seviye de görünürse stopu önce saymak daha temkinlidir.
            if stop_hit:
                exit_price, outcome, exit_offset = stop, "STOP", offset
                break
            if target_hit:
                exit_price, outcome, exit_offset = target, "TP1", offset
                break
        if mtf:
            stop_distance = abs(entry - stop)
            if stop_distance <= 0:
                index += max(step, exit_offset)
                continue
            risk_amount = mtf_equity * RISK_PER_TRADE
            quantity = risk_amount / stop_distance
            gross_pnl = quantity * (exit_price - entry)
            if setup["direction"] == "SHORT":
                gross_pnl *= -1
            fee = entry * quantity * fee_pct / 100
            net_pnl = gross_pnl - fee
            signal_time = int(candles[index]["time"]) + 15 * 60 if "time" in candles[index] else None
            exit_candle = candles[index + exit_offset]
            exit_time = int(exit_candle["time"]) + 15 * 60 if "time" in exit_candle else None
            trade_duration = (exit_time - signal_time) / 60 if signal_time is not None and exit_time is not None else None
            confidence_1h = mtf_confidences.get("1h")
            confidence_4h = mtf_confidences.get("4h")
            risk_reward = abs(target - entry) / stop_distance
            mtf_results.append({
                "outcome": outcome, "net_pnl": net_pnl,
                "symbol": symbol, "entry_time": signal_time, "exit_time": exit_time,
                "direction": setup["direction"],
                "confidence_15m": setup.get("confidence"), "confidence_1h": confidence_1h,
                "confidence_4h": confidence_4h, "mtf_alignment": mtf_alignment,
                "mtf_entry_permission": mtf_entry_permission,
                "entry_price": entry, "stop_loss": stop, "take_profit": target,
                "exit_price": exit_price, "exit_reason": outcome,
                "quantity": quantity, "risk_amount": risk_amount,
                "position_amount": quantity * entry, "gross_pnl": gross_pnl,
                "fee": fee, "trade_duration": trade_duration, "risk_reward": risk_reward,
                "volume_ratio": setup.get("volume_ratio"),
                "breakout_quality": setup.get("radar", {}).get("breakout_quality"),
                "trap_score": setup.get("radar", {}).get("trap_score"),
                "trap_level": setup.get("radar", {}).get("trap_level"),
            })
            mtf_equity += net_pnl
            mtf_peak_equity = max(mtf_peak_equity, mtf_equity)
            mtf_max_drawdown = min(
                mtf_max_drawdown,
                (mtf_equity / max(mtf_peak_equity, 0.00000001) - 1) * 100,
            )
            index += max(step, exit_offset)
            continue
        gross_return = ((exit_price - entry) / entry) * 100
        if setup["direction"] == "SHORT":
            gross_return *= -1
        net_return = gross_return - fee_pct
        results.append({"outcome": outcome, "net_return": net_return})
        index += max(step, exit_offset)

    if mtf:
        total = len(mtf_results)
        wins = sum(1 for item in mtf_results if item["net_pnl"] > 0)
        losses = sum(1 for item in mtf_results if item["net_pnl"] < 0)
        gross_wins = sum(item["net_pnl"] for item in mtf_results if item["net_pnl"] > 0)
        gross_losses = abs(sum(item["net_pnl"] for item in mtf_results if item["net_pnl"] < 0))
        net_pnl = mtf_equity - float(capital)
        win_rate = round(wins / total * 100, 1) if total else 0.0
        profit_factor = round(gross_wins / gross_losses, 2) if gross_losses else None
        return {
            "sampled_candles": len(candles), "total_trades": total, "trade_count": total,
            "wins": wins, "losses": losses, "win_rate": win_rate,
            "profit_factor": profit_factor, "net_pnl": round(net_pnl, 2),
            "max_drawdown": round(mtf_max_drawdown, 2),
            "max_drawdown_pct": round(mtf_max_drawdown, 2),
            "blocked_by_mtf": blocked_by_mtf, "mtf": True,
            "short_blocked_by_alignment": short_blocked_by_alignment,
            "short_filter": {"enabled": short_filter, "max_alignment": SHORT_MTF_ALIGNMENT_MAX},
            "capital": round(float(capital), 2), "fee_pct": fee_pct,
            "trade_log": mtf_results, "trade_log_count": total,
        }

    total = len(results)
    wins = sum(1 for item in results if item["net_return"] > 0)
    losses = sum(1 for item in results if item["net_return"] < 0)
    timeouts = sum(1 for item in results if item["outcome"] == "SÜRE")
    gross_wins = sum(item["net_return"] for item in results if item["net_return"] > 0)
    gross_losses = abs(sum(item["net_return"] for item in results if item["net_return"] < 0))
    equity, peak, max_drawdown = 100.0, 100.0, 0.0
    for item in results:
        equity *= 1 + item["net_return"] / 100
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, (equity / peak - 1) * 100)
    win_rate = round((wins / total) * 100, 1) if total else 0.0
    avg_return = round(sum(item["net_return"] for item in results) / total, 2) if total else 0.0
    net_return = round(equity - 100, 2)
    profit_factor = round(gross_wins / gross_losses, 2) if gross_losses else None
    if total < 8:
        verdict = "AZ ÖRNEK"
        note = "Bu aralıkta yeterli sayıda kaliteli geçmiş sinyal oluşmadı; sonucu tek başına karar olarak kullanma."
    elif net_return > 0 and win_rate >= 50 and max_drawdown >= -12:
        verdict = "TUTARLI"
        note = "Yakın geçmişte kurallar dengeli görünse de geçmiş sonuç gelecek performansı garanti etmez."
    else:
        verdict = "TEMKİNLİ"
        note = "Yakın geçmişte tutarlılık zayıf veya düşüş yüksek; Paper Bot için ek seçicilik gerekebilir."
    return {
        "sampled_candles": len(candles), "total_trades": total, "wins": wins, "losses": losses,
        "timeouts": timeouts, "win_rate": win_rate, "avg_return_pct": avg_return,
        "net_return_pct": net_return, "max_drawdown_pct": round(max_drawdown, 2),
        "profit_factor": profit_factor, "verdict": verdict, "note": note,
        "cost_assumption": "Hesaplamada giriş + çıkış için toplam %0,10 örnek maliyet varsayılmıştır.",
    }


def walk_forward_validation(candles: list[dict]) -> dict:
    """Kuralları ardışık zaman bölümlerinde sınayarak aşırı uyumu görünür kılar."""
    warmup, horizon = 220, 16
    terminal = len(candles) - horizon
    usable_span = terminal - warmup
    if usable_span < 180:
        return {
            "folds": [], "positive_folds": 0, "total_folds": 0,
            "out_of_sample": None, "verdict": "AZ VERİ",
            "note": "Walk-forward doğrulaması için daha uzun geçmiş mum verisi gerekiyor.",
        }
    boundaries = [warmup + round(usable_span * part / 3) for part in range(4)]
    boundaries[-1] = terminal
    labels = ["Bölüm 1", "Bölüm 2", "Bölüm 3 · Güncel"]
    folds = []
    for label, start, end in zip(labels, boundaries[:-1], boundaries[1:]):
        metrics = simulate_strategy(candles, start_index=start, end_index=end, max_results=20)
        folds.append({
            "label": label, "trades": metrics["total_trades"], "win_rate": metrics["win_rate"],
            "net_return_pct": metrics["net_return_pct"], "max_drawdown_pct": metrics["max_drawdown_pct"],
            "verdict": metrics["verdict"],
        })
    positive_folds = sum(
        1 for fold in folds
        if fold["trades"] >= 4 and fold["net_return_pct"] >= 0 and fold["win_rate"] >= 45
    )
    out_of_sample = folds[-1]
    if any(fold["trades"] < 4 for fold in folds):
        verdict = "AZ ÖRNEK"
        note = "Bazı zaman bölümlerinde yeterli kaliteli sinyal oluşmadı; sonucu kanıt olarak görme."
    elif positive_folds >= 2 and out_of_sample["net_return_pct"] >= 0:
        verdict = "ZAMAN TESTİNİ GEÇTİ"
        note = "Kurallar en az iki bağımsız zaman bölümünde dengeli kaldı. Bu sonuç gelecek performansı garanti etmez."
    else:
        verdict = "ZAMAN TESTİ TEMKİNLİ"
        note = "Sonuçlar zaman bölümleri arasında tutarlı değil; Paper Bot seçiciliği korunmalı."
    return {
        "folds": folds, "positive_folds": positive_folds, "total_folds": len(folds),
        "out_of_sample": out_of_sample, "verdict": verdict, "note": note,
    }


def stress_test_validation(candles: list[dict]) -> dict:
    """Stratejinin sentetik işlem maliyeti ve oynaklık baskısına hassasiyetini gösterir.

    Bu bir gelecek tahmini değildir. Aynı geçmiş sonuç üzerinde daha kötü spread,
    kayma ve gecikme varsayımlarının etkisini görünür kılan ihtiyatlı bir laboratuvardır.
    """
    baseline = simulate_strategy(candles)
    total_trades = baseline["total_trades"]
    scenarios = [
        ("SPREAD + KAYMA", "Giriş ve çıkış maliyeti normalden daha yüksek varsayılır.", 0.80, 0.25, 1.15, 0.40),
        ("VOLATİLİTE ŞOKU", "Hızlı fiyat hareketinde hedefe ulaşma ve stop riski zorlaştırılır.", 0.65, 0.40, 1.35, 0.80),
        ("GECİKME / FOMO", "Karar gecikmesi ve plana uzak girişte daha ağır bir baskı uygulanır.", 0.55, 0.55, 1.50, 1.20),
    ]
    results = []
    for label, description, return_retention, fixed_drag, drawdown_multiplier, drawdown_drag in scenarios:
        adjusted_net = round(baseline["net_return_pct"] * return_retention - fixed_drag, 2)
        adjusted_drawdown = round(min(
            baseline["max_drawdown_pct"] * drawdown_multiplier - drawdown_drag,
            -drawdown_drag,
        ), 2)
        if total_trades < 8:
            status = "AZ ÖRNEK"
        elif adjusted_net >= 0 and adjusted_drawdown >= -15:
            status = "DAYANIKLI"
        else:
            status = "TEMKİNLİ"
        results.append({
            "label": label, "description": description, "net_return_pct": adjusted_net,
            "max_drawdown_pct": adjusted_drawdown, "status": status,
        })
    survived = sum(1 for item in results if item["status"] == "DAYANIKLI")
    if total_trades < 8:
        verdict = "VERİ YETERSİZ"
        note = "Stres değerlendirmesi için daha fazla kaliteli geçmiş Paper benzeri sinyal gerekiyor."
    elif survived == len(results):
        verdict = "STRESE DAYANIKLI"
        note = "Model maliyet ve oynaklık baskısında da pozitif kalıyor; yine de yalnızca Paper doğrulaması olarak değerlendirilmelidir."
    elif survived >= 1:
        verdict = "KISMİ DAYANIKLI"
        note = "Bazı baskı senaryolarında strateji zayıflıyor; otomatik Paper seçiciliği korunmalıdır."
    else:
        verdict = "STRES ALTINDA TEMKİNLİ"
        note = "Maliyet, kayma veya gecikme varsayımlarında sonuç zayıflıyor; yeni kural eklemeden önce Paper verisi topla."
    return {
        "baseline": {
            "trades": total_trades, "net_return_pct": baseline["net_return_pct"],
            "max_drawdown_pct": baseline["max_drawdown_pct"], "verdict": baseline["verdict"],
        },
        "scenarios": results, "survived": survived, "total_scenarios": len(results),
        "verdict": verdict, "note": note,
        "assumption": "Bu sentetik stres testi, geçmiş simülasyon sonucuna ilave maliyet ve oynaklık baskısı uygular; gelecek performansı tahmin etmez.",
    }


@app.get("/api/lab/{symbol}")
async def strategy_lab(
    symbol: str,
    interval: str = "15m",
    mtf: bool = False,
    capital: float = Query(10_000.0, ge=100, le=1_000_000),
    short_filter: bool = True,
):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    key = (safe_symbol, interval, mtf, int(round(capital)), short_filter)
    cached = LAB_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 60:
        return {**cached[1], "cached": True}
    if mtf and (safe_symbol not in MTF_BACKTEST_SYMBOLS or interval != "15m"):
        raise HTTPException(422, "MTF backtest yalnızca desteklenen 6 sembol ve 15m ana sinyali destekler")
    requested = 10_000 if mtf else 700
    candles = await (
        historical_fetch_candles(safe_symbol, interval, requested)
        if mtf else fetch_candles(safe_symbol, interval, requested)
    )
    historical_mtf = {}
    if mtf:
        one_hour, four_hour = await asyncio.gather(
            historical_fetch_candles(safe_symbol, "1h", 10_000),
            historical_fetch_candles(safe_symbol, "4h", 10_000),
        )
        historical_mtf = {"1h": one_hour, "4h": four_hour}
    payload = {
        "symbol": safe_symbol, "interval": interval,
        **simulate_strategy(
            candles, mtf=mtf, mtf_candles=historical_mtf, capital=capital,
            symbol=safe_symbol, short_filter=short_filter,
        ),
    }
    if mtf:
        payload["historical_candles"] = {
            "15m": len(candles), "1h": len(one_hour), "4h": len(four_hour),
        }
    LAB_CACHE[key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/lab/walk-forward/{symbol}")
async def walk_forward_lab(symbol: str, interval: str = "15m"):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    key = (safe_symbol, interval)
    cached = WALK_FORWARD_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 90:
        return {**cached[1], "cached": True}
    candles = await fetch_candles(safe_symbol, interval, 1_000)
    payload = {"symbol": safe_symbol, "interval": interval, **walk_forward_validation(candles)}
    WALK_FORWARD_CACHE[key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/lab/stress/{symbol}")
async def stress_test_lab(symbol: str, interval: str = "15m"):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    key = (safe_symbol, interval)
    cached = STRESS_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 90:
        return {**cached[1], "cached": True}
    candles = await fetch_candles(safe_symbol, interval, 1_000)
    payload = {"symbol": safe_symbol, "interval": interval, **stress_test_validation(candles)}
    STRESS_CACHE[key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


def grid_price_decimals(price: float) -> int:
    if price >= 1_000:
        return 2
    if price >= 100:
        return 3
    if price >= 1:
        return 4
    return 8


def build_smart_grid_plan(
    symbol: str,
    interval: str,
    candles: list[dict],
    analysis: dict,
    regime: dict,
    liquidity: dict,
    capital: float,
) -> dict:
    """ATR, teknik aralık, likidite ve maliyet varsayımından Paper grid planı üretir.

    Üretilen seviyeler yalnızca planlama/simülasyon içindir. Bu fonksiyonun borsa
    bağlantısı, API anahtarı veya emir gönderme yeteneği yoktur.
    """
    closed = candles[:-1] if len(candles) > 1 else candles
    if len(closed) < 220:
        raise HTTPException(422, "Akıllı grid planı için yeterli kapanmış mum yok")
    price = float(analysis["entry"])
    atr = max(float(analysis["atr"]), price * 0.0001)
    atr_pct = atr / price * 100 if price else 0.0
    recent = closed[-80:]
    support = min(float(analysis["support"]), min(float(item["low"]) for item in recent))
    resistance = max(float(analysis["resistance"]), max(float(item["high"]) for item in recent))
    direction = str(analysis.get("direction") or "BEKLE")
    adx_value = float(analysis.get("adx") or 0.0)
    regime_label = str(regime.get("label") or "ÖLÇÜLÜYOR")

    if direction == "LONG" and adx_value >= 20:
        mode, plan_direction = "LONG GRID", "LONG"
        lower_target, upper_target = min(support, price - atr * 2.2), max(resistance, price + atr * 3.5)
    elif direction == "SHORT" and adx_value >= 20:
        mode, plan_direction = "SHORT GRID", "SHORT"
        lower_target, upper_target = min(support, price - atr * 3.5), max(resistance, price + atr * 2.2)
    elif adx_value < 22 or "SIKIŞMA" in regime_label or "YATAY" in regime_label:
        mode, plan_direction = "NÖTR GRID", "NÖTR"
        lower_target, upper_target = min(support, price - atr * 2.8), max(resistance, price + atr * 2.8)
    else:
        mode, plan_direction = "BEKLE", "BEKLE"
        lower_target, upper_target = price - atr * 2.5, price + atr * 2.5

    max_half_span = max(atr * 7.0, price * 0.035)
    minimum_half_span = max(atr * 1.8, price * 0.004)
    lower = max(0.00000001, max(price - max_half_span, min(lower_target, price - minimum_half_span)))
    upper = min(price + max_half_span, max(upper_target, price + minimum_half_span))
    if upper <= lower:
        lower, upper = max(0.00000001, price - minimum_half_span), price + minimum_half_span

    width_pct = (upper - lower) / price * 100 if price else 0.0
    spread_bps = max(0.0, float(liquidity.get("spread_bps") or 0.0))
    spread_pct = spread_bps / 100
    round_trip_fee_pct = GRID_FEE_SIDE_PCT * 2
    desired_step_pct = max(round_trip_fee_pct * 2.4, atr_pct * 0.55, spread_pct * 4.0)
    grid_count = max(6, min(24, round(width_pct / max(desired_step_pct, 0.01))))
    step = (upper - lower) / grid_count
    step_pct = step / price * 100 if price else 0.0
    fee_multiple = step_pct / round_trip_fee_pct if round_trip_fee_pct else 0.0
    net_edge_pct = step_pct - round_trip_fee_pct
    liquidity_score = max(0, min(100, int(liquidity.get("liquidity_score") or 0)))
    high_volatility = "YÜKSEK VOLATİLİTE" in regime_label

    fee_score = max(0.0, min(35.0, (fee_multiple - 1.0) * 18.0))
    liquidity_component = liquidity_score * 0.25
    if high_volatility:
        regime_component = 0.0
    elif mode == "NÖTR GRID" and adx_value < 22:
        regime_component = 25.0
    elif mode in {"LONG GRID", "SHORT GRID"} and bool(regime.get("auto_allowed")):
        regime_component = 25.0
    elif mode != "BEKLE":
        regime_component = 15.0
    else:
        regime_component = 0.0
    confidence_component = min(15.0, float(analysis.get("confidence") or 0.0) * 0.15)
    safety_score = round(min(99.0, fee_score + liquidity_component + regime_component + confidence_component))

    if high_volatility:
        viability = "VOLATİLİTE BEKLE"
        reason = "Piyasa rejimi yüksek volatilitede; grid aralığı görünür fakat Paper planı etkinleştirilmez."
    elif not bool(liquidity.get("auto_allowed")):
        viability = "LİKİDİTE BEKLE"
        reason = f"Likidite Kalkanı {liquidity.get('mode', 'ölçüm')} diyor; kayma riski nedeniyle plan yalnızca izlenir."
    elif fee_multiple < 1.75:
        viability = "KOMİSYONA ÇOK DAR"
        reason = "Kademe aralığı, ihtiyatlı çift yön maliyet varsayımına göre yeterli net pay bırakmıyor."
    elif mode == "BEKLE":
        viability = "YÖN / ARALIK BEKLE"
        reason = "Trend ve yatay piyasa ölçümleri aynı grid davranışında birleşmedi."
    elif safety_score < 68:
        viability = "TEMKİNLİ İZLE"
        reason = "Grid aralığı hesaplandı ancak güvenlik puanı Paper plan eşiğinin altında."
    else:
        viability = "PAPER PLANI UYGUN"
        reason = "Teknik aralık, likidite ve ihtiyatlı maliyet payı Paper senaryo planı için birlikte yeterli."
    paper_eligible = viability == "PAPER PLANI UYGUN"

    decimals = grid_price_decimals(price)
    levels = [round(lower + step * index, decimals) for index in range(grid_count + 1)]
    capital_per_grid = capital / grid_count
    gross_cycle = capital_per_grid * step_pct / 100
    fee_cycle = capital_per_grid * round_trip_fee_pct / 100
    safety_floor = max(0.00000001, lower - atr)
    safety_ceiling = upper + atr
    return {
        "symbol": symbol, "interval": interval, "mode": mode, "direction": plan_direction,
        "entry_reference": round(price, decimals), "lower": round(lower, decimals),
        "upper": round(upper, decimals), "grid_count": grid_count,
        "grid_step": round(step, decimals), "grid_step_pct": round(step_pct, 3),
        "range_width_pct": round(width_pct, 2), "levels": levels,
        "support": round(support, decimals), "resistance": round(resistance, decimals),
        "safety_floor": round(safety_floor, decimals), "safety_ceiling": round(safety_ceiling, decimals),
        "atr": round(atr, decimals), "atr_pct": round(atr_pct, 3),
        "capital": round(capital, 2), "capital_per_grid": round(capital_per_grid, 2),
        "max_planned_exposure": round(capital, 2),
        "fee_assumption": {
            "single_side_pct": GRID_FEE_SIDE_PCT, "round_trip_pct": round_trip_fee_pct,
            "step_to_fee_multiple": round(fee_multiple, 2),
            "label": "İhtiyatlı ve kullanıcı tarafından ileride değiştirilebilir varsayım",
        },
        "estimated_per_cycle": {
            "gross_usdt": round(gross_cycle, 4), "fee_usdt": round(fee_cycle, 4),
            "net_usdt": round(gross_cycle - fee_cycle, 4), "net_edge_pct": round(net_edge_pct, 3),
        },
        "safety_score": safety_score, "viability": viability,
        "paper_eligible": paper_eligible, "orders_enabled": False,
        "regime": regime_label, "liquidity_mode": liquidity.get("mode", "ÖLÇÜLÜYOR"),
        "liquidity_score": liquidity_score, "spread_bps": round(spread_bps, 2),
        "reason": reason,
        "note": "Bu plan yalnızca Paper hafızası ve senaryo analizi içindir; gerçek veya Testnet emir üretmez.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def smart_grid_plan(symbol: str, interval: str = "15m", capital: float = 1_000.0) -> dict:
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Desteklenmeyen zaman dilimi")
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    if not safe_symbol:
        raise HTTPException(400, "Geçerli bir parite gerekli")
    cache_key = (safe_symbol, interval, int(round(capital)))
    cached = GRID_PLAN_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < 20:
        return {**cached[1], "cached": True}
    candles = await fetch_candles(safe_symbol, interval, 500)
    closed = candles[:-1]
    if len(closed) < 220:
        raise HTTPException(422, "Akıllı grid planı için yeterli kapanmış mum yok")
    analysis = analyze(closed)
    regime, liquidity = await asyncio.gather(
        market_regime(safe_symbol, interval), liquidity_shield(safe_symbol),
    )
    payload = build_smart_grid_plan(safe_symbol, interval, candles, analysis, regime, liquidity, capital)
    GRID_PLAN_CACHE[cache_key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


def simulate_grid_plan(candles: list[dict], plan: dict) -> dict:
    """Sabit V5 grid planını geçmiş kapanış geçişlerinde ihtiyatlı biçimde oynatır."""
    levels = [float(value) for value in plan.get("levels", [])]
    capital = float(plan.get("capital") or 1_000.0)
    per_grid = float(plan.get("capital_per_grid") or capital / max(1, len(levels) - 1))
    fee_side_pct = float(plan.get("fee_assumption", {}).get("single_side_pct") or GRID_FEE_SIDE_PCT)
    mode = str(plan.get("mode") or "BEKLE")
    closes = [float(item["close"]) for item in candles[-420:] if float(item.get("close") or 0) > 0]
    if len(levels) < 2 or len(closes) < 20 or mode == "BEKLE":
        return {
            "sampled_candles": len(closes), "fills": 0, "completed_cycles": 0,
            "gross_profit_usdt": 0.0, "fees_usdt": 0.0, "net_realized_usdt": 0.0,
            "unrealized_usdt": 0.0, "marked_result_usdt": 0.0, "net_return_pct": 0.0,
            "max_drawdown_pct": 0.0, "max_inventory_grids": 0, "open_grids": 0,
            "verdict": "PLAN BEKLİYOR",
            "note": "Simülasyon için işlem yapılabilir bir grid aralığı ve yeterli kapanış verisi gerekir.",
        }

    def level_index(price: float) -> int:
        below = sum(1 for level in levels if level <= price)
        return max(0, min(len(levels) - 1, below - 1))

    short_mode = mode == "SHORT GRID"
    inventory: list[float] = []
    fills = 0
    cycles = 0
    gross_profit = 0.0
    fees = 0.0
    max_inventory = 0
    peak_equity = 0.0
    max_drawdown_pct = 0.0
    previous_index = level_index(closes[0])

    for close in closes[1:]:
        current_index = level_index(close)
        if short_mode:
            if current_index > previous_index:
                for crossed in list(range(previous_index + 1, current_index + 1))[:3]:
                    if len(inventory) >= len(levels) - 1:
                        break
                    inventory.append(levels[crossed])
                    fills += 1
                    fees += per_grid * fee_side_pct / 100
            elif current_index < previous_index:
                for crossed in list(range(previous_index - 1, current_index - 1, -1))[:3]:
                    if not inventory:
                        break
                    exit_price = levels[crossed]
                    entry_price = inventory[-1]
                    if exit_price >= entry_price:
                        continue
                    inventory.pop()
                    fills += 1
                    cycles += 1
                    gross_profit += per_grid * ((entry_price - exit_price) / entry_price)
                    fees += per_grid * fee_side_pct / 100
        else:
            if current_index < previous_index:
                for crossed in list(range(previous_index - 1, current_index - 1, -1))[:3]:
                    if len(inventory) >= len(levels) - 1:
                        break
                    inventory.append(levels[crossed])
                    fills += 1
                    fees += per_grid * fee_side_pct / 100
            elif current_index > previous_index:
                for crossed in list(range(previous_index + 1, current_index + 1))[:3]:
                    if not inventory:
                        break
                    exit_price = levels[crossed]
                    entry_price = inventory[-1]
                    if exit_price <= entry_price:
                        continue
                    inventory.pop()
                    fills += 1
                    cycles += 1
                    gross_profit += per_grid * ((exit_price - entry_price) / entry_price)
                    fees += per_grid * fee_side_pct / 100
        previous_index = current_index
        max_inventory = max(max_inventory, len(inventory))
        if short_mode:
            unrealized = sum(per_grid * ((entry - close) / entry) for entry in inventory)
        else:
            unrealized = sum(per_grid * ((close - entry) / entry) for entry in inventory)
        equity = gross_profit - fees + unrealized
        peak_equity = max(peak_equity, equity)
        drawdown_pct = (equity - peak_equity) / capital * 100 if capital else 0.0
        max_drawdown_pct = min(max_drawdown_pct, drawdown_pct)

    final_price = closes[-1]
    if short_mode:
        unrealized = sum(per_grid * ((entry - final_price) / entry) for entry in inventory)
    else:
        unrealized = sum(per_grid * ((final_price - entry) / entry) for entry in inventory)
    net_realized = gross_profit - fees
    marked_result = net_realized + unrealized
    net_return_pct = marked_result / capital * 100 if capital else 0.0
    if cycles < 4:
        verdict = "YETERSİZ GRID TEMASI"
    elif marked_result > 0 and max_drawdown_pct >= -12:
        verdict = "PAPER SENARYOSU OLUMLU"
    elif max_drawdown_pct < -18:
        verdict = "ENVANTER RİSKİ YÜKSEK"
    else:
        verdict = "TEMKİNLİ / AYAR GEREKİYOR"
    return {
        "sampled_candles": len(closes), "fills": fills, "completed_cycles": cycles,
        "gross_profit_usdt": round(gross_profit, 2), "fees_usdt": round(fees, 2),
        "net_realized_usdt": round(net_realized, 2), "unrealized_usdt": round(unrealized, 2),
        "marked_result_usdt": round(marked_result, 2), "net_return_pct": round(net_return_pct, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2), "max_inventory_grids": max_inventory,
        "open_grids": len(inventory), "verdict": verdict,
        "note": "Kapanışların grid seviyelerini geçişi konservatif olarak sayılır; mum içi sıra, kayma ve gelecek performansı garanti edilmez. Açık gridler son fiyatla işaretlenir.",
    }


class GridPlanRequest(BaseModel):
    symbol: str = Field(min_length=3, max_length=24)
    interval: Literal["1m", "5m", "15m", "1h", "4h", "1d"] = "15m"
    capital: float = Field(default=1_000.0, ge=100, le=10_000)


@app.get("/api/grid/plan/{symbol}")
async def grid_plan_endpoint(
    symbol: str,
    interval: str = "15m",
    capital: float = Query(1_000.0, ge=100, le=10_000),
):
    return await smart_grid_plan(symbol, interval, capital)


@app.get("/api/grid/lab/{symbol}")
async def grid_lab_endpoint(
    symbol: str,
    interval: str = "15m",
    capital: float = Query(1_000.0, ge=100, le=10_000),
):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    cache_key = (safe_symbol, interval, int(round(capital)))
    cached = GRID_LAB_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < 90:
        return {**cached[1], "cached": True}
    plan = await smart_grid_plan(safe_symbol, interval, capital)
    candles = await fetch_candles(safe_symbol, interval, 500)
    simulation = simulate_grid_plan(candles[:-1], plan)
    payload = {
        "symbol": safe_symbol, "interval": interval, "plan_mode": plan["mode"],
        "plan_viability": plan["viability"], **simulation,
    }
    GRID_LAB_CACHE[cache_key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/grid/plans")
async def saved_grid_plans():
    plans = list(app.state.paper.get("grid_plans", []))
    return {
        "plans": plans[:GRID_PLAN_LIMIT], "active_count": sum(1 for item in plans if item.get("active")),
        "orders_enabled": False,
        "message": "Kaydedilen planlar yalnızca kalıcı Paper hafızasıdır; borsaya emir göndermez.",
    }


@app.post("/api/grid/plan/save")
async def save_grid_plan(request: GridPlanRequest):
    plan = await smart_grid_plan(request.symbol, request.interval, request.capital)
    stored_plan = {key: value for key, value in plan.items() if key != "cached"}
    stored_plan.update({
        "id": f"grid-{int(time.time() * 1000)}-{stored_plan['symbol']}",
        "saved_at": datetime.now(timezone.utc).isoformat(), "active": True,
        "status": "PAPER HAZIR" if stored_plan["paper_eligible"] else "İZLEME PLANI",
    })
    paper = app.state.paper
    async with paper["lock"]:
        plans = paper.setdefault("grid_plans", [])
        for item in plans:
            if item.get("symbol") == stored_plan["symbol"] and item.get("interval") == stored_plan["interval"]:
                item["active"] = False
        plans.insert(0, stored_plan)
        del plans[GRID_PLAN_LIMIT:]
        add_paper_notification(
            paper, "V5 GRID PLANI",
            f"{stored_plan['symbol']} {stored_plan['mode']} kalıcı Paper hafızasına kaydedildi; emir gönderimi kapalı.",
        )
    asyncio.create_task(persist_paper_snapshot(app))
    return {
        "message": "Grid planı Paper hafızasına kaydedildi. Gerçek/Testnet emirleri kapalı kalır.",
        "plan": stored_plan, "orders_enabled": False,
    }


@app.post("/api/grid/plan/clear/{plan_id}")
async def clear_grid_plan(plan_id: str):
    paper = app.state.paper
    changed = False
    async with paper["lock"]:
        for item in paper.setdefault("grid_plans", []):
            if item.get("id") == plan_id and item.get("active"):
                item["active"] = False
                changed = True
                break
    if changed:
        asyncio.create_task(persist_paper_snapshot(app))
    return {
        "message": "Paper grid planı arşivlendi." if changed else "Aktif plan bulunamadı.",
        "orders_enabled": False,
    }


def build_grid_variant(base_plan: dict, profile: str) -> dict:
    """V6 için üç farklı Paper risk profili üretir; emir yeteneği eklemez."""
    settings = {
        "TEMKİNLİ": {"range": 1.12, "grids": 0.74, "inventory": 0.45, "slippage": 0.80, "label": "Geniş kademe · düşük envanter"},
        "DENGELİ": {"range": 1.00, "grids": 1.00, "inventory": 0.62, "slippage": 1.00, "label": "Dengeli maliyet ve dolum"},
        "ATAK": {"range": 0.90, "grids": 1.28, "inventory": 0.76, "slippage": 1.25, "label": "Sık kademe · yüksek seçicilik"},
    }
    selected = settings.get(profile, settings["DENGELİ"])
    plan = json.loads(json.dumps(base_plan))
    midpoint = (float(base_plan["lower"]) + float(base_plan["upper"])) / 2
    half_range = (float(base_plan["upper"]) - float(base_plan["lower"])) / 2 * selected["range"]
    lower = max(0.00000001, midpoint - half_range)
    upper = midpoint + half_range
    grid_count = max(6, min(28, round(int(base_plan["grid_count"]) * selected["grids"])))
    step = (upper - lower) / grid_count
    reference = float(base_plan["entry_reference"])
    step_pct = step / reference * 100 if reference else 0.0
    decimals = grid_price_decimals(reference)
    capital = float(base_plan["capital"])
    capital_per_grid = capital / grid_count
    fee_side_pct = float(base_plan["fee_assumption"]["single_side_pct"])
    round_trip_pct = fee_side_pct * 2
    fee_multiple = step_pct / round_trip_pct if round_trip_pct else 0.0
    spread_bps = float(base_plan.get("spread_bps") or 0.0)
    slippage_pct = max(0.01, spread_bps / 200) * selected["slippage"]
    cost_adjusted_edge_pct = step_pct - round_trip_pct - slippage_pct * 2
    profile_ready = bool(base_plan.get("mode") != "BEKLE" and fee_multiple >= 1.35 and cost_adjusted_edge_pct > 0)
    plan.update({
        "profile": profile, "profile_label": selected["label"],
        "lower": round(lower, decimals), "upper": round(upper, decimals),
        "grid_count": grid_count, "grid_step": round(step, decimals),
        "grid_step_pct": round(step_pct, 3),
        "range_width_pct": round((upper - lower) / reference * 100 if reference else 0.0, 2),
        "levels": [round(lower + step * index, decimals) for index in range(grid_count + 1)],
        "capital_per_grid": round(capital_per_grid, 2),
        "inventory_ratio": selected["inventory"],
        "inventory_limit": max(2, round(grid_count * selected["inventory"])),
        "slippage_assumption_pct": round(slippage_pct, 4),
        "cost_adjusted_edge_pct": round(cost_adjusted_edge_pct, 3),
        "profile_ready": profile_ready, "orders_enabled": False,
        "fee_assumption": {
            **base_plan["fee_assumption"], "step_to_fee_multiple": round(fee_multiple, 2),
        },
        "estimated_per_cycle": {
            "gross_usdt": round(capital_per_grid * step_pct / 100, 4),
            "fee_usdt": round(capital_per_grid * round_trip_pct / 100, 4),
            "slippage_usdt": round(capital_per_grid * slippage_pct * 2 / 100, 4),
            "net_usdt": round(capital_per_grid * cost_adjusted_edge_pct / 100, 4),
            "net_edge_pct": round(cost_adjusted_edge_pct, 3),
        },
    })
    return plan


def grid_level_index(levels: list[float], price: float) -> int:
    below = sum(1 for level in levels if float(level) <= price)
    return max(0, min(len(levels) - 1, below - 1))


def new_live_grid_runtime(plan: dict, current_price: float) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    levels = [float(value) for value in plan["levels"]]
    return {
        "profile": plan["profile"], "profile_label": plan["profile_label"],
        "symbol": plan["symbol"], "interval": plan["interval"], "mode": plan["mode"],
        "capital": float(plan["capital"]), "capital_per_grid": float(plan["capital_per_grid"]),
        "lower": float(plan["lower"]), "upper": float(plan["upper"]),
        "levels": levels, "grid_count": int(plan["grid_count"]),
        "grid_step_pct": float(plan["grid_step_pct"]),
        "fee_side_pct": float(plan["fee_assumption"]["single_side_pct"]),
        "slippage_pct": float(plan["slippage_assumption_pct"]),
        "inventory_limit": int(plan["inventory_limit"]), "inventory": [],
        "last_price": current_price, "last_level_index": grid_level_index(levels, current_price),
        "fills": [], "fill_count": 0, "completed_cycles": 0,
        "gross_profit_usdt": 0.0, "fees_usdt": 0.0, "slippage_usdt": 0.0,
        "net_realized_usdt": 0.0, "unrealized_usdt": 0.0, "marked_result_usdt": 0.0,
        "net_return_pct": 0.0, "max_inventory_grids": 0, "max_drawdown_pct": 0.0,
        "peak_equity_usdt": 0.0, "score": 50, "status": "CANLI PAPER",
        "inventory_blocked_count": 0, "recenter_status": "ARALIK İÇİNDE",
        "started_at": now, "last_tick_at": now, "orders_enabled": False,
    }


def append_live_grid_fill(
    runtime: dict,
    kind: str,
    price: float,
    fee_usdt: float,
    slippage_usdt: float,
    observed_at: str,
    cycle_pnl_usdt: float | None = None,
) -> dict:
    runtime["fill_count"] += 1
    fill = {
        "id": f"{runtime['profile']}-{runtime['fill_count']}", "profile": runtime["profile"],
        "kind": kind, "price": round(price, grid_price_decimals(price)),
        "fee_usdt": round(fee_usdt, 4), "slippage_usdt": round(slippage_usdt, 4),
        "cycle_pnl_usdt": round(cycle_pnl_usdt, 4) if cycle_pnl_usdt is not None else None,
        "created_at": observed_at, "paper_only": True,
    }
    runtime["fills"].insert(0, fill)
    del runtime["fills"][GRID_ENGINE_FILL_LIMIT:]
    return fill


def update_live_grid_metrics(runtime: dict, price: float) -> None:
    per_grid = float(runtime["capital_per_grid"])
    short_mode = runtime["mode"] == "SHORT GRID"
    if short_mode:
        unrealized = sum(per_grid * ((float(item["entry_price"]) - price) / float(item["entry_price"])) for item in runtime["inventory"])
    else:
        unrealized = sum(per_grid * ((price - float(item["entry_price"])) / float(item["entry_price"])) for item in runtime["inventory"])
    net_realized = float(runtime["gross_profit_usdt"]) - float(runtime["fees_usdt"]) - float(runtime["slippage_usdt"])
    marked = net_realized + unrealized
    capital = max(float(runtime["capital"]), 0.00000001)
    runtime["net_realized_usdt"] = round(net_realized, 4)
    runtime["unrealized_usdt"] = round(unrealized, 4)
    runtime["marked_result_usdt"] = round(marked, 4)
    runtime["net_return_pct"] = round(marked / capital * 100, 3)
    runtime["peak_equity_usdt"] = max(float(runtime["peak_equity_usdt"]), marked)
    drawdown_pct = (marked - float(runtime["peak_equity_usdt"])) / capital * 100
    runtime["max_drawdown_pct"] = round(min(float(runtime["max_drawdown_pct"]), drawdown_pct), 3)
    cycles = int(runtime["completed_cycles"])
    evidence = min(24.0, cycles * 3.0)
    return_component = max(-20.0, min(22.0, float(runtime["net_return_pct"]) * 6.0))
    drawdown_component = max(0.0, 22.0 - abs(float(runtime["max_drawdown_pct"])) * 2.0)
    inventory_ratio = len(runtime["inventory"]) / max(1, int(runtime["inventory_limit"]))
    inventory_component = max(0.0, 12.0 - inventory_ratio * 12.0)
    runtime["score"] = round(max(0.0, min(99.0, 20.0 + evidence + return_component + drawdown_component + inventory_component)))


def process_live_grid_tick(runtime: dict, price: float, observed_at: str | None = None) -> list[dict]:
    """Tek fiyat gözleminde yalnızca sanal grid dolumlarını işler."""
    if price <= 0:
        return []
    moment = observed_at or datetime.now(timezone.utc).isoformat()
    levels = [float(value) for value in runtime.get("levels", [])]
    if len(levels) < 2:
        return []
    previous_index = int(runtime.get("last_level_index", grid_level_index(levels, price)))
    current_index = grid_level_index(levels, price)
    short_mode = runtime.get("mode") == "SHORT GRID"
    per_grid = float(runtime["capital_per_grid"])
    fee_each = per_grid * float(runtime["fee_side_pct"]) / 100
    slippage_each = per_grid * float(runtime["slippage_pct"]) / 100
    events: list[dict] = []

    def open_inventory(entry_price: float, kind: str) -> None:
        if len(runtime["inventory"]) >= int(runtime["inventory_limit"]):
            runtime["inventory_blocked_count"] += 1
            runtime["status"] = "ENVANTER KİLİDİ"
            events.append({
                "kind": "ENVANTER KİLİDİ", "profile": runtime["profile"],
                "message": f"{runtime['profile']} envanter sınırına ulaştı; yeni sanal giriş kesildi.",
                "price": price, "created_at": moment,
            })
            return
        runtime["inventory"].append({"entry_price": entry_price, "opened_at": moment})
        runtime["fees_usdt"] += fee_each
        runtime["slippage_usdt"] += slippage_each
        fill = append_live_grid_fill(runtime, kind, entry_price, fee_each, slippage_each, moment)
        events.append({
            "kind": kind, "profile": runtime["profile"],
            "message": f"{runtime['profile']} {kind.lower()} doldu: {fill['price']}",
            "price": fill["price"], "created_at": moment,
        })

    def close_inventory(exit_price: float, kind: str) -> None:
        if not runtime["inventory"]:
            return
        entry_price = float(runtime["inventory"][-1]["entry_price"])
        favorable = exit_price < entry_price if short_mode else exit_price > entry_price
        if not favorable:
            return
        runtime["inventory"].pop()
        gross = per_grid * ((entry_price - exit_price) / entry_price if short_mode else (exit_price - entry_price) / entry_price)
        runtime["gross_profit_usdt"] += gross
        runtime["fees_usdt"] += fee_each
        runtime["slippage_usdt"] += slippage_each
        cycle_net = gross - (fee_each + slippage_each) * 2
        runtime["completed_cycles"] += 1
        fill = append_live_grid_fill(runtime, kind, exit_price, fee_each, slippage_each, moment, cycle_net)
        events.append({
            "kind": "GRID TURU", "profile": runtime["profile"],
            "message": f"{runtime['profile']} sanal tur tamamladı: {cycle_net:+.3f} USDT net varsayım.",
            "price": fill["price"], "created_at": moment,
        })

    if short_mode:
        if current_index > previous_index:
            for crossed in list(range(previous_index + 1, current_index + 1))[:3]:
                open_inventory(levels[crossed], "SANAL SHORT")
        elif current_index < previous_index:
            for crossed in list(range(previous_index - 1, current_index - 1, -1))[:3]:
                close_inventory(levels[crossed], "SANAL COVER")
    else:
        if current_index < previous_index:
            for crossed in list(range(previous_index - 1, current_index - 1, -1))[:3]:
                open_inventory(levels[crossed], "SANAL ALIŞ")
        elif current_index > previous_index:
            for crossed in list(range(previous_index + 1, current_index + 1))[:3]:
                close_inventory(levels[crossed], "SANAL SATIŞ")

    runtime["last_level_index"] = current_index
    runtime["last_price"] = price
    runtime["last_tick_at"] = moment
    runtime["max_inventory_grids"] = max(int(runtime["max_inventory_grids"]), len(runtime["inventory"]))
    if runtime["status"] == "ENVANTER KİLİDİ" and len(runtime["inventory"]) < int(runtime["inventory_limit"]):
        runtime["status"] = "CANLI PAPER"
    update_live_grid_metrics(runtime, price)
    return events


def live_twin_decision(profiles: list[dict]) -> dict:
    summaries = []
    for runtime in profiles:
        summaries.append({
            "profile": runtime["profile"], "profile_label": runtime["profile_label"],
            "score": int(runtime["score"]), "status": runtime["status"],
            "grid_count": int(runtime["grid_count"]), "grid_step_pct": float(runtime["grid_step_pct"]),
            "fills": int(runtime["fill_count"]), "completed_cycles": int(runtime["completed_cycles"]),
            "marked_result_usdt": round(float(runtime["marked_result_usdt"]), 2),
            "net_return_pct": round(float(runtime["net_return_pct"]), 2),
            "max_drawdown_pct": round(float(runtime["max_drawdown_pct"]), 2),
            "open_grids": len(runtime["inventory"]), "inventory_limit": int(runtime["inventory_limit"]),
            "fees_usdt": round(float(runtime["fees_usdt"]), 2),
            "slippage_usdt": round(float(runtime["slippage_usdt"]), 2),
            "evidence_ready": int(runtime["completed_cycles"]) >= 4,
        })
    eligible = [item for item in summaries if item["evidence_ready"]]
    if not eligible:
        winner, status, promotion_ready = "DENGELİ", "VERİ TOPLUYOR", False
    else:
        ranked = sorted(eligible, key=lambda item: (item["score"], item["marked_result_usdt"]), reverse=True)
        best = ranked[0]
        gap = best["score"] - ranked[1]["score"] if len(ranked) > 1 else best["score"]
        if best["marked_result_usdt"] <= 0:
            winner, status, promotion_ready = best["profile"], "POZİTİF KANIT BEKLİYOR", False
        elif gap < 4:
            winner, status, promotion_ready = best["profile"], "PROFİLLER ÇOK YAKIN", False
        else:
            winner, status, promotion_ready = best["profile"], "PROFİL ÖNERİSİ HAZIR", True
    return {"recommended_profile": winner, "status": status, "promotion_ready": promotion_ready, "profiles": summaries}


def grid_runtime_summary(runtime: dict | None) -> dict | None:
    if not runtime:
        return None
    return {
        "profile": runtime["profile"], "profile_label": runtime["profile_label"],
        "symbol": runtime["symbol"], "interval": runtime["interval"], "mode": runtime["mode"],
        "capital": runtime["capital"], "lower": runtime["lower"], "upper": runtime["upper"],
        "last_price": runtime["last_price"], "grid_count": runtime["grid_count"],
        "grid_step_pct": runtime["grid_step_pct"], "fill_count": runtime["fill_count"],
        "completed_cycles": runtime["completed_cycles"], "fees_usdt": round(float(runtime["fees_usdt"]), 2),
        "slippage_usdt": round(float(runtime["slippage_usdt"]), 2),
        "net_realized_usdt": round(float(runtime["net_realized_usdt"]), 2),
        "unrealized_usdt": round(float(runtime["unrealized_usdt"]), 2),
        "marked_result_usdt": round(float(runtime["marked_result_usdt"]), 2),
        "net_return_pct": round(float(runtime["net_return_pct"]), 2),
        "max_drawdown_pct": round(float(runtime["max_drawdown_pct"]), 2),
        "open_grids": len(runtime["inventory"]), "inventory_limit": runtime["inventory_limit"],
        "inventory_used_pct": round(len(runtime["inventory"]) / max(1, int(runtime["inventory_limit"])) * 100),
        "max_inventory_grids": runtime["max_inventory_grids"], "score": runtime["score"],
        "status": runtime["status"], "recenter_status": runtime["recenter_status"],
        "fills": runtime["fills"][:14], "last_tick_at": runtime["last_tick_at"],
        "orders_enabled": False,
    }


def grid_engine_payload(engine: dict) -> dict:
    profiles = engine.get("profiles", [])
    active = next((item for item in profiles if item.get("profile") == engine.get("active_profile")), profiles[0] if profiles else None)
    decision = live_twin_decision(profiles) if profiles else {"recommended_profile": "DENGELİ", "status": "VERİ BEKLİYOR", "promotion_ready": False, "profiles": []}
    return {
        "enabled": bool(engine.get("enabled")), "status": engine.get("status", "DURDU"),
        "symbol": engine.get("symbol"), "interval": engine.get("interval", "15m"),
        "capital": engine.get("capital", 1_000.0), "active_profile": engine.get("active_profile", "DENGELİ"),
        "recommended_profile": decision["recommended_profile"],
        "recommendation_status": decision["status"], "promotion_ready": decision["promotion_ready"],
        "active_runtime": grid_runtime_summary(active), "profiles": decision["profiles"],
        "events": engine.get("events", [])[:GRID_ENGINE_EVENT_LIMIT],
        "last_tick_at": engine.get("last_tick_at"), "started_at": engine.get("started_at"),
        "stopped_at": engine.get("stopped_at"), "last_action": engine.get("last_action"),
        "recenter_count": int(engine.get("recenter_count", 0)),
        "orders_enabled": False,
        "safety_note": "V6 yalnızca yerel Paper dolumları üretir; Binance Testnet veya gerçek borsa emri göndermez.",
    }


def apply_grid_recenter(engine: dict, base_plan: dict, current_price: float) -> tuple[bool, str]:
    profiles = engine.get("profiles", [])
    if any(runtime.get("inventory") for runtime in profiles):
        for runtime in profiles:
            runtime["recenter_status"] = "ENVANTER BOŞALMASI BEKLENİYOR"
        return False, "Aralık değişti fakat açık sanal gridler varken yeniden merkezleme yapılmadı."
    variants = [build_grid_variant(base_plan, name) for name in ("TEMKİNLİ", "DENGELİ", "ATAK")]
    by_profile = {runtime["profile"]: runtime for runtime in profiles}
    for variant in variants:
        runtime = by_profile.get(variant["profile"])
        if runtime is None:
            continue
        levels = [float(value) for value in variant["levels"]]
        runtime.update({
            "mode": variant["mode"], "lower": float(variant["lower"]), "upper": float(variant["upper"]),
            "levels": levels, "grid_count": int(variant["grid_count"]),
            "grid_step_pct": float(variant["grid_step_pct"]),
            "capital_per_grid": float(variant["capital_per_grid"]),
            "inventory_limit": int(variant["inventory_limit"]),
            "slippage_pct": float(variant["slippage_assumption_pct"]),
            "last_level_index": grid_level_index(levels, current_price),
            "last_price": current_price, "recenter_status": "GÜVENLİ YENİDEN MERKEZLENDİ",
        })
    engine["recenter_count"] = int(engine.get("recenter_count", 0)) + 1
    return True, "Açık sanal envanter olmadığı için grid aralığı yeni rejime güvenle merkezlendi."


async def grid_engine_cycle() -> None:
    paper = app.state.paper
    engine = paper.get("grid_engine", empty_grid_engine_state())
    if not engine.get("enabled") or not engine.get("symbol"):
        return
    if emergency_brake_payload(paper)["active"]:
        async with paper["lock"]:
            engine["enabled"] = False
            engine["status"] = "ACİL FREN"
            engine["last_action"] = "Acil Fren V6 Otonom Paper Grid motorunu durdurdu."
        asyncio.create_task(persist_paper_snapshot(app))
        return
    symbol = str(engine["symbol"])
    current_price = await latest_price(symbol)
    now_epoch = time.time()
    now_iso = datetime.now(timezone.utc).isoformat()
    async with paper["lock"]:
        events: list[dict] = []
        for runtime in engine.get("profiles", []):
            events.extend(process_live_grid_tick(runtime, current_price, now_iso))
        if events:
            engine.setdefault("events", [])[0:0] = events
            del engine["events"][GRID_ENGINE_EVENT_LIMIT:]
            engine["last_action"] = events[0]["message"]
        decision = live_twin_decision(engine.get("profiles", []))
        engine["recommended_profile"] = decision["recommended_profile"]
        engine["recommendation_status"] = decision["status"]
        engine["last_tick_at"] = now_iso
        last_plan_check = float(engine.get("last_plan_check_epoch") or 0.0)
        should_check_plan = now_epoch - last_plan_check >= GRID_RECENTER_SECONDS
        if should_check_plan:
            engine["last_plan_check_epoch"] = now_epoch
        last_persist = float(engine.get("last_persist_epoch") or 0.0)
        should_persist = bool(events) and now_epoch - last_persist >= 15
        if should_persist:
            engine["last_persist_epoch"] = now_epoch
    if should_check_plan:
        try:
            fresh_plan = await smart_grid_plan(symbol, str(engine["interval"]), float(engine["capital"]))
            active = next((item for item in engine.get("profiles", []) if item.get("profile") == engine.get("active_profile")), None)
            range_broken = bool(active and (current_price < float(active["lower"]) or current_price > float(active["upper"])))
            mode_changed = bool(active and fresh_plan.get("mode") != active.get("mode") and fresh_plan.get("mode") != "BEKLE")
            if fresh_plan.get("mode") != "BEKLE" and (range_broken or mode_changed):
                async with paper["lock"]:
                    changed, message = apply_grid_recenter(engine, fresh_plan, current_price)
                    engine["last_action"] = message
                    engine.setdefault("events", []).insert(0, {
                        "kind": "YENİDEN MERKEZLEME" if changed else "MERKEZLEME BEKLİYOR",
                        "profile": "SİSTEM", "message": message, "price": current_price, "created_at": now_iso,
                    })
                    del engine["events"][GRID_ENGINE_EVENT_LIMIT:]
                should_persist = True
        except Exception as exc:
            async with paper["lock"]:
                engine["last_action"] = f"Grid aralık kontrolü yeniden denenecek: {str(exc)[:70]}"
    if should_persist:
        asyncio.create_task(persist_paper_snapshot(app))


async def grid_engine_loop(application: FastAPI) -> None:
    while True:
        try:
            if application.state.paper.get("grid_engine", {}).get("enabled"):
                await grid_engine_cycle()
        except Exception as exc:
            engine = application.state.paper.get("grid_engine", {})
            engine["last_action"] = f"Canlı Paper Grid geçici hata; yeniden denenecek: {str(exc)[:70]}"
        await asyncio.sleep(GRID_ENGINE_TICK_SECONDS)


class GridEngineStartRequest(BaseModel):
    symbol: str = Field(min_length=3, max_length=24)
    interval: Literal["1m", "5m", "15m", "1h", "4h", "1d"] = "15m"
    capital: float = Field(default=1_000.0, ge=100, le=5_000)


class GridProfileRequest(BaseModel):
    profile: Literal["TEMKİNLİ", "DENGELİ", "ATAK"]


@app.get("/api/grid/engine")
async def grid_engine_status():
    return grid_engine_payload(app.state.paper.get("grid_engine", empty_grid_engine_state()))


@app.post("/api/grid/engine/start")
async def start_grid_engine(request: GridEngineStartRequest):
    paper = app.state.paper
    if emergency_brake_payload(paper)["active"]:
        raise HTTPException(409, "Acil Fren aktifken V6 Paper Grid başlatılamaz")
    current_engine = paper.get("grid_engine", empty_grid_engine_state())
    if current_engine.get("enabled"):
        raise HTTPException(409, "V6 Paper Grid zaten çalışıyor; önce mevcut motoru durdurun")
    plan, current_price = await asyncio.gather(
        smart_grid_plan(request.symbol, request.interval, request.capital),
        latest_price(request.symbol),
    )
    if plan.get("mode") == "BEKLE":
        raise HTTPException(409, "Net grid rejimi oluşmadı; V6 motoru güvenli biçimde bekliyor")
    variants = [build_grid_variant(plan, name) for name in ("TEMKİNLİ", "DENGELİ", "ATAK")]
    runtimes = [new_live_grid_runtime(variant, current_price) for variant in variants]
    now = datetime.now(timezone.utc).isoformat()
    safety_mode = "CANLI PAPER" if plan.get("paper_eligible") else "KORUMALI PAPER DENEYİ"
    engine = {
        **empty_grid_engine_state(), "enabled": True, "status": safety_mode,
        "symbol": plan["symbol"], "interval": request.interval, "capital": request.capital,
        "active_profile": "DENGELİ", "recommended_profile": "DENGELİ",
        "profiles": runtimes, "started_at": now, "last_tick_at": now,
        "last_plan_check_epoch": time.time(), "last_persist_epoch": 0.0,
        "last_action": f"{plan['symbol']} için üç Dijital İkiz canlı Paper yarışına başladı.",
        "events": [{
            "kind": "V6 BAŞLADI", "profile": "SİSTEM",
            "message": f"{plan['symbol']} · {plan['mode']} · üç Paper profil başlatıldı; borsa emri kapalı.",
            "price": current_price, "created_at": now,
        }],
    }
    async with paper["lock"]:
        paper["grid_engine"] = engine
        add_paper_notification(paper, "V6 PAPER GRID", engine["last_action"])
    asyncio.create_task(persist_paper_snapshot(app))
    return grid_engine_payload(engine)


@app.post("/api/grid/engine/stop")
async def stop_grid_engine():
    paper = app.state.paper
    async with paper["lock"]:
        engine = paper.get("grid_engine", empty_grid_engine_state())
        engine["enabled"] = False
        engine["status"] = "DURDU"
        engine["stopped_at"] = datetime.now(timezone.utc).isoformat()
        engine["last_action"] = "V6 Otonom Paper Grid kullanıcı tarafından durduruldu; açık kayıtlar hafızada korundu."
        add_paper_notification(paper, "V6 PAPER GRID", engine["last_action"])
    asyncio.create_task(persist_paper_snapshot(app))
    return grid_engine_payload(engine)


@app.post("/api/grid/engine/profile")
async def select_grid_profile(request: GridProfileRequest):
    paper = app.state.paper
    async with paper["lock"]:
        engine = paper.get("grid_engine", empty_grid_engine_state())
        if not any(item.get("profile") == request.profile for item in engine.get("profiles", [])):
            raise HTTPException(409, "Seçilecek Dijital İkiz profili henüz hazır değil")
        engine["active_profile"] = request.profile
        engine["last_action"] = f"Görüntülenen aktif Paper profil {request.profile} olarak değiştirildi."
        engine.setdefault("events", []).insert(0, {
            "kind": "PROFİL SEÇİLDİ", "profile": request.profile,
            "message": engine["last_action"], "price": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        del engine["events"][GRID_ENGINE_EVENT_LIMIT:]
    asyncio.create_task(persist_paper_snapshot(app))
    return grid_engine_payload(engine)


@app.post("/api/grid/engine/recenter")
async def recenter_grid_engine():
    paper = app.state.paper
    engine = paper.get("grid_engine", empty_grid_engine_state())
    if not engine.get("profiles") or not engine.get("symbol"):
        raise HTTPException(409, "Yeniden merkezlenecek bir V6 Paper Grid yok")
    plan, price = await asyncio.gather(
        smart_grid_plan(str(engine["symbol"]), str(engine["interval"]), float(engine["capital"])),
        latest_price(str(engine["symbol"])),
    )
    if plan.get("mode") == "BEKLE":
        raise HTTPException(409, "Yeni rejim grid merkezlemesi için uygun değil")
    async with paper["lock"]:
        changed, message = apply_grid_recenter(engine, plan, price)
        engine["last_action"] = message
    if not changed:
        raise HTTPException(409, message)
    asyncio.create_task(persist_paper_snapshot(app))
    return grid_engine_payload(engine)


def digital_twin_lab(base_plan: dict, candles: list[dict]) -> dict:
    """Üç grid profilini aynı geçmiş kapanışlarda, maliyet baskısıyla karşılaştırır."""
    results = []
    for profile in ("TEMKİNLİ", "DENGELİ", "ATAK"):
        variant = build_grid_variant(base_plan, profile)
        simulation = simulate_grid_plan(candles, variant)
        extra_slippage = (
            float(simulation["fills"]) * float(variant["capital_per_grid"])
            * float(variant["slippage_assumption_pct"]) / 100
        )
        adjusted_marked = float(simulation["marked_result_usdt"]) - extra_slippage
        adjusted_return = adjusted_marked / max(float(variant["capital"]), 0.00000001) * 100
        cycles = int(simulation["completed_cycles"])
        evidence_ready = cycles >= 4 and int(simulation["fills"]) >= 8
        evidence_score = min(24.0, cycles * 2.5)
        return_score = max(-22.0, min(22.0, adjusted_return * 6.0))
        drawdown_score = max(0.0, 22.0 - abs(float(simulation["max_drawdown_pct"])) * 2.0)
        edge_score = max(0.0, min(14.0, float(variant["cost_adjusted_edge_pct"]) * 18.0))
        score = round(max(0.0, min(99.0, 18.0 + evidence_score + return_score + drawdown_score + edge_score)))
        results.append({
            "profile": profile, "profile_label": variant["profile_label"],
            "score": score, "evidence_ready": evidence_ready,
            "grid_count": variant["grid_count"], "grid_step_pct": variant["grid_step_pct"],
            "cost_adjusted_edge_pct": variant["cost_adjusted_edge_pct"],
            "inventory_limit": variant["inventory_limit"],
            "fills": simulation["fills"], "completed_cycles": cycles,
            "fees_usdt": simulation["fees_usdt"], "slippage_usdt": round(extra_slippage, 2),
            "marked_result_usdt": round(adjusted_marked, 2), "net_return_pct": round(adjusted_return, 2),
            "max_drawdown_pct": simulation["max_drawdown_pct"], "open_grids": simulation["open_grids"],
            "verdict": simulation["verdict"],
        })
    evidence = [item for item in results if item["evidence_ready"]]
    ranked = sorted(evidence or results, key=lambda item: (item["score"], item["marked_result_usdt"]), reverse=True)
    winner = ranked[0]
    second_score = ranked[1]["score"] if len(ranked) > 1 else 0
    score_gap = winner["score"] - second_score
    promotion_ready = bool(
        winner["evidence_ready"] and winner["marked_result_usdt"] > 0
        and winner["score"] >= 62 and score_gap >= 4
    )
    if not any(item["evidence_ready"] for item in results):
        status = "VERİ TOPLUYOR"
        reason = "Hiçbir profil en az dört tamamlanmış grid turuna ulaşmadı; ayar değiştirme önerilmiyor."
    elif winner["marked_result_usdt"] <= 0:
        status = "POZİTİF KANIT YOK"
        reason = "En yüksek puanlı profil dahi maliyet sonrası pozitif kalmadı; mevcut ayarlar yalnızca izlenmeli."
    elif score_gap < 4:
        status = "FARK YETERSİZ"
        reason = "İlk iki profil arasındaki puan farkı küçük; rastgele profil değişimi yapılmıyor."
    else:
        status = "ÖNERİ HAZIR"
        reason = f"{winner['profile']} profil, aynı kapanış örneklerinde maliyet ve düşüş sonrası daha dengeli sonuç verdi."
    return {
        "winner": winner["profile"], "winner_score": winner["score"],
        "score_gap": score_gap, "promotion_ready": promotion_ready,
        "status": status, "reason": reason, "profiles": results,
        "orders_enabled": False,
        "note": "Dijital İkiz sonucu geçmiş kapanış senaryosudur; gelecek performansını veya kârı garanti etmez.",
    }


@app.get("/api/grid/twins/{symbol}")
async def digital_twin_lab_endpoint(
    symbol: str,
    interval: str = "15m",
    capital: float = Query(1_000.0, ge=100, le=5_000),
):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    cache_key = (safe_symbol, interval, int(round(capital)))
    cached = GRID_TWIN_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < 90:
        return {**cached[1], "cached": True}
    plan = await smart_grid_plan(safe_symbol, interval, capital)
    candles = await fetch_candles(safe_symbol, interval, 500)
    payload = {
        "symbol": safe_symbol, "interval": interval,
        **digital_twin_lab(plan, candles[:-1]),
    }
    GRID_TWIN_CACHE[cache_key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


async def orderbook_intelligence(symbol: str) -> dict:
    """Duvar büyüklüğünü tek kare yerine kısa süreli kalıcılıkla değerlendirir."""
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    cached = ORDERBOOK_INTELLIGENCE_CACHE.get(safe_symbol)
    if cached and time.monotonic() - cached[0] < 4:
        return {**cached[1], "cached": True}
    try:
        response = await app.state.http.get(
            f"{BINANCE_API}/api/v3/depth", params={"symbol": safe_symbol, "limit": 100},
        )
        response.raise_for_status()
        raw = response.json()
        bids = [(float(price), float(quantity)) for price, quantity in raw.get("bids", [])[:40]]
        asks = [(float(price), float(quantity)) for price, quantity in raw.get("asks", [])[:40]]
        if not bids or not asks:
            raise ValueError("Emir defteri boş")
        best_bid, best_ask = bids[0][0], asks[0][0]
        midpoint = (best_bid + best_ask) / 2
        bid_levels = [{"side": "ALIŞ", "price": price, "notional": price * quantity} for price, quantity in bids]
        ask_levels = [{"side": "SATIŞ", "price": price, "notional": price * quantity} for price, quantity in asks]
        bid_total = sum(item["notional"] for item in bid_levels)
        ask_total = sum(item["notional"] for item in ask_levels)
        total_depth = bid_total + ask_total
        pressure_pct = (bid_total - ask_total) / total_depth * 100 if total_depth else 0.0
        average_bid = bid_total / len(bid_levels)
        average_ask = ask_total / len(ask_levels)
        bid_wall = max(bid_levels, key=lambda item: item["notional"])
        ask_wall = max(ask_levels, key=lambda item: item["notional"])
        bid_strength = bid_wall["notional"] / max(average_bid, 0.00000001)
        ask_strength = ask_wall["notional"] / max(average_ask, 0.00000001)
        max_notional = max(item["notional"] for item in bid_levels + ask_levels)

        history = list(ORDERBOOK_WALL_HISTORY.get(safe_symbol, []))
        now_epoch = time.time()
        history.append({
            "at": now_epoch, "bid_wall_price": bid_wall["price"], "ask_wall_price": ask_wall["price"],
            "bid_strength": bid_strength, "ask_strength": ask_strength,
        })
        history = [item for item in history if now_epoch - float(item["at"]) <= 90][-18:]
        ORDERBOOK_WALL_HISTORY[safe_symbol] = history

        def persistence(side: str, wall_price: float) -> int:
            key = f"{side}_wall_price"
            tolerance_pct = 0.08
            return sum(
                1 for item in history
                if wall_price > 0 and abs(float(item[key]) / wall_price - 1) * 100 <= tolerance_pct
            )

        bid_persistence = persistence("bid", bid_wall["price"])
        ask_persistence = persistence("ask", ask_wall["price"])
        required_samples = 3
        dominant_side = "ALIŞ" if bid_strength >= ask_strength else "SATIŞ"
        dominant_strength = max(bid_strength, ask_strength)
        dominant_persistence = bid_persistence if dominant_side == "ALIŞ" else ask_persistence
        if dominant_strength >= 2.4 and dominant_persistence >= required_samples:
            mode = f"KALICI {dominant_side} DUVARI"
            spoof_risk = max(5, round(35 - dominant_persistence * 5))
            reason = f"{dominant_side} duvarı {dominant_persistence} ayrı gözlemde benzer bölgede kaldı; anlık sahte duvar olasılığı azaldı."
        elif dominant_strength >= 2.4:
            mode = "YENİ DUVAR / SPOOF İHTİMALİ"
            spoof_risk = min(92, round(45 + dominant_strength * 10 - dominant_persistence * 8))
            reason = f"Büyük {dominant_side.lower()} emri henüz yalnızca {dominant_persistence}/{required_samples} gözlemde görüldü; kalıcılık doğrulanmadan güvenilmiyor."
        else:
            mode = "NORMAL DERİNLİK"
            spoof_risk = max(8, round(28 - len(history)))
            reason = "İlk 40 kademede ortalamanın çok üzerinde ve kalıcı bir emir duvarı görünmüyor."

        strongest_levels = sorted(bid_levels, key=lambda item: item["notional"], reverse=True)[:6]
        strongest_levels += sorted(ask_levels, key=lambda item: item["notional"], reverse=True)[:6]
        heatmap = sorted([
            {
                "side": item["side"], "price": round(item["price"], grid_price_decimals(midpoint)),
                "notional_usdt": round(item["notional"], 2),
                "distance_pct": round((item["price"] / midpoint - 1) * 100, 3),
                "heat": round(item["notional"] / max_notional * 100),
            }
            for item in strongest_levels
        ], key=lambda item: (item["side"], abs(item["distance_pct"])))
        payload = {
            "symbol": safe_symbol, "mode": mode, "mid_price": midpoint,
            "spread_bps": round((best_ask - best_bid) / midpoint * 10_000, 2),
            "pressure_pct": round(pressure_pct, 1), "dominant_side": dominant_side,
            "spoof_risk_score": spoof_risk, "history_samples": len(history),
            "required_persistence_samples": required_samples,
            "bid_wall": {
                "price": round(bid_wall["price"], grid_price_decimals(midpoint)),
                "notional_usdt": round(bid_wall["notional"], 2),
                "strength": round(bid_strength, 2), "persistence": bid_persistence,
            },
            "ask_wall": {
                "price": round(ask_wall["price"], grid_price_decimals(midpoint)),
                "notional_usdt": round(ask_wall["notional"], 2),
                "strength": round(ask_strength, 2), "persistence": ask_persistence,
            },
            "heatmap": heatmap, "reason": reason, "orders_enabled": False,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
        payload = {
            "symbol": safe_symbol, "mode": "EMİR DEFTERİ BEKLENİYOR", "mid_price": 0.0,
            "spread_bps": 0.0, "pressure_pct": 0.0, "dominant_side": "NÖTR",
            "spoof_risk_score": 0, "history_samples": 0, "required_persistence_samples": 3,
            "bid_wall": None, "ask_wall": None, "heatmap": [],
            "reason": f"Kalıcı duvar analizi yeniden denenecek: {str(exc)[:80]}",
            "orders_enabled": False, "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    ORDERBOOK_INTELLIGENCE_CACHE[safe_symbol] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/orderbook/intelligence/{symbol}")
async def orderbook_intelligence_endpoint(symbol: str):
    return await orderbook_intelligence(symbol)


def ema_path(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2 / (period + 1)
    result = [float(values[0])]
    for value in values[1:]:
        result.append(float(value) * alpha + result[-1] * (1 - alpha))
    return result


def rolling_atr(candles: list[dict], index: int, period: int = 14) -> float:
    start = max(1, index - period + 1)
    ranges = []
    for cursor in range(start, index + 1):
        candle, previous = candles[cursor], candles[cursor - 1]
        ranges.append(max(
            float(candle["high"]) - float(candle["low"]),
            abs(float(candle["high"]) - float(previous["close"])),
            abs(float(candle["low"]) - float(previous["close"])),
        ))
    return sum(ranges) / max(1, len(ranges))


def simulate_v7_strategy(
    candles: list[dict],
    strategy: str,
    capital: float,
    cost_multiplier: float = 1.0,
    signal_delay: int = 0,
) -> dict:
    """Üç stratejiyi aynı kapanışlarda ve yalnızca sanal maliyetlerle oynatır."""
    safe_strategy = strategy if strategy in V7_STRATEGIES else "GRID"
    rows = candles[-760:]
    if len(rows) < 75:
        return {
            "strategy": safe_strategy, "trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0, "gross_result_usdt": 0.0, "costs_usdt": 0.0,
            "net_result_usdt": 0.0, "net_return_pct": 0.0, "max_drawdown_pct": 0.0,
            "profit_factor": None, "score": 0, "status": "VERİ YETERSİZ",
            "evidence_ready": False, "orders_enabled": False,
        }
    closes = [float(item["close"]) for item in rows]
    volumes = [float(item.get("volume") or 0.0) for item in rows]
    ema20, ema50 = ema_path(closes, 20), ema_path(closes, 50)
    fee_side_pct, slippage_side_pct = 0.10 * cost_multiplier, 0.025 * cost_multiplier
    position: dict | None = None
    results: list[float] = []
    gross_total = 0.0
    costs_total = 0.0
    equity = float(capital)
    peak_equity = equity
    max_drawdown_pct = 0.0
    cooldown_until = 0

    def close_position(exit_price: float) -> None:
        nonlocal position, gross_total, costs_total, equity, peak_equity, max_drawdown_pct
        if position is None:
            return
        entry = float(position["entry"])
        directional_return = (exit_price / entry - 1) * (1 if position["direction"] == "LONG" else -1)
        gross = float(position["notional"]) * directional_return
        cost = float(position["notional"]) * ((fee_side_pct + slippage_side_pct) * 2) / 100
        net = gross - cost
        gross_total += gross
        costs_total += cost
        equity += net
        results.append(net)
        peak_equity = max(peak_equity, equity)
        drawdown = (equity - peak_equity) / max(float(capital), 0.00000001) * 100
        max_drawdown_pct = min(max_drawdown_pct, drawdown)
        position = None

    for index in range(55, len(rows) - 1):
        candle = rows[index]
        close = closes[index]
        atr = max(rolling_atr(rows, index), close * 0.0001)
        mean20 = sum(closes[index - 19:index + 1]) / 20
        if position is not None:
            position["bars"] += 1
            stop, target = float(position["stop"]), float(position["target"])
            if position["direction"] == "LONG":
                if float(candle["low"]) <= stop:
                    close_position(stop)
                elif float(candle["high"]) >= target:
                    close_position(target)
                elif safe_strategy == "GRID" and close >= mean20:
                    close_position(close)
                elif safe_strategy == "TREND" and ema20[index] < ema50[index]:
                    close_position(close)
                elif safe_strategy == "KIRILIM" and position and position["bars"] >= 12:
                    close_position(close)
            else:
                if float(candle["high"]) >= stop:
                    close_position(stop)
                elif float(candle["low"]) <= target:
                    close_position(target)
                elif safe_strategy == "GRID" and close <= mean20:
                    close_position(close)
                elif safe_strategy == "TREND" and ema20[index] > ema50[index]:
                    close_position(close)
                elif safe_strategy == "KIRILIM" and position and position["bars"] >= 12:
                    close_position(close)
            if position is None:
                cooldown_until = index + 2
        if position is not None or index < cooldown_until:
            continue
        signal_index = index - signal_delay
        if signal_index < 55:
            continue
        signal_close = closes[signal_index]
        signal_atr = max(rolling_atr(rows, signal_index), signal_close * 0.0001)
        signal_mean = sum(closes[signal_index - 19:signal_index + 1]) / 20
        direction = "BEKLE"
        stop_distance, target_distance = signal_atr * 1.2, signal_atr * 1.8
        if safe_strategy == "GRID":
            if signal_close <= signal_mean - signal_atr * 0.72:
                direction = "LONG"
            elif signal_close >= signal_mean + signal_atr * 0.72:
                direction = "SHORT"
            stop_distance, target_distance = signal_atr * 1.45, signal_atr * 0.85
        elif safe_strategy == "TREND":
            spread = abs(ema20[signal_index] / max(ema50[signal_index], 0.00000001) - 1) * 100
            if ema20[signal_index] > ema50[signal_index] and ema20[signal_index] > ema20[signal_index - 3] and spread >= 0.08:
                direction = "LONG"
            elif ema20[signal_index] < ema50[signal_index] and ema20[signal_index] < ema20[signal_index - 3] and spread >= 0.08:
                direction = "SHORT"
            stop_distance, target_distance = signal_atr * 1.25, signal_atr * 2.15
        else:
            previous_high = max(float(item["high"]) for item in rows[signal_index - 20:signal_index])
            previous_low = min(float(item["low"]) for item in rows[signal_index - 20:signal_index])
            average_volume = sum(volumes[signal_index - 20:signal_index]) / 20
            volume_confirmed = volumes[signal_index] >= average_volume * 1.05
            if signal_close > previous_high and volume_confirmed:
                direction = "LONG"
            elif signal_close < previous_low and volume_confirmed:
                direction = "SHORT"
            stop_distance, target_distance = signal_atr * 1.05, signal_atr * 2.35
        if direction == "BEKLE":
            continue
        entry = float(rows[index + 1]["open"])
        notional = min(max(50.0, float(capital) * 0.18), max(50.0, equity * 0.22))
        position = {
            "direction": direction, "entry": entry, "notional": notional, "bars": 0,
            "stop": entry - stop_distance if direction == "LONG" else entry + stop_distance,
            "target": entry + target_distance if direction == "LONG" else entry - target_distance,
        }
    if position is not None:
        close_position(closes[-1])
    wins = sum(1 for value in results if value > 0)
    losses = len(results) - wins
    gross_wins = sum(value for value in results if value > 0)
    gross_losses = abs(sum(value for value in results if value <= 0))
    net_result = sum(results)
    net_return_pct = net_result / max(float(capital), 0.00000001) * 100
    win_rate = wins / len(results) * 100 if results else 0.0
    profit_factor = gross_wins / gross_losses if gross_losses else (99.0 if gross_wins > 0 else None)
    evidence_ready = len(results) >= 6
    if evidence_ready and (net_return_pct <= -0.75 or (profit_factor is not None and profit_factor < 0.72)):
        status = "KARANTİNA ADAYI"
    elif evidence_ready and net_return_pct > 0 and (profit_factor or 0) >= 1.05 and max_drawdown_pct >= -7:
        status = "KANITLI PAPER"
    else:
        status = "VERİ TOPLUYOR"
    profit_factor_score = min(16.0, max(0.0, ((profit_factor or 0.0) - 0.7) * 18))
    score = round(max(0.0, min(99.0,
        34 + min(16.0, len(results) * 1.4) + max(-20.0, min(22.0, net_return_pct * 6))
        + profit_factor_score - abs(max_drawdown_pct) * 1.4
    )))
    return {
        "strategy": safe_strategy, "trades": len(results), "wins": wins, "losses": losses,
        "win_rate": round(win_rate, 1), "gross_result_usdt": round(gross_total, 2),
        "costs_usdt": round(costs_total, 2), "net_result_usdt": round(net_result, 2),
        "net_return_pct": round(net_return_pct, 2), "max_drawdown_pct": round(max_drawdown_pct, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor is not None else None,
        "score": score, "status": status, "evidence_ready": evidence_ready,
        "cost_assumption": f"Tek yön ücret %{fee_side_pct:.3f} + kayma %{slippage_side_pct:.3f}",
        "orders_enabled": False,
    }


def v7_market_replay(candles: list[dict], horizon: str, capital: float) -> dict:
    horizon_candles = 96 if horizon == "24h" else 672
    sample = candles[-min(len(candles), horizon_candles + 60):]
    profiles = []
    for strategy in V7_STRATEGIES:
        baseline = simulate_v7_strategy(sample, strategy, capital)
        stress_cases = [
            {"label": "2X MALİYET", **simulate_v7_strategy(sample, strategy, capital, cost_multiplier=2.0)},
            {"label": "1 MUM GECİKME", **simulate_v7_strategy(sample, strategy, capital, signal_delay=1)},
            {"label": "ŞOK MALİYET + GECİKME", **simulate_v7_strategy(sample, strategy, capital, cost_multiplier=3.0, signal_delay=1)},
        ]
        survived = sum(1 for item in stress_cases if item["net_return_pct"] > -2.5 and item["max_drawdown_pct"] >= -9)
        certified = bool(
            baseline["evidence_ready"] and baseline["net_result_usdt"] > 0
            and baseline["max_drawdown_pct"] >= -7 and survived >= 2
        )
        profiles.append({
            **baseline, "stress_survived": survived, "stress_total": len(stress_cases),
            "certified": certified, "certification": "STRES ONAYLI" if certified else "KANIT BEKLİYOR",
            "ranking_score": min(99, baseline["score"] + survived * 3),
            "stress_cases": [{
                "label": item["label"], "net_return_pct": item["net_return_pct"],
                "max_drawdown_pct": item["max_drawdown_pct"], "trades": item["trades"],
                "status": "DAYANDI" if item["net_return_pct"] > -2.5 and item["max_drawdown_pct"] >= -9 else "ZAYIF",
            } for item in stress_cases],
        })
    ranked = sorted(profiles, key=lambda item: (item["certified"], item["ranking_score"], item["net_result_usdt"]), reverse=True)
    winner = ranked[0]
    score_gap = winner["ranking_score"] - ranked[1]["ranking_score"] if len(ranked) > 1 else winner["ranking_score"]
    promotion_ready = bool(winner["certified"] and score_gap >= 4)
    return {
        "horizon": horizon, "sampled_candles": max(0, len(sample) - 60),
        "winner": winner["strategy"] if promotion_ready else "BEKLE",
        "leading_strategy": winner["strategy"], "score_gap": score_gap,
        "promotion_ready": promotion_ready,
        "status": "STRATEJİ ÖNERİSİ HAZIR" if promotion_ready else "KANIT TOPLANIYOR",
        "profiles": profiles, "orders_enabled": False,
        "note": "V7 tekrar motoru kapanmış mum, ücret, kayma ve gecikme varsayımlarıyla Paper kanıt üretir; gelecek getiriyi garanti etmez.",
    }


def v7_strategy_decision(symbol: str, candles: list[dict], replay: dict, quarantined: list[str]) -> dict:
    closed = candles[:-1] if len(candles) > 1 else candles
    result = analyze(closed[-260:])
    close = float(closed[-1]["close"])
    atr_pct = float(result["atr"]) / max(close, 0.00000001) * 100
    recent_high = max(float(item["high"]) for item in closed[-21:-1])
    recent_low = min(float(item["low"]) for item in closed[-21:-1])
    average_volume = sum(float(item.get("volume") or 0) for item in closed[-21:-1]) / 20
    volume_ratio = float(closed[-1].get("volume") or 0) / max(average_volume, 0.00000001)
    breakout_direction = "LONG" if close > recent_high else "SHORT" if close < recent_low else "BEKLE"
    profile_map = {item["strategy"]: item for item in replay["profiles"]}
    selected, direction, regime = "BEKLE", "BEKLE", "KORUMALI BEKLE"
    reason = "Rejim ve strateji kanıtı birlikte doğrulanmadı."
    if atr_pct >= 1.6 or int(result["radar"]["trap_score"]) >= 55:
        reason = "Volatilite veya Tuzak Radarı riski yüksek; bütün stratejiler Paper beklemeye alındı."
    elif breakout_direction != "BEKLE" and volume_ratio >= 1.05:
        selected, direction, regime = "KIRILIM", breakout_direction, "HACİMLİ KIRILIM"
        reason = f"20 mum sınırı {breakout_direction} yönünde ve hacim {volume_ratio:.2f}x ile doğrulandı."
    elif result["direction"] in {"LONG", "SHORT"} and float(result["adx"]) >= 24:
        selected, direction, regime = "TREND", result["direction"], "GÜÇLÜ TREND"
        reason = f"EMA yapısı ve ADX {float(result['adx']):.1f} aynı yönlü trend rejimini destekliyor."
    elif float(result["adx"]) < 22 and atr_pct <= 1.1:
        selected, direction, regime = "GRID", "NÖTR", "YATAY / GRID"
        reason = "Trend gücü sınırlı ve oynaklık kontrollü; aralık stratejisi Paper gözlem için öne çıktı."
    selected_profile = profile_map.get(selected)
    if selected in quarantined or (selected_profile and selected_profile["status"] == "KARANTİNA ADAYI"):
        reason = f"{selected} rejime uygun görünse de Paper sonucu zayıf; strateji karantinada."
        selected, direction = "BEKLE", "BEKLE"
    profile_score = int(selected_profile["ranking_score"]) if selected_profile else 0
    confidence = round(min(96, max(0, float(result["confidence"]) * 0.55 + profile_score * 0.45))) if selected != "BEKLE" else 0
    risk_score = round(min(100, int(result["radar"]["trap_score"]) + atr_pct * 22 + max(0, 24 - float(result["adx"]))))
    allocation_ready = bool(selected != "BEKLE" and risk_score < 62 and confidence >= 52)
    returns = candle_returns(closed, 60) if "candle_returns" in globals() else [
        float(closed[index]["close"]) / float(closed[index - 1]["close"]) - 1
        for index in range(max(1, len(closed) - 60), len(closed))
    ]
    return {
        "symbol": symbol, "strategy": selected, "direction": direction, "regime": regime,
        "confidence": confidence, "risk_score": risk_score, "allocation_ready": allocation_ready,
        "price": close, "adx": round(float(result["adx"]), 1), "atr_pct": round(atr_pct, 2),
        "trap_score": int(result["radar"]["trap_score"]), "volume_ratio": round(volume_ratio, 2),
        "reason": reason, "replay_profiles": [{
            key: profile[key] for key in (
                "strategy", "trades", "win_rate", "net_result_usdt", "net_return_pct",
                "max_drawdown_pct", "profit_factor", "score", "status", "certified",
                "certification", "stress_survived", "stress_total", "ranking_score",
            )
        } for profile in replay["profiles"]],
        "return_fingerprint": returns[-60:], "orders_enabled": False,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def v7_strategy_council(symbol_rows: list[dict]) -> tuple[list[dict], list[str]]:
    council = []
    quarantined = []
    for strategy in V7_STRATEGIES:
        profiles = [
            profile for row in symbol_rows for profile in row.get("replay_profiles", [])
            if profile.get("strategy") == strategy
        ]
        trades = sum(int(item["trades"]) for item in profiles)
        certified = sum(1 for item in profiles if item.get("certified"))
        average_return = sum(float(item["net_return_pct"]) for item in profiles) / len(profiles) if profiles else 0.0
        average_score = round(sum(float(item["ranking_score"]) for item in profiles) / len(profiles)) if profiles else 0
        max_drawdown = min((float(item["max_drawdown_pct"]) for item in profiles), default=0.0)
        if trades >= 18 and average_return < 0 and certified == 0:
            status = "KARANTİNADA"
            quarantined.append(strategy)
        elif certified > 0 and average_return > 0:
            status = "KANITLI PAPER"
        else:
            status = "VERİ TOPLUYOR"
        council.append({
            "strategy": strategy, "status": status, "score": average_score,
            "trades": trades, "certified_symbols": certified,
            "average_return_pct": round(average_return, 2), "max_drawdown_pct": round(max_drawdown, 2),
            "quarantined": status == "KARANTİNADA", "orders_enabled": False,
        })
    return council, quarantined


def v7_allocate_capital(symbol_rows: list[dict], capital: float, quarantined: list[str] | None = None) -> dict:
    quarantine_set = set(quarantined or [])
    quarantined_rows = [
        row for row in symbol_rows
        if row.get("allocation_ready") and row.get("strategy") in quarantine_set
    ]
    ready = sorted(
        [
            row for row in symbol_rows
            if row.get("allocation_ready") and row.get("strategy") not in quarantine_set
        ],
        key=lambda row: (row.get("confidence", 0), -row.get("risk_score", 100)), reverse=True,
    )
    selected: list[dict] = []
    allocations = [{
        "symbol": row["symbol"], "strategy": row["strategy"], "direction": row["direction"],
        "allocation_pct": 0.0, "allocated_usdt": 0.0, "status": "STRATEJİ KARANTİNASI",
        "correlation_with": None, "correlation_pct": 0.0,
        "reason": f"{row['strategy']} Strateji Konseyi tarafından karantinaya alındı; Paper tahsis sıfırlandı.",
    } for row in quarantined_rows]
    for row in ready:
        correlation_lock = None
        for other in selected:
            if row.get("direction") == other.get("direction") and row.get("direction") in {"LONG", "SHORT"}:
                correlation = pearson_correlation(row.get("return_fingerprint", []), other.get("return_fingerprint", []))
                if correlation >= 0.82:
                    correlation_lock = {"symbol": other["symbol"], "correlation_pct": round(correlation * 100, 1)}
                    break
        if correlation_lock or len(selected) >= 3:
            allocations.append({
                "symbol": row["symbol"], "strategy": row["strategy"], "direction": row["direction"],
                "allocation_pct": 0.0, "allocated_usdt": 0.0, "status": "KORELASYON KİLİDİ" if correlation_lock else "POZİSYON SINIRI",
                "correlation_with": correlation_lock["symbol"] if correlation_lock else None,
                "correlation_pct": correlation_lock["correlation_pct"] if correlation_lock else 0.0,
                "reason": f"{correlation_lock['symbol']} ile aynı yönlü yüksek benzerlik." if correlation_lock else "En fazla üç Paper sermaye dilimi kullanılır.",
            })
            continue
        selected.append(row)
    total_weight = sum(max(1.0, float(row["confidence"]) - float(row["risk_score"]) * 0.35) for row in selected)
    for row in selected:
        weight = max(1.0, float(row["confidence"]) - float(row["risk_score"]) * 0.35)
        allocation_pct = min(40.0, weight / max(total_weight, 0.00000001) * 100)
        allocations.append({
            "symbol": row["symbol"], "strategy": row["strategy"], "direction": row["direction"],
            "allocation_pct": round(allocation_pct, 1), "allocated_usdt": round(float(capital) * allocation_pct / 100, 2),
            "status": "PAPER TAHSİS", "correlation_with": None, "correlation_pct": 0.0,
            "reason": f"Güven %{row['confidence']} · risk %{row['risk_score']} · strateji {row['strategy']}.",
        })
    allocated = sum(float(item["allocated_usdt"]) for item in allocations)
    return {
        "allocations": sorted(allocations, key=lambda item: item["allocated_usdt"], reverse=True),
        "allocated_usdt": round(allocated, 2), "idle_usdt": round(max(0.0, float(capital) - allocated), 2),
        "heat_pct": round(allocated / max(float(capital), 0.00000001) * 100, 1),
        "max_parallel_allocations": 3, "orders_enabled": False,
    }


def v7_orchestrator_payload(engine: dict) -> dict:
    symbol_rows = []
    for row in engine.get("symbols", []):
        symbol_rows.append({key: value for key, value in row.items() if key != "return_fingerprint"})
    council = engine.get("strategies", [])
    certified = sum(1 for item in council if item.get("status") == "KANITLI PAPER")
    return {
        "enabled": bool(engine.get("enabled")), "status": engine.get("status", "DURDU"),
        "interval": engine.get("interval", "15m"), "capital": engine.get("capital", 3_000.0),
        "universe": engine.get("universe", []), "symbols": symbol_rows,
        "allocations": engine.get("allocations", []), "allocation_summary": engine.get("allocation_summary", {
            "allocated_usdt": 0.0, "idle_usdt": engine.get("capital", 3_000.0), "heat_pct": 0.0,
        }),
        "strategies": council, "quarantined_strategies": engine.get("quarantined_strategies", []),
        "certification_status": f"{certified}/3 STRATEJİ KANITLI" if council else "KANIT BEKLİYOR",
        "cycles": int(engine.get("cycles", 0)), "events": engine.get("events", [])[:V7_ORCHESTRATOR_EVENT_LIMIT],
        "last_tick_at": engine.get("last_tick_at"), "started_at": engine.get("started_at"),
        "stopped_at": engine.get("stopped_at"), "last_action": engine.get("last_action"),
        "orders_enabled": False,
        "safety_note": "V7 strateji seçimi ve sermaye tahsisi yalnızca Paper gölge portföydür; borsa emri oluşturmaz.",
    }


async def strategy_orchestrator_cycle() -> None:
    paper = app.state.paper
    engine = paper.get("strategy_orchestrator", empty_strategy_orchestrator_state())
    if not engine.get("enabled") or not engine.get("universe"):
        return
    if emergency_brake_payload(paper)["active"]:
        async with paper["lock"]:
            engine["enabled"] = False
            engine["status"] = "ACİL FREN"
            engine["last_action"] = "Acil Fren V7 Otonom Strateji Orkestrasını durdurdu."
        asyncio.create_task(persist_paper_snapshot(app))
        return
    universe = list(engine["universe"])
    symbol = universe[int(engine.get("cycle_index", 0)) % len(universe)]
    try:
        candles = await fetch_candles(symbol, str(engine["interval"]), 740)
        per_symbol_capital = float(engine["capital"]) / max(1, len(universe))
        replay = v7_market_replay(candles[:-1], "7d", per_symbol_capital)
        decision = v7_strategy_decision(symbol, candles, replay, list(engine.get("quarantined_strategies", [])))
        active_v10 = (paper.get("strategy_evolution") or {}).get("active_champion")
        if isinstance(active_v10, dict) and active_v10.get("symbol") == symbol:
            decision["v10_champion"] = {
                "id": active_v10.get("id"), "family": active_v10.get("family"),
                "label": active_v10.get("label"), "score": active_v10.get("score"),
                "paper_policy": active_v10.get("paper_policy"), "orders_enabled": False,
            }
            decision["reason"] += f" V10 Paper şampiyonu: {active_v10.get('label')} ({active_v10.get('score')}/99)."
        now = datetime.now(timezone.utc).isoformat()
        async with paper["lock"]:
            rows = [row for row in engine.get("symbols", []) if row.get("symbol") != symbol]
            rows.append(decision)
            rows.sort(key=lambda row: universe.index(row["symbol"]) if row["symbol"] in universe else 999)
            council, quarantined = v7_strategy_council(rows)
            allocation = v7_allocate_capital(rows, float(engine["capital"]), quarantined)
            previous_quarantine = set(engine.get("quarantined_strategies", []))
            new_quarantine = set(quarantined) - previous_quarantine
            engine.update({
                "symbols": rows, "strategies": council, "quarantined_strategies": quarantined,
                "allocations": allocation["allocations"], "allocation_summary": {
                    "allocated_usdt": allocation["allocated_usdt"], "idle_usdt": allocation["idle_usdt"],
                    "heat_pct": allocation["heat_pct"], "max_parallel_allocations": allocation["max_parallel_allocations"],
                },
                "cycle_index": int(engine.get("cycle_index", 0)) + 1,
                "cycles": int(engine.get("cycles", 0)) + 1, "last_tick_at": now,
                "status": "CANLI PAPER ORKESTRA",
                "last_action": f"{symbol}: {decision['strategy']} · {decision['regime']} · güven %{decision['confidence']} · risk %{decision['risk_score']}.",
            })
            engine.setdefault("events", []).insert(0, {
                "kind": "STRATEJİ KARARI", "symbol": symbol, "strategy": decision["strategy"],
                "message": engine["last_action"], "created_at": now, "paper_only": True,
            })
            for strategy in new_quarantine:
                engine["events"].insert(0, {
                    "kind": "PAPER KARANTİNA", "symbol": "PORTFÖY", "strategy": strategy,
                    "message": f"{strategy} yeterli örnekte maliyet sonrası zayıf kaldı; yeni Paper tahsisleri kesildi.",
                    "created_at": now, "paper_only": True,
                })
            del engine["events"][V7_ORCHESTRATOR_EVENT_LIMIT:]
        asyncio.create_task(persist_paper_snapshot(app))
    except Exception as exc:
        async with paper["lock"]:
            engine["cycle_index"] = int(engine.get("cycle_index", 0)) + 1
            engine["last_action"] = f"{symbol} V7 verisi geçici olarak bekleniyor: {str(exc)[:80]}"


async def strategy_orchestrator_loop(application: FastAPI) -> None:
    while True:
        try:
            if application.state.paper.get("strategy_orchestrator", {}).get("enabled"):
                await strategy_orchestrator_cycle()
        except Exception as exc:
            engine = application.state.paper.get("strategy_orchestrator", {})
            engine["last_action"] = f"V7 Orkestra geçici hata; yeniden denenecek: {str(exc)[:70]}"
        await asyncio.sleep(V7_ORCHESTRATOR_TICK_SECONDS)


class StrategyOrchestratorStartRequest(BaseModel):
    symbols: list[str] = Field(default=["BTCUSDT", "ETHUSDT", "SOLUSDT"], min_length=1, max_length=5)
    interval: Literal["5m", "15m", "1h"] = "15m"
    capital: float = Field(default=3_000.0, ge=500, le=25_000)


@app.get("/api/v7/orchestrator")
async def strategy_orchestrator_status():
    return v7_orchestrator_payload(app.state.paper.get("strategy_orchestrator", empty_strategy_orchestrator_state()))


@app.post("/api/v7/orchestrator/start")
async def start_strategy_orchestrator(request: StrategyOrchestratorStartRequest):
    paper = app.state.paper
    if emergency_brake_payload(paper)["active"]:
        raise HTTPException(409, "Acil Fren aktifken V7 Strateji Orkestrası başlatılamaz")
    current = paper.get("strategy_orchestrator", empty_strategy_orchestrator_state())
    if current.get("enabled"):
        raise HTTPException(409, "V7 Strateji Orkestrası zaten çalışıyor")
    symbols = []
    for raw in request.symbols:
        safe = "".join(char for char in raw.upper() if char.isalnum())
        if safe.endswith("USDT") and safe not in symbols:
            symbols.append(safe)
    if not symbols:
        raise HTTPException(422, "En az bir geçerli USDT paritesi seçin")
    now = datetime.now(timezone.utc).isoformat()
    engine = {
        **empty_strategy_orchestrator_state(), "enabled": True, "status": "CANLI PAPER ORKESTRA",
        "interval": request.interval, "capital": request.capital, "universe": symbols,
        "started_at": now, "last_tick_at": now,
        "last_action": f"V7 {len(symbols)} parite için Grid, Trend ve Kırılım Paper konseyini başlattı.",
        "events": [{
            "kind": "V7 BAŞLADI", "symbol": "PORTFÖY", "strategy": "KONSEY",
            "message": f"{', '.join(symbols)} · toplam {request.capital:.0f} USDT sanal sermaye · borsa emri kapalı.",
            "created_at": now, "paper_only": True,
        }],
    }
    async with paper["lock"]:
        paper["strategy_orchestrator"] = engine
        add_paper_notification(paper, "V7 PAPER ORKESTRA", engine["last_action"])
    asyncio.create_task(strategy_orchestrator_cycle())
    asyncio.create_task(persist_paper_snapshot(app))
    return v7_orchestrator_payload(engine)


@app.post("/api/v7/orchestrator/stop")
async def stop_strategy_orchestrator():
    paper = app.state.paper
    async with paper["lock"]:
        engine = paper.get("strategy_orchestrator", empty_strategy_orchestrator_state())
        engine["enabled"] = False
        engine["status"] = "DURDU"
        engine["stopped_at"] = datetime.now(timezone.utc).isoformat()
        engine["last_action"] = "V7 Strateji Orkestrası kullanıcı tarafından durduruldu; Paper kanıtı hafızada korundu."
        add_paper_notification(paper, "V7 PAPER ORKESTRA", engine["last_action"])
    asyncio.create_task(persist_paper_snapshot(app))
    return v7_orchestrator_payload(engine)


@app.get("/api/v7/replay/{symbol}")
async def v7_replay_endpoint(
    symbol: str,
    interval: Literal["5m", "15m", "1h"] = "15m",
    horizon: Literal["24h", "7d"] = "7d",
    capital: float = Query(1_000.0, ge=100, le=25_000),
):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    cache_key = (safe_symbol, interval, horizon, int(round(capital)))
    cached = V7_REPLAY_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < 120:
        return {**cached[1], "cached": True}
    requested = 180 if horizon == "24h" else 740
    candles = await fetch_candles(safe_symbol, interval, requested)
    payload = {"symbol": safe_symbol, "interval": interval, **v7_market_replay(candles[:-1], horizon, capital)}
    V7_REPLAY_CACHE[cache_key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/v7/report/weekly")
async def v7_weekly_report():
    paper = app.state.paper
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    trades = []
    for trade in paper.get("trades", []):
        try:
            closed_at = datetime.fromisoformat(str(trade.get("closed_at") or "").replace("Z", "+00:00"))
        except ValueError:
            continue
        if closed_at >= cutoff:
            trades.append(trade)
    pnl = sum(float(item.get("realized_pnl") or 0.0) for item in trades)
    wins = sum(1 for item in trades if float(item.get("realized_pnl") or 0.0) > 0)
    engine = v7_orchestrator_payload(paper.get("strategy_orchestrator", empty_strategy_orchestrator_state()))
    return {
        "period": "SON 7 GÜN", "paper_trades": len(trades), "wins": wins,
        "win_rate": round(wins / len(trades) * 100, 1) if trades else 0.0,
        "paper_pnl_usdt": round(pnl, 2), "orchestrator_cycles": engine["cycles"],
        "certification_status": engine["certification_status"],
        "allocated_usdt": engine["allocation_summary"].get("allocated_usdt", 0.0),
        "idle_usdt": engine["allocation_summary"].get("idle_usdt", engine["capital"]),
        "quarantined_strategies": engine["quarantined_strategies"],
        "headline": engine["last_action"], "orders_enabled": False,
        "note": "Rapor yalnızca Paper işlem ve V7 gölge tahsislerini içerir.",
    }


def v8_percentile(values: list[float], quantile: float) -> float:
    """Harici sayısal paket olmadan doğrusal yüzdelik hesaplar."""
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    position = max(0.0, min(1.0, quantile)) * (len(ordered) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def v8_probability_corridor(candles: list[dict], interval: str, horizon: int = 12) -> dict:
    """Geçmiş getiri bloklarından deterministik, Paper-only bir olasılık koridoru üretir."""
    safe_horizon = horizon if horizon in V8_FORECAST_HORIZONS else 12
    rows = candles[-520:]
    if len(rows) < 110:
        raise ValueError("V8 Olasılık Koridoru için en az 110 kapanmış mum gerekir")
    closes = [float(item["close"]) for item in rows]
    returns = [closes[index] / max(closes[index - 1], 0.00000001) - 1 for index in range(1, len(closes))]
    return_mean = sum(returns[-80:]) / min(80, len(returns))
    variance = sum((value - return_mean) ** 2 for value in returns[-80:]) / max(1, min(80, len(returns)))
    volatility = max(math.sqrt(variance), 0.00005)
    recent_drift = sum(returns[-8:]) / min(8, len(returns))
    ema20, ema50 = ema_path(closes, 20), ema_path(closes, 50)
    trend_spread = ema20[-1] / max(ema50[-1], 0.00000001) - 1
    raw_bias = recent_drift * 0.32 + trend_spread / max(safe_horizon, 1) * 0.18
    bias = max(-volatility * 0.28, min(volatility * 0.28, raw_bias))
    historical_paths: list[list[float]] = []
    first_start = max(24, len(returns) - 390)
    last_start = len(returns) - safe_horizon
    entry = closes[-1]
    for start in range(first_start, last_start, 2):
        cumulative = 1.0
        path = []
        for step in range(safe_horizon):
            historical_return = returns[start + step]
            adjusted = historical_return + bias * (1 - step / max(safe_horizon * 1.5, 1))
            adjusted = max(-volatility * 4.5, min(volatility * 4.5, adjusted))
            cumulative *= 1 + adjusted
            path.append(entry * cumulative)
        historical_paths.append(path)
    if len(historical_paths) < 20:
        raise ValueError("V8 Olasılık Koridoru için yeterli tarihsel blok bulunamadı")
    step_seconds = INTERVAL_SECONDS.get(interval, 900)
    last_time = int(rows[-1]["time"])
    points = []
    for step in range(safe_horizon):
        step_prices = [path[step] for path in historical_paths]
        points.append({
            "time": last_time + step_seconds * (step + 1),
            "lower": round(v8_percentile(step_prices, 0.15), 8),
            "base": round(v8_percentile(step_prices, 0.50), 8),
            "upper": round(v8_percentile(step_prices, 0.85), 8),
        })
    terminal_returns = [path[-1] / entry - 1 for path in historical_paths]
    hurdle = max(0.0015, volatility * 0.30)
    bullish = round(sum(1 for value in terminal_returns if value > hurdle) / len(terminal_returns) * 100, 1)
    bearish = round(sum(1 for value in terminal_returns if value < -hurdle) / len(terminal_returns) * 100, 1)
    neutral = round(max(0.0, 100.0 - bullish - bearish), 1)
    correction = round(100.0 - bullish - bearish - neutral, 1)
    neutral = round(neutral + correction, 1)
    probabilities = {"YÜKSELİŞ": bullish, "YATAY": neutral, "DÜŞÜŞ": bearish}
    dominant = max(probabilities, key=probabilities.get)
    dominant_probability = probabilities[dominant]
    final_point = points[-1]
    terminal_mean_pct = sum(terminal_returns) / len(terminal_returns) * 100
    uncertainty_pct = (final_point["upper"] - final_point["lower"]) / max(entry, 0.00000001) * 100
    return {
        "interval": interval, "horizon_candles": safe_horizon, "entry_price": entry,
        "probabilities": probabilities, "dominant_scenario": dominant,
        "dominant_probability": dominant_probability,
        "confidence_status": "BELİRGİN" if dominant_probability >= 50 else "KARIŞIK" if dominant_probability >= 40 else "DÜŞÜK AYRIM",
        "terminal_mean_pct": round(terminal_mean_pct, 3),
        "uncertainty_pct": round(uncertainty_pct, 2),
        "cost_hurdle_pct": round(hurdle * 100, 3), "points": points,
        "targets": {
            "bear_case": final_point["lower"], "base_case": final_point["base"],
            "bull_case": final_point["upper"],
        },
        "_terminal_returns": terminal_returns,
        "orders_enabled": False,
        "note": "Koridor geçmiş kapanış bloklarının dağılımıdır; kesin fiyat tahmini veya getiri garantisi değildir.",
    }


def v8_calibration_report(candles: list[dict], interval: str, horizon: int = 12) -> dict:
    """Modeli geçmişte ileriye bakmadan tekrar tekrar sınar ve aşırı özgüveni cezalandırır."""
    safe_horizon = horizon if horizon in V8_FORECAST_HORIZONS else 12
    rows = candles[-520:]
    first_cut = max(125, len(rows) - 260)
    cuts = list(range(first_cut, len(rows) - safe_horizon, max(5, safe_horizon // 2)))[-24:]
    records = []
    brier_total = 0.0
    hits = 0
    confidence_total = 0.0
    labels = ("YÜKSELİŞ", "YATAY", "DÜŞÜŞ")
    for cut in cuts:
        try:
            forecast = v8_probability_corridor(rows[:cut], interval, safe_horizon)
        except ValueError:
            continue
        entry = float(rows[cut - 1]["close"])
        realized = float(rows[cut + safe_horizon - 1]["close"]) / max(entry, 0.00000001) - 1
        threshold = max(0.0015, float(forecast["cost_hurdle_pct"]) / 100)
        outcome = "YÜKSELİŞ" if realized > threshold else "DÜŞÜŞ" if realized < -threshold else "YATAY"
        predicted = str(forecast["dominant_scenario"])
        probabilities = forecast["probabilities"]
        brier = sum(((float(probabilities[label]) / 100) - (1.0 if outcome == label else 0.0)) ** 2 for label in labels)
        hit = predicted == outcome
        brier_total += brier
        hits += 1 if hit else 0
        confidence_total += float(forecast["dominant_probability"])
        records.append({
            "forecast_at": int(rows[cut - 1]["time"]), "predicted": predicted,
            "outcome": outcome, "confidence": forecast["dominant_probability"],
            "realized_move_pct": round(realized * 100, 2), "hit": hit,
        })
    samples = len(records)
    accuracy = hits / samples * 100 if samples else 0.0
    brier_score = brier_total / samples if samples else 0.0
    average_confidence = confidence_total / samples if samples else 0.0
    overconfidence_gap = max(0.0, average_confidence - accuracy)
    reliability = round(max(0.0, min(99.0, 100 - brier_score * 62 - overconfidence_gap * 0.65))) if samples else 0
    if samples < 12:
        status = "VERİ TOPLUYOR"
    elif accuracy < 30 or brier_score > 0.92 or overconfidence_gap > 28:
        status = "MODEL KARANTİNASI"
    elif accuracy >= 43 and brier_score <= 0.72 and overconfidence_gap <= 18:
        status = "KALİBRE"
    else:
        status = "GÖZLEMDE"
    return {
        "samples": samples, "hits": hits, "accuracy_pct": round(accuracy, 1),
        "brier_score": round(brier_score, 3), "average_confidence_pct": round(average_confidence, 1),
        "overconfidence_gap_pct": round(overconfidence_gap, 1), "reliability_score": reliability,
        "status": status, "quarantined": status == "MODEL KARANTİNASI",
        "records": list(reversed(records[-8:])), "orders_enabled": False,
        "method_note": "Her denemede yalnızca o anda bilinen mumlar kullanılır; sonraki mumlar sadece doğrulama içindir.",
    }


def v8_chaos_sentinel(candles: list[dict], orderbook: dict) -> dict:
    rows = candles[-260:]
    closes = [float(item["close"]) for item in rows]
    returns = [closes[index] / max(closes[index - 1], 0.00000001) - 1 for index in range(1, len(closes))]
    recent = returns[-12:]
    older = returns[-72:-12] or returns[:-12]

    def volatility(values: list[float]) -> float:
        if not values:
            return 0.0
        average = sum(values) / len(values)
        return math.sqrt(sum((value - average) ** 2 for value in values) / len(values))

    recent_volatility = volatility(recent)
    older_volatility = max(volatility(older), 0.00000001)
    volatility_ratio = recent_volatility / older_volatility
    last_close = closes[-1]
    current_atr = max(rolling_atr(rows, len(rows) - 1), last_close * 0.0001)
    last_range_atr = (float(rows[-1]["high"]) - float(rows[-1]["low"])) / current_atr
    last_move_pct = returns[-1] * 100
    volumes = [float(item.get("volume") or 0.0) for item in rows]
    average_volume = sum(volumes[-31:-1]) / max(1, len(volumes[-31:-1]))
    volume_ratio = volumes[-1] / max(average_volume, 0.00000001)
    spread_bps = float(orderbook.get("spread_bps") or 25.0)
    spoof_risk = float(orderbook.get("spoof_risk_score") or 55.0)
    pressure = abs(float(orderbook.get("pressure_pct") or 0.0))
    liquidity_shock = min(100.0,
        min(34.0, spread_bps * 2.2) + spoof_risk * 0.36
        + max(0.0, pressure - 45) * 0.45 + max(0.0, 0.75 - volume_ratio) * 32
    )
    ema20, ema50 = ema_path(closes, 20), ema_path(closes, 50)
    spread_now = ema20[-1] - ema50[-1]
    spread_before = ema20[-8] - ema50[-8]
    sign_flip = spread_now * spread_before < 0
    alternating = sum(1 for left, right in zip(recent, recent[1:]) if left * right < 0) / max(1, len(recent) - 1)
    regime_shift = min(100.0,
        (36 if sign_flip else 0) + max(0.0, volatility_ratio - 1) * 26
        + max(0.0, alternating - 0.55) * 45 + max(0.0, last_range_atr - 1.8) * 14
    )
    flash_move = min(100.0,
        abs(last_move_pct) * 21 + max(0.0, last_range_atr - 1.5) * 20
        + max(0.0, volume_ratio - 1.8) * 12
    )
    chaos_score = round(min(100.0, liquidity_shock * 0.42 + regime_shift * 0.34 + flash_move * 0.24))
    level = "YÜKSEK RİSK" if chaos_score >= 68 else "UYARI" if chaos_score >= 43 else "DENGELİ"
    reasons = []
    if spread_bps >= 10:
        reasons.append(f"Spread {spread_bps:.1f} bp ile geniş")
    if spoof_risk >= 55:
        reasons.append(f"Spoof riski %{spoof_risk:.0f}")
    if volatility_ratio >= 1.6:
        reasons.append(f"Kısa volatilite {volatility_ratio:.1f}x hızlandı")
    if sign_flip:
        reasons.append("EMA20/50 rejim yönü değişti")
    if abs(last_move_pct) >= 1.4:
        reasons.append(f"Son mum %{last_move_pct:.2f} hareket etti")
    if not reasons:
        reasons.append("Likidite ve rejim ölçümleri olağan aralıkta")
    return {
        "level": level, "chaos_score": chaos_score,
        "liquidity_shock_score": round(liquidity_shock), "regime_shift_score": round(regime_shift),
        "flash_move_score": round(flash_move), "spread_bps": round(spread_bps, 2),
        "spoof_risk_score": round(spoof_risk), "volatility_ratio": round(volatility_ratio, 2),
        "last_move_pct": round(last_move_pct, 2), "volume_ratio": round(volume_ratio, 2),
        "reasons": reasons, "veto_required": chaos_score >= 68 or liquidity_shock >= 72,
        "orders_enabled": False,
    }


def v8_execution_twin(forecast: dict, orderbook: dict, notional: float = 1_000.0) -> dict:
    safe_notional = max(100.0, min(25_000.0, float(notional)))
    heatmap = orderbook.get("heatmap") if isinstance(orderbook.get("heatmap"), list) else []
    visible_depth = sum(float(item.get("notional_usdt") or 0.0) for item in heatmap)
    visible_depth = max(visible_depth, safe_notional * 0.35)
    spread_bps = float(orderbook.get("spread_bps") or 25.0)
    spoof_risk = float(orderbook.get("spoof_risk_score") or 55.0)
    fill_quality = 100 - min(48.0, spread_bps * 1.7) - spoof_risk * 0.28
    depth_quality = min(100.0, visible_depth / safe_notional * 60)
    partial_fill_pct = round(max(12.0, min(100.0, fill_quality * 0.62 + depth_quality * 0.38)), 1)
    impact_bps = spread_bps / 2 + safe_notional / visible_depth * 7 + spoof_risk * 0.045 + 1.5
    single_side_cost_pct = 0.10 + impact_bps / 100
    round_trip_cost_pct = single_side_cost_pct * 2
    terminal_mean = float(forecast.get("terminal_mean_pct") or 0.0)
    probabilities = forecast.get("probabilities", {})
    scenarios = [
        {
            "action": "LONG", "probability": float(probabilities.get("YÜKSELİŞ", 0.0)),
            "expected_net_pct": round(terminal_mean - round_trip_cost_pct, 3),
            "worst_case_pct": round((float(forecast["targets"]["bear_case"]) / float(forecast["entry_price"]) - 1) * 100 - round_trip_cost_pct, 3),
        },
        {
            "action": "SHORT", "probability": float(probabilities.get("DÜŞÜŞ", 0.0)),
            "expected_net_pct": round(-terminal_mean - round_trip_cost_pct, 3),
            "worst_case_pct": round((1 - float(forecast["targets"]["bull_case"]) / float(forecast["entry_price"])) * 100 - round_trip_cost_pct, 3),
        },
        {
            "action": "BEKLE", "probability": float(probabilities.get("YATAY", 0.0)),
            "expected_net_pct": 0.0, "worst_case_pct": 0.0,
        },
    ]
    best = max(scenarios, key=lambda item: (item["expected_net_pct"], item["probability"]))
    best_action = best["action"] if best["expected_net_pct"] >= 0.12 and best["probability"] >= 38 else "BEKLE"
    return {
        "notional_usdt": safe_notional, "visible_depth_usdt": round(visible_depth, 2),
        "partial_fill_pct": partial_fill_pct, "estimated_impact_bps": round(impact_bps, 2),
        "single_side_cost_pct": round(single_side_cost_pct, 3),
        "round_trip_cost_pct": round(round_trip_cost_pct, 3),
        "latency_ms_assumption": round(120 + spread_bps * 5 + spoof_risk * 1.8),
        "scenarios": scenarios, "best_action": best_action,
        "status": "UYGULANABİLİR PAPER" if partial_fill_pct >= 70 and round_trip_cost_pct <= 0.9 else "YÜRÜTME VETOSU",
        "orders_enabled": False,
        "note": "Dolum, gecikme, ücret ve fiyat etkisi yalnızca Dijital Emir İkizi varsayımıdır.",
    }


def v8_portfolio_chaos_test(
    symbol: str,
    candles: list[dict],
    btc_candles: list[dict],
    orchestrator: dict,
    candidate_action: str,
) -> dict:
    symbol_returns = candle_returns(candles, 120)
    btc_returns = candle_returns(btc_candles, 120)
    correlation = pearson_correlation(symbol_returns, btc_returns)

    def volatility(values: list[float]) -> float:
        if not values:
            return 0.0
        average = sum(values) / len(values)
        return math.sqrt(sum((value - average) ** 2 for value in values) / len(values))

    btc_volatility = max(volatility(btc_returns), 0.00000001)
    beta = max(-3.0, min(3.0, correlation * volatility(symbol_returns) / btc_volatility))
    allocations = orchestrator.get("allocations", []) if isinstance(orchestrator, dict) else []
    active_allocations = [item for item in allocations if float(item.get("allocated_usdt") or 0.0) > 0]
    allocated = sum(float(item.get("allocated_usdt") or 0.0) for item in active_allocations)
    total_capital = max(float(orchestrator.get("capital") or 0.0), allocated, 1.0) if isinstance(orchestrator, dict) else 1.0
    if active_allocations:
        exposure_pct = min(100.0, allocated / total_capital * 100)
        directional = sum(
            float(item.get("allocated_usdt") or 0.0) * (1 if item.get("direction") == "LONG" else -1 if item.get("direction") == "SHORT" else 0.25)
            for item in active_allocations
        ) / max(allocated, 0.00000001)
        exposure_source = "V7 CANLI PAPER TAHSİSİ"
    else:
        exposure_pct = 30.0
        directional = 1.0 if candidate_action == "LONG" else -1.0 if candidate_action == "SHORT" else 0.25
        exposure_source = "VARSAYIMSAL PAPER %30"
    stress_definitions = [
        ("BTC -%3 HIZLI DÜŞÜŞ", -3.0, 0.18),
        ("BTC -%7 + SPREAD 3X", -7.0, 0.55),
        ("BTC +%5 SHORT SIKIŞMASI", 5.0, 0.35),
    ]
    scenarios = []
    for label, btc_move, friction in stress_definitions:
        projected_symbol_move = btc_move * beta
        portfolio_impact = projected_symbol_move * exposure_pct / 100 * directional - friction * exposure_pct / 100
        scenarios.append({
            "label": label, "btc_move_pct": btc_move,
            "projected_symbol_move_pct": round(projected_symbol_move, 2),
            "portfolio_impact_pct": round(portfolio_impact, 2),
            "status": "AĞIR" if portfolio_impact <= -3 else "UYARI" if portfolio_impact <= -1.5 else "DAYANIKLI",
        })
    worst = min(item["portfolio_impact_pct"] for item in scenarios)
    risk_per_allocation = abs(worst) / max(exposure_pct, 1.0)
    safe_allocation = 40.0 if risk_per_allocation <= 0.0001 else min(40.0, max(0.0, 2.5 / risk_per_allocation))
    level = "KAOS VETOSU" if worst <= -3.5 else "TEMKİNLİ" if worst <= -1.8 else "DAYANIKLI"
    return {
        "symbol": symbol, "btc_correlation_pct": round(correlation * 100, 1), "btc_beta": round(beta, 2),
        "exposure_pct": round(exposure_pct, 1), "exposure_source": exposure_source,
        "worst_case_pct": round(worst, 2), "safe_allocation_pct": round(safe_allocation, 1),
        "level": level, "scenarios": scenarios, "veto_required": level == "KAOS VETOSU",
        "orders_enabled": False,
        "note": "Sonuçlar V7 Paper tahsisi veya belirtilen varsayımsal maruziyet üzerindeki stres tahminidir.",
    }


def v8_veto_council(
    forecast: dict,
    calibration: dict,
    chaos: dict,
    execution: dict,
    portfolio: dict,
) -> dict:
    dominance = float(forecast.get("dominant_probability") or 0.0)
    gates = [
        {
            "key": "FORECAST", "label": "Olasılık ayrımı",
            "passed": dominance >= 40, "critical": False,
            "detail": f"Baskın senaryo %{dominance:.1f}.",
        },
        {
            "key": "CALIBRATION", "label": "Tahmin doğruluk karnesi",
            "passed": not calibration.get("quarantined"), "critical": True,
            "detail": f"{calibration.get('status')} · güvenilirlik {calibration.get('reliability_score')}/99.",
        },
        {
            "key": "CHAOS", "label": "Kaos Kalkanı",
            "passed": not chaos.get("veto_required"), "critical": True,
            "detail": f"{chaos.get('level')} · skor %{chaos.get('chaos_score')}.",
        },
        {
            "key": "EXECUTION", "label": "Dijital Emir İkizi",
            "passed": execution.get("status") != "YÜRÜTME VETOSU", "critical": True,
            "detail": f"Tahmini dolum %{execution.get('partial_fill_pct')} · tur maliyeti %{execution.get('round_trip_cost_pct')}.",
        },
        {
            "key": "PORTFOLIO", "label": "Portföy Kaos Testi",
            "passed": not portfolio.get("veto_required"), "critical": True,
            "detail": f"{portfolio.get('level')} · en kötü %{portfolio.get('worst_case_pct')}.",
        },
    ]
    vetoes = [gate for gate in gates if gate["critical"] and not gate["passed"]]
    candidate = str(execution.get("best_action") or "BEKLE")
    paper_allowed = not vetoes and candidate in {"LONG", "SHORT"} and dominance >= 40
    final_action = candidate if paper_allowed else "BEKLE"
    if vetoes:
        reason = " · ".join(f"{gate['label']}: {gate['detail']}" for gate in vetoes)
    elif candidate == "BEKLE":
        reason = "Maliyet sonrası LONG veya SHORT üstünlüğü oluşmadı; Paper senaryo bekliyor."
    elif dominance < 40:
        reason = "Olasılık dağılımı yeterince ayrışmadı; Paper senaryo bekliyor."
    else:
        reason = f"Bütün V8 denetimleri geçti; yalnızca {candidate} Paper senaryosu izlenebilir."
    confidence = round(min(dominance, float(calibration.get("reliability_score") or 0.0))) if final_action != "BEKLE" else 0
    return {
        "candidate_action": candidate, "final_action": final_action,
        "paper_scenario_allowed": paper_allowed, "confidence": confidence,
        "status": "PAPER SENARYO ONAYI" if paper_allowed else "VETO / BEKLE",
        "veto_count": len(vetoes), "vetoes": [gate["label"] for gate in vetoes],
        "gates": gates, "reason": reason, "orders_enabled": False,
        "safety_note": "V8 hiçbir koşulda borsa emri üretmez; onay yalnızca sanal senaryo iznidir.",
    }


@app.get("/api/v8/future-lab/{symbol}")
async def v8_future_lab_endpoint(
    symbol: str,
    interval: Literal["1m", "5m", "15m", "1h", "4h", "1d"] = "15m",
    horizon: Literal[12, 24] = 12,
    notional: float = Query(1_000.0, ge=100, le=25_000),
):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    if not safe_symbol.endswith("USDT"):
        raise HTTPException(422, "V8 için geçerli bir USDT paritesi seçin")
    cache_key = (safe_symbol, interval, int(horizon), int(round(notional)))
    cached = V8_FUTURE_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < 20:
        return {**cached[1], "cached": True}
    candle_task = fetch_candles(safe_symbol, interval, 520)
    orderbook_task = orderbook_intelligence(safe_symbol)
    btc_task = fetch_candles("BTCUSDT", interval, 520) if safe_symbol != "BTCUSDT" else candle_task
    if safe_symbol == "BTCUSDT":
        candle_result, orderbook_result = await asyncio.gather(candle_task, orderbook_task, return_exceptions=True)
        btc_result = candle_result
    else:
        candle_result, orderbook_result, btc_result = await asyncio.gather(candle_task, orderbook_task, btc_task, return_exceptions=True)
    if isinstance(candle_result, Exception):
        raise HTTPException(502, f"V8 piyasa verisi alınamadı: {str(candle_result)[:100]}")
    candles = candle_result[:-1] if len(candle_result) > 1 else candle_result
    if isinstance(btc_result, Exception):
        btc_candles = candles
    else:
        btc_candles = btc_result[:-1] if len(btc_result) > 1 else btc_result
    if isinstance(orderbook_result, Exception):
        orderbook = {
            "mode": "EMİR DEFTERİ BEKLENİYOR", "spread_bps": 25.0,
            "spoof_risk_score": 60, "pressure_pct": 0.0, "heatmap": [],
            "reason": "V8 güvenlik için eksik emir defterini yüksek risk varsayımıyla ele aldı.",
        }
    else:
        orderbook = orderbook_result
    forecast = v8_probability_corridor(candles, interval, int(horizon))
    calibration = v8_calibration_report(candles, interval, int(horizon))
    chaos = v8_chaos_sentinel(candles, orderbook)
    execution = v8_execution_twin(forecast, orderbook, notional)
    orchestrator = v7_orchestrator_payload(app.state.paper.get("strategy_orchestrator", empty_strategy_orchestrator_state()))
    portfolio = v8_portfolio_chaos_test(safe_symbol, candles, btc_candles, orchestrator, execution["best_action"])
    veto = v8_veto_council(forecast, calibration, chaos, execution, portfolio)
    public_forecast = {key: value for key, value in forecast.items() if not key.startswith("_")}
    payload = {
        "symbol": safe_symbol, "interval": interval, "horizon": int(horizon),
        "forecast": public_forecast, "calibration": calibration, "chaos": chaos,
        "execution_twin": execution, "portfolio_chaos": portfolio, "veto_council": veto,
        "orderbook_mode": orderbook.get("mode", "ÖLÇÜLÜYOR"),
        "generated_at": datetime.now(timezone.utc).isoformat(), "orders_enabled": False,
        "note": "V8 olasılık, kalibrasyon ve stres katmanları yalnızca Paper karar desteğidir; yatırım tavsiyesi değildir.",
    }
    V8_FUTURE_CACHE[cache_key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/scan")
async def smart_scan(limit: int = Query(18, ge=6, le=30), interval: str = "15m"):
    """Tarama sonuçları 45 saniye saklanır; Binance'e gereksiz tekrar istek atılmaz."""
    key = (limit, interval)
    cached = SCAN_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 45:
        return {"cached": True, "results": cached[1]}

    market_list = await markets(limit=limit)
    semaphore = asyncio.Semaphore(6)

    async def inspect(market: dict):
        try:
            async with semaphore:
                result = analyze(await fetch_candles(market["symbol"], interval, 260))
                mtf = await multi_timeframe_consensus(market["symbol"])
            confidence = float(result["confidence"])
            volume_ratio = float(result["volume_ratio"])
            breakout_quality = float(result["radar"].get("breakout_quality") or 0.0)
            trap_quality = 100.0 - float(result["radar"].get("trap_score") or 0.0)
            opportunity_score = round(max(0.0, min(100.0, (
                confidence * 0.40
                + float(mtf["alignment"]) * 0.30
                + max(0.0, min(100.0, volume_ratio * 100.0)) * 0.15
                + max(0.0, min(100.0, breakout_quality)) * 0.10
                + max(0.0, min(100.0, trap_quality)) * 0.05
            ))), 2)
            return {
                **market,
                "direction": result["direction"],
                "confidence": result["confidence"],
                "trend": result["trend"],
                "volume_ratio": round(volume_ratio, 2),
                "trap_score": result["radar"]["trap_score"],
                "trap_level": result["radar"]["trap_level"],
                "breakout": result["volume_ratio"] >= 1.25 and result["direction"] != "BEKLE",
                "mtf_direction": mtf["direction"],
                "mtf_alignment": mtf["alignment"],
                "mtf_entry_permission": mtf["entry_permission"],
                "opportunity_score": opportunity_score,
            }
        except Exception:
            return None

    scanned = await asyncio.gather(*(inspect(market) for market in market_list))
    results = [item for item in scanned if item is not None]
    results.sort(key=lambda item: (item["opportunity_score"], item["confidence"]), reverse=True)
    results = results[:limit]
    SCAN_CACHE[key] = (time.monotonic(), results)
    return {"cached": False, "results": results}


@app.get("/api/analysis-universe")
async def analysis_universe(interval: str = "15m", limit: int = Query(24, ge=1, le=24)):
    """Return cached, read-only technical snapshots for eligible USDT pairs."""
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Desteklenmeyen zaman dilimi")
    cached = ANALYSIS_UNIVERSE_CACHE.get(interval)
    if cached and time.monotonic() - cached[0] < 60:
        return {"cached": True, "interval": interval, "results": cached[1][:limit]}

    market_list = await markets(limit=limit)
    semaphore = asyncio.Semaphore(8)

    async def inspect(market: dict) -> dict | None:
        try:
            async with semaphore:
                candles = await fetch_candles(market["symbol"], interval, 500)
                if interval == "15m":
                    mtf_intervals = ("1h", "4h", "1d")
                    higher_timeframes = await asyncio.gather(
                        *(fetch_candles(market["symbol"], timeframe, 260) for timeframe in mtf_intervals)
                    )
                    mtf_candles = {"15m": candles[-260:]}
                    mtf_candles.update(dict(zip(mtf_intervals, higher_timeframes)))
                    result, mtf = await asyncio.gather(
                        asyncio.to_thread(analyze, candles),
                        consensus_from_candles(market["symbol"], mtf_candles),
                    )
                else:
                    result, mtf = await asyncio.gather(
                        asyncio.to_thread(analyze, candles),
                        multi_timeframe_consensus(market["symbol"]),
                    )
            direction = result["direction"]
            aligned = (
                result["ema"]["ema20"] > result["ema"]["ema50"] > result["ema"]["ema200"]
                if direction == "LONG" else
                result["ema"]["ema20"] < result["ema"]["ema50"] < result["ema"]["ema200"]
                if direction == "SHORT" else False
            )
            rsi_fit = 1.0 if (direction == "LONG" and 50 <= result["rsi"] <= 70) or (direction == "SHORT" and 30 <= result["rsi"] <= 50) else .45
            mtf_fit = min(100.0, float(mtf["alignment"]))
            volatility_pct = abs(candles[-1]["high"] - candles[-1]["low"]) / max(candles[-1]["close"], 1e-9) * 100
            volatility_fit = 1.0 if volatility_pct < 3 else .55
            smart_score = round(min(100.0, max(0.0, result["confidence"] * .30 + mtf_fit * .20 + (18 if aligned else 7) + rsi_fit * 14 + min(12, result["volume_ratio"] * 6) + min(10, result["risk_reward"] / 3 * 10) * volatility_fit)), 1)
            risk_pct = abs(result["entry"] - result["stop_loss"]) / result["entry"] * 100
            potential_tp3_pct = abs(result["tp3"] - result["entry"]) / result["entry"] * 100
            previous_volume = sum(candle["volume"] for candle in candles[-21:-1]) / max(1, len(candles[-21:-1]))
            previous_close = candles[-2]["close"] if len(candles) > 1 else candles[-1]["close"]
            anomaly = None
            if result["volume_ratio"] >= 2:
                anomaly = {"kind": "VOLUME_SPIKE", "label": "Volume spike", "strength": round(result["volume_ratio"] * 50)}
            elif abs(candles[-1]["close"] - previous_close) / max(previous_close, 1e-9) >= .03:
                anomaly = {"kind": "PRICE_SPIKE", "label": "Price spike", "strength": round(abs(candles[-1]["close"] - previous_close) / previous_close * 100)}
            elif aligned and result["ema"]["ema20"] != result["ema"]["ema50"]:
                anomaly = {"kind": "EMA_ALIGNMENT", "label": "EMA trend alignment", "strength": round(abs(result["ema"]["ema20"] - result["ema"]["ema50"]) / result["ema"]["ema50"] * 100, 2)}
            return {
                **market, "rsi": round(float(result["rsi"]), 2),
                "ema20": result["ema"]["ema20"], "ema50": result["ema"]["ema50"],
                "ema200": result["ema"]["ema200"], "trend": result["trend"],
                "direction": result["direction"], "confidence": result["confidence"],
                "entry": result["entry"], "stop_loss": result["stop_loss"],
                "tp1": result["tp1"], "tp2": result["tp2"], "tp3": result["tp3"],
                "support": result["support"], "resistance": result["resistance"],
                "volume_ratio": round(float(result["volume_ratio"]), 2),
                "risk_reward": result["risk_reward"], "smart_score": smart_score,
                "risk_pct": round(risk_pct, 2), "potential_tp3_pct": round(potential_tp3_pct, 2),
                "mtf_direction": mtf["direction"], "mtf_alignment": mtf["alignment"],
                "mtf_timeframes": mtf["timeframes"], "anomaly": anomaly,
                "volatility_pct": round(volatility_pct, 3),
                "volume_change_pct": round((candles[-1]["volume"] - previous_volume) / max(previous_volume, 1e-9) * 100, 2),
            }
        except Exception:
            return None

    inspected = await asyncio.gather(*(inspect(market) for market in market_list))
    results = [item for item in inspected if item is not None]
    results.sort(key=lambda item: item["volume"], reverse=True)
    paper = app.state.paper
    history_changed = False
    async with paper["lock"]:
        history = paper.setdefault("signal_history", [])
        now = datetime.now(timezone.utc).isoformat()
        for row in results:
            fingerprint = f'{row["symbol"]}|{interval}|{row["direction"]}|{row["entry"]:.10g}'
            existing = next((item for item in history[:200] if item.get("fingerprint") == fingerprint), None)
            status = "OPEN"
            if row["direction"] == "LONG":
                if row["price"] <= row["stop_loss"]: status = "STOP"
                elif row["price"] >= row["tp3"]: status = "TP3"
                elif row["price"] >= row["tp2"]: status = "TP2"
                elif row["price"] >= row["tp1"]: status = "TP1"
            elif row["direction"] == "SHORT":
                if row["price"] >= row["stop_loss"]: status = "STOP"
                elif row["price"] <= row["tp3"]: status = "TP3"
                elif row["price"] <= row["tp2"]: status = "TP2"
                elif row["price"] <= row["tp1"]: status = "TP1"
            if existing:
                if existing.get("status") != status:
                    existing.update({"status": status, "updated_at": now}); history_changed = True
                continue
            history.insert(0, {
                "id": f'signal-{int(datetime.now(timezone.utc).timestamp() * 1000)}-{len(history)}',
                "fingerprint": fingerprint, "symbol": row["symbol"], "timestamp": now,
                "signal": row["direction"], "score": row["smart_score"], "entry": row["entry"],
                "stop": row["stop_loss"], "tp1": row["tp1"], "tp2": row["tp2"], "tp3": row["tp3"],
                "timeframe": interval, "mtf": row["mtf_direction"], "risk_reward": row["risk_reward"],
                "status": status, "updated_at": now,
            }); history_changed = True
        del history[500:]
    if history_changed:
        asyncio.create_task(persist_paper_snapshot(app))
    alert_changed = False
    async with paper["lock"]:
        for alert in paper.get("alerts", []):
            if not alert.get("active"):
                continue
            row = next((item for item in results if item["symbol"] == alert.get("symbol")), None)
            if not row:
                continue
            bullish = sum(item["direction"] == "LONG" for item in row["mtf_timeframes"])
            bearish = sum(item["direction"] == "SHORT" for item in row["mtf_timeframes"])
            mtf_match = alert.get("mtf") == "ANY" or (alert.get("mtf") == "BULLISH_3" and bullish >= 3) or (alert.get("mtf") == "BULLISH_4" and bullish == 4) or (alert.get("mtf") == "BEARISH_3" and bearish >= 3) or (alert.get("mtf") == "BEARISH_4" and bearish == 4)
            matches = (alert.get("signal") == "ANY" or alert.get("signal") == row["direction"]) and row["smart_score"] >= alert.get("score_min", 0) and row["rsi"] >= alert.get("rsi_min", 0) and (not alert.get("volume_spike") or row["volume_ratio"] >= 2) and (not alert.get("price_crosses_ema20") or row["price"] >= row["ema20"]) and mtf_match
            if matches:
                add_paper_notification(paper, "SCANNER ALARM", f'{row["symbol"]} alarm koşulları sağlandı · {row["direction"]} · skor {row["smart_score"]}')
                alert["last_triggered_at"] = datetime.now(timezone.utc).isoformat()
                alert_changed = True
    if alert_changed:
        asyncio.create_task(persist_paper_snapshot(app))
    ANALYSIS_UNIVERSE_CACHE[interval] = (time.monotonic(), results)
    return {"cached": False, "interval": interval, "results": results[:limit]}


@app.get("/api/signal-history/{symbol}")
async def signal_history(symbol: str, limit: int = Query(12, ge=1, le=50)):
    await refresh_decision_memory()
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    rows = [item for item in app.state.paper.get("signal_history", []) if item.get("symbol") == safe_symbol]
    return {"symbol": safe_symbol, "history": rows[:limit], "available": bool(rows)}


@app.get("/api/signal-performance/{symbol}")
async def signal_performance(symbol: str):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    rows = [item for item in app.state.paper.get("signal_history", []) if item.get("symbol") == safe_symbol and item.get("status") != "OPEN"]
    if len(rows) < 5:
        return {"available": False, "message": "Not enough historical data", "total_signals": len(rows)}
    total = len(rows)
    return {
        "available": True, "total_signals": total,
        "tp1_hit_rate": round(sum(item.get("status") in {"TP1", "TP2", "TP3"} for item in rows) / total * 100, 1),
        "tp2_hit_rate": round(sum(item.get("status") in {"TP2", "TP3"} for item in rows) / total * 100, 1),
        "tp3_hit_rate": round(sum(item.get("status") == "TP3" for item in rows) / total * 100, 1),
        "stop_rate": round(sum(item.get("status") == "STOP" for item in rows) / total * 100, 1),
        "average_risk_reward": round(sum(float(item.get("risk_reward") or 0) for item in rows) / total, 2),
    }


@app.get("/api/scanner-alerts")
async def scanner_alerts():
    return {"alerts": app.state.paper.get("alerts", [])}


@app.post("/api/scanner-alerts")
async def scanner_alert_create(alert: ScannerAlert):
    safe_symbol = "".join(char for char in alert.symbol.upper() if char.isalnum())
    if not safe_symbol.endswith("USDT"):
        raise HTTPException(422, "Alarm için geçerli bir USDT paritesi seçin")
    item = {
        **alert.model_dump(), "id": f"alert-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
        "symbol": safe_symbol, "active": True, "created_at": datetime.now(timezone.utc).isoformat(),
        "last_triggered_at": None,
    }
    app.state.paper.setdefault("alerts", []).insert(0, item)
    await persist_paper_snapshot(app)
    return item


@app.delete("/api/scanner-alerts/{alert_id}")
async def scanner_alert_disable(alert_id: str):
    item = next((row for row in app.state.paper.get("alerts", []) if row.get("id") == alert_id), None)
    if not item:
        raise HTTPException(404, "Alarm bulunamadı")
    item["active"] = False
    await persist_paper_snapshot(app)
    return item


async def consensus_from_candles(safe_symbol: str, candles_by_interval: dict[str, list[dict]]) -> dict:
    """Build the shared MTF decision from already fetched candle snapshots."""
    intervals = [("15m", 0.20), ("1h", 0.30), ("4h", 0.30), ("1d", 0.20)]

    async def inspect(interval: str, weight: float):
        result = await asyncio.to_thread(analyze, candles_by_interval[interval])
        return {
            "timeframe": interval,
            "weight": weight,
            "direction": result["direction"],
            "confidence": result["confidence"],
            "trend": result["trend"],
            "radar_level": result["radar"]["trap_level"],
        }

    timeframes = await asyncio.gather(*(inspect(interval, weight) for interval, weight in intervals))
    scores = {"LONG": 0.0, "SHORT": 0.0, "BEKLE": 0.0}
    for item in timeframes:
        scores[item["direction"]] += item["weight"] * item["confidence"] / 100
    dominant_direction = max(scores, key=scores.get)
    alignment = round(scores[dominant_direction] * 100)
    matching = sum(1 for item in timeframes if item["direction"] == dominant_direction)
    by_timeframe = {item["timeframe"]: item["direction"] for item in timeframes}
    entry_direction = by_timeframe["15m"]
    higher_timeframe_confirmation = (
        entry_direction in {"LONG", "SHORT"}
        and by_timeframe["1h"] == entry_direction
        and by_timeframe["4h"] == entry_direction
        and by_timeframe["1d"] == entry_direction
    )
    # Keep the 15m signal as the entry direction only after 1h and 4h confirm it.
    direction = entry_direction if higher_timeframe_confirmation else "BEKLE"
    if higher_timeframe_confirmation and matching == 4 and alignment >= 70:
        verdict, permission = "GÜÇLÜ ONAY", True
        reason = "Dört zaman dilimi aynı yönü doğruluyor."
    elif higher_timeframe_confirmation and matching >= 2 and alignment >= 55:
        verdict, permission = "KISMİ ONAY", False
        reason = "Yön baskın ancak işlem öncesi mum kapanışı ve Tuzak Radarı kontrol edilmeli."
    else:
        verdict, permission = "UYUMSUZ", False
        reason = "Zaman dilimleri aynı görüşte değil; yeni pozisyon için beklemek daha güvenli."
    payload = {
        "symbol": safe_symbol, "direction": direction, "alignment": alignment,
        "verdict": verdict, "entry_permission": permission, "reason": reason,
        "timeframes": timeframes,
    }
    return payload


@app.get("/api/consensus/{symbol}")
async def multi_timeframe_consensus(symbol: str):
    """15m, 1h, 4h ve 1d analizlerini birleştirir; emir izni vermez."""
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    cached = CONSENSUS_CACHE.get(safe_symbol)
    if cached and time.monotonic() - cached[0] < 45:
        return {**cached[1], "cached": True}

    candles_by_interval = {
        interval: candles
        for interval, candles in zip(
            ("15m", "1h", "4h", "1d"),
            await asyncio.gather(*(fetch_candles(safe_symbol, interval, 260) for interval in ("15m", "1h", "4h", "1d"))),
        )
    }
    payload = await consensus_from_candles(safe_symbol, candles_by_interval)
    CONSENSUS_CACHE[safe_symbol] = (time.monotonic(), payload)
    return {**payload, "cached": False}


async def market_guard(symbol: str) -> dict:
    """Piyasa rejimini BTC referansı ve seçili paritenin yapısıyla ölçer.

    Bu katman bir tahmin değildir; otomatik Paper Bot için ek güvenlik filtresidir.
    """
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    cached = GUARD_CACHE.get(safe_symbol)
    if cached and time.monotonic() - cached[0] < 20:
        return {**cached[1], "cached": True}

    if safe_symbol == "BTCUSDT":
        btc_candles = await fetch_candles("BTCUSDT", "15m", 260)
        selected_candles = btc_candles
    else:
        selected_candles, btc_candles = await asyncio.gather(
            fetch_candles(safe_symbol, "15m", 260),
            fetch_candles("BTCUSDT", "15m", 260),
        )
    selected = analyze(selected_candles)
    btc = analyze(btc_candles)
    btc_atr_pct = round((btc["atr"] / btc["entry"]) * 100, 2) if btc["entry"] else 0.0
    last_candle_move_pct = round(abs((btc_candles[-1]["close"] / btc_candles[-2]["close"] - 1) * 100), 2)
    direction_conflict = (
        selected["direction"] in {"LONG", "SHORT"}
        and btc["direction"] in {"LONG", "SHORT"}
        and selected["direction"] != btc["direction"]
    )

    risk_score = 0
    reasons: list[str] = []
    if btc_atr_pct >= 1.2:
        risk_score += 38
        reasons.append("BTC volatilitesi yüksek")
    elif btc_atr_pct >= 0.7:
        risk_score += 18
        reasons.append("BTC volatilitesi artıyor")
    if last_candle_move_pct >= 0.75:
        risk_score += 25
        reasons.append("BTC son mum hareketi sert")
    if btc["radar"]["trap_score"] >= 45:
        risk_score += 18
        reasons.append("BTC tuzak riski yüksek")
    if selected["radar"]["trap_score"] >= 40:
        risk_score += 18
        reasons.append("Seçili paritede tuzak riski yükseldi")
    if direction_conflict:
        risk_score += 20
        reasons.append("Parite yönü BTC ile uyumsuz")
    if selected["adx"] < 18:
        risk_score += 10
        reasons.append("Parite trend gücü zayıf")
    risk_score = min(100, risk_score)

    trend_aligned = (
        selected["direction"] in {"LONG", "SHORT"}
        and selected["direction"] == btc["direction"]
        and selected["adx"] >= 18
        and btc["adx"] >= 20
    )
    if risk_score >= 55:
        market_mode, auto_allowed = "KORUMA MODU", False
        reason = reasons[0] if reasons else "Piyasa koşulları belirsiz"
    elif trend_aligned and risk_score <= 35:
        market_mode, auto_allowed = "TREND UYUMLU", True
        reason = "BTC ve parite aynı yönde, trend gücü yeterli."
    elif selected["adx"] < 18 or btc["adx"] < 20:
        market_mode, auto_allowed = "SIKIŞMA / BEKLE", False
        reason = "Trend gücü otomatik giriş için yeterli değil."
    else:
        market_mode, auto_allowed = "TEMKİNLİ İZLE", False
        reason = reasons[0] if reasons else "Otomatik giriş için ek onay bekleniyor."

    payload = {
        "symbol": safe_symbol,
        "market_mode": market_mode,
        "auto_allowed": auto_allowed,
        "risk_score": risk_score,
        "reason": reason,
        "btc_direction": btc["direction"],
        "btc_adx": round(btc["adx"], 1),
        "volatility_pct": btc_atr_pct,
        "last_candle_move_pct": last_candle_move_pct,
        "symbol_direction": selected["direction"],
    }
    GUARD_CACHE[safe_symbol] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/guard/{symbol}")
async def market_guard_endpoint(symbol: str):
    return await market_guard(symbol)


async def market_regime(symbol: str, interval: str = "15m") -> dict:
    """Piyasanın işlem yapılabilir rejimini kapanmış mumlardan sınıflandırır.

    Bu motor sinyal üretmez. Mevcut sinyalin trend, sıkışma veya yüksek
    volatilite ortamında otomatik Paper Bot için uygun olup olmadığını söyler.
    """
    if interval not in ALLOWED_INTERVALS:
        raise HTTPException(400, "Desteklenmeyen zaman dilimi")
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    key = (safe_symbol, interval)
    cached = REGIME_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 20:
        return {**cached[1], "cached": True}

    candles = await fetch_candles(safe_symbol, interval, 260)
    closed = candles[:-1]
    if len(closed) < 225:
        raise HTTPException(422, "Piyasa rejimi için yeterli kapanmış mum yok")
    analysis = analyze(closed)
    price = closed[-1]["close"]
    atr_pct = (analysis["atr"] / price) * 100 if price else 0.0
    recent = closed[-16:]
    recent_range = max(candle["high"] for candle in recent) - min(candle["low"] for candle in recent)
    range_atr = recent_range / max(analysis["atr"], 0.00000001)
    ema = analysis["ema"]
    ema_spread_pct = abs(ema["ema20"] - ema["ema200"]) / price * 100 if price else 0.0
    last_move_pct = abs((closed[-1]["close"] / closed[-2]["close"] - 1) * 100)
    direction = analysis["direction"]
    ema_aligned = (
        (direction == "LONG" and ema["ema20"] > ema["ema50"] > ema["ema200"])
        or (direction == "SHORT" and ema["ema20"] < ema["ema50"] < ema["ema200"])
    )
    strength = min(100, round(analysis["adx"] * 2.2 + ema_spread_pct * 16 + analysis["confidence"] * 0.18))

    if atr_pct >= 1.25 or last_move_pct >= max(0.9, atr_pct * 1.35):
        label, auto_allowed, policy, multiplier = "YÜKSEK VOLATİLİTE", False, "RİSK KAPALI", 0.0
        reason = "Fiyat hareketi normal rejimin üzerinde; otomatik Paper girişi için bekleme tercih edildi."
    elif (
        direction in {"LONG", "SHORT"}
        and analysis["adx"] >= 24
        and ema_aligned
        and analysis["radar"]["trap_score"] <= 30
    ):
        label, auto_allowed, policy = f"GÜÇLÜ {direction} TREND", True, "TREND TAKİP"
        multiplier = 1.15 if strength >= 80 and analysis["confidence"] >= 85 else 1.0
        reason = "Kapanmış mumlar, EMA hizası ve trend gücü aynı yönü destekliyor."
    elif analysis["adx"] < 18 and range_atr <= 4.2:
        label, auto_allowed, policy, multiplier = "SIKIŞMA HAZIRLIK", False, "KIRILIM BEKLE", 0.0
        reason = "Fiyat dar bir aralıkta; yön teyidi olmadan otomatik giriş kapalı tutuluyor."
    else:
        label, auto_allowed, policy, multiplier = "YATAY / TEMKİNLİ", False, "SEÇİCİ BEKLE", 0.0
        reason = "Trend ve volatilite ölçümleri otomatik Paper girişi için dengeli değil."

    payload = {
        "symbol": safe_symbol, "interval": interval, "label": label,
        "auto_allowed": auto_allowed, "preferred_direction": direction if auto_allowed else "BEKLE",
        "entry_policy": policy, "strength": strength, "atr_pct": round(atr_pct, 3),
        "range_atr": round(range_atr, 2), "position_multiplier": multiplier,
        "reason": reason,
    }
    REGIME_CACHE[key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/regime/{symbol}")
async def market_regime_endpoint(symbol: str, interval: str = "15m"):
    return await market_regime(symbol, interval)


def regime_stability_gate(symbol: str, interval: str, regime: dict, observed_at: float | None = None) -> dict:
    """Rejim/yönün kısa süreli gürültü yerine tutarlı olduğunu doğrular.

    Aynı rejim, yön ve otomatik politika üç ayrı gözlemde korunmadan Paper
    Bot'un giriş izni açılmaz. Bu yalnızca yerel Paper karar zinciridir.
    """
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    current_time = time.monotonic() if observed_at is None else observed_at
    key = (safe_symbol, interval)
    direction = str(regime.get("preferred_direction") or "BEKLE").upper()
    label = str(regime.get("label") or "BİLİNMİYOR")
    regime_open = bool(regime.get("auto_allowed"))
    signature = (label, direction, regime_open)
    history = list(REGIME_STABILITY_HISTORY.get(key, []))
    if not history or current_time - float(history[-1]["at"]) >= 8:
        history.append({"at": current_time, "signature": signature})
    history = [item for item in history if current_time - float(item["at"]) <= 90][-6:]
    REGIME_STABILITY_HISTORY[key] = history

    stable_samples = 0
    for item in reversed(history):
        if item["signature"] != signature:
            break
        stable_samples += 1
    required_samples = 3
    stability_score = min(100, round((stable_samples / required_samples) * 100))
    if not regime_open:
        mode, auto_allowed = "REJİM KAPALI", False
        reason = f"{label} rejimi otomatik girişe uygun değil; kararlılık sayacı yalnızca izleme amaçlı tutuluyor."
    elif stable_samples < required_samples:
        mode, auto_allowed = "REJİM OTURUYOR", False
        reason = f"{label} / {direction} {stable_samples}/{required_samples} kez tutarlı görüldü; ani rejim değişimine karşı bekleniyor."
    else:
        mode, auto_allowed = "REJİM SABİT", True
        reason = f"{label} / {direction} {stable_samples}/{required_samples} ardışık gözlemde tutarlı; Paper Bot için ek rejim doğrulaması tamamlandı."

    return {
        "symbol": safe_symbol, "interval": interval, "regime_label": label,
        "direction": direction, "mode": mode, "auto_allowed": auto_allowed,
        "stable_samples": stable_samples, "required_samples": required_samples,
        "stability_score": stability_score, "reason": reason,
    }


@app.get("/api/regime/stability/{symbol}")
async def regime_stability_endpoint(symbol: str, interval: str = "15m"):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    try:
        regime = await market_regime(safe_symbol, interval)
        return regime_stability_gate(safe_symbol, interval, regime)
    except Exception:
        # Piyasa sağlayıcısı kısa süreli 5xx verdiğinde bu yardımcı gösterge
        # bütün kokpiti düşürmemeli. Otomatik giriş kapalı, geçerli bir şema
        # döndürülür ve bir sonraki ölçümde kendiliğinden yeniden denenir.
        return {
            "symbol": safe_symbol, "interval": interval,
            "regime_label": "VERİ BEKLENİYOR", "direction": "BEKLE",
            "mode": "GÜVENLİ BEKLEME", "auto_allowed": False,
            "stable_samples": 0, "required_samples": 3,
            "stability_score": 0,
            "reason": "Canlı rejim verisi geçici olarak alınamadı; Paper girişleri güvenlik için kapalı tutuluyor.",
            "degraded": True,
        }


async def liquidity_shield(symbol: str) -> dict:
    """Açık emir defterinden kayma ve tek-yönlü likidite riskini ölçer.

    Sadece Binance herkese açık depth verisini okur. Bu katman borsaya emir
    göndermez; Paper Bot'un sığ veya aşırı dengesiz defterde beklemesini sağlar.
    """
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    cached = LIQUIDITY_CACHE.get(safe_symbol)
    if cached and time.monotonic() - cached[0] < 5:
        return {**cached[1], "cached": True}
    base = {
        "symbol": safe_symbol, "mode": "LİKİDİTE ÖLÇÜLÜYOR", "auto_allowed": False,
        "liquidity_score": 0, "spread_bps": 0.0, "imbalance_pct": 0.0,
        "depth_usdt": 0.0, "best_bid": None, "best_ask": None,
    }
    try:
        response = await app.state.http.get(f"{BINANCE_API}/api/v3/depth", params={"symbol": safe_symbol, "limit": 20})
        response.raise_for_status()
        order_book = response.json()
        bids = [(float(price), float(quantity)) for price, quantity in order_book["bids"][:20]]
        asks = [(float(price), float(quantity)) for price, quantity in order_book["asks"][:20]]
        if not bids or not asks:
            raise ValueError("Boş emir defteri")
        best_bid, best_ask = bids[0][0], asks[0][0]
        mid_price = (best_bid + best_ask) / 2
        if mid_price <= 0:
            raise ValueError("Geçersiz fiyat")
        bid_depth = sum(price * quantity for price, quantity in bids)
        ask_depth = sum(price * quantity for price, quantity in asks)
        depth_usdt = bid_depth + ask_depth
        spread_bps = (best_ask - best_bid) / mid_price * 10_000
        imbalance_pct = ((bid_depth - ask_depth) / depth_usdt * 100) if depth_usdt else 0.0
        thin_penalty = max(0.0, (60_000 - depth_usdt) / 60_000 * 40)
        imbalance_penalty = max(0.0, abs(imbalance_pct) - 45) * 0.6
        liquidity_score = max(0, min(100, round(100 - spread_bps * 4 - thin_penalty - imbalance_penalty)))
        if depth_usdt < 60_000:
            mode, auto_allowed = "SIĞ LİKİDİTE", False
            reason = f"İlk 20 kademe toplamı yaklaşık {depth_usdt:,.0f} USDT; kayma riski için bekleniyor."
        elif spread_bps > 18:
            mode, auto_allowed = "SPREAD UYARISI", False
            reason = f"Alış-satış aralığı {spread_bps:.1f} bp; giriş fiyatında kayma riski yüksek."
        elif abs(imbalance_pct) > 75:
            mode, auto_allowed = "TEK YÖNLÜ DEFTER", False
            reason = f"Emir defteri dengesizliği %{imbalance_pct:+.0f}; ani çekilme riskine karşı bekleniyor."
        else:
            mode, auto_allowed = "DENGELİ LİKİDİTE", True
            reason = f"Spread {spread_bps:.1f} bp, defter derinliği {depth_usdt:,.0f} USDT ve denge %{imbalance_pct:+.0f}."
        payload = {
            **base, "mode": mode, "auto_allowed": auto_allowed, "liquidity_score": liquidity_score,
            "spread_bps": round(spread_bps, 2), "imbalance_pct": round(imbalance_pct, 1),
            "depth_usdt": round(depth_usdt, 2), "best_bid": best_bid, "best_ask": best_ask,
            "reason": reason,
        }
    except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
        payload = {**base, "mode": "LİKİDİTE VERİSİ BEKLENİYOR", "reason": f"Emir defteri doğrulanamadı: {str(exc)[:80]}"}
    LIQUIDITY_CACHE[safe_symbol] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/liquidity/{symbol}")
async def liquidity_shield_endpoint(symbol: str):
    return await liquidity_shield(symbol)


def candle_returns(candles: list[dict], count: int = 60) -> list[float]:
    closes = [float(candle["close"]) for candle in candles[-(count + 1):]]
    return [closes[index] / closes[index - 1] - 1 for index in range(1, len(closes)) if closes[index - 1] > 0]


def pearson_correlation(left: list[float], right: list[float]) -> float:
    size = min(len(left), len(right))
    if size < 12:
        return 0.0
    left, right = left[-size:], right[-size:]
    left_mean, right_mean = sum(left) / size, sum(right) / size
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right))
    left_variance = sum((x - left_mean) ** 2 for x in left)
    right_variance = sum((y - right_mean) ** 2 for y in right)
    denominator = (left_variance * right_variance) ** 0.5
    return max(-1.0, min(1.0, numerator / denominator)) if denominator else 0.0


async def portfolio_exposure_guard(symbol: str, direction: str) -> dict:
    """Aynı yönlü, yüksek korelasyonlu Paper pozisyon kümelerini engeller."""
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    safe_direction = direction.upper()
    paper = app.state.paper
    async with paper["lock"]:
        open_positions = [
            {"symbol": item["symbol"], "direction": item["direction"]}
            for item in paper["positions"] if item["status"] == "AÇIK"
        ]
    signature = tuple(sorted((item["symbol"], item["direction"]) for item in open_positions))
    key = (safe_symbol, safe_direction, signature)
    cached = PORTFOLIO_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 20:
        return {**cached[1], "cached": True}

    base = {
        "symbol": safe_symbol, "direction": safe_direction, "open_positions": len(open_positions),
        "matched_symbol": None, "correlation_pct": 0.0, "heat": 0,
    }
    if safe_direction not in {"LONG", "SHORT"}:
        payload = {**base, "mode": "SİNYAL BEKLİYOR", "auto_allowed": False, "reason": "Önce net LONG veya SHORT sinyali bekleniyor."}
    elif not open_positions:
        payload = {**base, "mode": "DENGELİ PORTFÖY", "auto_allowed": True, "reason": "Açık Paper pozisyon yok; korelasyon yoğunluğu oluşmadı."}
    elif len(open_positions) >= 3:
        payload = {**base, "mode": "POZİSYON SINIRI", "auto_allowed": False, "heat": 100, "reason": "Maksimum üç açık Paper pozisyon sınırına ulaşıldı."}
    else:
        symbols = [safe_symbol, *[item["symbol"] for item in open_positions]]
        unique_symbols = list(dict.fromkeys(symbols))
        rows = await asyncio.gather(*(fetch_candles(item, "15m", 72) for item in unique_symbols), return_exceptions=True)
        if any(isinstance(row, Exception) for row in rows):
            payload = {**base, "mode": "KORELASYON ÖLÇÜLÜYOR", "auto_allowed": False, "reason": "Karşılaştırmalı mum verisi hazır olmadan otomatik giriş açılmıyor."}
        else:
            candle_map = dict(zip(unique_symbols, rows))
            candidate_returns = candle_returns(candle_map[safe_symbol])
            same_direction = [item for item in open_positions if item["direction"] == safe_direction]
            matches = [
                (item["symbol"], pearson_correlation(candidate_returns, candle_returns(candle_map[item["symbol"]])))
                for item in same_direction
            ]
            matched_symbol, highest = max(matches, key=lambda item: item[1], default=(None, 0.0))
            correlation_pct = round(highest * 100, 1)
            heat = min(100, round((len(open_positions) / 3) * 45 + max(0.0, highest) * 55))
            if highest >= 0.82:
                mode, auto_allowed = "KORELASYON KİLİDİ", False
                reason = f"{matched_symbol} ile aynı yönlü %{correlation_pct:.0f} korelasyon görüldü; yeni yoğunlaşma engellendi."
            elif highest >= 0.65:
                mode, auto_allowed = "YOĞUNLAŞMA UYARISI", False
                reason = f"{matched_symbol} ile aynı yönlü %{correlation_pct:.0f} korelasyon var; daha bağımsız sinyal bekleniyor."
            else:
                mode, auto_allowed = "DENGELİ PORTFÖY", True
                reason = "Açık pozisyonlarla belirgin aynı yönlü korelasyon bulunmadı."
            payload = {
                **base, "mode": mode, "auto_allowed": auto_allowed, "matched_symbol": matched_symbol,
                "correlation_pct": correlation_pct, "heat": heat, "reason": reason,
            }
    PORTFOLIO_CACHE[key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/portfolio/guard/{symbol}")
async def portfolio_exposure_endpoint(symbol: str, direction: str = "BEKLE"):
    return await portfolio_exposure_guard(symbol, direction)


def adaptive_quality_gate(paper: dict, regime_label: str, direction: str) -> dict:
    """Kapanmış otomatik Paper sonuçlarından ihtiyatlı bir güven eşiği üretir.

    Bu fonksiyon tahmin üretmez ve gerçek emir göndermez. Aynı rejim/yön için
    yeterli Paper örneklemi oluştuğunda, botun yeni girişlerde ne kadar seçici
    davranacağını ayarlar. Zayıf geçmişte kapıyı kilitlemek yerine eşiği
    yükseltir; böylece yalnızca çok güçlü yeni sinyaller ölçümlere katkı yapar.
    """
    safe_regime = str(regime_label or "GENEL").upper()
    safe_direction = str(direction or "BEKLE").upper()
    matching = [
        trade for trade in paper.get("trades", [])
        if trade.get("source") == "AUTO"
        and str(trade.get("regime_label") or "GENEL").upper() == safe_regime
        and str(trade.get("direction") or "").upper() == safe_direction
        and trade.get("realized_pnl") is not None
    ][:12]
    results = [float(trade["realized_pnl"]) for trade in matching]
    sample_size = len(results)
    wins = sum(1 for value in results if value > 0)
    net_pnl = round(sum(results), 2)
    win_rate = round((wins / sample_size) * 100, 1) if sample_size else 0.0
    average_pnl = round(net_pnl / sample_size, 2) if sample_size else 0.0

    if safe_direction not in {"LONG", "SHORT"}:
        mode, min_confidence = "SİNYAL BEKLİYOR", 100
        auto_allowed = False
        reason = "Önce net LONG veya SHORT yönü oluşmalı; kalite eşiği yönsüz sinyalde çalışmaz."
    elif sample_size < 6:
        mode, min_confidence = "ÖĞRENİYOR", 78
        auto_allowed = True
        reason = f"{safe_regime} / {safe_direction} için {sample_size}/6 otomatik Paper örneği var; temel güven eşiği korunuyor."
    elif win_rate < 45 or net_pnl < -15:
        mode, min_confidence = "KALİBRASYON MODU", 88
        auto_allowed = True
        reason = f"Son {sample_size} Paper sonuç zayıf (%{win_rate:.0f}, {net_pnl:+.2f} USDT); bot yalnızca %88+ güvenli sinyalleri deneyecek."
    elif win_rate < 55 or net_pnl < 0:
        mode, min_confidence = "SEÇİCİ MOD", 84
        auto_allowed = True
        reason = f"Son {sample_size} Paper sonucu karışık (%{win_rate:.0f}, {net_pnl:+.2f} USDT); güven eşiği %84'e yükseltildi."
    else:
        mode, min_confidence = "KANITLI MOD", 78
        auto_allowed = True
        reason = f"Son {sample_size} Paper sonucu tutarlı (%{win_rate:.0f}, {net_pnl:+.2f} USDT); temel seçicilik eşiği korunuyor."

    return {
        "regime": safe_regime, "direction": safe_direction, "mode": mode,
        "auto_allowed": auto_allowed, "min_confidence": min_confidence,
        "sample_size": sample_size, "wins": wins, "win_rate": win_rate,
        "net_pnl": net_pnl, "average_pnl": average_pnl, "reason": reason,
    }


@app.get("/api/adaptive/gate")
async def adaptive_quality_endpoint(regime: str = "GENEL", direction: str = "BEKLE"):
    return adaptive_quality_gate(app.state.paper, regime, direction)


SESSION_WINDOWS = [
    ("ASYA", "Asya · 00–08 UTC", 0, 8),
    ("AVRUPA", "Avrupa · 08–16 UTC", 8, 16),
    ("ABD", "ABD · 16–24 UTC", 16, 24),
]


def session_window(moment: datetime) -> tuple[str, str]:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    hour = moment.astimezone(timezone.utc).hour
    for key, label, start, end in SESSION_WINDOWS:
        if start <= hour < end:
            return key, label
    return "ASYA", "Asya · 00–08 UTC"


def trade_utc_hour(trade: dict) -> int | None:
    closed_at = trade.get("closed_at")
    if not closed_at:
        return None
    try:
        parsed = datetime.fromisoformat(str(closed_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).hour


def session_intelligence(
    paper: dict,
    symbol: str | None = None,
    regime_label: str | None = None,
    direction: str | None = None,
    now: datetime | None = None,
) -> dict:
    """Paper sonuçlarını UTC seanslarına ayırır; zayıf saatte seçiciliği artırır."""
    safe_symbol = "".join(char for char in (symbol or "").upper() if char.isalnum())
    safe_regime = str(regime_label or "").upper()
    safe_direction = str(direction or "").upper()
    all_auto = [
        trade for trade in paper.get("trades", [])
        if trade.get("source") == "AUTO" and trade.get("realized_pnl") is not None
    ]
    scoped = [
        trade for trade in all_auto
        if (not safe_symbol or trade.get("symbol") == safe_symbol)
        and (not safe_regime or str(trade.get("regime_label") or "").upper() == safe_regime)
        and (safe_direction not in {"LONG", "SHORT"} or trade.get("direction") == safe_direction)
    ]
    sample = scoped if len(scoped) >= 4 else all_auto
    scope = "PARİTE + REJİM" if sample is scoped else "GENEL PAPER"
    moment = now or datetime.now(timezone.utc)
    current_key, current_label = session_window(moment)
    sessions = []
    for key, label, start, end in SESSION_WINDOWS:
        entries = [trade for trade in sample if (hour := trade_utc_hour(trade)) is not None and start <= hour < end]
        pnl_values = [float(trade["realized_pnl"]) for trade in entries]
        count = len(pnl_values)
        wins = sum(1 for value in pnl_values if value > 0)
        win_rate = round((wins / count) * 100, 1) if count else 0.0
        net_pnl = round(sum(pnl_values), 2)
        if count < 3:
            status, bonus = "ÖĞRENİYOR", 0
        elif net_pnl < 0 or win_rate < 45:
            status, bonus = "SEÇİCİ", 6
        else:
            status, bonus = "KANITLI", 0
        sessions.append({
            "key": key, "label": label, "trades": count, "wins": wins,
            "win_rate": win_rate, "net_pnl": net_pnl, "status": status,
            "confidence_bonus": bonus,
        })
    current = next(item for item in sessions if item["key"] == current_key)
    candidates = [item for item in sessions if item["trades"] >= 3]
    weakest = min(candidates, key=lambda item: item["net_pnl"], default=None)
    if current["status"] == "SEÇİCİ":
        reason = f"{current_label} seansı geçmişte zayıf; otomatik giriş güven eşiğine +{current['confidence_bonus']} puan eklendi."
    elif current["status"] == "KANITLI":
        reason = f"{current_label} seansında yeterli Paper sonucu dengeli; ek güven cezası yok."
    else:
        reason = f"{current_label} seansı için yeterli Paper örneği oluşmadı; temel kurallar korunuyor."
    return {
        "scope": scope, "sample_size": len(sample), "current_session": current,
        "sessions": sessions, "weakest_session": weakest, "confidence_bonus": current["confidence_bonus"],
        "auto_allowed": True, "reason": reason,
    }


@app.get("/api/session/intelligence")
async def session_intelligence_endpoint(symbol: str = "", regime: str = "", direction: str = "BEKLE"):
    return session_intelligence(app.state.paper, symbol, regime, direction)


def add_paper_notification(paper: dict, kind: str, message: str) -> None:
    notifications = paper.setdefault("notifications", [])
    if notifications and notifications[0].get("kind") == kind and notifications[0].get("message") == message:
        return
    notifications.insert(0, {"kind": kind, "message": message, "created_at": datetime.now(timezone.utc).isoformat()})
    del notifications[20:]


def decision_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def record_decision_memory(
    paper: dict,
    *,
    symbol: str,
    direction: str,
    confidence: int | float | None,
    entry_price: float | int | None,
    decision: str,
    reason: str,
    source: str = "AUTO",
    gates: dict | None = None,
) -> dict | None:
    """Paper kararlarının sonradan karşılaştırılabilen, kalıcı hafızasını tutar."""
    safe_symbol = "".join(char for char in str(symbol).upper() if char.isalnum())
    safe_direction = str(direction).upper()
    try:
        safe_price = float(entry_price or 0)
    except (TypeError, ValueError):
        safe_price = 0.0
    if not safe_symbol or safe_direction not in {"LONG", "SHORT"} or safe_price <= 0:
        return None
    now = datetime.now(timezone.utc)
    short_reason = str(reason or "Karar açıklaması bekleniyor.")[:240]
    fingerprint = f"{safe_symbol}|{safe_direction}|{decision}|{short_reason[:96]}"
    memory = paper.setdefault("decision_memory", [])
    for existing in memory[:24]:
        if existing.get("fingerprint") != fingerprint:
            continue
        created_at = decision_time(existing.get("created_at"))
        if created_at and now - created_at < timedelta(minutes=10):
            return None
    event = {
        "id": f"decision-{int(now.timestamp() * 1000)}-{len(memory)}",
        "created_at": now.isoformat(), "symbol": safe_symbol, "direction": safe_direction,
        "confidence": round(float(confidence or 0), 1), "entry_price": round(safe_price, 10),
        "decision": str(decision).upper(), "source": source, "reason": short_reason,
        "gates": gates or {}, "reviews": {}, "fingerprint": fingerprint,
    }
    memory.insert(0, event)
    del memory[DECISION_MEMORY_LIMIT:]
    return event


def latest_decision_review(event: dict) -> dict | None:
    reviews = event.get("reviews", {})
    if not isinstance(reviews, dict):
        return None
    values = [item for item in reviews.values() if isinstance(item, dict)]
    return max(values, key=lambda item: int(item.get("minutes", 0)), default=None)


async def refresh_decision_memory() -> bool:
    """Süresi gelen karar kayıtlarını canlı fiyatla gözlemler; emir göndermez."""
    paper = app.state.paper
    now = datetime.now(timezone.utc)
    async with paper["lock"]:
        due: list[tuple[str, str, int]] = []
        for event in paper.get("decision_memory", []):
            created_at = decision_time(event.get("created_at"))
            if not created_at or not event.get("entry_price"):
                continue
            reviews = event.setdefault("reviews", {})
            for minutes in DECISION_REVIEW_MINUTES:
                review_key = str(minutes)
                if review_key not in reviews and now - created_at >= timedelta(minutes=minutes):
                    due.append((event.get("id", ""), event.get("symbol", ""), minutes))
    if not due:
        return False
    symbols = sorted({symbol for _, symbol, _ in due if symbol})
    prices: dict[str, float] = {}
    results = await asyncio.gather(*(latest_price(symbol) for symbol in symbols), return_exceptions=True)
    for symbol, result in zip(symbols, results):
        if not isinstance(result, Exception) and result > 0:
            prices[symbol] = float(result)
    if not prices:
        return False
    changed = False
    async with paper["lock"]:
        by_id = {event.get("id"): event for event in paper.get("decision_memory", [])}
        for event_id, symbol, minutes in due:
            event = by_id.get(event_id)
            if not event or symbol not in prices:
                continue
            reviews = event.setdefault("reviews", {})
            review_key = str(minutes)
            if review_key in reviews:
                continue
            entry_price = float(event["entry_price"])
            raw_return = (prices[symbol] / entry_price - 1) * 100
            direction_return = raw_return if event.get("direction") == "LONG" else -raw_return
            if direction_return > 0.15:
                status = "LEHTE"
            elif direction_return < -0.15:
                status = "ALEYTE"
            else:
                status = "NÖTR"
            reviews[review_key] = {
                "minutes": minutes, "price": round(prices[symbol], 10),
                "return_pct": round(direction_return, 3), "status": status,
                "observed_at": now.isoformat(),
            }
            changed = True
    if changed:
        asyncio.create_task(persist_paper_snapshot(app))
    return changed


def decision_blackbox_payload(paper: dict) -> dict:
    """Kararların sonradan nasıl geliştiğini açıklanabilir bir özet halinde sunar."""
    events = paper.get("decision_memory", [])
    blocked = [event for event in events if event.get("decision") == "ENGELLENDİ"]
    reviewed_blocked = [event for event in blocked if latest_decision_review(event) is not None]
    shield_hits = sum(
        1 for event in reviewed_blocked
        if float(latest_decision_review(event).get("return_pct", 0)) <= 0
    )
    missed_opportunities = sum(
        1 for event in reviewed_blocked
        if float(latest_decision_review(event).get("return_pct", 0)) > 0.20
    )
    shield_accuracy = round((shield_hits / len(reviewed_blocked)) * 100, 1) if reviewed_blocked else 0.0
    pending = sum(1 for event in events if latest_decision_review(event) is None)
    if len(reviewed_blocked) < 6:
        status = "ÖĞRENİYOR"
        summary = "Karar Kara Kutusu henüz yeterli karşı-olasılık örneği toplamıyor; filtreleri değiştirmek için erken."
    elif shield_accuracy >= 60:
        status = "KALKAN TUTARLI"
        summary = "İncelenen engellenmiş sinyallerin çoğu seçilen yönde lehte gelişmedi; koruma katmanları ihtiyatlı davranıyor."
    elif missed_opportunities > shield_hits:
        status = "FIRSAT MALİYETİ İZLENİYOR"
        summary = "Engellenen sinyallerin bir kısmı sonradan lehte gelişti; filtreler için daha fazla Paper örneğiyle kalibrasyon gerekli."
    else:
        status = "KALİBRASYON"
        summary = "Koruma sonuçları karışık; sistem yeni Paper kanıtı toplarken temkinli eşikleri koruyor."
    event_views = []
    for event in events[:8]:
        event_views.append({**event, "latest_review": latest_decision_review(event)})
    return {
        "records": len(events), "pending": pending,
        "opened": sum(1 for event in events if event.get("decision") == "AÇILDI"),
        "shadow": sum(1 for event in events if event.get("decision") == "GÖLGE"),
        "blocked": len(blocked), "reviewed_rejections": len(reviewed_blocked),
        "shield_hits": shield_hits, "shield_accuracy_pct": shield_accuracy,
        "missed_opportunities": missed_opportunities, "status": status,
        "summary": summary, "events": event_views,
        "method_note": "15, 30 ve 60 dakika sonrası, karar anındaki yön için canlı fiyatla gözlemlenir. Bu kayıt analiz içindir; yatırım tavsiyesi veya performans garantisi değildir.",
    }


@app.get("/api/decision/blackbox")
async def decision_blackbox_endpoint():
    await refresh_decision_memory()
    return decision_blackbox_payload(app.state.paper)


def emergency_brake_payload(paper: dict) -> dict:
    brake = paper.setdefault("emergency_brake", {})
    brake.setdefault("active", False)
    brake.setdefault("reason", "Acil fren kapalı.")
    brake.setdefault("source", None)
    brake.setdefault("triggered_at", None)
    return {
        "active": bool(brake["active"]), "reason": brake["reason"],
        "source": brake["source"], "triggered_at": brake["triggered_at"],
    }


def activate_emergency_brake(paper: dict, reason: str, source: str) -> bool:
    brake = emergency_brake_payload(paper)
    if brake["active"] and brake["reason"] == reason:
        return False
    paper["emergency_brake"].update({
        "active": True, "reason": reason, "source": source,
        "triggered_at": datetime.now(timezone.utc).isoformat(),
    })
    add_paper_notification(paper, "ACİL FREN", f"{source}: {reason}")
    return True


def clear_emergency_brake(paper: dict) -> bool:
    brake = emergency_brake_payload(paper)
    if not brake["active"]:
        return False
    paper["emergency_brake"].update({
        "active": False, "reason": "Kullanıcı kontrolüyle fren kaldırıldı; Paper Bot yeniden güvenlik kapılarından geçecek.",
        "source": "KULLANICI", "triggered_at": None,
    })
    add_paper_notification(paper, "FREN KALDIRILDI", "Acil fren kaldırıldı; otomatik girişler yine tüm güvenlik kapılarına tabidir.")
    return True


def shadow_payload(paper: dict) -> dict:
    shadow = paper.setdefault("shadow", {"enabled": False, "events": []})
    shadow.setdefault("enabled", False)
    shadow.setdefault("events", [])
    return {"enabled": bool(shadow["enabled"]), "events": shadow["events"][:10]}


async def record_shadow_candidate(
    candidate: dict,
    amount: float,
    regime: dict,
    liquidity: dict,
    session: dict,
    confidence_floor: int,
) -> dict:
    """Tüm kapıları geçen ama Paper pozisyon açılmayan adayları saklar."""
    paper = app.state.paper
    async with paper["lock"]:
        shadow = paper.setdefault("shadow", {"enabled": False, "events": []})
        event = {
            "created_at": datetime.now(timezone.utc).isoformat(), "symbol": candidate["symbol"],
            "direction": candidate["direction"], "confidence": candidate["confidence"], "amount": amount,
            "regime": regime["label"], "liquidity": liquidity["mode"],
            "session": session["current_session"]["key"], "confidence_floor": confidence_floor,
            "message": "Tüm güvenlik kapıları geçti; Gölge Modu nedeniyle Paper pozisyon açılmadı.",
        }
        shadow.setdefault("events", []).insert(0, event)
        del shadow["events"][20:]
        add_paper_notification(paper, "GÖLGE KAYDI", f"{candidate['display']} için {candidate['direction']} simülasyon kararı kaydedildi.")
        record_decision_memory(
            paper, symbol=candidate["symbol"], direction=candidate["direction"],
            confidence=candidate["confidence"], entry_price=candidate.get("price"),
            decision="GÖLGE", reason=event["message"], source="AUTO",
            gates={"rejim": regime["label"], "likidite": liquidity["mode"], "seans": session["current_session"]["status"]},
        )
    asyncio.create_task(persist_paper_snapshot(app))
    return event


def testnet_readiness() -> dict:
    """Report the real Binance Futures Demo transport used by V26."""
    credential_check = globals().get("demo_credentials_configured")
    credentials_configured = bool(credential_check()) if callable(credential_check) else bool(
        os.getenv("BINANCE_DEMO_API_KEY", "").strip() and os.getenv("BINANCE_DEMO_SECRET_KEY", "").strip()
    )
    application = globals().get("app")
    demo_state = getattr(getattr(application, "state", None), "binance_demo", {}) if application is not None else {}
    connected = bool(demo_state.get("connected"))
    arm_check = globals().get("demo_armed")
    armed_now = bool(demo_state) and bool(arm_check(demo_state)) if callable(arm_check) else False
    status = "10 DK EMİR KİLİDİ AÇIK" if armed_now else "BAĞLI · KİLİTLİ" if connected else "BAĞLANTI BEKLİYOR" if credentials_configured else "ANAHTAR BEKLİYOR"
    return {
        "mode": "V26 BINANCE FUTURES DEMO", "status": status, "credentials_configured": credentials_configured,
        "orders_enabled": armed_now,
        "reason": "Emirler yalnızca Binance Futures Demo hesabına gider; gerçek para kanalı ayrı ve varsayılan olarak kilitlidir.",
        "checks": [
            {"label": "Binance Futures Demo anahtarları", "status": "ALGILANDI" if credentials_configured else "BEKLENİYOR", "passed": credentials_configured},
            {"label": "Salt-okunur hesap bağlantısı", "status": "BAĞLI" if connected else "BEKLENİYOR", "passed": connected},
            {"label": "10 dakikalık emir izni", "status": "AÇIK" if armed_now else "KİLİTLİ", "passed": armed_now},
        ],
    }


@app.get("/api/testnet/readiness")
async def testnet_readiness_endpoint():
    return testnet_readiness()


async def decision_explanation(symbol: str, interval: str = "15m") -> dict:
    """Tüm karar kapılarının anlık, açıklanabilir özetini üretir; emir vermez."""
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    key = (safe_symbol, interval)
    cached = EXPLANATION_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < 12:
        return {**cached[1], "cached": True}
    try:
        guard, regime, liquidity, gate, freshness, consensus = await asyncio.gather(
            market_guard(safe_symbol), market_regime(safe_symbol, interval),
            liquidity_shield(safe_symbol), candle_close_gate(safe_symbol, interval),
            signal_freshness(safe_symbol, interval), multi_timeframe_consensus(safe_symbol),
        )
        direction = regime["preferred_direction"] if regime["preferred_direction"] in {"LONG", "SHORT"} else gate["direction"]
        adaptive = adaptive_quality_gate(app.state.paper, regime["label"], direction)
        session = session_intelligence(app.state.paper, safe_symbol, regime["label"], direction)
        checks = [
            {"key": "piyasa", "label": "Piyasa Kalkanı", "passed": bool(guard["auto_allowed"]), "detail": guard["market_mode"]},
            {"key": "rejim", "label": "Piyasa Rejimi", "passed": bool(regime["auto_allowed"]), "detail": regime["entry_policy"]},
            {"key": "onay", "label": "Çoklu Zaman Onayı", "passed": bool(consensus["entry_permission"] and consensus["direction"] == direction), "detail": consensus["verdict"]},
            {"key": "likidite", "label": "Likidite", "passed": bool(liquidity["auto_allowed"]), "detail": liquidity["mode"]},
            {"key": "mum", "label": "Mum Kapanışı", "passed": bool(gate["entry_allowed"] and gate["direction"] == direction), "detail": gate["status"]},
            {"key": "tazelik", "label": "Fiyat Tazeliği", "passed": bool(freshness["auto_allowed"]), "detail": freshness["status"]},
            {"key": "kalite", "label": "Seans + Kalite", "passed": bool(adaptive["auto_allowed"] and direction in {"LONG", "SHORT"}), "detail": f"{session['current_session']['status']} · eşik %{min(95, adaptive['min_confidence'] + session['confidence_bonus'])}"},
        ]
        readiness_score = sum(15 if item["key"] != "kalite" else 10 for item in checks if item["passed"])
        blockers = [item for item in checks if not item["passed"]]
        if not blockers:
            status = "PAPER GİRİŞE YAKIN"
            summary = f"{safe_symbol} için {direction} yönünde anlık kapılar açık. Paper Bot yine rejim sabitleme ve portföy kontrolünü ayrıca doğrular."
            next_action = "Rejim sabitliği ve portföy yoğunluğu korunursa yalnızca Paper/Gölge kararına izin verilir."
        else:
            status = "KORUMA BEKLİYOR"
            summary = f"{safe_symbol} için {direction if direction in {'LONG', 'SHORT'} else 'net yön'} henüz tüm kapılardan geçmedi. İlk bekleten katman: {blockers[0]['label']} · {blockers[0]['detail']}."
            next_action = "Kapıların tamamı geçmeden Paper Bot yeni otomatik pozisyon açmaz."
        payload = {
            "symbol": safe_symbol, "interval": interval, "direction": direction,
            "readiness_score": readiness_score, "status": status, "summary": summary,
            "next_action": next_action, "checks": checks,
            "session": session["current_session"], "adaptive": adaptive,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception:
        payload = {
            "symbol": safe_symbol, "interval": interval, "direction": "BEKLE",
            "readiness_score": 0, "status": "KARAR KAPILARI ÖLÇÜLÜYOR",
            "summary": "Canlı karar bileşenleri henüz birlikte doğrulanamadı; Paper Bot güvenli biçimde bekliyor.",
            "next_action": "Piyasa verisi tamamlanana kadar otomatik giriş kapalı kalır.",
            "checks": [], "session": None, "adaptive": None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    EXPLANATION_CACHE[key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.get("/api/decision/explain/{symbol}")
async def decision_explanation_endpoint(symbol: str, interval: str = "15m"):
    return await decision_explanation(symbol, interval)


class PaperOrder(BaseModel):
    symbol: str
    direction: Literal["LONG", "SHORT"]
    amount: float = Field(default=100, ge=10, le=2_000)
    stop_loss: float = Field(gt=0)
    take_profit: float = Field(gt=0)
    tp2: float | None = Field(default=None, gt=0)
    tp3: float | None = Field(default=None, gt=0)
    max_holding_minutes: int = Field(default=1440, ge=15, le=10080)
    source: Literal["MANUAL", "AUTO", "DEMO"] = "MANUAL"
    signal_confidence: int | None = Field(default=None, ge=0, le=100)
    guard_mode: str | None = None
    gate_status: str | None = None
    freshness_status: str | None = None
    entry_drift_atr: float | None = Field(default=None, ge=0)
    regime_label: str | None = None
    regime_policy: str | None = None
    portfolio_mode: str | None = None
    portfolio_correlation: float | None = None
    adaptive_mode: str | None = None
    adaptive_confidence_floor: int | None = Field(default=None, ge=0, le=100)
    stability_mode: str | None = None
    stability_samples: int | None = Field(default=None, ge=0, le=10)
    liquidity_mode: str | None = None
    liquidity_score: int | None = Field(default=None, ge=0, le=100)
    liquidity_spread_bps: float | None = Field(default=None, ge=0)
    session_label: str | None = None
    session_mode: str | None = None
    session_confidence_bonus: int | None = Field(default=None, ge=0, le=20)


class ScannerAlert(BaseModel):
    symbol: str
    signal: Literal["LONG", "SHORT", "ANY"] = "ANY"
    score_min: float = Field(default=0, ge=0, le=100)
    rsi_min: float = Field(default=0, ge=0, le=100)
    volume_spike: bool = False
    price_crosses_ema20: bool = False
    mtf: Literal["ANY", "BULLISH_3", "BULLISH_4", "BEARISH_3", "BEARISH_4"] = "ANY"


class PaperLimitOrder(BaseModel):
    """Kullanıcının belirlediği, yalnızca yerel Paper cüzdanda çalışan limit planı."""
    symbol: str
    direction: Literal["LONG", "SHORT"]
    amount: float = Field(default=50, ge=10, le=2_000)
    limit_price: float = Field(gt=0)
    stop_loss: float = Field(gt=0)
    tp1: float = Field(gt=0)
    tp2: float = Field(gt=0)
    tp3: float = Field(gt=0)
    grid_count: int = Field(default=8, ge=3, le=24)
    grid_lower: float | None = Field(default=None, gt=0)
    grid_upper: float | None = Field(default=None, gt=0)
    expires_minutes: int = Field(default=1440, ge=15, le=10080)


async def archive_trade_decision(order: PaperOrder, entry_price: float) -> None:
    """Veritabanı erişilebilir olduğunda Paper kararını kalıcı arşive yazar.

    Arşiv hatası Paper işlemini hiçbir zaman engellemez.
    """
    pool = app.state.db_pool
    if pool is None:
        return
    safe_symbol = "".join(char for char in order.symbol.upper() if char.isalnum())
    explanation = " · ".join(filter(None, [
        f"Paper {order.source}",
        f"Güven %{order.signal_confidence}" if order.signal_confidence is not None else None,
        order.guard_mode,
        order.gate_status,
        order.freshness_status,
        order.regime_label,
        order.regime_policy,
        order.portfolio_mode,
        f"Korelasyon %{order.portfolio_correlation:.0f}" if order.portfolio_correlation is not None else None,
        order.adaptive_mode,
        f"Kalite eşiği %{order.adaptive_confidence_floor}" if order.adaptive_confidence_floor is not None else None,
        order.stability_mode,
        f"Rejim doğrulama {order.stability_samples}/3" if order.stability_samples is not None else None,
        order.liquidity_mode,
        f"Spread {order.liquidity_spread_bps:.1f} bp" if order.liquidity_spread_bps is not None else None,
        order.session_label,
        order.session_mode,
        f"Seans eşiği +{order.session_confidence_bonus}" if order.session_confidence_bonus else None,
        f"Sapma {order.entry_drift_atr:.2f} ATR" if order.entry_drift_atr is not None else None,
    ]))
    try:
        await pool.execute(
            """
            INSERT INTO trade_decisions (symbol, direction, confidence, entry_price, stop_loss, tp1, explanation)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            safe_symbol, order.direction, float(order.signal_confidence or 0), entry_price,
            order.stop_loss, order.take_profit, explanation or "Paper işlemi",
        )
    except Exception:
        return


@app.get("/api/archive/decisions")
async def decision_archive(limit: int = Query(8, ge=1, le=20)):
    pool = app.state.db_pool
    if pool is None:
        return {"available": False, "entries": [], "message": "TimescaleDB bağlantısı bekleniyor."}
    try:
        rows = await pool.fetch(
            """
            SELECT id, created_at, symbol, direction, confidence, entry_price, stop_loss, tp1, explanation
            FROM trade_decisions ORDER BY id DESC LIMIT $1
            """,
            limit,
        )
    except Exception:
        return {"available": False, "entries": [], "message": "Karar arşivi şu an okunamıyor."}
    entries = [
        {
            "id": row["id"], "created_at": row["created_at"].isoformat(), "symbol": row["symbol"],
            "direction": row["direction"], "confidence": float(row["confidence"]),
            "entry_price": float(row["entry_price"]) if row["entry_price"] is not None else None,
            "stop_loss": float(row["stop_loss"]) if row["stop_loss"] is not None else None,
            "tp1": float(row["tp1"]) if row["tp1"] is not None else None,
            "explanation": row["explanation"],
        }
        for row in rows
    ]
    return {"available": True, "entries": entries, "message": "Karar arşivi TimescaleDB'den okunuyor."}


async def latest_price(symbol: str) -> float:
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    try:
        response = await app.state.http.get(f"{BINANCE_API}/api/v3/ticker/price", params={"symbol": safe_symbol})
        response.raise_for_status()
        return float(response.json()["price"])
    except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
        raise HTTPException(502, f"Canlı fiyat alınamadı: {exc}") from exc


def reset_daily_risk_if_needed(paper: dict) -> None:
    risk = paper["risk"]
    today = datetime.now(timezone.utc).date().isoformat()
    if risk["day"] != today:
        risk.update({
            "day": today,
            "daily_realized_pnl": 0.0,
            "consecutive_losses": 0,
            "cooldown_until": None,
            "daily_locked": False,
            "reason": "Yeni gün başladı; Risk Kasası aktif.",
        })


def paper_risk_payload(paper: dict) -> dict:
    reset_daily_risk_if_needed(paper)
    risk = paper["risk"]
    now = datetime.now(timezone.utc)
    cooldown_until = risk.get("cooldown_until")
    if cooldown_until and now >= datetime.fromisoformat(cooldown_until):
        risk["cooldown_until"] = None
        if not risk["daily_locked"]:
            risk["reason"] = "Soğuma süresi tamamlandı; Risk Kasası korumalı çalışıyor."
    cooling_down = bool(risk.get("cooldown_until"))
    auto_paused = risk["daily_locked"] or cooling_down
    if risk["daily_locked"]:
        status = "GÜNLÜK KİLİT"
    elif cooling_down:
        status = "SOĞUMA MODU"
    else:
        status = "KORUMALI"
    remaining_loss_budget = max(0.0, risk["daily_loss_limit"] + min(0.0, risk["daily_realized_pnl"]))
    return {
        "status": status,
        "auto_paused": auto_paused,
        "daily_realized_pnl": round(risk["daily_realized_pnl"], 2),
        "daily_loss_limit": risk["daily_loss_limit"],
        "remaining_loss_budget": round(remaining_loss_budget, 2),
        "consecutive_losses": risk["consecutive_losses"],
        "consecutive_loss_limit": risk["consecutive_loss_limit"],
        "cooldown_until": risk.get("cooldown_until"),
        "reason": risk["reason"],
    }


def register_paper_result(paper: dict, net_pnl: float) -> None:
    reset_daily_risk_if_needed(paper)
    risk = paper["risk"]
    risk["daily_realized_pnl"] += net_pnl
    if net_pnl < 0:
        risk["consecutive_losses"] += 1
    else:
        risk["consecutive_losses"] = 0
    if risk["daily_realized_pnl"] <= -risk["daily_loss_limit"]:
        risk["daily_locked"] = True
        risk["cooldown_until"] = None
        risk["reason"] = "Günlük sanal zarar limiti doldu; otomatik giriş yeni güne kadar kilitlendi."
    elif risk["consecutive_losses"] >= risk["consecutive_loss_limit"]:
        risk["cooldown_until"] = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
        risk["reason"] = "İki ardışık sanal kayıp görüldü; Paper Bot 30 dakika soğumada."


def auto_position_size(confidence: int | float, risk_score: int | float) -> float:
    """Yüksek kalite sinyalde bile Paper maruziyetini sınırlı tutar."""
    if confidence >= 90 and risk_score <= 15:
        return 125.0
    if confidence >= 84 and risk_score <= 25:
        return 100.0
    return 75.0


def paper_bot_policy(training_mode: bool) -> dict:
    """Paper Bot'un iki açık ve denetlenebilir çalışma politikasını döndürür.

    Eğitim modu yalnızca sanal hesapta daha hızlı örnek üretir. Günlük zarar
    kilidi, acil fren, pozisyon sınırı ve gerçek/Testnet emir kilidi değişmez.
    """
    if training_mode:
        return {
            "mode": "HIZLI PAPER EĞİTİM",
            "minimum_confidence": 70,
            "maximum_trap_score": 60,
            "confidence_floor": 72,
            "cycle_seconds": 15,
            "amount_cap": 75.0,
            "orders_enabled": False,
            "testnet_orders_enabled": False,
        }
    return {
        "mode": "SIKI PAPER DOĞRULAMA",
        "minimum_confidence": 78,
        "maximum_trap_score": 35,
        "confidence_floor": 95,
        "cycle_seconds": 45,
        "amount_cap": 125.0,
        "orders_enabled": False,
        "testnet_orders_enabled": False,
    }


def v20_profile_policy(profile: str) -> dict:
    """V25.1 Paper Autopilot profillerini tek ve denetlenebilir tabloda tutar."""
    normalized = str(profile or "DENGELI").strip().upper().replace("İ", "I")
    policies = {
        "TEMKINLI": {
            "label": "TEMKİNLİ", "mode": "V25.1 TEMKİNLİ OTONOM PAPER",
            "minimum_confidence": 84, "maximum_trap_score": 35,
            "confidence_floor": 88, "cycle_seconds": 45,
        },
        "DENGELI": {
            "label": "DENGELİ", "mode": "V25.1 DENGELİ OTONOM PAPER",
            "minimum_confidence": 75, "maximum_trap_score": 50,
            "confidence_floor": 78, "cycle_seconds": 20,
        },
        "HIZLI": {
            "label": "HIZLI", "mode": "V25.1 HIZLI OTONOM PAPER",
            "minimum_confidence": 70, "maximum_trap_score": 60,
            "confidence_floor": 72, "cycle_seconds": 15,
        },
    }
    key = normalized if normalized in policies else "DENGELI"
    # Kept local as well as in ``paper_autonomy`` so legacy AST-based safety
    # tests can execute this policy function in isolation.
    allocations = {
        "TEMKINLI": {
            "universe_size": 18, "shortlist_size": 6, "risk_per_trade_pct": 0.18,
            "max_allocation_pct": 8.0, "max_total_exposure_pct": 24.0,
            "minimum_projected_net_usdt": 2.5,
        },
        "DENGELI": {
            "universe_size": 24, "shortlist_size": 8, "risk_per_trade_pct": 0.30,
            "max_allocation_pct": 15.0, "max_total_exposure_pct": 45.0,
            "minimum_projected_net_usdt": 5.0,
        },
        "HIZLI": {
            "universe_size": 30, "shortlist_size": 10, "risk_per_trade_pct": 0.40,
            "max_allocation_pct": 18.0, "max_total_exposure_pct": 54.0,
            "minimum_projected_net_usdt": 5.0,
        },
    }
    allocation = {
        **allocations[key], "maximum_positions": 3, "maximum_order_usdt": 2_000.0,
        "daily_reference_usdt": 5.0, "profit_guaranteed": False,
    }
    return {
        "profile": key, **policies[key],
        **{
            name: allocation[name] for name in (
                "universe_size", "shortlist_size", "risk_per_trade_pct",
                "max_allocation_pct", "max_total_exposure_pct",
                "minimum_projected_net_usdt", "maximum_positions",
                "maximum_order_usdt", "daily_reference_usdt", "profit_guaranteed",
            )
        },
        # Kept for older clients; sizing is now calculated dynamically and this
        # value is only the absolute per-position ceiling.
        "amount_cap": allocation["maximum_order_usdt"],
        "orders_enabled": False, "testnet_orders_enabled": False,
        "paper_only": True,
    }


def active_paper_bot_policy(bot: dict) -> dict:
    """Sıkı doğrulama kapatılmadıysa V20 Paper profilini uygular."""
    if not bool(bot.get("training_mode", True)):
        strict = paper_bot_policy(False)
        allocation = autonomy_policy("TEMKINLI")
        return {
            "profile": "SIKI", "label": "SIKI", **strict,
            **{
                name: allocation[name] for name in (
                    "universe_size", "shortlist_size", "risk_per_trade_pct",
                    "max_allocation_pct", "max_total_exposure_pct",
                    "minimum_projected_net_usdt", "maximum_positions",
                    "maximum_order_usdt", "daily_reference_usdt", "profit_guaranteed",
                )
            },
        }
    return v20_profile_policy(str(bot.get("profile") or "DENGELI"))


def paper_training_liquidity_allowed(liquidity: dict, training_mode: bool) -> bool:
    """Eğitim modunda yalnızca düşük kaymalı tek-yönlü defteri tolere eder."""
    if bool(liquidity.get("auto_allowed")):
        return True
    if not training_mode or liquidity.get("mode") != "TEK YÖNLÜ DEFTER":
        return False
    return (
        float(liquidity.get("spread_bps") or 999.0) <= 12.0
        and float(liquidity.get("depth_usdt") or 0.0) >= 30_000.0
        and int(liquidity.get("liquidity_score") or 0) >= 45
    )


def v20_target_plan(
    entry_price: float,
    stop_loss: float,
    direction: str,
    tp1: float | None = None,
    tp2: float | None = None,
    tp3: float | None = None,
) -> dict:
    """Giriş ve stop mesafesinden sıralı 1R/2R/3R Paper hedefleri üretir."""
    entry = float(entry_price)
    stop = float(stop_loss)
    side = str(direction).upper()
    if entry <= 0 or stop <= 0 or side not in {"LONG", "SHORT"}:
        raise ValueError("Geçerli giriş, stop ve yön gerekli")
    if (side == "LONG" and stop >= entry) or (side == "SHORT" and stop <= entry):
        raise ValueError("Stop seviyesi işlem yönüne uygun değil")
    risk = abs(entry - stop)
    sign = 1.0 if side == "LONG" else -1.0
    defaults = [entry + sign * risk * multiple for multiple in (1.0, 2.0, 3.0)]
    supplied = [tp1, tp2, tp3]
    values = [float(value) if value is not None else defaults[index] for index, value in enumerate(supplied)]
    valid_order = values[0] < values[1] < values[2] if side == "LONG" else values[0] > values[1] > values[2]
    valid_side = all(value > entry for value in values) if side == "LONG" else all(value < entry for value in values)
    if not valid_order or not valid_side:
        values = defaults
    return {
        "tp1": round(values[0], 10), "tp2": round(values[1], 10),
        "tp3": round(values[2], 10), "risk_per_unit": round(risk, 10),
        "risk_reward": 3.0, "partial_plan": [35, 35, 30],
    }


def paper_grid_levels(lower: float, upper: float, count: int) -> list[float]:
    """Alt ve üst sınır arasında, iki uç dahil eşit Paper grid kademeleri üretir."""
    low, high = float(lower), float(upper)
    steps = int(count)
    if low <= 0 or high <= low or steps < 3 or steps > 24:
        raise ValueError("Grid alt/üst sınırı ve 3–24 arası kademe sayısı gerekli")
    distance = (high - low) / (steps - 1)
    return [round(low + distance * index, 10) for index in range(steps)]


def paper_limit_triggered(direction: str, limit_price: float, market_price: float) -> bool:
    """Paper limit emrinin canlı fiyat tarafından geçilip geçilmediğini söyler."""
    side = str(direction).upper()
    if side == "LONG":
        return float(market_price) <= float(limit_price)
    if side == "SHORT":
        return float(market_price) >= float(limit_price)
    return False


def normalize_paper_limit_plan(plan: PaperLimitOrder) -> dict:
    """Limit, stop, hedef ve grid sırasını borsa emri üretmeden doğrular."""
    entry = float(plan.limit_price)
    targets = v20_target_plan(entry, float(plan.stop_loss), plan.direction, plan.tp1, plan.tp2, plan.tp3)
    supplied = [round(float(plan.tp1), 10), round(float(plan.tp2), 10), round(float(plan.tp3), 10)]
    calculated = [targets["tp1"], targets["tp2"], targets["tp3"]]
    if supplied != calculated:
        raise ValueError(
            "LONG için STOP < LİMİT < TP1 < TP2 < TP3; SHORT için TP3 < TP2 < TP1 < LİMİT < STOP olmalı"
        )
    default_lower = min(float(plan.stop_loss), float(plan.tp3))
    default_upper = max(float(plan.stop_loss), float(plan.tp3))
    lower = float(plan.grid_lower) if plan.grid_lower is not None else default_lower
    upper = float(plan.grid_upper) if plan.grid_upper is not None else default_upper
    if not lower <= entry <= upper:
        raise ValueError("Limit giriş fiyatı grid alt ve üst sınırının içinde olmalı")
    levels = paper_grid_levels(lower, upper, int(plan.grid_count))
    return {
        "limit_price": round(entry, 10), "stop_loss": round(float(plan.stop_loss), 10),
        "tp1": targets["tp1"], "tp2": targets["tp2"], "tp3": targets["tp3"],
        "grid_lower": round(lower, 10), "grid_upper": round(upper, 10),
        "grid_count": int(plan.grid_count), "grid_levels": levels,
    }


def build_paper_position(
    order: PaperOrder,
    entry_price: float,
    position_id: int,
    limit_order: dict | None = None,
) -> dict:
    """Piyasa veya limit kaynağından aynı denetlenebilir Paper pozisyonunu kurar."""
    entry = float(entry_price)
    target_plan = v20_target_plan(entry, order.stop_loss, order.direction, order.take_profit, order.tp2, order.tp3)
    quantity = float(order.amount) / entry
    mapped_grid = list(limit_order.get("grid_levels", [])) if limit_order else []
    return {
        "id": int(position_id), "symbol": "".join(char for char in order.symbol.upper() if char.isalnum()),
        "direction": order.direction, "amount": float(order.amount), "quantity": quantity,
        "entry_price": entry, "original_amount": float(order.amount), "original_quantity": quantity,
        "current_price": entry, "stop_loss": float(order.stop_loss), "take_profit": target_plan["tp3"],
        "tp1": target_plan["tp1"], "tp2": target_plan["tp2"], "tp3": target_plan["tp3"],
        "partial_plan": target_plan["partial_plan"], "partial_targets_hit": [],
        "partial_realized_pnl": 0.0, "fee": 0.0, "lifecycle_events": [],
        "max_holding_minutes": int(order.max_holding_minutes),
        "unrealized_pnl": 0.0, "status": "AÇIK", "source": order.source,
        "entry_order_type": "LIMIT" if limit_order else "PİYASA",
        "limit_order_id": limit_order.get("id") if limit_order else None,
        "limit_price": float(limit_order.get("limit_price")) if limit_order else None,
        "grid_levels": mapped_grid,
        "grid_lower": limit_order.get("grid_lower") if limit_order else None,
        "grid_upper": limit_order.get("grid_upper") if limit_order else None,
        "grid_count": limit_order.get("grid_count") if limit_order else 0,
        "initial_stop_loss": float(order.stop_loss), "peak_price": entry,
        "protection_status": "PLAN KORUNUYOR", "protection_level": 0, "protection_updated_at": None,
        "signal_confidence": order.signal_confidence, "guard_mode": order.guard_mode,
        "gate_status": order.gate_status, "freshness_status": order.freshness_status,
        "entry_drift_atr": order.entry_drift_atr, "regime_label": order.regime_label,
        "regime_policy": order.regime_policy, "portfolio_mode": order.portfolio_mode,
        "portfolio_correlation": order.portfolio_correlation, "adaptive_mode": order.adaptive_mode,
        "adaptive_confidence_floor": order.adaptive_confidence_floor,
        "stability_mode": order.stability_mode, "stability_samples": order.stability_samples,
        "liquidity_mode": order.liquidity_mode, "liquidity_score": order.liquidity_score,
        "liquidity_spread_bps": order.liquidity_spread_bps, "session_label": order.session_label,
        "session_mode": order.session_mode, "session_confidence_bonus": order.session_confidence_bonus,
        "opened_at": datetime.now(timezone.utc).isoformat(),
    }


def demo_paper_trade_plan(analysis: dict, live_price: float) -> dict:
    """Güncel fiyata göre küçük ve geçerli bir Paper eğitim planı üretir."""
    price = max(float(live_price), 0.00000001)
    direction = str(analysis.get("direction") or "BEKLE").upper()
    ema = analysis.get("ema") if isinstance(analysis.get("ema"), dict) else {}
    if direction not in {"LONG", "SHORT"}:
        direction = "LONG" if float(ema.get("ema20") or price) >= float(ema.get("ema50") or price) else "SHORT"
    atr = abs(float(analysis.get("atr") or 0.0))
    risk_distance = max(atr * 0.75, price * 0.0025)
    if direction == "LONG":
        stop_loss = price - risk_distance
    else:
        stop_loss = price + risk_distance
    targets = v20_target_plan(price, stop_loss, direction)
    return {
        "direction": direction,
        "entry_reference": round(price, 10),
        "stop_loss": round(max(stop_loss, price * 0.01), 10),
        "take_profit": targets["tp1"],
        "tp1": targets["tp1"], "tp2": targets["tp2"], "tp3": targets["tp3"],
        "risk_reward": targets["risk_reward"], "partial_plan": targets["partial_plan"],
        "amount": 50.0,
        "paper_only": True,
        "orders_enabled": False,
        "testnet_orders_enabled": False,
    }


def advance_v20_position(position: dict, current_price: float, now: datetime | None = None) -> dict:
    """Tek bir Paper pozisyonunu TP1/TP2/TP3, stop ve zaman aşımıyla ilerletir.

    Fonksiyon yalnızca verilen sözlüğü değiştirir; bakiye veya borsa bağlantısına
    dokunmaz. Dönen ``realized_delta`` çağıran katman tarafından bir kez yazılır.
    """
    if position.get("status") != "AÇIK":
        return {"changed": False, "realized_delta": 0.0, "closed": False, "event": None}
    now_value = now or datetime.now(timezone.utc)
    entry = float(position["entry_price"])
    side = str(position["direction"]).upper()
    current = float(current_price)
    original_stop = float(position.get("initial_stop_loss", position["stop_loss"]))
    targets = v20_target_plan(
        entry, original_stop, side,
        position.get("tp1") or position.get("take_profit"),
        position.get("tp2"), position.get("tp3"),
    )
    position.update({"tp1": targets["tp1"], "tp2": targets["tp2"], "tp3": targets["tp3"], "take_profit": targets["tp3"]})
    original_quantity = max(float(position.get("original_quantity") or position.get("quantity") or 0.0), 0.0)
    original_amount = max(float(position.get("original_amount") or position.get("amount") or 0.0), 0.0)
    position.setdefault("original_quantity", original_quantity)
    position.setdefault("original_amount", original_amount)
    position.setdefault("partial_targets_hit", [])
    position.setdefault("partial_realized_pnl", 0.0)
    position.setdefault("fee", 0.0)
    position.setdefault("lifecycle_events", [])
    position.setdefault("max_holding_minutes", 1440)
    position["current_price"] = current
    changed = True
    realized_delta = 0.0
    last_event = None

    def close_quantity(quantity: float, label: str) -> None:
        nonlocal realized_delta, last_event
        remaining = max(float(position.get("quantity") or 0.0), 0.0)
        closing = min(max(quantity, 0.0), remaining)
        if closing <= 0 or original_quantity <= 0:
            return
        gross = (current - entry) * closing
        if side == "SHORT":
            gross *= -1
        released_amount = original_amount * (closing / original_quantity)
        fee = released_amount * 0.001
        net = gross - fee
        position["quantity"] = max(0.0, remaining - closing)
        position["amount"] = max(0.0, float(position.get("amount") or 0.0) - released_amount)
        position["partial_realized_pnl"] = float(position.get("partial_realized_pnl") or 0.0) + net
        position["fee"] = float(position.get("fee") or 0.0) + fee
        realized_delta += net
        last_event = {
            "kind": label, "price": round(current, 10), "quantity": round(closing, 12),
            "net_pnl": round(net, 8), "created_at": now_value.isoformat(),
        }
        position["lifecycle_events"].insert(0, last_event)
        del position["lifecycle_events"][20:]

    stop = float(position["stop_loss"])
    hit_stop = current <= stop if side == "LONG" else current >= stop
    if hit_stop:
        close_quantity(float(position.get("quantity") or 0.0), "STOP")
        position["status"] = "STOP"
    else:
        hit_list = position["partial_targets_hit"]
        checks = (("TP1", targets["tp1"], 0.35), ("TP2", targets["tp2"], 0.35), ("TP3", targets["tp3"], 1.0))
        for label, target, share in checks:
            reached = current >= target if side == "LONG" else current <= target
            if not reached or label in hit_list:
                continue
            closing = float(position.get("quantity") or 0.0) if label == "TP3" else original_quantity * share
            close_quantity(closing, label)
            hit_list.append(label)
            if label == "TP1":
                position["stop_loss"] = max(float(position["stop_loss"]), entry) if side == "LONG" else min(float(position["stop_loss"]), entry)
                position["protection_level"] = max(1, int(position.get("protection_level") or 0))
                position["protection_status"] = "TP1 · ZARARSIZ STOP"
            elif label == "TP2":
                one_r_lock = entry + (targets["risk_per_unit"] * 0.75 if side == "LONG" else -targets["risk_per_unit"] * 0.75)
                position["stop_loss"] = max(float(position["stop_loss"]), one_r_lock) if side == "LONG" else min(float(position["stop_loss"]), one_r_lock)
                position["protection_level"] = max(2, int(position.get("protection_level") or 0))
                position["protection_status"] = "TP2 · KÂR KİLİTLİ"
            else:
                position["status"] = "TP3"
                break

        opened_at = position.get("opened_at")
        try:
            opened = datetime.fromisoformat(str(opened_at).replace("Z", "+00:00"))
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError):
            opened = now_value
        expired = now_value - opened >= timedelta(minutes=int(position.get("max_holding_minutes") or 1440))
        if position.get("status") == "AÇIK" and expired:
            close_quantity(float(position.get("quantity") or 0.0), "ZAMAN")
            position["status"] = "ZAMAN"

    remaining = max(float(position.get("quantity") or 0.0), 0.0)
    unrealized = (current - entry) * remaining
    if side == "SHORT":
        unrealized *= -1
    position["unrealized_pnl"] = unrealized if position.get("status") == "AÇIK" else 0.0
    position["protection_updated_at"] = now_value.isoformat()
    closed = position.get("status") != "AÇIK"
    if closed:
        position["quantity"] = 0.0
        position["amount"] = 0.0
        position["realized_pnl"] = float(position.get("partial_realized_pnl") or 0.0)
        position["closed_at"] = now_value.isoformat()
    return {"changed": changed, "realized_delta": realized_delta, "closed": closed, "event": last_event}


def apply_position_protection(position: dict, current_price: float) -> bool:
    """Paper pozisyonunun stop seviyesini sadece kâr yönünde günceller.

    0,8R'de zararsız stop, 1,25R'de ise kârı kilitleyen takip stopu devreye girer.
    Bu fonksiyon yalnızca uygulama içi Paper durumunu değiştirir; borsaya emir göndermez.
    """
    changed = False
    entry = float(position["entry_price"])
    initial_stop = float(position.get("initial_stop_loss", position["stop_loss"]))
    if "initial_stop_loss" not in position:
        position["initial_stop_loss"] = initial_stop
        changed = True
    risk_per_unit = abs(entry - initial_stop)
    if risk_per_unit <= 0:
        return changed

    direction = position["direction"]
    peak = float(position.get("peak_price", entry))
    if "peak_price" not in position:
        position["peak_price"] = peak
        changed = True
    if direction == "LONG":
        new_peak = max(peak, current_price)
        progress_r = (new_peak - entry) / risk_per_unit
    else:
        new_peak = min(peak, current_price)
        progress_r = (entry - new_peak) / risk_per_unit
    if new_peak != peak:
        position["peak_price"] = new_peak
        changed = True

    level = int(position.get("protection_level", 0))
    current_stop = float(position["stop_loss"])
    next_stop, next_level = current_stop, level
    fee_buffer = min(risk_per_unit * 0.15, entry * 0.0004)
    if progress_r >= 0.8:
        break_even = entry + fee_buffer if direction == "LONG" else entry - fee_buffer
        next_stop = max(next_stop, break_even) if direction == "LONG" else min(next_stop, break_even)
        next_level = max(next_level, 1)
    if progress_r >= 1.25:
        trailing_stop = new_peak - risk_per_unit * 0.65 if direction == "LONG" else new_peak + risk_per_unit * 0.65
        next_stop = max(next_stop, trailing_stop) if direction == "LONG" else min(next_stop, trailing_stop)
        next_level = max(next_level, 2)

    if next_stop != current_stop:
        position["stop_loss"] = next_stop
        changed = True
    if next_level != level or "protection_status" not in position:
        position["protection_level"] = next_level
        position["protection_status"] = "KÂR KORUNUYOR" if next_level >= 2 else "ZARARSIZ STOP" if next_level == 1 else "PLAN KORUNUYOR"
        changed = True
    if changed:
        position["protection_updated_at"] = datetime.now(timezone.utc).isoformat()
    return changed


def paper_lifecycle_event_message(position: dict, event: dict) -> str:
    """Paper yaşam döngüsü olayını kullanıcıya okunur tek satıra dönüştürür."""
    symbol = str(position.get("symbol") or "PAPER")
    direction = str(position.get("direction") or "—")
    source = str(position.get("source") or "PAPER")
    kind = str(event.get("kind") or position.get("status") or "GÜNCELLEME")
    event_pnl = float(event.get("net_pnl") or 0.0)
    total_pnl = float(position.get("partial_realized_pnl") or position.get("realized_pnl") or 0.0)
    signed_event = f"{event_pnl:+.2f} USDT"
    signed_total = f"{total_pnl:+.2f} USDT"
    if kind in {"TP1", "TP2"} and position.get("status") == "AÇIK":
        return f"{symbol} {direction} · {kind} gerçekleşti · bölüm {signed_event} · toplam {signed_total} · pozisyon açık"
    close_label = {
        "TP3": "son hedefte kapandı",
        "STOP": "stop ile kapandı",
        "ZAMAN": "süre sonunda kapandı",
        "MANUEL": "elle kapatıldı",
    }.get(kind, "kapandı")
    return f"{symbol} {direction} · {close_label} · sonuç {signed_total} · {source}"


async def refresh_paper_positions() -> None:
    paper = app.state.paper
    async with paper["lock"]:
        live = [position.copy() for position in paper["positions"] if position["status"] == "AÇIK"]
    if not live:
        return
    prices = await asyncio.gather(*(latest_price(position["symbol"]) for position in live), return_exceptions=True)
    changed = False
    activity_events: list[tuple[str, str, str]] = []
    async with paper["lock"]:
        for position, current_price in zip(live, prices):
            if isinstance(current_price, Exception):
                continue
            stored = next((item for item in paper["positions"] if item["id"] == position["id"]), None)
            if not stored or stored["status"] != "AÇIK":
                continue
            known_events = {
                (str(item.get("kind") or ""), str(item.get("created_at") or ""))
                for item in stored.get("lifecycle_events", [])
            }
            outcome = advance_v20_position(stored, float(current_price))
            if outcome["realized_delta"]:
                paper["balance"] += float(outcome["realized_delta"])
            new_lifecycle_events = [
                item for item in stored.get("lifecycle_events", [])
                if (str(item.get("kind") or ""), str(item.get("created_at") or "")) not in known_events
            ]
            for lifecycle_event in reversed(new_lifecycle_events):
                event_kind = str(lifecycle_event.get("kind") or "PAPER")
                event_message = paper_lifecycle_event_message(stored, lifecycle_event)
                add_paper_notification(paper, event_kind, event_message)
                activity_events.append((event_kind, event_message, str(stored.get("symbol") or "")))
                changed = True
            if outcome["closed"]:
                register_paper_result(paper, float(stored.get("realized_pnl") or 0.0))
                paper["trades"].insert(0, stored.copy())
                changed = True
            elif apply_position_protection(stored, float(current_price)):
                changed = True
            elif outcome["changed"] or outcome["realized_delta"]:
                changed = True
    for event_kind, event_message, event_symbol in activity_events:
        record_bot_event(event_kind, event_message, event_symbol)
    if changed:
        asyncio.create_task(persist_paper_snapshot(app))


async def refresh_paper_limit_orders() -> None:
    """Bekleyen yerel Paper limitlerini canlı fiyatla tetikler; borsaya emir göndermez."""
    paper = app.state.paper
    async with paper["lock"]:
        pending = [item.copy() for item in paper.get("limit_orders", []) if item.get("status") == "BEKLİYOR"]
    if not pending:
        return

    symbols = list(dict.fromkeys(str(item.get("symbol") or "") for item in pending))
    price_results = await asyncio.gather(*(latest_price(symbol) for symbol in symbols), return_exceptions=True)
    live_prices = {
        symbol: float(price)
        for symbol, price in zip(symbols, price_results)
        if not isinstance(price, Exception)
    }
    if not live_prices:
        return

    changed = False
    bot_events: list[tuple[str, str, str]] = []
    archive_jobs: list[tuple[PaperOrder, float]] = []
    now = datetime.now(timezone.utc)
    async with paper["lock"]:
        for snapshot in pending:
            order = next(
                (item for item in paper.get("limit_orders", []) if item.get("id") == snapshot.get("id")),
                None,
            )
            if not order or order.get("status") != "BEKLİYOR":
                continue
            current_price = live_prices.get(str(order.get("symbol") or ""))
            if current_price is None:
                continue
            order["last_price"] = round(current_price, 10)
            order["distance_pct"] = round((current_price / float(order["limit_price"]) - 1) * 100, 4)
            try:
                expires_at = datetime.fromisoformat(str(order.get("expires_at")))
            except (TypeError, ValueError):
                expires_at = now + timedelta(minutes=1440)
                order["expires_at"] = expires_at.isoformat()
            if now >= expires_at:
                order.update({"status": "SÜRESİ DOLDU", "expired_at": now.isoformat(), "wait_reason": None})
                message = f"{order['symbol']} {order['direction']} Paper limit emrinin süresi doldu."
                add_paper_notification(paper, "LİMİT SÜRESİ", message)
                bot_events.append(("LİMİT SÜRESİ", message, str(order["symbol"])))
                changed = True
                continue
            if not paper_limit_triggered(str(order["direction"]), float(order["limit_price"]), current_price):
                if order.get("wait_reason") not in {None, "FİYAT BEKLENİYOR"}:
                    changed = True
                order["wait_reason"] = "FİYAT BEKLENİYOR"
                continue

            open_positions = [item for item in paper["positions"] if item.get("status") == "AÇIK"]
            risk = paper_risk_payload(paper)
            brake = emergency_brake_payload(paper)
            if len(open_positions) >= 3:
                if order.get("wait_reason") != "POZİSYON SINIRI":
                    changed = True
                order["wait_reason"] = "POZİSYON SINIRI"
                continue
            if risk["auto_paused"]:
                if order.get("wait_reason") != "RİSK KASASI":
                    changed = True
                order["wait_reason"] = "RİSK KASASI"
                continue
            if brake["active"]:
                if order.get("wait_reason") != "ACİL FREN":
                    changed = True
                order["wait_reason"] = "ACİL FREN"
                continue
            used_margin = sum(float(item.get("amount") or 0.0) for item in open_positions)
            if float(order["amount"]) > float(paper["balance"]) - used_margin:
                order.update({"status": "YETERSİZ BAKİYE", "failed_at": now.isoformat(), "wait_reason": None})
                message = f"{order['symbol']} Paper limit emri sanal bakiye yetersiz olduğu için açılmadı."
                add_paper_notification(paper, "LİMİT ENGEL", message)
                bot_events.append(("LİMİT ENGEL", message, str(order["symbol"])))
                changed = True
                continue

            paper_order = PaperOrder(
                symbol=str(order["symbol"]), direction=order["direction"], amount=float(order["amount"]),
                stop_loss=float(order["stop_loss"]), take_profit=float(order["tp1"]),
                tp2=float(order["tp2"]), tp3=float(order["tp3"]),
                max_holding_minutes=int(order.get("max_holding_minutes") or 1440),
                source="MANUAL", guard_mode="KULLANICI LİMİT PLANI",
                gate_status="PAPER LİMİT TETİKLENDİ", freshness_status="CANLI FİYAT GEÇİŞİ",
            )
            position = build_paper_position(
                paper_order, float(order["limit_price"]), int(paper["next_id"]), limit_order=order,
            )
            paper["next_id"] += 1
            paper["positions"].append(position)
            order.update({
                "status": "TETİKLENDİ", "triggered_at": now.isoformat(),
                "fill_price": float(order["limit_price"]), "position_id": position["id"],
                "wait_reason": None,
            })
            message = (
                f"{position['symbol']} {position['direction']} Paper limit tetiklendi · "
                f"{float(order['amount']):.2f} USDT · giriş {float(order['limit_price']):.8g} · "
                f"{len(position['grid_levels'])} grid çizgisi aktif"
            )
            add_paper_notification(paper, "LİMİT TETİKLENDİ", message)
            bot_events.append(("LİMİT TETİKLENDİ", message, str(position["symbol"])))
            archive_jobs.append((paper_order, float(order["limit_price"])))
            changed = True

    for event_kind, event_message, event_symbol in bot_events:
        record_bot_event(event_kind, event_message, event_symbol)
    for paper_order, fill_price in archive_jobs:
        asyncio.create_task(archive_trade_decision(paper_order, fill_price))
    if changed:
        asyncio.create_task(persist_paper_snapshot(app))


async def paper_limit_loop(application: FastAPI) -> None:
    """Uygulama açıkken bekleyen Paper limitlerini sık aralıkla takip eder."""
    while True:
        try:
            await refresh_paper_limit_orders()
        except Exception:
            pass
        await asyncio.sleep(2)


def paper_performance_payload(paper: dict) -> dict:
    trades = paper["trades"]
    realized = [float(item.get("realized_pnl", 0.0)) for item in trades]
    winners = [value for value in realized if value > 0]
    losers = [value for value in realized if value < 0]
    closed_count = len(realized)
    gross_wins, gross_losses = sum(winners), abs(sum(losers))
    return {
        "closed_count": closed_count,
        "wins": len(winners),
        "losses": len(losers),
        "win_rate": round((len(winners) / closed_count) * 100, 1) if closed_count else 0.0,
        "realized_pnl": round(sum(realized), 2),
        "average_pnl": round(sum(realized) / closed_count, 2) if closed_count else 0.0,
        "profit_factor": round(gross_wins / gross_losses, 2) if gross_losses else None,
        "best_trade": round(max(realized), 2) if realized else 0.0,
        "worst_trade": round(min(realized), 2) if realized else 0.0,
        "auto_trades": sum(1 for item in trades if item.get("source") == "AUTO"),
        "demo_trades": sum(1 for item in trades if item.get("source") == "DEMO"),
        "manual_trades": sum(1 for item in trades if item.get("source") == "MANUAL"),
    }


def daily_report_payload(paper: dict) -> dict:
    """Günlük Paper performansını ve uygulama içi uyarıları tek kartta toplar."""
    today = datetime.now(timezone.utc).date().isoformat()
    today_trades = [
        trade for trade in paper.get("trades", [])
        if str(trade.get("closed_at") or "").startswith(today)
    ]
    today_pnl = round(sum(float(trade.get("realized_pnl", 0.0)) for trade in today_trades), 2)
    risk = paper_risk_payload(paper)
    brake = emergency_brake_payload(paper)
    shadow = shadow_payload(paper)
    blackbox = decision_blackbox_payload(paper)
    if brake["active"]:
        status = "ACİL FREN AKTİF"
        headline = brake["reason"]
    elif risk["auto_paused"]:
        status = risk["status"]
        headline = risk["reason"]
    elif today_pnl > 0:
        status = "POZİTİF GÜN"
        headline = "Paper sonuçları bugün artıda; güvenlik eşikleri korunuyor."
    elif today_pnl < 0:
        status = "TEMKİNLİ GÜN"
        headline = "Paper sonuçları bugün ekside; yeni girişler tüm filtrelerden geçmek zorunda."
    else:
        status = "İZLEMEDE"
        headline = "Bugün yeterli kapanmış Paper işlem yok; sistem veri toplamayı sürdürüyor."
    return {
        "date": today, "status": status, "headline": headline, "today_pnl": today_pnl,
        "closed_trades": len(today_trades), "open_positions": sum(1 for item in paper["positions"] if item["status"] == "AÇIK"),
        "remaining_loss_budget": risk["remaining_loss_budget"], "shadow_records": len(shadow["events"]),
        "blackbox_records": blackbox["records"], "blackbox_status": blackbox["status"],
        "active_grid_plans": sum(1 for item in paper.get("grid_plans", []) if item.get("active")),
        "emergency_active": brake["active"], "notifications": paper.get("notifications", [])[:6],
    }


def paper_payload() -> dict:
    paper = app.state.paper
    positions = paper["positions"]
    open_positions = [position for position in positions if position["status"] == "AÇIK"]
    limit_orders = list(paper.get("limit_orders", []))
    pending_orders = [order for order in limit_orders if order.get("status") == "BEKLİYOR"]
    unrealized = sum(position.get("unrealized_pnl", 0.0) for position in open_positions)
    used_margin = sum(position["amount"] for position in open_positions)
    reserved_margin = sum(float(order.get("amount") or 0.0) for order in pending_orders)
    return {
        "mode": "PAPER", "balance": round(paper["balance"], 2), "equity": round(paper["balance"] + unrealized, 2),
        "available": round(paper["balance"] - used_margin - reserved_margin, 2),
        "used_margin": round(used_margin, 2), "reserved_margin": round(reserved_margin, 2),
        "unrealized_pnl": round(unrealized, 2), "positions": open_positions, "recent_trades": paper["trades"][:10],
        "pending_orders": pending_orders,
        "recent_limit_orders": limit_orders[:12],
        "risk": paper_risk_payload(paper), "performance": paper_performance_payload(paper),
        "shadow": shadow_payload(paper), "emergency_brake": emergency_brake_payload(paper),
        "blackbox": decision_blackbox_payload(paper),
        "grid_plans": paper.get("grid_plans", [])[:GRID_PLAN_LIMIT],
        "strategy_orchestrator": v7_orchestrator_payload(paper.get("strategy_orchestrator", empty_strategy_orchestrator_state())),
        "notifications": paper.get("notifications", [])[:8],
    }


def v20_max_drawdown(trades: list[dict], initial_balance: float = 10_000.0) -> float:
    """Kapanmış Paper sonuçlarından sermayeye göre tepe-dip düşüşünü ölçer."""
    equity = float(initial_balance)
    peak = equity
    worst = 0.0
    for trade in reversed(list(trades)):
        equity += float(trade.get("realized_pnl") or 0.0)
        peak = max(peak, equity)
        drawdown = ((peak - equity) / peak * 100) if peak > 0 else 0.0
        worst = max(worst, drawdown)
    return round(worst, 2)


def v20_ghost_twin_payload(paper: dict) -> dict:
    """Alınan ve engellenen Paper kararlarını karşı-olasılık olarak karşılaştırır."""
    trades = list(paper.get("trades", []))
    events = list(paper.get("decision_memory", []))
    blocked = [event for event in events if event.get("decision") == "ENGELLENDİ"]
    reviewed = [(event, latest_decision_review(event)) for event in blocked]
    reviewed = [(event, review) for event, review in reviewed if review is not None]
    shield_saves = [(event, review) for event, review in reviewed if float(review.get("return_pct") or 0.0) <= 0]
    missed = [(event, review) for event, review in reviewed if float(review.get("return_pct") or 0.0) > 0.20]
    counterfactual_returns = [float(review.get("return_pct") or 0.0) for _, review in reviewed]
    taken_pnl = sum(float(trade.get("realized_pnl") or 0.0) for trade in trades)
    rows = []
    for event, review in reviewed[:10]:
        direction_return = float(review.get("return_pct") or 0.0)
        rows.append({
            "created_at": event.get("created_at"), "symbol": event.get("symbol"),
            "direction": event.get("direction"), "decision": "ENGELLENDİ",
            "reason": event.get("reason"), "review_minutes": review.get("minutes"),
            "counterfactual_return_pct": round(direction_return, 3),
            "outcome": "KALKAN KORUDU" if direction_return <= 0 else "KAÇAN FIRSAT",
        })
    evidence = len(reviewed)
    if evidence < 10:
        status = "KANIT TOPLUYOR"
    elif len(shield_saves) >= len(missed):
        status = "KALKAN AVANTAJLI"
    else:
        status = "EŞİK KALİBRASYONU"
    return {
        "version": "20.2.0", "status": status,
        "taken_trades": len(trades), "taken_pnl_usdt": round(taken_pnl, 2),
        "blocked_reviewed": evidence, "shield_saves": len(shield_saves),
        "missed_opportunities": len(missed),
        "shield_save_rate_pct": round(len(shield_saves) / evidence * 100, 1) if evidence else 0.0,
        "counterfactual_edge_pct": round(-sum(counterfactual_returns) / evidence, 3) if evidence else 0.0,
        "rows": rows, "orders_enabled": False, "testnet_orders_enabled": False,
        "method_note": "Hayalet İkiz, engellenen kararın 15/30/60 dakika sonraki yönsel sonucunu alınan Paper işlemlerle karşılaştırır. Sonuç garanti değildir.",
    }


def v20_release_certificate(paper: dict) -> dict:
    """Paper kanıt kapılarını raporlar; hiçbir emir kanalını açmaz."""
    performance = paper_performance_payload(paper)
    blackbox = decision_blackbox_payload(paper)
    risk = paper_risk_payload(paper)
    brake = emergency_brake_payload(paper)
    drawdown = v20_max_drawdown(paper.get("trades", []), float(paper.get("initial_balance") or 10_000.0))
    profit_factor = performance.get("profit_factor")
    profit_gate_passed = (profit_factor is not None and profit_factor >= 1.20) or (
        profit_factor is None and performance["wins"] > 0 and performance["losses"] == 0
    )
    gates = [
        {"key": "trades", "label": "30 kapanmış Paper işlem", "passed": performance["closed_count"] >= 30, "value": f"{performance['closed_count']} / 30"},
        {"key": "profit", "label": "Profit Factor ≥ 1.20", "passed": profit_gate_passed, "value": "∞" if profit_factor is None and performance["wins"] > 0 else str(profit_factor or "—")},
        {"key": "drawdown", "label": "Maksimum düşüş ≤ %10", "passed": drawdown <= 10 and performance["closed_count"] > 0, "value": f"%{drawdown:.2f}"},
        {"key": "ghost", "label": "10 incelenmiş engel", "passed": blackbox["reviewed_rejections"] >= 10, "value": f"{blackbox['reviewed_rejections']} / 10"},
        {"key": "risk", "label": "Risk Kasası açık", "passed": not risk["auto_paused"], "value": risk["status"]},
        {"key": "brake", "label": "Acil fren normal", "passed": not brake["active"], "value": "NORMAL" if not brake["active"] else "AKTİF"},
    ]
    passed = sum(1 for gate in gates if gate["passed"])
    evidence_ready = all(gate["passed"] for gate in gates)
    status = "PAPER KANITI TAMAMLANDI" if evidence_ready else "PAPER KANITI TOPLANIYOR"
    return {
        "version": "20.2.0", "status": status, "score": round(passed / len(gates) * 100),
        "passed_gates": passed, "total_gates": len(gates), "gates": gates,
        "paper_ready": evidence_ready,
        "testnet_candidate": evidence_ready and bool(testnet_readiness()["credentials_configured"]),
        "testnet_ready": False, "live_ready": False,
        "orders_enabled": False, "testnet_orders_enabled": False,
        "reason": "V20 kanıt raporu hazır olsa bile Testnet ve gerçek borsa emirleri bu pakette fiziksel olarak kilitlidir.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def v20_command_payload(paper: dict, bot: dict, infrastructure: dict) -> dict:
    """V6–V20 durumlarını tek komuta ekranı için birleştirir."""
    account = paper_payload() if paper is app.state.paper else {
        "balance": round(float(paper.get("balance") or 0.0), 2),
        "performance": paper_performance_payload(paper),
        "positions": [item for item in paper.get("positions", []) if item.get("status") == "AÇIK"],
        "risk": paper_risk_payload(paper),
    }
    modules = [
        {"version": "V6", "name": "Canlı Paper Grid", "active": bool(paper.get("grid_engine", {}).get("enabled")), "status": paper.get("grid_engine", {}).get("status", "BEKLEMEDE")},
        {"version": "V7", "name": "Strateji Orkestrası", "active": bool(paper.get("strategy_orchestrator", {}).get("enabled")), "status": paper.get("strategy_orchestrator", {}).get("status", "BEKLEMEDE")},
        {"version": "V8", "name": "Gelecek ve Kaos Laboratuvarı", "active": True, "status": "ANALİZ HAZIR"},
        {"version": "V9", "name": "Canlı Borsa İkizi", "active": bool(getattr(app.state, "market_twin", {}).get("enabled")), "status": getattr(app.state, "market_twin", {}).get("stream_health", "BEKLEMEDE")},
        {"version": "V10", "name": "Strateji Evrimi", "active": bool(paper.get("strategy_evolution", {}).get("enabled")), "status": paper.get("strategy_evolution", {}).get("status", "BEKLEMEDE")},
        {"version": "V11", "name": "Otonom Risk Beyni", "active": bool(paper.get("portfolio_risk", {}).get("enabled")), "status": paper.get("portfolio_risk", {}).get("status", "BEKLEMEDE")},
        {"version": "V25.1", "name": "Otonom Paper Avcısı", "active": bool(bot.get("enabled")), "status": bot.get("mode", "BEKLEMEDE")},
    ]
    return {
        "version": PAPER_AUTONOMY_VERSION, "edition": "AUTONOMOUS PAPER ALLOCATION",
        "account": account, "bot": {**bot, "policy": active_paper_bot_policy(bot)},
        "ghost_twin": v20_ghost_twin_payload(paper), "certificate": v20_release_certificate(paper),
        "testnet": testnet_readiness(), "modules": modules, "infrastructure": infrastructure,
        "orders_enabled": False, "testnet_orders_enabled": False,
        "safety_note": "Canlı piyasa verisi okunur; tüm işlemler sanal Paper cüzdanda gerçekleşir.",
    }


@app.get("/api/v20/ghost-twin")
async def v20_ghost_twin_endpoint():
    await refresh_decision_memory()
    return v20_ghost_twin_payload(app.state.paper)


@app.get("/api/v20/certificate")
async def v20_certificate_endpoint():
    await refresh_paper_positions()
    return v20_release_certificate(app.state.paper)


@app.get("/api/v20/command")
async def v20_command_endpoint():
    await refresh_paper_positions()
    paper_autonomy_payload(app.state.paper, app.state.paper_bot)
    return v20_command_payload(app.state.paper, app.state.paper_bot, app.state.infrastructure)


@app.get("/api/v20/replay/{symbol}")
async def v20_replay_endpoint(
    symbol: str,
    interval: Literal["5m", "15m", "1h"] = "15m",
    horizon: Literal["24h", "7d"] = "7d",
    capital: float = Query(1_000.0, ge=100, le=25_000),
):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    requested = 180 if horizon == "24h" else 740
    candles = await fetch_candles(safe_symbol, interval, requested)
    replay = v7_market_replay(candles[:-1], horizon, capital)
    return {
        "version": "20.2.0", "symbol": safe_symbol, "interval": interval,
        **replay, "ghost_twin": v20_ghost_twin_payload(app.state.paper),
        "orders_enabled": False, "testnet_orders_enabled": False,
    }


@app.get("/api/paper/account")
async def paper_account():
    refresh_warning = None
    try:
        await refresh_paper_limit_orders()
        await refresh_paper_positions()
    except Exception as exc:
        # Canlı fiyat yenilemesi başarısız olsa bile son sağlam Paper kayıtları
        # okunabilsin. Bu uç nokta hiçbir zaman gerçek borsa emri üretmez.
        refresh_warning = f"Canlı Paper fiyatı geçici olarak yenilenemedi: {str(exc)[:120]}"
    try:
        payload = paper_payload()
        if refresh_warning:
            payload["warning"] = refresh_warning
        return payload
    except Exception:
        paper = app.state.paper
        balance = float(paper.get("balance") or 0.0)
        return {
            "mode": "PAPER", "balance": round(balance, 2), "equity": round(balance, 2),
            "available": round(balance, 2), "used_margin": 0.0, "reserved_margin": 0.0,
            "unrealized_pnl": 0.0, "positions": [], "pending_orders": [],
            "recent_limit_orders": [], "recent_trades": [],
            "risk": {"status": "GÜVENLİ BEKLEME", "auto_paused": True, "daily_realized_pnl": 0.0,
                     "daily_loss_limit": 0.0, "remaining_loss_budget": 0.0, "consecutive_losses": 0,
                     "consecutive_loss_limit": 0, "cooldown_until": None,
                     "reason": "Paper kayıtları güvenli biçimde yeniden hazırlanıyor."},
            "performance": {"closed_count": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                            "realized_pnl": 0.0, "average_pnl": 0.0, "profit_factor": None,
                            "best_trade": 0.0, "worst_trade": 0.0, "auto_trades": 0,
                            "demo_trades": 0, "manual_trades": 0},
            "shadow": {"enabled": False, "events": []},
            "emergency_brake": {"active": False, "reason": "", "source": None, "triggered_at": None},
            "notifications": [], "warning": "Paper hesap özeti kurtarma modunda; gerçek emir kanalları kapalı.",
        }


@app.get("/api/report/daily")
async def daily_report():
    await refresh_paper_limit_orders()
    await refresh_paper_positions()
    return daily_report_payload(app.state.paper)


@app.post("/api/shadow/toggle")
async def toggle_shadow_mode():
    paper = app.state.paper
    async with paper["lock"]:
        shadow = paper.setdefault("shadow", {"enabled": False, "events": []})
        shadow["enabled"] = not bool(shadow.get("enabled"))
        message = "Gölge Modu açıldı; güvenli adaylar yalnızca kaydedilecek." if shadow["enabled"] else "Gölge Modu kapatıldı; Paper Bot uygun sinyallerde sanal pozisyon açabilir."
        add_paper_notification(paper, "GÖLGE MODU", message)
    asyncio.create_task(persist_paper_snapshot(app))
    return {"message": message, "account": paper_payload()}


@app.post("/api/emergency/trigger")
async def trigger_emergency_brake():
    paper = app.state.paper
    async with paper["lock"]:
        activate_emergency_brake(paper, "Kullanıcı Paper Bot için acil freni etkinleştirdi.", "KULLANICI")
        paper.get("grid_engine", {})["enabled"] = False
        paper.get("strategy_orchestrator", {})["enabled"] = False
        if paper.get("grid_engine"):
            paper["grid_engine"]["status"] = "ACİL FREN"
        if paper.get("strategy_orchestrator"):
            paper["strategy_orchestrator"]["status"] = "ACİL FREN"
    app.state.paper_bot["enabled"] = False
    record_bot_event("ACİL FREN", "Kullanıcı Paper Bot'u acil frenle durdurdu")
    asyncio.create_task(persist_paper_snapshot(app))
    return {"message": "Acil fren etkin; Paper Bot, V6 Grid ve V7 Orkestra durduruldu.", "account": paper_payload(), "bot": app.state.paper_bot}


@app.post("/api/emergency/reset")
async def reset_emergency_brake():
    paper = app.state.paper
    async with paper["lock"]:
        changed = clear_emergency_brake(paper)
    asyncio.create_task(persist_paper_snapshot(app))
    return {"message": "Acil fren kaldırıldı." if changed else "Acil fren zaten kapalı.", "account": paper_payload()}


@app.post("/api/paper/limit")
async def paper_limit_create(plan: PaperLimitOrder):
    """Kullanıcı seviyelerini kalıcı Paper limit kuyruğuna ekler."""
    safe_symbol = "".join(char for char in plan.symbol.upper() if char.isalnum())
    if not safe_symbol.endswith("USDT"):
        raise HTTPException(422, "Paper limit için geçerli bir USDT paritesi seçin")
    try:
        normalized = normalize_paper_limit_plan(plan)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    await refresh_paper_positions()
    current_price = await latest_price(safe_symbol)
    paper = app.state.paper
    now = datetime.now(timezone.utc)
    async with paper["lock"]:
        pending = [item for item in paper.get("limit_orders", []) if item.get("status") == "BEKLİYOR"]
        open_positions = [item for item in paper["positions"] if item.get("status") == "AÇIK"]
        reserved = sum(float(item.get("amount") or 0.0) for item in pending)
        used_margin = sum(float(item.get("amount") or 0.0) for item in open_positions)
        if len(pending) >= 8:
            raise HTTPException(409, "Aynı anda en fazla 8 bekleyen Paper limit emri tutulabilir")
        if float(plan.amount) > float(paper["balance"]) - used_margin - reserved:
            raise HTTPException(409, "Açık pozisyonlar ve bekleyen emirlerden sonra yeterli sanal bakiye yok")
        order_id = int(paper["next_limit_id"])
        paper["next_limit_id"] += 1
        marketable = paper_limit_triggered(plan.direction, normalized["limit_price"], current_price)
        order = {
            "id": order_id, "symbol": safe_symbol, "direction": plan.direction,
            "amount": round(float(plan.amount), 2), **normalized,
            "status": "BEKLİYOR", "wait_reason": "TETİKLENİYOR" if marketable else "FİYAT BEKLENİYOR",
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=int(plan.expires_minutes))).isoformat(),
            "expires_minutes": int(plan.expires_minutes),
            "max_holding_minutes": int(plan.expires_minutes),
            "last_price": round(current_price, 10),
            "distance_pct": round((current_price / normalized["limit_price"] - 1) * 100, 4),
            "paper_only": True, "orders_enabled": False, "testnet_orders_enabled": False,
        }
        paper["limit_orders"].insert(0, order)
        del paper["limit_orders"][40:]
        message = (
            f"{safe_symbol} {plan.direction} Paper limit kaydedildi · "
            f"{float(plan.amount):.2f} USDT · limit {normalized['limit_price']:.8g} · "
            f"{normalized['grid_count']} grid"
        )
        add_paper_notification(paper, "LİMİT BEKLİYOR", message)
    record_bot_event("LİMİT BEKLİYOR", message, safe_symbol)
    await persist_paper_snapshot(app)
    await refresh_paper_limit_orders()
    stored = next((item for item in paper.get("limit_orders", []) if item.get("id") == order_id), order)
    return {
        "message": "Paper limit emri kaydedildi; canlı fiyat otomatik izleniyor.",
        "order": stored, "account": paper_payload(),
        "orders_enabled": False, "testnet_orders_enabled": False,
    }


@app.post("/api/paper/limit/cancel/{order_id}")
async def paper_limit_cancel(order_id: int):
    paper = app.state.paper
    async with paper["lock"]:
        order = next((item for item in paper.get("limit_orders", []) if item.get("id") == order_id), None)
        if not order:
            raise HTTPException(404, "Paper limit emri bulunamadı")
        if order.get("status") != "BEKLİYOR":
            raise HTTPException(409, f"Bu Paper limit artık iptal edilemez: {order.get('status')}")
        order.update({"status": "İPTAL", "cancelled_at": datetime.now(timezone.utc).isoformat(), "wait_reason": None})
        message = f"{order['symbol']} {order['direction']} Paper limit emri kullanıcı tarafından iptal edildi."
        add_paper_notification(paper, "LİMİT İPTAL", message)
    record_bot_event("LİMİT İPTAL", message, str(order.get("symbol") or ""))
    asyncio.create_task(persist_paper_snapshot(app))
    return {"message": "Paper limit emri iptal edildi.", "order": order, "account": paper_payload()}


@app.post("/api/paper/open")
async def paper_open(order: PaperOrder):
    await refresh_paper_positions()
    paper = app.state.paper
    async with paper["lock"]:
        open_positions = [position for position in paper["positions"] if position["status"] == "AÇIK"]
        used_margin = sum(position["amount"] for position in open_positions)
        reserved_margin = sum(
            float(item.get("amount") or 0.0)
            for item in paper.get("limit_orders", [])
            if item.get("status") == "BEKLİYOR"
        )
        risk = paper_risk_payload(paper)
        brake = emergency_brake_payload(paper)
        if len(open_positions) >= 3:
            raise HTTPException(409, "Paper Trading aynı anda en fazla 3 pozisyon açar")
        if order.amount > paper["balance"] - used_margin - reserved_margin:
            raise HTTPException(409, "Sanal bakiye bu işlem için yeterli değil")
        if order.source in {"AUTO", "DEMO"} and risk["auto_paused"]:
            raise HTTPException(409, f"Risk Kasası otomatik girişi durdurdu: {risk['reason']}")
        if order.source in {"AUTO", "DEMO"} and brake["active"]:
            raise HTTPException(409, f"Acil fren otomatik girişi durdurdu: {brake['reason']}")
    entry_price = await latest_price(order.symbol)
    if order.direction == "LONG" and not order.stop_loss < entry_price < order.take_profit:
        raise HTTPException(422, "LONG için stop girişin altında, hedef girişin üstünde olmalı")
    if order.direction == "SHORT" and not order.take_profit < entry_price < order.stop_loss:
        raise HTTPException(422, "SHORT için hedef girişin altında, stop girişin üstünde olmalı")
    try:
        v20_target_plan(entry_price, order.stop_loss, order.direction, order.take_profit, order.tp2, order.tp3)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    stop_distance = abs(entry_price - float(order.stop_loss))
    if stop_distance <= 0:
        raise HTTPException(422, "Giriş ve stop-loss fiyatları farklı olmalı")
    # AUTO allocations are already sized by the Paper autonomy risk model.
    if order.source == "AUTO":
        position_amount = float(order.amount)
    else:
        risk_budget = float(paper["balance"]) * RISK_PER_TRADE
        quantity = risk_budget / stop_distance
        position_amount = quantity * entry_price
    async with paper["lock"]:
        open_positions = [position for position in paper["positions"] if position["status"] == "AÇIK"]
        used_margin = sum(position["amount"] for position in open_positions)
        reserved_margin = sum(
            float(item.get("amount") or 0.0)
            for item in paper.get("limit_orders", [])
            if item.get("status") == "BEKLİYOR"
        )
        if position_amount > paper["balance"] - used_margin - reserved_margin:
            raise HTTPException(409, "Sanal bakiye bu işlem için yeterli değil")
        order.amount = position_amount
        position = build_paper_position(order, entry_price, int(paper["next_id"]))
        paper["next_id"] += 1
        paper["positions"].append(position)
        source_label = {"DEMO": "HIZLI DEMO", "AUTO": "OTOMATİK", "MANUAL": "MANUEL"}.get(order.source, order.source)
        add_paper_notification(
            paper,
            f"{source_label} AÇILDI",
            f"{position['symbol']} {order.direction} açıldı · {order.amount:.2f} USDT · giriş {entry_price:.8g} · TP1/TP2/TP3 aktif",
        )
        if order.source in {"AUTO", "DEMO"}:
            record_decision_memory(
                paper, symbol=position["symbol"], direction=order.direction,
                confidence=order.signal_confidence, entry_price=entry_price,
                decision="AÇILDI", reason="Paper eğitim planı doğrulandı; sanal pozisyon açıldı." if order.source == "DEMO" else "Tüm Paper güvenlik kapıları geçildi; sanal pozisyon açıldı.",
                source=order.source, gates={
                    "rejim": order.regime_label, "likidite": order.liquidity_mode,
                    "seans": order.session_mode, "kalite": order.adaptive_mode,
                },
            )
    asyncio.create_task(archive_trade_decision(order, entry_price))
    asyncio.create_task(persist_paper_snapshot(app))
    return {"message": "Sanal pozisyon açıldı", "position": position, "account": paper_payload()}


@app.post("/api/paper/demo/{symbol}")
async def paper_demo_open(
    symbol: str,
    interval: Literal["1m", "5m", "15m", "1h", "4h", "1d"] = "15m",
):
    """Kullanıcı isteğiyle küçük bir canlı-veri Paper eğitim işlemi açar."""
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    if not safe_symbol.endswith("USDT"):
        raise HTTPException(422, "Demo işlem için geçerli bir USDT paritesi seçin")
    await refresh_paper_positions()
    if any(item["status"] == "AÇIK" and item["symbol"] == safe_symbol for item in app.state.paper["positions"]):
        raise HTTPException(409, "Bu paritede zaten açık bir Paper pozisyon var")
    analysis, live_price = await asyncio.gather(
        technical_analysis(safe_symbol, interval),
        latest_price(safe_symbol),
    )
    plan = demo_paper_trade_plan(analysis, live_price)
    result = await paper_open(PaperOrder(
        symbol=safe_symbol,
        direction=plan["direction"],
        amount=plan["amount"],
        stop_loss=plan["stop_loss"],
        take_profit=plan["take_profit"],
        tp2=plan["tp2"], tp3=plan["tp3"],
        source="DEMO",
        signal_confidence=int(analysis.get("confidence") or 0),
        guard_mode="KULLANICI ONAYLI PAPER EĞİTİM",
        gate_status="DEMO PLAN",
        freshness_status="CANLI FİYAT",
        adaptive_mode="EĞİTİM MODU",
    ))
    app.state.paper_bot["last_blocker"] = None
    record_bot_event(
        "DEMO AÇILDI",
        f"{safe_symbol} {plan['direction']} eğitim işlemi açıldı · 50 USDT · R/O 1:{plan['risk_reward']}",
        safe_symbol,
    )
    return {
        **result,
        "message": "50 USDT tutarında canlı veriye bağlı demo Paper işlemi açıldı.",
        "plan": plan,
        "bot": app.state.paper_bot,
        "orders_enabled": False,
        "testnet_orders_enabled": False,
    }


def record_bot_event(kind: str, message: str, symbol: str | None = None) -> None:
    """Paper Bot kararlarını kullanıcıya açıklanabilir biçimde saklar."""
    bot = app.state.paper_bot
    created_at = datetime.now(timezone.utc).isoformat()
    bot["last_action"] = message
    bot["last_check"] = created_at
    bot["events"].insert(0, {"kind": kind, "message": message, "symbol": symbol, "created_at": created_at})
    del bot["events"][12:]


def paper_autonomy_payload(paper: dict, bot: dict) -> dict:
    """Keep the V25.1 Paper autonomy dashboard current and fail closed."""
    policy = active_paper_bot_policy(bot)
    profile = str(policy.get("profile") or "DENGELI")
    allocation_profile = profile if profile in {"TEMKINLI", "DENGELI", "HIZLI"} else "TEMKINLI"
    base = autonomy_policy(allocation_profile)
    existing = bot.get("autonomy") if isinstance(bot.get("autonomy"), dict) else {}
    open_positions = [item for item in paper.get("positions", []) if item.get("status") == "AÇIK"]
    current_exposure = sum(float(item.get("amount") or 0.0) for item in open_positions)
    unrealized = sum(float(item.get("unrealized_pnl") or 0.0) for item in open_positions)
    realized_today = float(paper_risk_payload(paper).get("daily_realized_pnl") or 0.0)
    payload = {
        **base,
        "profile": profile,
        "universe_size": int(policy.get("universe_size") or base["universe_size"]),
        "shortlist_size": int(policy.get("shortlist_size") or base["shortlist_size"]),
        "risk_per_trade_pct": float(policy.get("risk_per_trade_pct") or base["risk_per_trade_pct"]),
        "max_allocation_pct": float(policy.get("max_allocation_pct") or base["max_allocation_pct"]),
        "max_total_exposure_pct": float(policy.get("max_total_exposure_pct") or base["max_total_exposure_pct"]),
        "minimum_projected_net_usdt": float(policy.get("minimum_projected_net_usdt") or base["minimum_projected_net_usdt"]),
        "shortlist": list(existing.get("shortlist") or []),
        "last_allocation": existing.get("last_allocation"),
        "last_scan_at": existing.get("last_scan_at"),
        "current_exposure_usdt": round(current_exposure, 2),
        "current_exposure_pct": round(current_exposure / max(float(paper.get("balance") or 1.0), 1.0) * 100.0, 2),
        "unrealized_pnl_usdt": round(unrealized, 2),
        "daily_reference": daily_reference_progress(realized_today),
        "note": "Coin seçimi ve sermaye tahsisi otomatiktir; yalnızca Paper cüzdanda çalışır ve kâr garantisi vermez.",
        "orders_enabled": False,
        "testnet_orders_enabled": False,
        "paper_only": True,
        "profit_guaranteed": False,
    }
    bot["autonomy"] = payload
    return payload


async def paper_bot_cycle() -> None:
    bot = app.state.paper_bot
    if bot["busy"]:
        return
    bot["busy"] = True
    training_mode = bool(bot.get("training_mode", True))
    policy = active_paper_bot_policy(bot)
    bot.update({
        "mode": policy["mode"],
        "scan_interval_seconds": policy["cycle_seconds"],
        "orders_enabled": False,
        "testnet_orders_enabled": False,
    })
    memory_changed = False
    try:
        await refresh_paper_positions()
        await refresh_decision_memory()
        brake = emergency_brake_payload(app.state.paper)
        if brake["active"]:
            bot["cycles"] += 1
            bot["last_blocker"] = f"Acil fren: {brake['reason']}"
            record_bot_event("ACİL FREN", f"Paper Bot acil frende: {brake['reason']}")
            return
        risk = paper_risk_payload(app.state.paper)
        if risk["auto_paused"]:
            bot["cycles"] += 1
            bot["last_blocker"] = f"Risk Kasası: {risk['reason']}"
            record_bot_event("RİSK", f"Risk Kasası Paper Bot'u beklemeye aldı: {risk['reason']}")
            return
        open_count = sum(1 for item in app.state.paper["positions"] if item["status"] == "AÇIK")
        if open_count >= 3:
            bot["cycles"] += 1
            bot["last_blocker"] = "Maksimum 3 sanal pozisyon sınırı dolu"
            record_bot_event("SINIR", "Maksimum 3 sanal pozisyon açık; yeni sinyal bekliyor")
            return
        global_guard = await market_guard("BTCUSDT")
        if global_guard["risk_score"] >= 70:
            async with app.state.paper["lock"]:
                activated = activate_emergency_brake(
                    app.state.paper,
                    f"BTC piyasa riski %{global_guard['risk_score']}: {global_guard['reason']}",
                    "OTOMATİK KALKAN",
                )
            if activated:
                asyncio.create_task(persist_paper_snapshot(app))
            bot["cycles"] += 1
            bot["last_blocker"] = "Aşırı BTC piyasa riski"
            record_bot_event("ACİL FREN", "Aşırı piyasa riski nedeniyle yeni Paper girişleri donduruldu", "BTCUSDT")
            return
        if global_guard["risk_score"] >= 55:
            bot["cycles"] += 1
            bot["last_blocker"] = f"Piyasa Kalkanı: {global_guard['reason']}"
            record_bot_event("KORUMA", f"Piyasa Kalkanı girişleri durdurdu: {global_guard['reason']}", "BTCUSDT")
            return
        scan = await smart_scan(limit=int(policy["universe_size"]), interval="15m")
        allocation_profile = str(policy.get("profile") or "DENGELI")
        if allocation_profile not in {"TEMKINLI", "DENGELI", "HIZLI"}:
            allocation_profile = "TEMKINLI"
        ranked = rank_paper_candidates(
            [
                {
                    **item,
                    "profile_minimum_confidence": policy["minimum_confidence"],
                }
                for item in scan["results"]
            ],
            allocation_profile,
        )
        autonomy = paper_autonomy_payload(app.state.paper, bot)
        autonomy.update({
            "version": PAPER_AUTONOMY_VERSION,
            "shortlist": ranked,
            "last_scan_at": datetime.now(timezone.utc).isoformat(),
        })
        candidates = [item for item in ranked if item["eligible"]]
        bot["last_candidate_count"] = len(candidates)
        guard_note = None
        open_symbols = {item["symbol"] for item in app.state.paper["positions"] if item["status"] == "AÇIK"}
        for candidate in candidates:
            if candidate["symbol"] in open_symbols:
                continue

            def remember(reason: str, gate_name: str, gate_status: str) -> None:
                nonlocal memory_changed
                event = record_decision_memory(
                    app.state.paper, symbol=candidate["symbol"], direction=candidate["direction"],
                    confidence=candidate["confidence"], entry_price=candidate.get("price"),
                    decision="ENGELLENDİ", reason=reason, source="AUTO",
                    gates={"kapı": gate_name, "durum": gate_status},
                )
                memory_changed = memory_changed or event is not None

            guard = await market_guard(candidate["symbol"])
            guard_direction = guard.get("symbol_direction")
            guard_allowed = guard["auto_allowed"] or (
                training_mode
                and guard["risk_score"] < 55
                and guard_direction in {candidate["direction"], "BEKLE", None}
            )
            if not guard_allowed:
                guard_note = guard_note or f"{candidate['display']}: {guard['market_mode']}"
                remember(f"Piyasa Kalkanı engelledi: {guard['reason']}", "Piyasa Kalkanı", guard["market_mode"])
                continue
            regime = await market_regime(candidate["symbol"], "15m")
            high_risk_regime = regime["label"] in {"YÜKSEK VOLATİLİTE", "RİSK KAPALI"}
            regime_direction = regime.get("preferred_direction")
            regime_allowed = (
                regime["auto_allowed"] and regime_direction == candidate["direction"]
            ) or (
                training_mode
                and not high_risk_regime
                and regime_direction in {candidate["direction"], "BEKLE", None}
            )
            if not regime_allowed:
                guard_note = guard_note or f"{candidate['display']}: {regime['entry_policy']}"
                remember(f"Rejim politikası bekletti: {regime['reason']}", "Piyasa Rejimi", regime["entry_policy"])
                continue
            stability = regime_stability_gate(candidate["symbol"], "15m", regime)
            if not stability["auto_allowed"] and not training_mode:
                guard_note = guard_note or f"{candidate['display']}: {stability['mode']} · {stability['stable_samples']}/{stability['required_samples']}"
                remember(stability["reason"], "Rejim Sabitleyici", stability["mode"])
                continue
            liquidity = await liquidity_shield(candidate["symbol"])
            if not paper_training_liquidity_allowed(liquidity, training_mode):
                guard_note = guard_note or f"{candidate['display']}: {liquidity['mode']}"
                remember(liquidity["reason"], "Likidite Kalkanı", liquidity["mode"])
                continue
            portfolio = await portfolio_exposure_guard(candidate["symbol"], candidate["direction"])
            if not portfolio["auto_allowed"]:
                guard_note = guard_note or f"{candidate['display']}: {portfolio['mode']}"
                remember(portfolio["reason"], "Portföy Kalkanı", portfolio["mode"])
                continue
            adaptive = adaptive_quality_gate(app.state.paper, regime["label"], candidate["direction"])
            session = session_intelligence(app.state.paper, candidate["symbol"], regime["label"], candidate["direction"])
            if training_mode:
                confidence_floor = min(policy["confidence_floor"], adaptive["min_confidence"] + session["confidence_bonus"])
                adaptive_allowed = True
            else:
                confidence_floor = min(95, adaptive["min_confidence"] + session["confidence_bonus"])
                adaptive_allowed = adaptive["auto_allowed"]
            if not adaptive_allowed or candidate["confidence"] < confidence_floor:
                guard_note = guard_note or f"{candidate['display']}: {adaptive['mode']} · seans eşiği %{confidence_floor}"
                remember(f"{adaptive['reason']} Seans eşiği %{confidence_floor}.", "Seans + Kalite", adaptive["mode"])
                continue
            consensus = await multi_timeframe_consensus(candidate["symbol"])
            consensus_map = {item.get("timeframe"): item for item in consensus.get("timeframes", [])}
            paper_mtf_decision = shared_mtf_decision(
                symbol=candidate["symbol"],
                entry_direction=candidate["direction"],
                confidence_15m=float(candidate.get("confidence") or 0.0),
                timeframe_results={
                    "1h": {
                        "direction": consensus_map.get("1h", {}).get("direction", "BEKLE"),
                        "confidence": float(consensus_map.get("1h", {}).get("confidence") or 0.0),
                    },
                    "4h": {
                        "direction": consensus_map.get("4h", {}).get("direction", "BEKLE"),
                        "confidence": float(consensus_map.get("4h", {}).get("confidence") or 0.0),
                    },
                },
                short_filter=True,
                short_alignment_max=SHORT_MTF_ALIGNMENT_MAX,
            )
            consensus_opposes = paper_mtf_decision["direction"] in {"LONG", "SHORT"} and paper_mtf_decision["direction"] != candidate["direction"]
            consensus_allowed = (
                paper_mtf_decision["entry_permission"] and paper_mtf_decision["direction"] == candidate["direction"]
            ) or (
                training_mode and not (consensus_opposes and paper_mtf_decision.get("alignment", 0) >= 60)
            )
            if not consensus_allowed:
                guard_note = guard_note or f"{candidate['display']}: {paper_mtf_decision['verdict']}"
                remember(paper_mtf_decision["reason"], "Çoklu Zaman Onayı", paper_mtf_decision["verdict"])
                continue
            gate = await candle_close_gate(candidate["symbol"], "15m")
            gate_allowed = gate["direction"] == candidate["direction"] and (gate["entry_allowed"] or training_mode)
            if not gate_allowed:
                guard_note = guard_note or f"{candidate['display']}: {gate['status']}"
                remember(gate["reason"], "Mum Kapanış Kapısı", gate["status"])
                continue
            freshness = await signal_freshness(candidate["symbol"], "15m")
            if not freshness["auto_allowed"]:
                guard_note = guard_note or f"{candidate['display']}: {freshness['status']}"
                remember(freshness["reason"], "Fiyat Sapma Kalkanı", freshness["status"])
                continue
            analysis = gate["analysis"]
            if training_mode:
                current_price = await latest_price(candidate["symbol"])
                training_plan = demo_paper_trade_plan({**analysis, "direction": candidate["direction"]}, current_price)
                stop_loss, take_profit = training_plan["stop_loss"], training_plan["take_profit"]
                tp2, tp3 = training_plan["tp2"], training_plan["tp3"]
                entry_reference = current_price
            else:
                stop_loss, take_profit = analysis["stop_loss"], analysis["tp1"]
                tp2, tp3 = analysis["tp2"], analysis["tp3"]
                entry_reference = float(freshness.get("live_price") or candidate.get("price") or analysis.get("entry") or 0.0)
            regime_multiplier = max(float(regime["position_multiplier"]), 0.60) if training_mode else float(regime["position_multiplier"])
            open_positions = [item for item in app.state.paper["positions"] if item.get("status") == "AÇIK"]
            current_exposure = sum(float(item.get("amount") or 0.0) for item in open_positions)
            reserved_margin = sum(
                float(item.get("amount") or 0.0)
                for item in app.state.paper.get("limit_orders", [])
                if item.get("status") == "BEKLİYOR"
            )
            available = max(0.0, float(app.state.paper["balance"]) - current_exposure - reserved_margin)
            allocation = dynamic_paper_allocation(
                balance=float(app.state.paper["balance"]),
                available=available,
                current_exposure=current_exposure,
                entry_price=entry_reference,
                stop_loss=float(stop_loss),
                tp3=float(tp3),
                confidence=float(candidate["confidence"]),
                risk_score=float(guard["risk_score"]),
                regime_multiplier=regime_multiplier,
                profile=allocation_profile,
            )
            allocation.update({
                "symbol": candidate["symbol"],
                "display": candidate["display"],
                "direction": candidate["direction"],
                "confidence": candidate["confidence"],
                "edge_score": candidate["edge_score"],
                "calculated_at": datetime.now(timezone.utc).isoformat(),
            })
            autonomy["last_allocation"] = allocation
            if not allocation["approved"]:
                guard_note = guard_note or f"{candidate['display']}: {allocation['status']}"
                remember(allocation["reason"], "Otonom Sermaye", allocation["status"])
                continue
            amount = float(allocation["amount"])
            if shadow_payload(app.state.paper)["enabled"]:
                await record_shadow_candidate(candidate, amount, regime, liquidity, session, confidence_floor)
                bot["cycles"] += 1
                bot["last_blocker"] = "Gölge Modu açık; sinyal kaydedildi fakat pozisyon açılmadı"
                record_bot_event("GÖLGE", f"{candidate['display']} için uygun sinyal kaydedildi; Paper pozisyon açılmadı", candidate["symbol"])
                return
            await paper_open(PaperOrder(
                symbol=candidate["symbol"], direction=candidate["direction"],
                amount=amount,
                stop_loss=stop_loss, take_profit=take_profit, tp2=tp2, tp3=tp3, source="AUTO",
                signal_confidence=candidate["confidence"], guard_mode=guard["market_mode"], gate_status=gate["status"],
                freshness_status=freshness["status"], entry_drift_atr=freshness["drift_atr"],
                regime_label=regime["label"], regime_policy=regime["entry_policy"],
                portfolio_mode=portfolio["mode"], portfolio_correlation=portfolio["correlation_pct"],
                adaptive_mode=f"EĞİTİM · {adaptive['mode']}" if training_mode else adaptive["mode"],
                adaptive_confidence_floor=confidence_floor,
                stability_mode=stability["mode"], stability_samples=stability["stable_samples"],
                liquidity_mode=liquidity["mode"], liquidity_score=liquidity["liquidity_score"],
                liquidity_spread_bps=liquidity["spread_bps"], session_label=session["current_session"]["label"],
                session_mode=session["current_session"]["status"], session_confidence_bonus=session["confidence_bonus"],
            ))
            bot["cycles"] += 1
            bot["last_blocker"] = None
            event_kind = "EĞİTİM AÇILDI" if training_mode else "AÇILDI"
            record_bot_event(
                event_kind,
                f"OTONOM {candidate['direction']} açıldı: {candidate['display']} · "
                f"{amount:.0f} USDT · plan net senaryosu {allocation['projected_plan_net_usdt']:.2f} USDT · "
                f"stop riski {allocation['projected_stop_loss_usdt']:.2f} USDT · {regime['label']}",
                candidate["symbol"],
            )
            return
        bot["cycles"] += 1
        bot["last_blocker"] = guard_note or "Tarama eşiğini geçen güvenli sinyal bulunamadı"
        record_bot_event("BEKLE", bot["last_blocker"])
    except Exception as exc:
        bot["last_blocker"] = f"Geçici tarama hatası: {str(exc)[:80]}"
        record_bot_event("HATA", f"Tarama bekleniyor: {str(exc)[:80]}")
    finally:
        if memory_changed:
            asyncio.create_task(persist_paper_snapshot(app))
        bot["busy"] = False


async def paper_bot_loop(application: FastAPI) -> None:
    while True:
        try:
            if application.state.paper_bot["enabled"]:
                await paper_bot_cycle()
        except Exception as exc:
            record_bot_event("HATA", f"Bot geçici hata: {str(exc)[:70]}")
        policy = active_paper_bot_policy(application.state.paper_bot)
        await asyncio.sleep(policy["cycle_seconds"])


@app.get("/api/paper/bot")
async def paper_bot_status():
    app.state.paper_bot["orders_enabled"] = False
    app.state.paper_bot["testnet_orders_enabled"] = False
    paper_autonomy_payload(app.state.paper, app.state.paper_bot)
    return app.state.paper_bot


@app.post("/api/paper/bot/start")
async def paper_bot_start():
    bot = app.state.paper_bot
    bot["enabled"] = True
    policy = active_paper_bot_policy(bot)
    bot.update({
        "mode": policy["mode"],
        "scan_interval_seconds": policy["cycle_seconds"],
        "orders_enabled": False,
        "testnet_orders_enabled": False,
    })
    paper_autonomy_payload(app.state.paper, bot)
    record_bot_event(
        "BAŞLADI",
        f"{policy['mode']} aktif; {policy['universe_size']} coin taranıp sanal sermaye Stop riskine göre dağıtılacak",
    )
    asyncio.create_task(paper_bot_cycle())
    return bot


@app.post("/api/paper/bot/stop")
async def paper_bot_stop():
    app.state.paper_bot["enabled"] = False
    record_bot_event("DURDU", "Paper Bot kullanıcı tarafından durduruldu")
    return app.state.paper_bot


@app.post("/api/paper/bot/training/toggle")
async def paper_bot_training_toggle():
    """Yalnızca sanal hesapta hızlı örnek üretme politikasını açıp kapatır."""
    bot = app.state.paper_bot
    bot["training_mode"] = not bool(bot.get("training_mode", True))
    policy = active_paper_bot_policy(bot)
    bot.update({
        "mode": policy["mode"],
        "scan_interval_seconds": policy["cycle_seconds"],
        "last_blocker": None,
        "orders_enabled": False,
        "testnet_orders_enabled": False,
    })
    record_bot_event("MOD", f"{policy['mode']} seçildi · tarama {policy['cycle_seconds']} saniyede bir")
    if bot["enabled"]:
        asyncio.create_task(paper_bot_cycle())
    return bot


@app.get("/api/v20/profile")
async def v20_profile_status():
    bot = app.state.paper_bot
    return {"active": bot.get("profile", "DENGELI"), "policy": active_paper_bot_policy(bot), "profiles": [v20_profile_policy(item) for item in ("TEMKINLI", "DENGELI", "HIZLI")]}


@app.post("/api/v20/profile/{profile}")
async def v20_profile_select(profile: str):
    normalized = str(profile).strip().upper().replace("İ", "I")
    if normalized not in {"TEMKINLI", "DENGELI", "HIZLI"}:
        raise HTTPException(422, "Profil TEMKINLI, DENGELI veya HIZLI olmalı")
    bot = app.state.paper_bot
    bot["profile"] = normalized
    bot["training_mode"] = True
    policy = active_paper_bot_policy(bot)
    bot.update({
        "mode": policy["mode"], "scan_interval_seconds": policy["cycle_seconds"],
        "last_blocker": None, "orders_enabled": False, "testnet_orders_enabled": False,
    })
    record_bot_event("V20 PROFİL", f"{policy['label']} Paper profili seçildi · {policy['cycle_seconds']} saniye tarama")
    if bot["enabled"]:
        asyncio.create_task(paper_bot_cycle())
    return bot


@app.post("/api/paper/close/{position_id}")
async def paper_close(position_id: int):
    await refresh_paper_positions()
    paper = app.state.paper
    async with paper["lock"]:
        position = next((item for item in paper["positions"] if item["id"] == position_id and item["status"] == "AÇIK"), None)
        if not position:
            raise HTTPException(404, "Açık sanal pozisyon bulunamadı")
    price = await latest_price(position["symbol"])
    async with paper["lock"]:
        gross_pnl = (price - position["entry_price"]) * position["quantity"]
        if position["direction"] == "SHORT":
            gross_pnl *= -1
        fee = position["amount"] * 0.001
        net_pnl = gross_pnl - fee
        total_pnl = float(position.get("partial_realized_pnl") or 0.0) + net_pnl
        total_fee = float(position.get("fee") or 0.0) + fee
        closed_at = datetime.now(timezone.utc).isoformat()
        lifecycle = position.setdefault("lifecycle_events", [])
        lifecycle.insert(0, {"kind": "MANUEL", "price": round(price, 10), "quantity": round(float(position["quantity"]), 12), "net_pnl": round(net_pnl, 8), "created_at": closed_at})
        position.update({
            "current_price": price, "unrealized_pnl": 0.0, "realized_pnl": total_pnl,
            "partial_realized_pnl": total_pnl, "fee": total_fee, "status": "MANUEL",
            "quantity": 0.0, "amount": 0.0, "closed_at": closed_at,
        })
        paper["balance"] += net_pnl
        register_paper_result(paper, total_pnl)
        paper["trades"].insert(0, position.copy())
        close_message = paper_lifecycle_event_message(position, lifecycle[0])
        add_paper_notification(paper, "MANUEL", close_message)
    record_bot_event("MANUEL", close_message, str(position.get("symbol") or ""))
    asyncio.create_task(persist_paper_snapshot(app))
    return {"message": "Sanal pozisyon kapatıldı", "account": paper_payload()}


# ---------------------------------------------------------------------------
# V9 · Canlı Dijital Borsa İkizi
# ---------------------------------------------------------------------------

BINANCE_WS = "wss://stream.binance.com:443/stream"


def empty_v9_market_twin_state() -> dict:
    """V9 her yeniden başlatmada güvenli ve kullanıcı onayı bekleyen durumda açılır."""
    return {
        "enabled": False,
        "status": "KULLANICI ONAYI BEKLİYOR",
        "stream_health": "BEKLEMEDE",
        "universe": list(V9_DEFAULT_UNIVERSE),
        "latest": {},
        "tick_history": {},
        "ticks_captured": 0,
        "cycles": 0,
        "gap_count": 0,
        "recovered_candles": 0,
        "reconnect_count": 0,
        "error_count": 0,
        "generation": 0,
        "started_at": None,
        "stopped_at": None,
        "last_tick_at": None,
        "last_action": "V9 canlı veri kaydı yalnızca kullanıcı başlatınca çalışır.",
        "events": [],
        "paper_fills": [],
        "rollback": {
            "active": False,
            "status": "HAZIR",
            "safe_profile": "V7 DURDUR · V6 DENGELİ PAPER",
            "last_action": "Kritik strateji sapmasında Paper motorları güvenli profile alınır.",
            "triggered_at": None,
        },
        "orders_enabled": False,
        "testnet_orders_enabled": False,
        "mode": "PUBLIC_DATA_AND_PAPER_ONLY",
    }


def v9_add_event(state: dict, kind: str, message: str, symbol: str | None = None, details: dict | None = None) -> dict:
    event = {
        "kind": kind,
        "message": message,
        "symbol": symbol,
        "details": details or {},
        "created_at": datetime.now(timezone.utc).isoformat(),
        "paper_only": True,
    }
    state.setdefault("events", []).insert(0, event)
    state["events"] = state["events"][:V9_EVENT_LIMIT]
    return event


async def ensure_v9_schema(application: FastAPI) -> None:
    """İlk kurulumdan sonra gelen yükseltmelerde de V9 tablolarını güvenle ekler."""
    pool = application.state.db_pool
    if pool is None:
        return
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS market_ticks (
          time TIMESTAMPTZ NOT NULL,
          symbol TEXT NOT NULL,
          price DOUBLE PRECISION NOT NULL,
          bid DOUBLE PRECISION NOT NULL,
          ask DOUBLE PRECISION NOT NULL,
          bid_qty DOUBLE PRECISION NOT NULL,
          ask_qty DOUBLE PRECISION NOT NULL,
          spread_bps DOUBLE PRECISION NOT NULL,
          quote_volume_24h DOUBLE PRECISION NOT NULL,
          source TEXT NOT NULL,
          PRIMARY KEY (time, symbol)
        )
        """
    )
    await pool.execute("SELECT create_hypertable('market_ticks', by_range('time'), if_not_exists => TRUE)")
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS orderbook_snapshots (
          time TIMESTAMPTZ NOT NULL,
          symbol TEXT NOT NULL,
          best_bid DOUBLE PRECISION NOT NULL,
          best_ask DOUBLE PRECISION NOT NULL,
          bid_qty DOUBLE PRECISION NOT NULL,
          ask_qty DOUBLE PRECISION NOT NULL,
          spread_bps DOUBLE PRECISION NOT NULL,
          PRIMARY KEY (time, symbol)
        )
        """
    )
    await pool.execute("SELECT create_hypertable('orderbook_snapshots', by_range('time'), if_not_exists => TRUE)")
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS market_twin_events (
          id BIGSERIAL PRIMARY KEY,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          kind TEXT NOT NULL,
          symbol TEXT,
          message TEXT NOT NULL,
          details JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_twin_fills (
          id TEXT PRIMARY KEY,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          strategy TEXT NOT NULL,
          requested_notional DOUBLE PRECISION NOT NULL,
          filled_notional DOUBLE PRECISION NOT NULL,
          fill_pct DOUBLE PRECISION NOT NULL,
          execution_price DOUBLE PRECISION NOT NULL,
          quantity DOUBLE PRECISION NOT NULL,
          fee_usdt DOUBLE PRECISION NOT NULL,
          impact_bps DOUBLE PRECISION NOT NULL,
          latency_ms INTEGER NOT NULL,
          paper_only BOOLEAN NOT NULL DEFAULT TRUE
        )
        """
    )


async def restore_v9_history(application: FastAPI) -> None:
    """Kayıt geçmişini geri getirir; canlı akışı hiçbir zaman otomatik başlatmaz."""
    pool = application.state.db_pool
    if pool is None:
        return
    try:
        fill_rows = await pool.fetch(
            """
            SELECT id, created_at, symbol, side, strategy, requested_notional,
                   filled_notional, fill_pct, execution_price, quantity, fee_usdt,
                   impact_bps, latency_ms, paper_only
            FROM paper_twin_fills ORDER BY created_at DESC LIMIT $1
            """,
            V9_FILL_LIMIT,
        )
        event_rows = await pool.fetch(
            """
            SELECT created_at, kind, symbol, message, details
            FROM market_twin_events ORDER BY created_at DESC LIMIT $1
            """,
            V9_EVENT_LIMIT,
        )
    except Exception:
        return
    async with application.state.market_twin_lock:
        state = application.state.market_twin
        state["paper_fills"] = [
            {
                **dict(row),
                "created_at": row["created_at"].isoformat(),
                "orders_enabled": False,
            }
            for row in fill_rows
        ]
        state["events"] = [
            {
                **dict(row),
                "created_at": row["created_at"].isoformat(),
                "details": safe_json_object(row["details"]),
                "paper_only": True,
            }
            for row in event_rows
        ]
        state["enabled"] = False
        state["status"] = "YENİDEN BAŞLATMA ONAYI"
        state["stream_health"] = "BEKLEMEDE"
        state["last_action"] = "Geçmiş geri yüklendi; canlı kayıt için yeniden kullanıcı onayı gerekiyor."


def v9_detect_gap(previous: datetime | str | float | int | None, current: datetime, expected_seconds: float = 2.0) -> dict:
    if previous is None:
        return {"detected": False, "gap_seconds": 0.0, "missing_windows": 0}
    try:
        if isinstance(previous, datetime):
            previous_dt = previous
        elif isinstance(previous, (int, float)):
            previous_dt = datetime.fromtimestamp(float(previous), timezone.utc)
        else:
            previous_dt = datetime.fromisoformat(str(previous).replace("Z", "+00:00"))
        if previous_dt.tzinfo is None:
            previous_dt = previous_dt.replace(tzinfo=timezone.utc)
        gap_seconds = max(0.0, (current - previous_dt).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return {"detected": False, "gap_seconds": 0.0, "missing_windows": 0}
    threshold = max(3.0, expected_seconds * 3.0)
    detected = gap_seconds > threshold
    missing = max(0, int(gap_seconds // max(expected_seconds, 0.5)) - 1) if detected else 0
    return {"detected": detected, "gap_seconds": round(gap_seconds, 2), "missing_windows": missing}


def v9_paper_fill_model(side: str, notional: float, book: dict) -> dict:
    """Canlı en iyi fiyat/derinlikten deterministik bir sanal dolum üretir."""
    safe_side = side.upper()
    if safe_side not in {"BUY", "SELL"}:
        raise ValueError("V9 Paper tarafı BUY veya SELL olmalı")
    safe_notional = max(10.0, min(float(notional), 5_000.0))
    bid = float(book.get("bid") or 0.0)
    ask = float(book.get("ask") or 0.0)
    bid_qty = max(0.0, float(book.get("bid_qty") or 0.0))
    ask_qty = max(0.0, float(book.get("ask_qty") or 0.0))
    reference = ask if safe_side == "BUY" else bid
    visible_qty = ask_qty if safe_side == "BUY" else bid_qty
    if reference <= 0:
        raise ValueError("Canlı emir defteri fiyatı henüz hazır değil")
    visible_usdt = visible_qty * reference
    fill_pct = min(100.0, max(0.0, (visible_usdt / safe_notional) * 100.0))
    filled_notional = safe_notional * fill_pct / 100.0
    spread_bps = max(0.0, float(book.get("spread_bps") or 0.0))
    participation = filled_notional / max(visible_usdt, 1.0)
    impact_bps = min(120.0, max(spread_bps / 2.0, participation * 3.5))
    direction = 1.0 if safe_side == "BUY" else -1.0
    execution_price = reference * (1.0 + direction * impact_bps / 10_000.0)
    quantity = filled_notional / execution_price if execution_price > 0 else 0.0
    fee_usdt = filled_notional * 0.001
    latency_ms = int(min(900, 45 + spread_bps * 8 + participation * 180))
    return {
        "side": safe_side,
        "requested_notional": round(safe_notional, 2),
        "filled_notional": round(filled_notional, 4),
        "fill_pct": round(fill_pct, 2),
        "execution_price": round(execution_price, 8),
        "quantity": round(quantity, 12),
        "fee_usdt": round(fee_usdt, 6),
        "impact_bps": round(impact_bps, 2),
        "latency_ms": latency_ms,
        "status": "TAM DOLUM" if fill_pct >= 99.95 else "KISMİ DOLUM" if fill_pct > 0 else "DOLUM YOK",
        "paper_only": True,
        "orders_enabled": False,
    }


def v9_strategy_drift(trades: list[dict]) -> dict:
    """Yakın dönem Paper sonuçlarını önceki dönemle karşılaştırır."""
    usable = []
    for trade in trades:
        try:
            amount = max(float(trade.get("amount") or 0.0), 1.0)
            pnl = float(trade.get("realized_pnl"))
        except (TypeError, ValueError):
            continue
        usable.append({
            "strategy": str(trade.get("strategy") or trade.get("source") or "PAPER"),
            "return_pct": pnl / amount * 100.0,
            "win": pnl > 0,
        })
    if len(usable) < 12:
        return {
            "status": "ÖĞRENİYOR",
            "drift_score": min(35, len(usable) * 2),
            "samples": len(usable),
            "recent_win_rate": 0.0,
            "baseline_win_rate": 0.0,
            "recent_return_pct": 0.0,
            "baseline_return_pct": 0.0,
            "worst_strategy": None,
            "rollback_required": False,
            "reason": "Sapma ölçümü için en az 12 kapanmış Paper işlem gerekir.",
            "orders_enabled": False,
        }
    window = min(10, len(usable) // 2)
    recent = usable[:window]
    baseline = usable[window:window * 2]
    recent_win = sum(item["win"] for item in recent) / len(recent) * 100.0
    baseline_win = sum(item["win"] for item in baseline) / len(baseline) * 100.0
    recent_return = sum(item["return_pct"] for item in recent) / len(recent)
    baseline_return = sum(item["return_pct"] for item in baseline) / len(baseline)
    win_gap = recent_win - baseline_win
    return_gap = recent_return - baseline_return
    score = min(99.0, abs(min(0.0, win_gap)) * 0.75 + abs(min(0.0, return_gap)) * 18.0 + max(0.0, -recent_return) * 12.0)
    by_strategy: dict[str, list[float]] = {}
    for item in recent:
        by_strategy.setdefault(item["strategy"], []).append(item["return_pct"])
    worst_strategy = min(by_strategy, key=lambda key: sum(by_strategy[key]) / len(by_strategy[key])) if by_strategy else None
    rollback_required = score >= 70 or (recent_return < -1.0 and recent_win <= 30.0)
    status = "KRİTİK SAPMA" if rollback_required else "UYARI" if score >= 42 else "DENGELİ"
    reason = (
        "Yakın dönem Paper sonucu taban çizgisinden belirgin koptu; güvenli geri dönüş gerekli."
        if rollback_required else
        "Yakın dönem zayıflıyor; yeni Paper tahsisleri daha sıkı izlenmeli."
        if status == "UYARI" else
        "Yakın dönem sonuçları tarihsel Paper davranışıyla uyumlu."
    )
    return {
        "status": status,
        "drift_score": round(score, 1),
        "samples": len(usable),
        "recent_win_rate": round(recent_win, 1),
        "baseline_win_rate": round(baseline_win, 1),
        "recent_return_pct": round(recent_return, 3),
        "baseline_return_pct": round(baseline_return, 3),
        "worst_strategy": worst_strategy,
        "rollback_required": rollback_required,
        "reason": reason,
        "orders_enabled": False,
    }


def v9_pnl_attribution(trades: list[dict], paper_fills: list[dict], latest: dict) -> dict:
    groups: dict[str, dict] = {}

    def add(group: str, realized: float = 0.0, unrealized: float = 0.0, fees: float = 0.0, count: int = 1) -> None:
        row = groups.setdefault(group, {"source": group, "trades": 0, "realized_pnl": 0.0, "unrealized_pnl": 0.0, "fees_usdt": 0.0})
        row["trades"] += count
        row["realized_pnl"] += realized
        row["unrealized_pnl"] += unrealized
        row["fees_usdt"] += fees

    for trade in trades:
        try:
            realized = float(trade.get("realized_pnl") or 0.0)
            fees = float(trade.get("fee") or 0.0)
        except (TypeError, ValueError):
            continue
        group = str(trade.get("strategy") or f"PAPER {trade.get('source') or 'MANUEL'}")
        add(group, realized=realized, fees=fees)
    for fill in paper_fills:
        symbol = str(fill.get("symbol") or "")
        mark = latest.get(symbol, {}).get("price")
        try:
            mark_price = float(mark)
            entry = float(fill.get("execution_price") or 0.0)
            quantity = float(fill.get("quantity") or 0.0)
            fee = float(fill.get("fee_usdt") or 0.0)
        except (TypeError, ValueError):
            continue
        gross = (mark_price - entry) * quantity
        if str(fill.get("side")) == "SELL":
            gross *= -1.0
        add(f"V9 {fill.get('strategy') or 'MANUEL'}", unrealized=gross - fee, fees=fee)
    items = []
    for row in groups.values():
        row = {**row}
        row["realized_pnl"] = round(row["realized_pnl"], 2)
        row["unrealized_pnl"] = round(row["unrealized_pnl"], 2)
        row["fees_usdt"] = round(row["fees_usdt"], 2)
        row["net_pnl"] = round(row["realized_pnl"] + row["unrealized_pnl"], 2)
        items.append(row)
    items.sort(key=lambda item: item["net_pnl"], reverse=True)
    return {
        "items": items,
        "total_realized_pnl": round(sum(item["realized_pnl"] for item in items), 2),
        "total_unrealized_pnl": round(sum(item["unrealized_pnl"] for item in items), 2),
        "total_fees_usdt": round(sum(item["fees_usdt"] for item in items), 2),
        "net_pnl": round(sum(item["net_pnl"] for item in items), 2),
        "orders_enabled": False,
        "note": "Kaynak katkıları yalnızca Paper işlemler ve V9 sanal dolumlardan hesaplanır.",
    }


def v9_daily_report_payload(state: dict, drift: dict, attribution: dict, database_status: str = "BEKLENİYOR") -> dict:
    ticks = int(state.get("ticks_captured") or 0)
    gaps = int(state.get("gap_count") or 0)
    quality = max(0.0, 100.0 - gaps / max(ticks, 1) * 100.0)
    if drift.get("rollback_required"):
        status = "KORUMA AKTİF"
    elif state.get("enabled") and state.get("stream_health") == "BAĞLI":
        status = "CANLI VE SAĞLIKLI"
    elif state.get("enabled"):
        status = "YENİDEN BAĞLANIYOR"
    else:
        status = "KULLANICI ONAYI BEKLİYOR"
    return {
        "date": datetime.now(timezone.utc).date().isoformat(),
        "status": status,
        "ticks_captured": ticks,
        "data_quality_pct": round(quality, 2),
        "gap_count": gaps,
        "recovered_candles": int(state.get("recovered_candles") or 0),
        "reconnect_count": int(state.get("reconnect_count") or 0),
        "database": database_status,
        "drift_status": drift.get("status"),
        "paper_net_pnl": attribution.get("net_pnl", 0.0),
        "paper_fills": len(state.get("paper_fills") or []),
        "headline": "V9 piyasa kopyası veri, boşluk, Paper maliyet ve strateji sapmasını birlikte denetliyor.",
        "orders_enabled": False,
        "safety_note": "Gerçek ve Testnet emir kanalları kapalıdır; rapor yalnızca gözlem ve Paper sonuçlarını içerir.",
    }


def v9_market_twin_payload(state: dict, paper_trades: list[dict], database_status: str = "BEKLENİYOR") -> dict:
    now = datetime.now(timezone.utc)
    symbols = []
    latest = state.get("latest") or {}
    for symbol in state.get("universe") or []:
        tick = latest.get(symbol)
        age_seconds = None
        if tick and tick.get("time"):
            try:
                seen = datetime.fromisoformat(str(tick["time"]).replace("Z", "+00:00"))
                age_seconds = max(0.0, (now - seen).total_seconds())
            except (TypeError, ValueError):
                age_seconds = None
        symbols.append({
            "symbol": symbol,
            "price": tick.get("price") if tick else None,
            "bid": tick.get("bid") if tick else None,
            "ask": tick.get("ask") if tick else None,
            "spread_bps": tick.get("spread_bps") if tick else None,
            "quote_volume_24h": tick.get("quote_volume_24h") if tick else None,
            "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
            "health": "CANLI" if age_seconds is not None and age_seconds <= V9_STREAM_STALE_SECONDS else "BAYAT" if tick else "BEKLİYOR",
        })
    drift = v9_strategy_drift(paper_trades)
    attribution = v9_pnl_attribution(paper_trades, state.get("paper_fills") or [], latest)
    coverage = sum(1 for item in symbols if item["health"] == "CANLI") / max(len(symbols), 1) * 100.0
    report = v9_daily_report_payload(state, drift, attribution, database_status)
    return {
        "version": "9.0.0",
        "enabled": bool(state.get("enabled")),
        "status": state.get("status"),
        "stream_health": state.get("stream_health"),
        "universe": list(state.get("universe") or []),
        "symbols": symbols,
        "coverage_pct": round(coverage, 1),
        "ticks_captured": int(state.get("ticks_captured") or 0),
        "cycles": int(state.get("cycles") or 0),
        "gap_count": int(state.get("gap_count") or 0),
        "recovered_candles": int(state.get("recovered_candles") or 0),
        "reconnect_count": int(state.get("reconnect_count") or 0),
        "error_count": int(state.get("error_count") or 0),
        "started_at": state.get("started_at"),
        "stopped_at": state.get("stopped_at"),
        "last_tick_at": state.get("last_tick_at"),
        "last_action": state.get("last_action"),
        "events": list(state.get("events") or [])[:12],
        "paper_fills": list(state.get("paper_fills") or [])[:12],
        "drift": drift,
        "pnl_attribution": attribution,
        "rollback": dict(state.get("rollback") or {}),
        "daily_report": report,
        "database": database_status,
        "orders_enabled": False,
        "testnet_orders_enabled": False,
        "safety_note": "V9 yalnızca Binance herkese açık verisini okur ve yerel Paper dolum üretir; borsaya emir göndermez.",
    }


async def v9_persist_event(application: FastAPI, event: dict) -> None:
    pool = application.state.db_pool
    if pool is None or not application.state.market_twin_schema_ready:
        return
    try:
        await pool.execute(
            """
            INSERT INTO market_twin_events (created_at, kind, symbol, message, details)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            """,
            datetime.fromisoformat(event["created_at"]), event["kind"], event.get("symbol"),
            event["message"], json.dumps(event.get("details") or {}),
        )
    except Exception:
        pass


async def v9_persist_tick(application: FastAPI, tick: dict) -> None:
    pool = application.state.db_pool
    if pool is None or not application.state.market_twin_schema_ready:
        return
    timestamp = datetime.fromisoformat(tick["time"])
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO market_ticks
                      (time, symbol, price, bid, ask, bid_qty, ask_qty, spread_bps, quote_volume_24h, source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (time, symbol) DO NOTHING
                    """,
                    timestamp, tick["symbol"], tick["price"], tick["bid"], tick["ask"],
                    tick["bid_qty"], tick["ask_qty"], tick["spread_bps"], tick["quote_volume_24h"], tick["source"],
                )
                await connection.execute(
                    """
                    INSERT INTO orderbook_snapshots
                      (time, symbol, best_bid, best_ask, bid_qty, ask_qty, spread_bps)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (time, symbol) DO NOTHING
                    """,
                    timestamp, tick["symbol"], tick["bid"], tick["ask"],
                    tick["bid_qty"], tick["ask_qty"], tick["spread_bps"],
                )
    except Exception:
        pass


async def v9_backfill_candles(application: FastAPI, symbol: str, gap_seconds: float) -> int:
    pool = application.state.db_pool
    if pool is None or not application.state.market_twin_schema_ready:
        return 0
    limit = max(3, min(120, int(gap_seconds // 60) + 3))
    try:
        candles = await fetch_candles(symbol, "1m", limit)
        rows = [
            (
                datetime.fromtimestamp(item["time"], timezone.utc), symbol, "1m",
                item["open"], item["high"], item["low"], item["close"], item["volume"],
            )
            for item in candles
        ]
        await pool.executemany(
            """
            INSERT INTO candles (time, symbol, timeframe, open, high, low, close, volume)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (time, symbol, timeframe) DO NOTHING
            """,
            rows,
        )
        return len(rows)
    except Exception:
        return 0


async def v9_apply_safe_rollback(application: FastAPI) -> None:
    paper = application.state.paper
    async with paper["lock"]:
        trades = [dict(item) for item in paper.get("trades", [])]
        drift = v9_strategy_drift(trades)
        orchestrator = paper.get("strategy_orchestrator", {})
        triggered = bool(drift.get("rollback_required") and orchestrator.get("enabled"))
        if triggered:
            orchestrator["enabled"] = False
            orchestrator["status"] = "V9 GÜVENLİ GERİ DÖNÜŞ"
            orchestrator["last_action"] = "V9 Drift Radarı kritik sapma gördü; yalnızca Paper Orkestra durduruldu."
    if not triggered:
        return
    now = datetime.now(timezone.utc).isoformat()
    async with application.state.market_twin_lock:
        state = application.state.market_twin
        state["rollback"] = {
            "active": True,
            "status": "UYGULANDI",
            "safe_profile": "V7 DURDUR · V6 DENGELİ PAPER",
            "last_action": "Kritik sapma nedeniyle V7 Paper Orkestra durduruldu; gerçek emir etkilenmedi çünkü zaten kilitli.",
            "triggered_at": now,
        }
        event = v9_add_event(state, "GÜVENLİ GERİ DÖNÜŞ", state["rollback"]["last_action"], details={"drift_score": drift["drift_score"]})
    asyncio.create_task(persist_paper_snapshot(application))
    asyncio.create_task(v9_persist_event(application, event))


async def v9_process_stream_message(application: FastAPI, payload: dict) -> None:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return
    symbol = str(data.get("s") or "").upper()
    if not symbol:
        return
    try:
        price = float(data.get("c") or 0.0)
        bid = float(data.get("b") or 0.0)
        ask = float(data.get("a") or 0.0)
        bid_qty = float(data.get("B") or 0.0)
        ask_qty = float(data.get("A") or 0.0)
        quote_volume = float(data.get("q") or 0.0)
    except (TypeError, ValueError):
        return
    if price <= 0 or bid <= 0 or ask <= 0 or ask < bid:
        return
    event_ms = int(data.get("E") or int(time.time() * 1000))
    observed_at = datetime.fromtimestamp(event_ms / 1000.0, timezone.utc)
    spread_bps = (ask - bid) / max((ask + bid) / 2.0, 1e-12) * 10_000.0
    tick = {
        "time": observed_at.isoformat(), "symbol": symbol, "price": price,
        "bid": bid, "ask": ask, "bid_qty": bid_qty, "ask_qty": ask_qty,
        "spread_bps": round(spread_bps, 4), "quote_volume_24h": quote_volume,
        "source": "BINANCE_PUBLIC_WEBSOCKET", "orders_enabled": False,
    }
    gap = None
    gap_event = None
    async with application.state.market_twin_lock:
        state = application.state.market_twin
        if symbol not in state.get("universe", []):
            return
        previous = (state.get("latest") or {}).get(symbol, {}).get("time")
        gap = v9_detect_gap(previous, observed_at)
        state.setdefault("latest", {})[symbol] = tick
        history = state.setdefault("tick_history", {}).setdefault(symbol, [])
        history.append({"time": tick["time"], "price": tick["price"], "spread_bps": tick["spread_bps"]})
        state["tick_history"][symbol] = history[-60:]
        state["ticks_captured"] += 1
        state["cycles"] += 1
        state["last_tick_at"] = tick["time"]
        state["stream_health"] = "BAĞLI"
        state["status"] = "CANLI KAYIT"
        state["last_action"] = f"{symbol} canlı fiyat ve en iyi emir defteri seviyesi kaydedildi."
        if gap["detected"]:
            state["gap_count"] += 1
            gap_event = v9_add_event(
                state, "VERİ BOŞLUĞU", f"{symbol} akışında {gap['gap_seconds']:.1f} saniyelik boşluk algılandı; mum onarımı başlatıldı.",
                symbol, gap,
            )
        cycle = state["cycles"]
    await v9_persist_tick(application, tick)
    if gap_event:
        await v9_persist_event(application, gap_event)
        recovered = await v9_backfill_candles(application, symbol, gap["gap_seconds"])
        async with application.state.market_twin_lock:
            application.state.market_twin["recovered_candles"] += recovered
            if recovered:
                application.state.market_twin["last_action"] = f"{symbol} için {recovered} mum güvenli biçimde yeniden yazıldı."
    if cycle % 20 == 0:
        await v9_apply_safe_rollback(application)


async def v9_market_twin_loop(application: FastAPI) -> None:
    """Kullanıcı onayından sonra çoklu-parite halka açık WebSocket akışını tutar."""
    while True:
        try:
            async with application.state.market_twin_lock:
                state = application.state.market_twin
                enabled = bool(state.get("enabled"))
                universe = list(state.get("universe") or V9_DEFAULT_UNIVERSE)
                generation = int(state.get("generation") or 0)
            if not enabled:
                await asyncio.sleep(1)
                continue
            streams = "/".join(f"{symbol.lower()}@ticker" for symbol in universe)
            uri = f"{BINANCE_WS}?streams={streams}"
            async with application.state.market_twin_lock:
                state = application.state.market_twin
                state["status"] = "BAĞLANIYOR"
                state["stream_health"] = "BAĞLANIYOR"
                state["last_action"] = "Binance halka açık çoklu WebSocket akışı kuruluyor."
            async with websockets.connect(uri, open_timeout=12, ping_interval=20, ping_timeout=20, close_timeout=5, max_queue=512) as socket:
                async with application.state.market_twin_lock:
                    state = application.state.market_twin
                    state["status"] = "CANLI KAYIT"
                    state["stream_health"] = "BAĞLI"
                    connected_event = v9_add_event(state, "AKIŞ BAĞLANDI", f"{len(universe)} parite için V9 canlı kayıt başladı.")
                asyncio.create_task(v9_persist_event(application, connected_event))
                while True:
                    async with application.state.market_twin_lock:
                        state = application.state.market_twin
                        still_active = bool(state.get("enabled")) and int(state.get("generation") or 0) == generation
                    if not still_active:
                        break
                    raw = await asyncio.wait_for(socket.recv(), timeout=15)
                    await v9_process_stream_message(application, json.loads(raw))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            async with application.state.market_twin_lock:
                state = application.state.market_twin
                if state.get("enabled"):
                    state["status"] = "YENİDEN BAĞLANIYOR"
                    state["stream_health"] = "YENİDEN BAĞLANIYOR"
                    state["reconnect_count"] += 1
                    state["error_count"] += 1
                    state["last_action"] = "Canlı akış kesildi; V9 güvenli biçimde yeniden bağlanıyor."
                    error_event = v9_add_event(state, "AKIŞ YENİDEN BAĞLANIYOR", f"Geçici veri bağlantısı: {str(exc)[:90]}")
                else:
                    error_event = None
            if error_event:
                asyncio.create_task(v9_persist_event(application, error_event))
            await asyncio.sleep(3)


class V9TwinStart(BaseModel):
    symbols: list[str] = Field(default_factory=lambda: list(V9_DEFAULT_UNIVERSE), min_length=1, max_length=8)


class V9PaperTwinOrder(BaseModel):
    symbol: str = Field(min_length=6, max_length=20)
    side: Literal["BUY", "SELL"]
    notional: float = Field(default=100.0, ge=10.0, le=5_000.0)
    strategy: str = Field(default="MANUEL", min_length=2, max_length=30)


async def v9_snapshot_payload() -> dict:
    async with app.state.market_twin_lock:
        state = json.loads(json.dumps(app.state.market_twin))
    async with app.state.paper["lock"]:
        trades = [dict(item) for item in app.state.paper.get("trades", [])]
    database_status = "KALICI" if app.state.market_twin_schema_ready and app.state.db_pool is not None else "HAFIZA MODU"
    return v9_market_twin_payload(state, trades, database_status)


@app.get("/api/v9/twin")
async def v9_twin_status():
    return await v9_snapshot_payload()


@app.post("/api/v9/twin/start")
async def v9_twin_start(config: V9TwinStart):
    symbols = []
    for raw_symbol in config.symbols:
        symbol = "".join(char for char in raw_symbol.upper() if char.isalnum())
        if symbol.endswith("USDT") and symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise HTTPException(422, "En az bir geçerli USDT paritesi seçin")
    symbols = symbols[:8]
    async with app.state.market_twin_lock:
        state = app.state.market_twin
        state["enabled"] = True
        state["status"] = "BAĞLANIYOR"
        state["stream_health"] = "BAĞLANIYOR"
        state["universe"] = symbols
        state["generation"] += 1
        state["started_at"] = datetime.now(timezone.utc).isoformat()
        state["stopped_at"] = None
        state["last_action"] = "Kullanıcı onayı alındı; V9 çoklu-parite akışı başlatılıyor."
        event = v9_add_event(state, "V9 BAŞLADI", f"Canlı Dijital Borsa İkizi {len(symbols)} pariteyle başlatıldı.")
    asyncio.create_task(v9_persist_event(app, event))
    return await v9_snapshot_payload()


@app.post("/api/v9/twin/stop")
async def v9_twin_stop():
    async with app.state.market_twin_lock:
        state = app.state.market_twin
        state["enabled"] = False
        state["status"] = "KULLANICI TARAFINDAN DURDURULDU"
        state["stream_health"] = "BEKLEMEDE"
        state["generation"] += 1
        state["stopped_at"] = datetime.now(timezone.utc).isoformat()
        state["last_action"] = "V9 canlı kayıt güvenli biçimde durduruldu; geçmiş veriler korundu."
        event = v9_add_event(state, "V9 DURDU", state["last_action"])
    asyncio.create_task(v9_persist_event(app, event))
    return await v9_snapshot_payload()


@app.post("/api/v9/paper/order")
async def v9_paper_twin_order(order: V9PaperTwinOrder):
    symbol = "".join(char for char in order.symbol.upper() if char.isalnum())
    async with app.state.market_twin_lock:
        state = app.state.market_twin
        if not state.get("enabled"):
            raise HTTPException(409, "Önce V9 Canlı Dijital Borsa İkizi'ni başlatın")
        if symbol not in state.get("universe", []):
            raise HTTPException(422, "Parite V9 canlı izleme evreninde değil")
        book = dict((state.get("latest") or {}).get(symbol) or {})
    try:
        result = v9_paper_fill_model(order.side, order.notional, book)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    created_at = datetime.now(timezone.utc).isoformat()
    fill = {
        "id": f"V9-{int(time.time() * 1000)}-{symbol}",
        "created_at": created_at,
        "symbol": symbol,
        "strategy": order.strategy.upper(),
        **result,
    }
    async with app.state.market_twin_lock:
        state = app.state.market_twin
        state.setdefault("paper_fills", []).insert(0, fill)
        state["paper_fills"] = state["paper_fills"][:V9_FILL_LIMIT]
        state["last_action"] = f"{symbol} {order.side} için %{fill['fill_pct']:.1f} sanal dolum üretildi."
        event = v9_add_event(state, "PAPER DOLUM", state["last_action"], symbol, {"fill_id": fill["id"]})
    pool = app.state.db_pool
    if pool is not None and app.state.market_twin_schema_ready:
        try:
            await pool.execute(
                """
                INSERT INTO paper_twin_fills
                  (id, created_at, symbol, side, strategy, requested_notional, filled_notional,
                   fill_pct, execution_price, quantity, fee_usdt, impact_bps, latency_ms, paper_only)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,TRUE)
                ON CONFLICT (id) DO NOTHING
                """,
                fill["id"], datetime.fromisoformat(created_at), symbol, fill["side"], fill["strategy"],
                fill["requested_notional"], fill["filled_notional"], fill["fill_pct"], fill["execution_price"],
                fill["quantity"], fill["fee_usdt"], fill["impact_bps"], fill["latency_ms"],
            )
        except Exception:
            pass
    asyncio.create_task(v9_persist_event(app, event))
    return {
        "message": "Sanal dolum yalnızca V9 yerel Paper borsa defterine yazıldı; borsaya emir gönderilmedi.",
        "fill": fill,
        "orders_enabled": False,
    }


@app.get("/api/v9/report/daily")
async def v9_daily_report():
    payload = await v9_snapshot_payload()
    return payload["daily_report"]


# ---------------------------------------------------------------------------
# V10 · Yapay Zekâ Strateji Evrim Laboratuvarı
# ---------------------------------------------------------------------------

def v10_generate_genomes(generation: int = 1) -> list[dict]:
    """Üç strateji ailesinden tekrarlanabilir ve denetlenebilir 12 Paper adayı üretir."""
    templates = [
        ("GRID", "Esnek Toplayıcı", {"lookback": 20, "entry_z": 0.65, "hold": 5, "stop_atr": 1.35, "target_atr": 1.80}),
        ("GRID", "Dengeli Salınım", {"lookback": 28, "entry_z": 0.85, "hold": 7, "stop_atr": 1.55, "target_atr": 2.10}),
        ("GRID", "Seçici Dönüş", {"lookback": 36, "entry_z": 1.05, "hold": 9, "stop_atr": 1.70, "target_atr": 2.45}),
        ("GRID", "Derin Sapma", {"lookback": 48, "entry_z": 1.25, "hold": 12, "stop_atr": 1.90, "target_atr": 2.80}),
        ("TREND", "Hızlı Trend", {"fast": 8, "slow": 34, "threshold_pct": 0.04, "hold": 6, "stop_atr": 1.45, "target_atr": 2.20}),
        ("TREND", "Dengeli Trend", {"fast": 12, "slow": 42, "threshold_pct": 0.07, "hold": 9, "stop_atr": 1.65, "target_atr": 2.55}),
        ("TREND", "Ana Akım", {"fast": 16, "slow": 50, "threshold_pct": 0.10, "hold": 12, "stop_atr": 1.85, "target_atr": 2.90}),
        ("TREND", "Sabırlı Trend", {"fast": 20, "slow": 60, "threshold_pct": 0.14, "hold": 16, "stop_atr": 2.05, "target_atr": 3.20}),
        ("KIRILIM", "Erken Kırılım", {"lookback": 12, "buffer_pct": 0.03, "hold": 5, "stop_atr": 1.40, "target_atr": 2.10}),
        ("KIRILIM", "Dengeli Kırılım", {"lookback": 20, "buffer_pct": 0.06, "hold": 8, "stop_atr": 1.60, "target_atr": 2.50}),
        ("KIRILIM", "Onaylı Kırılım", {"lookback": 30, "buffer_pct": 0.10, "hold": 11, "stop_atr": 1.85, "target_atr": 2.90}),
        ("KIRILIM", "Büyük Dalga", {"lookback": 42, "buffer_pct": 0.15, "hold": 15, "stop_atr": 2.10, "target_atr": 3.30}),
    ]
    family_counts: dict[str, int] = {}
    genomes = []
    for family, label, params in templates:
        family_counts[family] = family_counts.get(family, 0) + 1
        genomes.append({
            "id": f"G{generation}-{family}-{family_counts[family]}",
            "generation": generation, "family": family, "label": label,
            "params": params, "parent_id": None, "orders_enabled": False,
        })
    return genomes


def v10_ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    period = max(1, int(period))
    alpha = 2.0 / (period + 1.0)
    result = float(values[0])
    for value in values[1:]:
        result = float(value) * alpha + result * (1.0 - alpha)
    return result


def v10_atr(candles: list[dict], index: int, period: int = 14) -> float:
    if not candles:
        return 0.0
    end = min(max(0, int(index)), len(candles) - 1)
    start = max(1, end - max(2, int(period)) + 1)
    ranges = []
    for position in range(start, end + 1):
        candle = candles[position]
        previous_close = float(candles[position - 1]["close"])
        ranges.append(max(
            float(candle["high"]) - float(candle["low"]),
            abs(float(candle["high"]) - previous_close),
            abs(float(candle["low"]) - previous_close),
        ))
    return sum(ranges) / len(ranges) if ranges else max(float(candles[end]["high"]) - float(candles[end]["low"]), 1e-12)


def v10_signal(candles: list[dict], index: int, genome: dict) -> str:
    """Yalnızca index ve öncesini görür; gelecekteki mumlara erişmez."""
    if index <= 1 or index >= len(candles):
        return "BEKLE"
    family = str(genome.get("family") or "").upper()
    params = genome.get("params") or {}
    closes = [float(item["close"]) for item in candles[:index + 1]]
    close = closes[-1]
    if family == "TREND":
        fast = int(params.get("fast", 12))
        slow = int(params.get("slow", 42))
        if len(closes) < slow + 3:
            return "BEKLE"
        fast_ema = v10_ema(closes[-slow * 2:], fast)
        slow_ema = v10_ema(closes[-slow * 2:], slow)
        prior_fast = v10_ema(closes[-slow * 2:-1], fast)
        separation = abs(fast_ema - slow_ema) / max(close, 1e-12) * 100.0
        threshold = float(params.get("threshold_pct", 0.07))
        if fast_ema > slow_ema and fast_ema > prior_fast and separation >= threshold:
            return "LONG"
        if fast_ema < slow_ema and fast_ema < prior_fast and separation >= threshold:
            return "SHORT"
        return "BEKLE"
    if family == "KIRILIM":
        lookback = int(params.get("lookback", 20))
        if index < lookback:
            return "BEKLE"
        previous = candles[index - lookback:index]
        upper = max(float(item["high"]) for item in previous)
        lower = min(float(item["low"]) for item in previous)
        buffer_pct = float(params.get("buffer_pct", 0.06)) / 100.0
        if close > upper * (1.0 + buffer_pct):
            return "LONG"
        if close < lower * (1.0 - buffer_pct):
            return "SHORT"
        return "BEKLE"
    if family == "GRID":
        lookback = int(params.get("lookback", 28))
        if len(closes) < lookback:
            return "BEKLE"
        sample = closes[-lookback:]
        mean = sum(sample) / len(sample)
        variance = sum((value - mean) ** 2 for value in sample) / len(sample)
        deviation = math.sqrt(max(variance, 0.0))
        if deviation <= 1e-12:
            return "BEKLE"
        z_score = (close - mean) / deviation
        entry_z = float(params.get("entry_z", 0.85))
        if z_score <= -entry_z:
            return "LONG"
        if z_score >= entry_z:
            return "SHORT"
    return "BEKLE"


def v10_simulate_genome(
    candles: list[dict], genome: dict, capital: float = 1_000.0,
    cost_multiplier: float = 1.0, execution_delay: int = 1,
) -> dict:
    """Muhafazakâr, tek pozisyonlu ve maliyet sonrası Paper tekrar motoru."""
    if len(candles) < 70:
        return {
            "trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
            "net_result_usdt": 0.0, "net_return_pct": 0.0, "max_drawdown_pct": 0.0,
            "profit_factor": None, "costs_usdt": 0.0, "trade_log": [], "orders_enabled": False,
        }
    params = genome.get("params") or {}
    warmup = max(62, int(params.get("slow", 0)) + 3, int(params.get("lookback", 0)) + 3)
    delay = max(1, int(execution_delay))
    hold = max(2, int(params.get("hold", 8)))
    stop_atr = max(0.5, float(params.get("stop_atr", 1.6)))
    target_atr = max(stop_atr + 0.1, float(params.get("target_atr", 2.4)))
    starting_capital = max(100.0, float(capital))
    equity = starting_capital
    peak = equity
    max_drawdown = 0.0
    wins = losses = 0
    gross_wins = gross_losses = costs = 0.0
    log = []
    index = warmup
    while index < len(candles) - delay - 1:
        side = v10_signal(candles, index, genome)
        if side == "BEKLE":
            index += 1
            continue
        entry_index = index + delay
        entry = float(candles[entry_index]["open"])
        atr = max(v10_atr(candles, index), entry * 0.0005)
        stop = entry - atr * stop_atr if side == "LONG" else entry + atr * stop_atr
        target = entry + atr * target_atr if side == "LONG" else entry - atr * target_atr
        last_index = min(len(candles) - 1, entry_index + hold)
        exit_price = float(candles[last_index]["close"])
        exit_reason = "SÜRE"
        exit_index = last_index
        for position in range(entry_index, last_index + 1):
            candle = candles[position]
            if side == "LONG":
                # Aynı mumda hedef ve stop görülürse muhafazakâr biçimde stop önce sayılır.
                if float(candle["low"]) <= stop:
                    exit_price, exit_reason, exit_index = stop, "STOP", position
                    break
                if float(candle["high"]) >= target:
                    exit_price, exit_reason, exit_index = target, "HEDEF", position
                    break
            else:
                if float(candle["high"]) >= stop:
                    exit_price, exit_reason, exit_index = stop, "STOP", position
                    break
                if float(candle["low"]) <= target:
                    exit_price, exit_reason, exit_index = target, "HEDEF", position
                    break
        gross_pct = ((exit_price - entry) / max(entry, 1e-12) * 100.0) * (1.0 if side == "LONG" else -1.0)
        cost_pct = 0.20 * max(1.0, float(cost_multiplier)) + 0.015 * delay
        net_pct = gross_pct - cost_pct
        notional = min(starting_capital, max(100.0, equity)) * 0.25
        pnl = notional * net_pct / 100.0
        fee = notional * cost_pct / 100.0
        equity += pnl
        costs += fee
        if pnl > 0:
            wins += 1
            gross_wins += pnl
        else:
            losses += 1
            gross_losses += abs(pnl)
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, (peak - equity) / max(peak, 1e-12) * 100.0)
        log.append({
            "side": side, "entry_index": entry_index, "exit_index": exit_index,
            "entry": round(entry, 8), "exit": round(exit_price, 8), "reason": exit_reason,
            "gross_pct": round(gross_pct, 4), "net_pct": round(net_pct, 4),
            "pnl_usdt": round(pnl, 4), "paper_only": True, "orders_enabled": False,
        })
        index = exit_index + 1
    trades = wins + losses
    net_result = equity - starting_capital
    return {
        "trades": trades, "wins": wins, "losses": losses,
        "win_rate": round(wins / trades * 100.0, 1) if trades else 0.0,
        "net_result_usdt": round(net_result, 2),
        "net_return_pct": round(net_result / starting_capital * 100.0, 2),
        "max_drawdown_pct": round(max_drawdown, 2),
        "profit_factor": round(gross_wins / gross_losses, 2) if gross_losses else None,
        "costs_usdt": round(costs, 2), "trade_log": log[-16:], "orders_enabled": False,
    }


def v10_market_regime(candles: list[dict]) -> dict:
    if len(candles) < 65:
        return {"label": "VERİ BEKLİYOR", "preferred_family": "BEKLE", "strength": 0, "reason": "Rejim için yeterli kapanmış mum yok."}
    closes = [float(item["close"]) for item in candles]
    close = closes[-1]
    ema20 = v10_ema(closes[-100:], 20)
    ema50 = v10_ema(closes[-120:], 50)
    separation = abs(ema20 - ema50) / max(close, 1e-12) * 100.0
    returns = [abs(closes[index] / closes[index - 1] - 1.0) * 100.0 for index in range(len(closes) - 30, len(closes))]
    volatility = sum(returns) / len(returns)
    previous = candles[-31:-1]
    breakout_up = close > max(float(item["high"]) for item in previous)
    breakout_down = close < min(float(item["low"]) for item in previous)
    if volatility >= 1.15:
        label, family, reason = "YÜKSEK VOLATİLİTE", "BEKLE", "Kısa dönem hareketi yüksek; otomatik terfi yerine izleme seçildi."
    elif breakout_up or breakout_down:
        label, family, reason = "KIRILIM", "KIRILIM", "Fiyat son 30 mum aralığının dışına çıktı; kırılım ailesi öne alındı."
    elif separation >= 0.45:
        label, family, reason = "TREND", "TREND", "Kısa ve orta EMA ayrışması kalıcı yön davranışına işaret ediyor."
    else:
        label, family, reason = "YATAY", "GRID", "EMA ayrışması sınırlı; ortalamaya dönüş ailesi öne alındı."
    strength = round(min(99.0, separation * 80.0 + volatility * 24.0))
    return {
        "label": label, "preferred_family": family, "strength": strength,
        "ema_separation_pct": round(separation, 3), "average_move_pct": round(volatility, 3),
        "reason": reason,
    }


def v10_mutate_champion(champion: dict, generation: int | None = None) -> list[dict]:
    base = champion.get("genome", champion)
    params = dict(base.get("params") or {})
    next_generation = int(generation or int(base.get("generation", 1)) + 1)
    variants = []
    for number, factor in enumerate((0.88, 1.08, 1.18), 1):
        mutated = {}
        for key, value in params.items():
            if isinstance(value, int):
                mutated[key] = max(2, int(round(value * factor)))
            elif isinstance(value, float):
                mutated[key] = round(max(0.01, value * factor), 4)
            else:
                mutated[key] = value
        if "slow" in mutated and "fast" in mutated:
            mutated["slow"] = max(int(mutated["fast"]) + 8, int(mutated["slow"]))
        if "target_atr" in mutated and "stop_atr" in mutated:
            mutated["target_atr"] = round(max(float(mutated["stop_atr"]) + 0.25, float(mutated["target_atr"])), 4)
        variants.append({
            "id": f"G{next_generation}-{base.get('family', 'ADAY')}-M{number}",
            "generation": next_generation, "family": base.get("family", "GRID"),
            "label": f"{base.get('label', 'Şampiyon')} Mutasyon {number}", "params": mutated,
            "parent_id": base.get("id"), "orders_enabled": False,
        })
    return variants


def v10_build_next_pool(champion: dict, generation: int) -> list[dict]:
    base = champion.get("genome", champion)
    pool = [{**base, "generation": generation}]
    pool.extend(v10_mutate_champion(champion, generation))
    for genome in v10_generate_genomes(generation):
        if len(pool) >= 12:
            break
        pool.append(genome)
    return pool[:12]


def v10_evolution_tournament(candles: list[dict], capital: float = 1_000.0, genomes: list[dict] | None = None) -> dict:
    """Kronolojik üç kat, görünmeyen son kat ve çift maliyet stresiyle adayları yarıştırır."""
    closed = list(candles[-720:])
    if len(closed) < 360:
        raise ValueError("V10 turnuvası için en az 360 kapanmış mum gerekir")
    active_genomes = list(genomes or v10_generate_genomes(1))[:16]
    fold_size = len(closed) // 3
    labels = ("GELİŞTİRME", "DOĞRULAMA", "GÖRÜNMEYEN TEST")
    folds = [closed[index * fold_size:(index + 1) * fold_size if index < 2 else len(closed)] for index in range(3)]
    leaderboard = []
    for genome in active_genomes:
        fold_results = []
        for label, fold in zip(labels, folds):
            result = v10_simulate_genome(fold, genome, capital)
            fold_results.append({"label": label, **{key: result[key] for key in (
                "trades", "wins", "losses", "win_rate", "net_result_usdt", "net_return_pct", "max_drawdown_pct", "profit_factor", "costs_usdt",
            )}, "orders_enabled": False})
        stress = v10_simulate_genome(folds[-1], genome, capital, cost_multiplier=2.0, execution_delay=2)
        total_trades = sum(int(item["trades"]) for item in fold_results)
        total_wins = sum(int(item["wins"]) for item in fold_results)
        positive_folds = sum(1 for item in fold_results if float(item["net_return_pct"]) > 0)
        train_return = sum(float(item["net_return_pct"]) for item in fold_results[:2]) / 2.0
        test_return = float(fold_results[-1]["net_return_pct"])
        average_return = sum(float(item["net_return_pct"]) for item in fold_results) / 3.0
        max_drawdown = max(float(item["max_drawdown_pct"]) for item in fold_results)
        gap = max(0.0, train_return - test_return)
        overfit_risk = min(99.0, gap * 8.0 + max(0, 2 - positive_folds) * 16.0 + (22.0 if stress["net_return_pct"] <= 0 else 0.0) + (14.0 if total_trades < 6 else 0.0))
        win_rate = total_wins / total_trades * 100.0 if total_trades else 0.0
        score = min(99.0, max(0.0,
            48.0 + average_return * 4.0 + max(-8.0, float(stress["net_return_pct"]) * 2.0)
            + positive_folds * 7.0 + (win_rate - 45.0) * 0.18 - max_drawdown * 2.0 - overfit_risk * 0.22
        ))
        certified = bool(
            total_trades >= 6 and positive_folds >= 2 and test_return > 0
            and float(stress["net_return_pct"]) > 0 and max_drawdown <= 12.0
            and overfit_risk < 55.0 and score >= 55.0
        )
        if certified:
            status = "KANITLI PAPER"
        elif overfit_risk >= 55:
            status = "AŞIRI ÖĞRENME REDDİ"
        elif float(stress["net_return_pct"]) <= 0:
            status = "MALİYETTE ELENDİ"
        else:
            status = "İZLEME"
        leaderboard.append({
            "rank": 0, "genome": genome, "id": genome.get("id"), "family": genome.get("family"),
            "label": genome.get("label"), "score": round(score), "status": status,
            "certified": certified, "trades": total_trades, "win_rate": round(win_rate, 1),
            "net_return_pct": round(average_return, 2), "train_return_pct": round(train_return, 2),
            "test_return_pct": round(test_return, 2), "stress_return_pct": stress["net_return_pct"],
            "max_drawdown_pct": round(max_drawdown, 2), "positive_folds": positive_folds,
            "overfit_risk": round(overfit_risk), "folds": fold_results,
            "explanation": f"{positive_folds}/3 pozitif dönem · görünmeyen test %{test_return:.2f} · 2X maliyet %{stress['net_return_pct']:.2f}.",
            "orders_enabled": False,
        })
    leaderboard.sort(key=lambda item: (bool(item["certified"]), int(item["score"]), float(item["stress_return_pct"])), reverse=True)
    for rank, row in enumerate(leaderboard, 1):
        row["rank"] = rank
    leader = leaderboard[0] if leaderboard else None
    champion = next((row for row in leaderboard if row["certified"]), None)
    challenger = next((row for row in leaderboard if champion is None or row["id"] != champion["id"]), None)
    regime = v10_market_regime(closed)
    regime_champions = {}
    for family in ("GRID", "TREND", "KIRILIM"):
        family_rows = [row for row in leaderboard if row["family"] == family]
        best = family_rows[0] if family_rows else None
        regime_champions[family] = None if best is None else {
            "id": best["id"], "label": best["label"], "score": best["score"],
            "status": best["status"], "certified": best["certified"], "orders_enabled": False,
        }
    preferred = regime.get("preferred_family")
    preferred_row = next((row for row in leaderboard if row["family"] == preferred and row["certified"]), None)
    paper_candidate = preferred_row or champion
    subject = paper_candidate or leader
    gates = [
        {"key": "sample", "label": "Yeterli Paper örneği", "passed": bool(subject and subject["trades"] >= 6), "detail": f"{subject['trades'] if subject else 0} işlem"},
        {"key": "folds", "label": "Zaman dayanıklılığı", "passed": bool(subject and subject["positive_folds"] >= 2), "detail": f"{subject['positive_folds'] if subject else 0}/3 pozitif dönem"},
        {"key": "unseen", "label": "Görünmeyen test", "passed": bool(subject and subject["test_return_pct"] > 0), "detail": f"%{subject['test_return_pct'] if subject else 0}"},
        {"key": "stress", "label": "2X maliyet + gecikme", "passed": bool(subject and subject["stress_return_pct"] > 0), "detail": f"%{subject['stress_return_pct'] if subject else 0}"},
        {"key": "overfit", "label": "Aşırı öğrenme kalkanı", "passed": bool(subject and subject["overfit_risk"] < 55), "detail": f"risk %{subject['overfit_risk'] if subject else 99}"},
        {"key": "drawdown", "label": "Düşüş limiti", "passed": bool(subject and subject["max_drawdown_pct"] <= 12), "detail": f"%{subject['max_drawdown_pct'] if subject else 0}"},
        {"key": "orders", "label": "Borsa emir kilidi", "passed": True, "detail": "Gerçek ve Testnet emirleri kapalı"},
    ]
    next_generation = v10_mutate_champion(subject, int((subject or {}).get("genome", {}).get("generation", 1)) + 1) if subject else []
    return {
        "generation": int((leader or {}).get("genome", {}).get("generation", 1)),
        "sampled_candles": len(closed), "genome_count": len(active_genomes),
        "regime": regime, "leader": leader, "champion": paper_candidate,
        "challenger": challenger, "leaderboard": leaderboard,
        "regime_champions": regime_champions, "promotion_gates": gates,
        "promotion_ready": bool(paper_candidate and all(item["passed"] for item in gates)),
        "promotion_status": "PAPER TERFİSİNE HAZIR" if paper_candidate and all(item["passed"] for item in gates) else "KANIT TOPLUYOR",
        "next_generation": next_generation,
        "explanation": (
            f"{paper_candidate['label']} {regime['label']} rejimi için görünmeyen test ve maliyet stresini geçti; yalnızca Paper politikaya aday."
            if paper_candidate else "Hiçbir aday bütün kanıt kapılarını geçmedi; mevcut güvenli Paper politika korunuyor."
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "orders_enabled": False, "testnet_orders_enabled": False,
        "safety_note": "V10 strateji üretir ve Paper kanıtı toplar; borsaya emir göndermez, kesin getiri vaat etmez.",
    }


def v10_evolution_payload(engine: dict) -> dict:
    return {
        "version": "10.0.0", "enabled": bool(engine.get("enabled")),
        "busy": bool(engine.get("busy")), "status": engine.get("status", "DURDU"),
        "interval": engine.get("interval", "15m"), "capital": float(engine.get("capital", 1_000.0)),
        "universe": list(engine.get("universe") or []), "generation": int(engine.get("generation", 1)),
        "cycles": int(engine.get("cycles", 0)), "active_champion": engine.get("active_champion"),
        "previous_champion": engine.get("previous_champion"), "champions": dict(engine.get("champions") or {}),
        "leaderboard": list(engine.get("leaderboard") or [])[:12],
        "latest_tournament": engine.get("latest_tournament"), "events": list(engine.get("events") or [])[:16],
        "started_at": engine.get("started_at"), "stopped_at": engine.get("stopped_at"),
        "last_tick_at": engine.get("last_tick_at"), "last_action": engine.get("last_action"),
        "promotion_gate": engine.get("promotion_gate"), "orders_enabled": False,
        "testnet_orders_enabled": False, "mode": "PAPER_ONLY",
        "safety_note": "Terfi yalnızca V7 Paper politika hafızasına yapılır; gerçek ve Testnet emir kanalları kapalıdır.",
    }


def v10_add_event(engine: dict, kind: str, message: str, symbol: str | None = None, genome_id: str | None = None) -> None:
    engine.setdefault("events", []).insert(0, {
        "kind": kind, "message": message, "symbol": symbol, "genome_id": genome_id,
        "created_at": datetime.now(timezone.utc).isoformat(), "paper_only": True, "orders_enabled": False,
    })
    engine["events"] = engine["events"][:V10_EVENT_LIMIT]


async def v10_evolution_cycle(application: FastAPI) -> None:
    paper = application.state.paper
    async with paper["lock"]:
        engine = paper.get("strategy_evolution", empty_v10_evolution_state())
        if not engine.get("enabled") or engine.get("busy") or not engine.get("universe"):
            return
        engine["busy"] = True
        universe = list(engine["universe"])
        symbol = universe[int(engine.get("cycle_index", 0)) % len(universe)]
        interval = str(engine.get("interval", "15m"))
        capital = float(engine.get("capital", 1_000.0))
        generation = int(engine.get("generation", 1))
        genomes = list(engine.get("candidate_genomes") or v10_generate_genomes(generation))
    try:
        candles = await fetch_candles(symbol, interval, 740)
        tournament = v10_evolution_tournament(candles[:-1], capital, genomes)
        now = datetime.now(timezone.utc).isoformat()
        async with paper["lock"]:
            engine = paper["strategy_evolution"]
            candidate = tournament.get("champion") if tournament.get("promotion_ready") else None
            active = engine.get("active_champion")
            action = f"{symbol}: {tournament['genome_count']} aday yarıştı; kanıt kapıları izleniyor."
            if candidate:
                promoted = {
                    "symbol": symbol, "regime": tournament["regime"]["label"],
                    "promoted_at": now, "paper_policy": "V7 PAPER GÖLGE POLİTİKASI",
                    **{key: candidate[key] for key in (
                        "id", "family", "label", "score", "status", "certified", "trades",
                        "win_rate", "net_return_pct", "test_return_pct", "stress_return_pct",
                        "max_drawdown_pct", "overfit_risk", "genome", "explanation",
                    )},
                    "orders_enabled": False,
                }
                key = f"{symbol}:{tournament['regime']['label']}"
                engine.setdefault("champions", {})[key] = promoted
                if not active or (candidate["id"] != active.get("id") and int(candidate["score"]) >= int(active.get("score", 0)) + 3):
                    engine["previous_champion"] = active
                    engine["active_champion"] = promoted
                    action = f"{candidate['label']} yalnızca Paper şampiyonu oldu; skor {candidate['score']}/99."
                    v10_add_event(engine, "PAPER TERFİ", action, symbol, candidate["id"])
                else:
                    action = f"{candidate['label']} kanıtlı aday; mevcut Paper şampiyonu skor üstünlüğünü koruyor."
                    v10_add_event(engine, "ŞAMPİYON KORUNDU", action, symbol, candidate["id"])
            else:
                v10_add_event(engine, "KANIT TURU", action, symbol, (tournament.get("leader") or {}).get("id"))
            subject = candidate or tournament.get("leader")
            next_generation = generation + 1
            engine.update({
                "busy": False, "status": "PAPER EVRİMİ ÇALIŞIYOR", "latest_tournament": tournament,
                "leaderboard": tournament.get("leaderboard", []), "cycle_index": int(engine.get("cycle_index", 0)) + 1,
                "cycles": int(engine.get("cycles", 0)) + 1, "generation": next_generation,
                "candidate_genomes": v10_build_next_pool(subject, next_generation) if subject else v10_generate_genomes(next_generation),
                "last_tick_at": now, "last_action": action, "orders_enabled": False, "testnet_orders_enabled": False,
            })
        asyncio.create_task(persist_paper_snapshot(application))
    except Exception as exc:
        async with paper["lock"]:
            engine = paper.get("strategy_evolution", empty_v10_evolution_state())
            engine["busy"] = False
            engine["cycle_index"] = int(engine.get("cycle_index", 0)) + 1
            engine["last_action"] = f"{symbol} V10 verisi geçici olarak bekleniyor: {str(exc)[:100]}"
            v10_add_event(engine, "GEÇİCİ VERİ HATASI", engine["last_action"], symbol)


async def v10_evolution_loop(application: FastAPI) -> None:
    while True:
        try:
            if application.state.paper.get("strategy_evolution", {}).get("enabled"):
                await v10_evolution_cycle(application)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine = application.state.paper.get("strategy_evolution", {})
            engine["busy"] = False
            engine["last_action"] = f"V10 geçici hata; güvenli biçimde yeniden denenecek: {str(exc)[:80]}"
        await asyncio.sleep(V10_EVOLUTION_TICK_SECONDS)


class V10EvolutionStartRequest(BaseModel):
    symbols: list[str] = Field(default_factory=lambda: list(V9_DEFAULT_UNIVERSE), min_length=1, max_length=6)
    interval: Literal["5m", "15m", "1h"] = "15m"
    capital: float = Field(default=1_000.0, ge=500.0, le=25_000.0)


@app.get("/api/v10/evolution")
async def v10_evolution_status():
    async with app.state.paper["lock"]:
        return v10_evolution_payload(app.state.paper.get("strategy_evolution", empty_v10_evolution_state()))


@app.get("/api/v10/evolution/{symbol}")
async def v10_evolution_preview(
    symbol: str,
    interval: Literal["5m", "15m", "1h"] = "15m",
    capital: float = Query(1_000.0, ge=500.0, le=25_000.0),
):
    safe_symbol = "".join(char for char in symbol.upper() if char.isalnum())
    if not safe_symbol.endswith("USDT"):
        raise HTTPException(422, "Geçerli bir USDT paritesi seçin")
    cache_key = (safe_symbol, interval, int(round(capital)))
    cached = V10_EVOLUTION_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < 60:
        return {**cached[1], "cached": True}
    candles = await fetch_candles(safe_symbol, interval, 740)
    payload = {"symbol": safe_symbol, "interval": interval, **v10_evolution_tournament(candles[:-1], capital)}
    V10_EVOLUTION_CACHE[cache_key] = (time.monotonic(), payload)
    return {**payload, "cached": False}


@app.post("/api/v10/evolution/start")
async def v10_evolution_start(config: V10EvolutionStartRequest):
    paper = app.state.paper
    if emergency_brake_payload(paper)["active"]:
        raise HTTPException(409, "Acil Fren aktifken V10 Evrim Laboratuvarı başlatılamaz")
    symbols = []
    for raw_symbol in config.symbols:
        symbol = "".join(char for char in raw_symbol.upper() if char.isalnum())
        if symbol.endswith("USDT") and symbol not in symbols:
            symbols.append(symbol)
    if not symbols:
        raise HTTPException(422, "En az bir geçerli USDT paritesi seçin")
    now = datetime.now(timezone.utc).isoformat()
    async with paper["lock"]:
        engine = paper.get("strategy_evolution", empty_v10_evolution_state())
        if engine.get("enabled"):
            raise HTTPException(409, "V10 Evrim Laboratuvarı zaten çalışıyor")
        engine.update({
            "enabled": True, "busy": False, "status": "PAPER EVRİMİ ÇALIŞIYOR",
            "interval": config.interval, "capital": config.capital, "universe": symbols[:6],
            "started_at": now, "stopped_at": None, "last_tick_at": now,
            "candidate_genomes": engine.get("candidate_genomes") or v10_generate_genomes(int(engine.get("generation", 1))),
            "last_action": f"V10 {len(symbols[:6])} parite için 12 strateji genomunu Paper arenada yarıştırmaya başladı.",
            "orders_enabled": False, "testnet_orders_enabled": False,
        })
        v10_add_event(engine, "V10 BAŞLADI", engine["last_action"], "PORTFÖY")
    asyncio.create_task(v10_evolution_cycle(app))
    asyncio.create_task(persist_paper_snapshot(app))
    return v10_evolution_payload(engine)


@app.post("/api/v10/evolution/stop")
async def v10_evolution_stop():
    paper = app.state.paper
    async with paper["lock"]:
        engine = paper.get("strategy_evolution", empty_v10_evolution_state())
        engine["enabled"] = False
        engine["busy"] = False
        engine["status"] = "KULLANICI TARAFINDAN DURDURULDU"
        engine["stopped_at"] = datetime.now(timezone.utc).isoformat()
        engine["last_action"] = "V10 Paper evrimi durduruldu; şampiyon ve kanıt geçmişi korundu."
        v10_add_event(engine, "V10 DURDU", engine["last_action"], "PORTFÖY")
    asyncio.create_task(persist_paper_snapshot(app))
    return v10_evolution_payload(engine)


@app.post("/api/v10/evolution/rollback")
async def v10_evolution_rollback():
    paper = app.state.paper
    async with paper["lock"]:
        engine = paper.get("strategy_evolution", empty_v10_evolution_state())
        previous = engine.get("previous_champion")
        active = engine.get("active_champion")
        if previous is None:
            raise HTTPException(409, "Geri dönülecek önceki Paper şampiyonu henüz yok")
        engine["active_champion"] = previous
        engine["previous_champion"] = active
        engine["last_action"] = f"Paper şampiyonu güvenli biçimde {previous.get('label', previous.get('id'))} profiline geri alındı."
        v10_add_event(engine, "PAPER GERİ DÖNÜŞ", engine["last_action"], previous.get("symbol"), previous.get("id"))
    asyncio.create_task(persist_paper_snapshot(app))
    return v10_evolution_payload(engine)


# ---------------------------------------------------------------------------
# V11 · Otonom Risk Beyni ve Portföy Komutanı
# ---------------------------------------------------------------------------

def v11_returns(candles: list[dict], limit: int = 360) -> list[float]:
    closes = [float(item["close"]) for item in candles if float(item.get("close") or 0.0) > 0]
    closes = closes[-max(3, int(limit) + 1):]
    return [closes[index] / closes[index - 1] - 1.0 for index in range(1, len(closes))]


def v11_std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(float(value) for value in values) / len(values)
    return math.sqrt(sum((float(value) - mean) ** 2 for value in values) / len(values))


def v11_correlation(left: list[float], right: list[float]) -> float:
    length = min(len(left), len(right))
    if length < 3:
        return 0.0
    x_values = [float(value) for value in left[-length:]]
    y_values = [float(value) for value in right[-length:]]
    x_mean = sum(x_values) / length
    y_mean = sum(y_values) / length
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
    x_scale = math.sqrt(sum((x - x_mean) ** 2 for x in x_values))
    y_scale = math.sqrt(sum((y - y_mean) ** 2 for y in y_values))
    if x_scale <= 1e-15 or y_scale <= 1e-15:
        return 0.0
    return max(-1.0, min(1.0, numerator / (x_scale * y_scale)))


def v11_correlation_matrix(returns_by_symbol: dict[str, list[float]]) -> dict:
    symbols = list(returns_by_symbol)
    rows = []
    pairs = []
    for left in symbols:
        values = []
        for right in symbols:
            correlation = 1.0 if left == right else v11_correlation(returns_by_symbol[left], returns_by_symbol[right])
            values.append(round(correlation * 100.0, 1))
            if symbols.index(right) > symbols.index(left):
                pairs.append({
                    "left": left, "right": right, "correlation_pct": round(correlation * 100.0, 1),
                    "level": "YÜKSEK" if abs(correlation) >= 0.78 else "ORTA" if abs(correlation) >= 0.50 else "DÜŞÜK",
                })
        rows.append({"symbol": left, "values": values})
    average_abs = sum(abs(float(item["correlation_pct"])) for item in pairs) / len(pairs) if pairs else 0.0
    max_pair = max(pairs, key=lambda item: abs(float(item["correlation_pct"])), default=None)
    return {
        "symbols": symbols, "rows": rows, "pairs": pairs,
        "average_abs_correlation_pct": round(average_abs, 1), "max_pair": max_pair,
        "orders_enabled": False,
    }


def v11_correlation_clusters(matrix: dict, threshold: float = 0.78) -> list[dict]:
    symbols = list(matrix.get("symbols") or [])
    parent = {symbol: symbol for symbol in symbols}

    def find(symbol: str) -> str:
        while parent[symbol] != symbol:
            parent[symbol] = parent[parent[symbol]]
            symbol = parent[symbol]
        return symbol

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[right_root] = left_root

    for pair in matrix.get("pairs") or []:
        if float(pair.get("correlation_pct") or 0.0) / 100.0 >= threshold:
            union(str(pair["left"]), str(pair["right"]))
    grouped: dict[str, list[str]] = {}
    for symbol in symbols:
        grouped.setdefault(find(symbol), []).append(symbol)
    clusters = []
    for index, members in enumerate(grouped.values(), 1):
        internal = [
            float(pair["correlation_pct"]) for pair in matrix.get("pairs") or []
            if pair["left"] in members and pair["right"] in members
        ]
        clusters.append({
            "id": f"K{index}", "members": members, "size": len(members),
            "average_correlation_pct": round(sum(internal) / len(internal), 1) if internal else 0.0,
            "status": "YOĞUNLAŞMA" if len(members) >= 2 else "BAĞIMSIZ",
            "orders_enabled": False,
        })
    return sorted(clusters, key=lambda item: item["size"], reverse=True)


def v11_risk_parity_allocations(
    returns_by_symbol: dict[str, list[float]], matrix: dict, clusters: list[dict],
    capital: float, exposure_ratio: float = 0.80,
) -> list[dict]:
    symbols = list(returns_by_symbol)
    if not symbols:
        return []
    volatility = {symbol: max(v11_std(returns_by_symbol[symbol]), 1e-7) for symbol in symbols}
    average_positive_correlation = {}
    for symbol in symbols:
        correlations = [
            max(0.0, float(pair["correlation_pct"]) / 100.0)
            for pair in matrix.get("pairs") or [] if symbol in {pair["left"], pair["right"]}
        ]
        average_positive_correlation[symbol] = sum(correlations) / len(correlations) if correlations else 0.0
    scores = {
        symbol: 1.0 / (volatility[symbol] * (1.0 + average_positive_correlation[symbol] * 0.85))
        for symbol in symbols
    }
    cap = max(0.35, min(0.60, 1.0 / len(symbols) + 0.05))
    weights = {symbol: 0.0 for symbol in symbols}
    remaining = set(symbols)
    remaining_weight = 1.0
    while remaining:
        score_total = sum(scores[symbol] for symbol in remaining)
        proposed = {
            symbol: remaining_weight * scores[symbol] / max(score_total, 1e-12)
            for symbol in remaining
        }
        oversized = [symbol for symbol, weight in proposed.items() if weight > cap + 1e-12]
        if not oversized:
            for symbol, weight in proposed.items():
                weights[symbol] = weight
            break
        for symbol in oversized:
            weights[symbol] = cap
            remaining.remove(symbol)
            remaining_weight -= cap
        if remaining_weight <= 1e-12:
            break
    contribution_raw = {
        symbol: weights[symbol] * volatility[symbol] * (1.0 + average_positive_correlation[symbol])
        for symbol in symbols
    }
    contribution_total = sum(contribution_raw.values())
    cluster_lookup = {
        symbol: cluster["id"] for cluster in clusters for symbol in cluster.get("members", [])
    }
    return sorted([{
        "symbol": symbol, "weight_pct": round(weights[symbol] * 100.0, 1),
        "paper_budget_usdt": round(float(capital) * max(0.0, min(1.0, exposure_ratio)) * weights[symbol], 2),
        "volatility_pct": round(volatility[symbol] * 100.0, 3),
        "average_correlation_pct": round(average_positive_correlation[symbol] * 100.0, 1),
        "risk_contribution_pct": round(contribution_raw[symbol] / max(contribution_total, 1e-12) * 100.0, 1),
        "cluster": cluster_lookup.get(symbol, "K0"),
        "status": "KORELASYON İNDİRİMİ" if average_positive_correlation[symbol] >= 0.65 else "RİSK PARİTESİ",
        "orders_enabled": False,
    } for symbol in symbols], key=lambda item: item["weight_pct"], reverse=True)


def v11_monte_carlo(
    returns_by_symbol: dict[str, list[float]], weights: dict[str, float], capital: float,
    horizon_candles: int = 24, simulations: int = 500,
) -> dict:
    symbols = [symbol for symbol in returns_by_symbol if symbol in weights]
    common = min((len(returns_by_symbol[symbol]) for symbol in symbols), default=0)
    if common < 60:
        raise ValueError("Monte Carlo için en az 60 ortak getiri gerekir")
    horizon = max(4, min(96, int(horizon_candles)))
    count = max(100, min(2_000, int(simulations)))
    aligned = {symbol: returns_by_symbol[symbol][-common:] for symbol in symbols}
    outcomes = []
    drawdowns = []
    for simulation in range(count):
        state = (11_071 + simulation * 2_654_435_761) & 0xFFFFFFFF
        equity = peak = 1.0
        max_drawdown = 0.0
        for _step in range(horizon):
            state = (1_664_525 * state + 1_013_904_223) & 0xFFFFFFFF
            sample_index = state % common
            portfolio_return = sum(
                float(weights[symbol]) * float(aligned[symbol][sample_index]) for symbol in symbols
            )
            equity *= max(0.70, 1.0 + portfolio_return)
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, (peak - equity) / max(peak, 1e-12) * 100.0)
        outcomes.append((equity - 1.0) * 100.0)
        drawdowns.append(max_drawdown)
    ordered = sorted(outcomes)
    fifth_index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * 0.05)))
    var_boundary = ordered[fifth_index]
    tail = [value for value in ordered if value <= var_boundary]
    expected = sum(outcomes) / len(outcomes)
    var_95 = max(0.0, -var_boundary)
    cvar_95 = max(0.0, -(sum(tail) / len(tail))) if tail else var_95
    probability_loss = sum(1 for value in outcomes if value < 0) / len(outcomes) * 100.0
    probability_drawdown_5 = sum(1 for value in drawdowns if value >= 5.0) / len(drawdowns) * 100.0
    ruin_probability = sum(1 for value, drawdown in zip(outcomes, drawdowns) if value <= -10.0 or drawdown >= 12.0) / len(outcomes) * 100.0
    quantile = lambda level: ordered[max(0, min(len(ordered) - 1, int((len(ordered) - 1) * level)))]
    buckets = [(-99.0, -5.0, "≤ -5%"), (-5.0, -2.0, "-5/-2%"), (-2.0, 0.0, "-2/0%"), (0.0, 2.0, "0/+2%"), (2.0, 5.0, "+2/+5%"), (5.0, 99.0, "≥ +5%")]
    distribution = [{
        "label": label, "count": sum(1 for value in outcomes if lower <= value < upper),
        "percentage": round(sum(1 for value in outcomes if lower <= value < upper) / len(outcomes) * 100.0, 1),
    } for lower, upper, label in buckets]
    return {
        "simulations": count, "horizon_candles": horizon, "capital": float(capital),
        "expected_return_pct": round(expected, 2), "expected_pnl_usdt": round(float(capital) * expected / 100.0, 2),
        "var_95_pct": round(var_95, 2), "var_95_usdt": round(float(capital) * var_95 / 100.0, 2),
        "cvar_95_pct": round(cvar_95, 2), "cvar_95_usdt": round(float(capital) * cvar_95 / 100.0, 2),
        "probability_loss_pct": round(probability_loss, 1),
        "probability_drawdown_5_pct": round(probability_drawdown_5, 1),
        "ruin_probability_pct": round(ruin_probability, 1),
        "worst_path_pct": round(min(outcomes), 2), "best_path_pct": round(max(outcomes), 2),
        "quantiles": {"p05": round(quantile(0.05), 2), "p25": round(quantile(0.25), 2), "p50": round(quantile(0.50), 2), "p75": round(quantile(0.75), 2), "p95": round(quantile(0.95), 2)},
        "distribution": distribution, "orders_enabled": False,
        "method_note": "Aynı tarih indeksini bütün paritelerde birlikte örnekleyen deterministik blok-bootstrap; kesin gelecek tahmini değildir.",
    }


def v11_stress_scenarios(
    returns_by_symbol: dict[str, list[float]], weights: dict[str, float], matrix: dict,
    capital: float, concentration_pct: float,
) -> list[dict]:
    symbols = list(weights)
    btc = "BTCUSDT" if "BTCUSDT" in symbols else symbols[0]
    btc_vol = max(v11_std(returns_by_symbol[btc]), 1e-7)
    correlations = {btc: 1.0}
    for pair in matrix.get("pairs") or []:
        if btc == pair["left"]:
            correlations[pair["right"]] = float(pair["correlation_pct"]) / 100.0
        elif btc == pair["right"]:
            correlations[pair["left"]] = float(pair["correlation_pct"]) / 100.0
    btc_crash = 0.0
    for symbol in symbols:
        beta = correlations.get(symbol, 0.35) * min(1.8, max(0.35, v11_std(returns_by_symbol[symbol]) / btc_vol))
        shock = -5.0 if symbol == btc else -5.0 * beta
        btc_crash += float(weights[symbol]) * shock
    weighted_volatility = sum(float(weights[symbol]) * v11_std(returns_by_symbol[symbol]) for symbol in symbols) * 100.0
    scenarios = [
        ("BTC -%5 ŞOKU", btc_crash, "BTC düşüşünün beta ve korelasyon yoluyla portföye yayılması"),
        ("KORELASYON DEPREMİ", -3.2 - max(0.0, float(concentration_pct) - 25.0) * 0.045, "Normalde farklı hareket eden coinlerin aynı anda düşmesi"),
        ("VOLATİLİTE 2X", -max(1.0, weighted_volatility * math.sqrt(24.0) * 2.2), "Tarihsel oynaklığın iki katına çıkması"),
        ("LİKİDİTE DONMASI", -1.1 - float(concentration_pct) * 0.035 - weighted_volatility * 1.8, "Spread, kayma ve çıkış gecikmesinin birlikte artması"),
    ]
    return [{
        "label": label, "portfolio_impact_pct": round(impact, 2),
        "loss_usdt": round(float(capital) * max(0.0, -impact) / 100.0, 2),
        "status": "KRİTİK" if impact <= -8.0 else "UYARI" if impact <= -4.0 else "DAYANIKLI",
        "description": description, "orders_enabled": False,
    } for label, impact, description in scenarios]


def v11_portfolio_risk_lab(
    candles_by_symbol: dict[str, list[dict]], capital: float = 5_000.0,
    interval: str = "15m", simulations: int = 500, horizon_candles: int = 24,
) -> dict:
    usable = {
        symbol: candles for symbol, candles in candles_by_symbol.items()
        if isinstance(candles, list) and len(candles) >= 260
    }
    if len(usable) < 2:
        raise ValueError("V11 portföy riski için en az iki paritede 260 kapanmış mum gerekir")
    returns_by_symbol = {symbol: v11_returns(candles, 360) for symbol, candles in usable.items()}
    common = min(len(values) for values in returns_by_symbol.values())
    returns_by_symbol = {symbol: values[-common:] for symbol, values in returns_by_symbol.items()}
    matrix = v11_correlation_matrix(returns_by_symbol)
    clusters = v11_correlation_clusters(matrix)
    preliminary = v11_risk_parity_allocations(returns_by_symbol, matrix, clusters, capital, 1.0)
    weights = {item["symbol"]: float(item["weight_pct"]) / 100.0 for item in preliminary}
    weight_total = sum(weights.values())
    weights = {symbol: weight / max(weight_total, 1e-12) for symbol, weight in weights.items()}
    concentration = sum(weight ** 2 for weight in weights.values()) * 100.0
    monte_carlo = v11_monte_carlo(returns_by_symbol, weights, capital, horizon_candles, simulations)
    stress = v11_stress_scenarios(returns_by_symbol, weights, matrix, capital, concentration)
    worst_stress = min(stress, key=lambda item: item["portfolio_impact_pct"])
    max_correlation = abs(float((matrix.get("max_pair") or {}).get("correlation_pct") or 0.0))
    diversification_score = max(0.0, min(99.0,
        100.0 - float(matrix["average_abs_correlation_pct"]) * 0.55 - concentration * 0.70
    ))
    risk_score = min(99.0, max(0.0,
        float(monte_carlo["cvar_95_pct"]) * 7.0
        + float(monte_carlo["ruin_probability_pct"]) * 0.65
        + max_correlation * 0.18 + concentration * 0.22
        + max(0.0, -float(worst_stress["portfolio_impact_pct"]) - 3.0) * 2.8
    ))
    critical = bool(risk_score >= 68 or float(monte_carlo["cvar_95_pct"]) >= 6.0 or float(worst_stress["portfolio_impact_pct"]) <= -10.0)
    warning = bool(not critical and (risk_score >= 40 or max_correlation >= 82.0))
    level = "KIRMIZI" if critical else "SARI" if warning else "YEŞİL"
    exposure_ratio = 0.0 if critical else 0.50 if warning else 0.80
    allocations = v11_risk_parity_allocations(returns_by_symbol, matrix, clusters, capital, exposure_ratio)
    gates = [
        {"key": "sample", "label": "Ortak veri", "passed": common >= 240, "detail": f"{common} getiri"},
        {"key": "cvar", "label": "CVaR limiti", "passed": float(monte_carlo["cvar_95_pct"]) <= 5.0, "detail": f"%{monte_carlo['cvar_95_pct']}"},
        {"key": "correlation", "label": "Korelasyon limiti", "passed": max_correlation < 85.0, "detail": f"en yüksek %{round(max_correlation, 1)}"},
        {"key": "stress", "label": "Kara-kuğu stresi", "passed": float(worst_stress["portfolio_impact_pct"]) > -8.0, "detail": f"%{worst_stress['portfolio_impact_pct']}"},
        {"key": "concentration", "label": "Yoğunlaşma limiti", "passed": concentration <= 45.0, "detail": f"HHI %{round(concentration, 1)}"},
        {"key": "orders", "label": "Borsa emir kilidi", "passed": True, "detail": "Gerçek ve Testnet emirleri kapalı"},
    ]
    fingerprints = [{
        "key": "TAIL", "label": "Kuyruk Riski", "score": round(min(99.0, float(monte_carlo["cvar_95_pct"]) * 12.0)),
    }, {
        "key": "CORR", "label": "Korelasyon", "score": round(min(99.0, float(matrix["average_abs_correlation_pct"]))),
    }, {
        "key": "CONC", "label": "Yoğunlaşma", "score": round(min(99.0, concentration * 1.8)),
    }, {
        "key": "VOL", "label": "Volatilite", "score": round(min(99.0, sum(v11_std(values) for values in returns_by_symbol.values()) / len(returns_by_symbol) * 8_000.0)),
    }, {
        "key": "RUIN", "label": "İflas Olasılığı", "score": round(min(99.0, float(monte_carlo["ruin_probability_pct"]) * 2.0)),
    }]
    paper_action = "PAPER RİSK VETOSU" if critical else "PAPER RİSKİ AZALT" if warning else "RİSK PARİTESİ UYGUN"
    return {
        "version": "20.2.0", "interval": interval, "capital": float(capital),
        "symbols": list(returns_by_symbol), "sampled_returns": common,
        "risk_score": round(risk_score), "risk_level": level, "paper_action": paper_action,
        "veto_required": critical, "exposure_ratio_pct": round(exposure_ratio * 100.0),
        "invested_budget_usdt": round(float(capital) * exposure_ratio, 2),
        "cash_reserve_usdt": round(float(capital) * (1.0 - exposure_ratio), 2),
        "diversification_score": round(diversification_score), "concentration_pct": round(concentration, 1),
        "allocations": allocations, "correlation_matrix": matrix, "clusters": clusters,
        "monte_carlo": monte_carlo, "stress_scenarios": stress, "worst_scenario": worst_stress,
        "risk_fingerprint": fingerprints, "gates": gates,
        "summary": (
            f"{worst_stress['label']} en ağır senaryo (%{worst_stress['portfolio_impact_pct']}); Paper motorları durdurulmalı."
            if critical else
            f"Korelasyon veya kuyruk riski yükseldi; Paper bütçesi %{round(exposure_ratio * 100)} ile sınırlandı."
            if warning else
            f"Portföy dağılımı dengeli; %{round((1.0 - exposure_ratio) * 100)} nakit rezervi korunarak risk-paritesi bütçesi önerildi."
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "orders_enabled": False, "testnet_orders_enabled": False,
        "safety_note": "V11 yalnızca Paper risk bütçesi ve veto üretir; açık pozisyon kapatmaz ve borsaya emir göndermez.",
    }


def v11_risk_payload(engine: dict) -> dict:
    return {
        "version": "20.2.0", "enabled": bool(engine.get("enabled")), "busy": bool(engine.get("busy")),
        "status": engine.get("status", "DURDU"), "interval": engine.get("interval", "15m"),
        "capital": float(engine.get("capital", 5_000.0)), "universe": list(engine.get("universe") or []),
        "simulations": int(engine.get("simulations", 500)), "horizon_candles": int(engine.get("horizon_candles", 24)),
        "cycles": int(engine.get("cycles", 0)), "latest_report": engine.get("latest_report"),
        "approved_allocations": list(engine.get("approved_allocations") or []),
        "intervention": dict(engine.get("intervention") or {}), "events": list(engine.get("events") or [])[:16],
        "started_at": engine.get("started_at"), "stopped_at": engine.get("stopped_at"),
        "last_tick_at": engine.get("last_tick_at"), "last_action": engine.get("last_action"),
        "orders_enabled": False, "testnet_orders_enabled": False, "mode": "PAPER_ONLY",
        "safety_note": "V11 gerçek/Testnet emir kanallarını açmaz; müdahale yalnızca yerel Paper motorlarını durdurur.",
    }


def v11_add_event(engine: dict, kind: str, message: str) -> None:
    engine.setdefault("events", []).insert(0, {
        "kind": kind, "message": message, "created_at": datetime.now(timezone.utc).isoformat(),
        "paper_only": True, "orders_enabled": False,
    })
    engine["events"] = engine["events"][:V11_EVENT_LIMIT]


async def v11_fetch_universe(symbols: list[str], interval: str) -> dict[str, list[dict]]:
    results = await asyncio.gather(
        *(fetch_candles(symbol, interval, 500) for symbol in symbols), return_exceptions=True,
    )
    return {
        symbol: result[:-1] for symbol, result in zip(symbols, results)
        if isinstance(result, list) and len(result) >= 261
    }


async def v11_risk_cycle(application: FastAPI) -> None:
    paper = application.state.paper
    async with paper["lock"]:
        engine = paper.get("portfolio_risk", empty_v11_risk_state())
        if not engine.get("enabled") or engine.get("busy"):
            return
        engine["busy"] = True
        symbols = list(engine.get("universe") or V9_DEFAULT_UNIVERSE)
        interval = str(engine.get("interval", "15m"))
        capital = float(engine.get("capital", 5_000.0))
        simulations = int(engine.get("simulations", 500))
        horizon = int(engine.get("horizon_candles", 24))
    try:
        candles = await v11_fetch_universe(symbols, interval)
        report = v11_portfolio_risk_lab(candles, capital, interval, simulations, horizon)
        now = datetime.now(timezone.utc).isoformat()
        async with paper["lock"]:
            engine = paper["portfolio_risk"]
            intervention = dict(engine.get("intervention") or empty_v11_risk_state()["intervention"])
            stopped = []
            if report["veto_required"]:
                orchestrator = paper.get("strategy_orchestrator", {})
                grid_engine = paper.get("grid_engine", {})
                if orchestrator.get("enabled"):
                    orchestrator["enabled"] = False
                    orchestrator["status"] = "V11 PAPER RİSK VETOSU"
                    orchestrator["last_action"] = "V11 kritik portföy riski gördü; V7 Paper Orkestra durduruldu."
                    stopped.append("V7 ORKESTRA")
                if grid_engine.get("enabled"):
                    grid_engine["enabled"] = False
                    grid_engine["status"] = "V11 PAPER RİSK VETOSU"
                    grid_engine["last_action"] = "V11 kritik portföy riski gördü; V6 Paper Grid durduruldu."
                    stopped.append("V6 GRID")
                if application.state.paper_bot.get("enabled"):
                    application.state.paper_bot["enabled"] = False
                    application.state.paper_bot["last_action"] = "V11 kritik risk vetosu Paper Botu durdurdu."
                    stopped.append("PAPER BOT")
                intervention = {
                    "active": True, "status": "RİSK VETOSU UYGULANDI",
                    "reason": report["summary"], "triggered_at": now,
                    "paper_orchestrator_stopped": bool(stopped), "stopped_engines": stopped,
                }
                engine["approved_allocations"] = []
                action = f"Kritik risk %{report['risk_score']}: Paper bütçesi sıfırlandı; {', '.join(stopped) or 'motorlar zaten duruyordu'}."
                v11_add_event(engine, "OTOMATİK PAPER RİSK VETOSU", action)
                add_paper_notification(paper, "V11 RİSK VETOSU", action)
            else:
                engine["approved_allocations"] = list(report["allocations"])
                action = f"Risk %{report['risk_score']} · {report['risk_level']}: {report['invested_budget_usdt']:.0f} USDT Paper bütçesi dağıtıldı."
                v11_add_event(engine, "RİSK BÜTÇESİ", action)
            engine.update({
                "busy": False, "status": "PORTFÖY RİSKİ İZLENİYOR", "latest_report": report,
                "intervention": intervention, "cycles": int(engine.get("cycles", 0)) + 1,
                "last_tick_at": now, "last_action": action,
                "orders_enabled": False, "testnet_orders_enabled": False,
            })
        asyncio.create_task(persist_paper_snapshot(application))
    except Exception as exc:
        async with paper["lock"]:
            engine = paper.get("portfolio_risk", empty_v11_risk_state())
            engine["busy"] = False
            engine["last_action"] = f"V11 risk verisi geçici olarak bekleniyor: {str(exc)[:100]}"
            v11_add_event(engine, "GEÇİCİ VERİ HATASI", engine["last_action"])


async def v11_risk_loop(application: FastAPI) -> None:
    while True:
        try:
            if application.state.paper.get("portfolio_risk", {}).get("enabled"):
                await v11_risk_cycle(application)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine = application.state.paper.get("portfolio_risk", {})
            engine["busy"] = False
            engine["last_action"] = f"V11 geçici hata; güvenli biçimde yeniden denenecek: {str(exc)[:80]}"
        await asyncio.sleep(V11_RISK_TICK_SECONDS)


class V11RiskStartRequest(BaseModel):
    symbols: list[str] = Field(default_factory=lambda: list(V9_DEFAULT_UNIVERSE), min_length=2, max_length=8)
    interval: Literal["5m", "15m", "1h"] = "15m"
    capital: float = Field(default=5_000.0, ge=500.0, le=100_000.0)
    simulations: int = Field(default=500, ge=100, le=2_000)
    horizon_candles: int = Field(default=24, ge=4, le=96)


@app.get("/api/v11/risk")
async def v11_risk_status():
    async with app.state.paper["lock"]:
        return v11_risk_payload(app.state.paper.get("portfolio_risk", empty_v11_risk_state()))


@app.get("/api/v11/risk-lab")
async def v11_risk_preview(
    symbols: str = Query("BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT", min_length=13, max_length=180),
    interval: Literal["5m", "15m", "1h"] = "15m",
    capital: float = Query(5_000.0, ge=500.0, le=100_000.0),
    simulations: int = Query(500, ge=100, le=2_000),
    horizon_candles: int = Query(24, ge=4, le=96),
):
    universe = []
    for raw in symbols.split(","):
        symbol = "".join(char for char in raw.upper() if char.isalnum())
        if symbol.endswith("USDT") and symbol not in universe:
            universe.append(symbol)
    if len(universe) < 2:
        raise HTTPException(422, "V11 için en az iki geçerli USDT paritesi seçin")
    universe = universe[:8]
    cache_key = (
        tuple(universe), interval, int(round(capital)),
        int(simulations), int(horizon_candles),
    )
    cached = V11_RISK_CACHE.get(cache_key)
    if cached and time.monotonic() - cached[0] < 60:
        safe_cached = json_safe_payload(cached[1])
        return {**safe_cached, "cached": True, "response_patch": DEPLOYMENT_PATCH}
    try:
        candles = await v11_fetch_universe(universe, interval)
        raw_payload = v11_portfolio_risk_lab(candles, capital, interval, simulations, horizon_candles)
        payload = json_safe_payload(raw_payload)
        if not isinstance(payload, dict):
            raise ValueError("Risk laboratuvarı geçerli bir rapor üretemedi")
    except Exception as exc:
        # Üçüncü taraf mum verisi eksikken ValueError'ın ASGI sürecine kadar
        # taşınıp 500 üretmesini engeller. Arayüz bu 503'ü güvenli bekleme
        # durumu olarak ele alır ve son sağlam raporu korur.
        raise HTTPException(
            status_code=503,
            detail="V11 risk laboratuvarı için yeterli canlı mum verisi henüz alınamadı; otomatik Paper riski kapalı tutuluyor.",
        ) from exc
    V11_RISK_CACHE[cache_key] = (time.monotonic(), payload)
    return {**payload, "cached": False, "response_patch": DEPLOYMENT_PATCH}


@app.post("/api/v11/risk/start")
async def v11_risk_start(config: V11RiskStartRequest):
    paper = app.state.paper
    symbols = []
    for raw in config.symbols:
        symbol = "".join(char for char in raw.upper() if char.isalnum())
        if symbol.endswith("USDT") and symbol not in symbols:
            symbols.append(symbol)
    if len(symbols) < 2:
        raise HTTPException(422, "V11 için en az iki geçerli USDT paritesi seçin")
    now = datetime.now(timezone.utc).isoformat()
    async with paper["lock"]:
        engine = paper.get("portfolio_risk", empty_v11_risk_state())
        if engine.get("enabled"):
            raise HTTPException(409, "V11 Risk Beyni zaten çalışıyor")
        engine.update({
            "enabled": True, "busy": False, "status": "PORTFÖY RİSKİ İZLENİYOR",
            "interval": config.interval, "capital": config.capital, "universe": symbols[:8],
            "simulations": config.simulations, "horizon_candles": config.horizon_candles,
            "started_at": now, "stopped_at": None, "last_tick_at": now,
            "last_action": f"V11 {len(symbols[:8])} parite için Monte Carlo ve korelasyon risk izlemesini başlattı.",
            "orders_enabled": False, "testnet_orders_enabled": False,
        })
        v11_add_event(engine, "V11 BAŞLADI", engine["last_action"])
    asyncio.create_task(v11_risk_cycle(app))
    asyncio.create_task(persist_paper_snapshot(app))
    return v11_risk_payload(engine)


@app.post("/api/v11/risk/stop")
async def v11_risk_stop():
    paper = app.state.paper
    async with paper["lock"]:
        engine = paper.get("portfolio_risk", empty_v11_risk_state())
        engine["enabled"] = False
        engine["busy"] = False
        engine["status"] = "KULLANICI TARAFINDAN DURDURULDU"
        engine["stopped_at"] = datetime.now(timezone.utc).isoformat()
        engine["last_action"] = "V11 risk izlemesi durduruldu; rapor ve Paper bütçeleri hafızada korundu."
        v11_add_event(engine, "V11 DURDU", engine["last_action"])
    asyncio.create_task(persist_paper_snapshot(app))
    return v11_risk_payload(engine)


@app.post("/api/v11/risk/reset")
async def v11_risk_reset_intervention():
    paper = app.state.paper
    async with paper["lock"]:
        engine = paper.get("portfolio_risk", empty_v11_risk_state())
        report = engine.get("latest_report") or {}
        if report.get("veto_required"):
            raise HTTPException(409, "Kritik risk devam ederken Paper risk vetosu kaldırılamaz")
        engine["intervention"] = {
            "active": False, "status": "KULLANICI TARAFINDAN SIFIRLANDI",
            "reason": "Risk normale döndü; motorlar otomatik başlamaz, ayrı kullanıcı onayı gerekir.",
            "triggered_at": None, "paper_orchestrator_stopped": False,
        }
        engine["last_action"] = "V11 Paper risk vetosu sıfırlandı; V6/V7/Paper Bot güvenlik için kapalı kalır."
        v11_add_event(engine, "RİSK VETOSU SIFIRLANDI", engine["last_action"])
    asyncio.create_task(persist_paper_snapshot(app))
    return v11_risk_payload(engine)
