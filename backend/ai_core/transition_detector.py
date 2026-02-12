import json
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =========================================================
# Transition Detector (rule-based, multi-day, regime-aware)
# =========================================================

@dataclass
class DailyPoint:
    d: date
    price: Optional[float]
    change_24h: Optional[float]
    volume: Optional[float]
    macro: Optional[float]
    market: Optional[float]
    technical: Optional[float]
    setup: Optional[float]


def _to_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _safe_json(obj: Any) -> Any:
    """
    JSON-safe serialization
    """
    from datetime import datetime

    if obj is None:
        return None
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_json(v) for v in obj]
    return obj


def _slope(values: List[Optional[float]]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    return (clean[-1] - clean[0]) / max(1, (len(clean) - 1))


def _std(values: List[Optional[float]]) -> Optional[float]:
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    m = sum(clean) / len(clean)
    var = sum((x - m) ** 2 for x in clean) / (len(clean) - 1)
    return var ** 0.5


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or a == 0:
        return None
    return (b - a) / a * 100.0


def _classify_strength(v: Optional[float]) -> Optional[str]:
    if v is None:
        return None
    if v >= 75:
        return "strong"
    if v >= 55:
        return "moderate"
    if v >= 40:
        return "weak"
    return "fragile"


# =========================================================
# DATA FETCH
# =========================================================

def fetch_recent_points(user_id: int, lookback_days: int = 14) -> List[DailyPoint]:
    conn = get_db_connection()
    if not conn:
        return []

    start_date = date.today() - timedelta(days=lookback_days)
    points: Dict[date, DailyPoint] = {}

    try:
        with conn.cursor() as cur:

            # daily_scores
            cur.execute(
                """
                SELECT report_date, macro_score, market_score, technical_score, setup_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date >= %s
                ORDER BY report_date ASC;
                """,
                (user_id, start_date),
            )

            for r in cur.fetchall():
                d = r[0]
                points[d] = DailyPoint(
                    d=d,
                    price=None,
                    change_24h=None,
                    volume=None,
                    macro=_to_float(r[1]),
                    market=_to_float(r[2]),
                    technical=_to_float(r[3]),
                    setup=_to_float(r[4]),
                )

            # market snapshots
            cur.execute(
                """
                SELECT DISTINCT ON (DATE(timestamp))
                    DATE(timestamp) as d,
                    price, change_24h, volume
                FROM market_data
                WHERE DATE(timestamp) >= %s
                ORDER BY DATE(timestamp), timestamp DESC;
                """,
                (start_date,),
            )

            for r in cur.fetchall():
                d = r[0]
                if d not in points:
                    points[d] = DailyPoint(
                        d=d,
                        price=_to_float(r[1]),
                        change_24h=_to_float(r[2]),
                        volume=_to_float(r[3]),
                        macro=None,
                        market=None,
                        technical=None,
                        setup=None,
                    )
                else:
                    points[d].price = _to_float(r[1])
                    points[d].change_24h = _to_float(r[2])
                    points[d].volume = _to_float(r[3])

    finally:
        conn.close()

    out = list(points.values())
    out.sort(key=lambda x: x.d)
    return out


# =========================================================
# CORE DETECTOR
# =========================================================

def compute_transition_detector(user_id: int, lookback_days: int = 14) -> Dict[str, Any]:

    pts = fetch_recent_points(user_id=user_id, lookback_days=lookback_days)

    if len(pts) < 5:
        return {
            "transition_risk": 50,
            "normalized_risk": 0.5,
            "primary_flag": "insufficient_history",
            "signals": {"note": "Not enough multi-day history."},
            "narrative": "Transition signals unavailable. Insufficient history.",
            "confidence": 0.25,
        }

    w5 = pts[-5:]

    prices_5 = [p.price for p in w5]
    vols_5 = [p.volume for p in w5]
    tech_5 = [p.technical for p in w5]
    mkt_5 = [p.market for p in w5]
    chg_5 = [p.change_24h for p in w5]

    vol_slope = _slope(vols_5)
    price_slope = _slope(prices_5)
    tech_slope = _slope(tech_5)
    mkt_slope = _slope(mkt_5)

    price_5_pct = _pct(prices_5[0], prices_5[-1]) if prices_5 else None
    vol_5_pct = _pct(vols_5[0], vols_5[-1]) if vols_5 else None

    abs_chg = [abs(x) if x is not None else None for x in chg_5]
    vol_of_vol = _std(abs_chg)

    risk = 50
    flags: List[str] = []
    notes: List[str] = []
    confidence = 0.55

    # Distribution
    if vol_slope is not None and price_slope is not None:
        if vol_slope > 0 and price_slope <= 0:
            risk += 20
            flags.append("distribution_build")

    if vol_5_pct and price_5_pct:
        if vol_5_pct > 8 and price_5_pct < 1.5:
            risk += 15
            flags.append("volume_price_divergence")

    # Momentum fade
    if tech_slope and mkt_slope is not None:
        if tech_slope < 0 and mkt_slope >= 0:
            risk += 10
            flags.append("momentum_fade")

    # Bull trap
    if price_slope and price_slope > 0:
        if (vol_slope and vol_slope < 0) or (tech_slope and tech_slope < 0):
            risk += 10
            flags.append("bull_trap_risk")

    # Compression
    if vol_of_vol is not None:
        if vol_of_vol < 0.8:
            risk += 8
            flags.append("volatility_compression")
        elif vol_of_vol > 2.0:
            risk += 6
            flags.append("volatility_expansion")

    # Risk asymmetry
    if mkt_slope and tech_slope:
        if mkt_slope < 0 and tech_slope < 0:
            risk += 12
            flags.append("risk_asymmetry_negative")

    risk = max(0, min(100, risk))
    normalized_risk = round(risk / 100.0, 4)

    if len(flags) == 0:
        confidence = 0.45
    elif len(flags) >= 3:
        confidence = 0.72
    elif len(flags) >= 2:
        confidence = 0.62

    primary_flag = flags[0] if flags else "no_transition_signal"

    narrative_map = {
        "distribution_build": "Distribution characteristics increasing. Upside convexity deteriorating.",
        "bull_trap_risk": "Upside attempts lack quality. Bull-trap risk elevated.",
        "volatility_compression": "Compression building. Break risk rising.",
        "risk_asymmetry_negative": "Downside asymmetry widening. Risk management prioritized.",
        "momentum_fade": "Momentum fading. Regime maturity increasing.",
    }

    narrative = narrative_map.get(primary_flag, "No clear transition signature. Regime likely persistent.")

    signals = {
        "window_days": 5,
        "vol_slope": vol_slope,
        "price_slope": price_slope,
        "tech_slope": tech_slope,
        "market_slope": mkt_slope,
        "price_5d_pct": price_5_pct,
        "volume_5d_pct": vol_5_pct,
        "volatility_proxy": vol_of_vol,
        "flags": flags,
        "technical_strength": _classify_strength(tech_5[-1] if tech_5 else None),
        "market_strength": _classify_strength(mkt_5[-1] if mkt_5 else None),
    }

    return {
        "transition_risk": risk,
        "normalized_risk": normalized_risk,  # 🔥 ENGINE INPUT
        "primary_flag": primary_flag,
        "signals": _safe_json(signals),
        "narrative": narrative,
        "confidence": round(confidence, 2),
    }


# =========================================================
# ENGINE HELPER (VERY IMPORTANT)
# =========================================================

def get_transition_risk_value(user_id: int) -> float:
    """
    Clean engine accessor.
    Always returns a float.
    Never crashes the bot.
    """

    try:
        snap = compute_transition_detector(user_id)
        return float(snap.get("normalized_risk", 0.5))
    except Exception as e:
        logger.warning("Transition risk fallback triggered: %s", e)
        return 0.5
