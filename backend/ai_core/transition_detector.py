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
    """
    Simple slope proxy: (last - first) / (n-1), ignoring None.
    """
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
    """
    Percent change from a -> b (a as base).
    """
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


def fetch_recent_points(user_id: int, lookback_days: int = 14) -> List[DailyPoint]:
    """
    Pulls daily points for last N days (best-effort):
    - market_data: price/change_24h/volume (last snapshot of each day)
    - daily_scores: macro/market/technical/setup (by report_date)
    """
    conn = get_db_connection()
    if not conn:
        return []

    start_date = date.today() - timedelta(days=lookback_days)

    points: Dict[date, DailyPoint] = {}

    try:
        with conn.cursor() as cur:
            # daily_scores (by report_date)
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

            # market_data (last snapshot per day)
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


def compute_transition_detector(user_id: int, lookback_days: int = 14) -> Dict[str, Any]:
    """
    Returns a transition snapshot:
    {
      "transition_risk": 0-100,
      "primary_flag": "...",
      "signals": {...},
      "narrative": "Hedge-fund style one-liner",
      "confidence": 0-1
    }
    """
    pts = fetch_recent_points(user_id=user_id, lookback_days=lookback_days)
    if len(pts) < 5:
        return {
            "transition_risk": 50,
            "primary_flag": "insufficient_history",
            "signals": {"note": "Not enough multi-day history to detect transitions."},
            "narrative": "Transition signals unavailable. Insufficient multi-day history.",
            "confidence": 0.25,
        }

    # windows
    w5 = pts[-5:]
    w10 = pts[-10:] if len(pts) >= 10 else pts

    prices_5 = [p.price for p in w5]
    vols_5 = [p.volume for p in w5]
    tech_5 = [p.technical for p in w5]
    mkt_5 = [p.market for p in w5]
    chg_5 = [p.change_24h for p in w5]

    # trend proxies
    vol_slope = _slope(vols_5)
    price_slope = _slope(prices_5)
    tech_slope = _slope(tech_5)
    mkt_slope = _slope(mkt_5)

    # magnitude proxies
    price_5_pct = _pct(prices_5[0], prices_5[-1]) if prices_5 else None
    vol_5_pct = _pct(vols_5[0], vols_5[-1]) if vols_5 else None

    # volatility proxy: std of abs change_24h
    abs_chg = [abs(x) if x is not None else None for x in chg_5]
    vol_of_vol = _std(abs_chg)  # lower -> compression

    # risk scoring (start neutral)
    risk = 50
    flags: List[str] = []
    notes: List[str] = []
    confidence = 0.55

    # =========================================================
    # 1) Distribution: volume rising while price stalls / fades
    # =========================================================
    if vol_slope is not None and price_slope is not None:
        if vol_slope > 0 and price_slope <= 0:
            risk += 20
            flags.append("distribution_build")
            notes.append("Participation rising while price fails to advance.")

    # also consider % changes
    if vol_5_pct is not None and price_5_pct is not None:
        if vol_5_pct > 8 and price_5_pct < 1.5:
            risk += 15
            flags.append("volume_price_divergence")
            notes.append("Volume expansion without price follow-through.")

    # =========================================================
    # 2) Trend exhaustion: technical weakening while market stable
    # =========================================================
    if tech_slope is not None and mkt_slope is not None:
        if tech_slope < 0 and mkt_slope >= 0:
            risk += 10
            flags.append("momentum_fade")
            notes.append("Technical impulse deteriorating under a stable tape.")

    # =========================================================
    # 3) Bull-trap risk: price up but quality down (volume down / tech down)
    # =========================================================
    if price_slope is not None and price_slope > 0:
        if (vol_slope is not None and vol_slope < 0) or (tech_slope is not None and tech_slope < 0):
            risk += 10
            flags.append("bull_trap_risk")
            notes.append("Upside attempt lacks participation or momentum support.")

    # =========================================================
    # 4) Volatility compression: low vol-of-vol tends to precede expansion
    # =========================================================
    if vol_of_vol is not None:
        if vol_of_vol < 0.8:  # heuristic; change if your data scale differs
            risk += 8
            flags.append("volatility_compression")
            notes.append("Compression building. Expansion risk rising.")
        elif vol_of_vol > 2.0:
            risk += 6
            flags.append("volatility_expansion")
            notes.append("Expansion regime. Whipsaw risk elevated.")

    # =========================================================
    # 5) Risk asymmetry: scores weakening together
    # =========================================================
    # If market + technical both down over the window -> downside asymmetry increases
    if mkt_slope is not None and tech_slope is not None:
        if mkt_slope < 0 and tech_slope < 0:
            risk += 12
            flags.append("risk_asymmetry_negative")
            notes.append("Market + technical drift negative. Downside asymmetry widening.")

    # clamp
    risk = max(0, min(100, risk))

    # confidence tweaks
    if len(flags) == 0:
        confidence = 0.45
    elif len(flags) >= 3:
        confidence = 0.72
    elif len(flags) >= 2:
        confidence = 0.62

    primary_flag = flags[0] if flags else "no_transition_signal"

    # Hedge-fund narrative (short, not retail)
    if "distribution_build" in flags or "volume_price_divergence" in flags:
        narrative = "Distribution characteristics increasing. Upside convexity deteriorating."
    elif "bull_trap_risk" in flags:
        narrative = "Upside attempts lack quality. Bull-trap risk elevated."
    elif "volatility_compression" in flags:
        narrative = "Compression building. Break risk rising. Direction still unconfirmed."
    elif "risk_asymmetry_negative" in flags:
        narrative = "Downside asymmetry widening. Risk management takes priority over exposure."
    elif "momentum_fade" in flags:
        narrative = "Momentum fading. Regime maturity increasing."
    else:
        narrative = "No clear transition signature. Regime likely persistent."

    signals = {
        "window_days": 5,
        "vol_slope": vol_slope,
        "price_slope": price_slope,
        "tech_slope": tech_slope,
        "market_slope": mkt_slope,
        "price_5d_pct": price_5_pct,
        "volume_5d_pct": vol_5_pct,
        "volatility_proxy_std_abs_change": vol_of_vol,
        "flags": flags,
        "notes": notes,
        "technical_strength": _classify_strength(tech_5[-1] if tech_5 else None),
        "market_strength": _classify_strength(mkt_5[-1] if mkt_5 else None),
    }

    return {
        "transition_risk": risk,
        "primary_flag": primary_flag,
        "signals": _safe_json(signals),
        "narrative": narrative,
        "confidence": round(confidence, 2),
    }
