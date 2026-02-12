import logging
from datetime import date, timedelta
from typing import Dict, Any, List

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# Helpers
# =====================================================

def avg(values: List[float]) -> float:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0


def compute_volume_streak(volumes: List[float]) -> int:
    """
    telt hoeveel dagen volume stijgt
    """
    streak = 0

    for i in range(1, len(volumes)):
        if volumes[i] > volumes[i - 1]:
            streak += 1
        else:
            streak = 0

    return streak


# =====================================================
# CORE DETECTION
# =====================================================

def detect_regime(user_id: int) -> Dict[str, Any]:

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            # laatste 7 dagen market data
            cur.execute("""
                SELECT DATE(timestamp), price, volume
                FROM market_data
                WHERE user_id = %s
                ORDER BY timestamp DESC
                LIMIT 7;
            """, (user_id,))

            rows = cur.fetchall()

        if len(rows) < 5:
            return {
                "label": "insufficient_data",
                "confidence": 0.2,
                "signals": {},
                "narrative": "Not enough data to classify regime."
            }

        prices = [float(r[1]) for r in reversed(rows)]
        volumes = [float(r[2]) for r in reversed(rows)]

        price_return = (prices[-1] - prices[0]) / prices[0]
        volume_trend = avg(volumes[-3:]) > avg(volumes[:3])
        volume_streak = compute_volume_streak(volumes)

        signals = {
            "price_return_7d": price_return,
            "volume_trend": volume_trend,
            "volume_streak": volume_streak
        }

        score = 0

        # =============================
        # DISTRIBUTION
        # =============================

        if volume_trend:
            score += 1

        if abs(price_return) < 0.01:
            score += 1

        if volume_streak >= 3:
            score += 1

        if score >= 2:
            return {
                "label": "distribution",
                "confidence": min(0.6 + score * 0.1, 0.9),
                "signals": signals,
                "narrative": "Distribution characteristics continue to build."
            }

        # =============================
        # TREND UP
        # =============================

        if price_return > 0.04 and volume_trend:
            return {
                "label": "trend_up",
                "confidence": 0.8,
                "signals": signals,
                "narrative": "Participation confirms upside trend."
            }

        # =============================
        # TREND DOWN
        # =============================

        if price_return < -0.04:
            return {
                "label": "trend_down",
                "confidence": 0.75,
                "signals": signals,
                "narrative": "Downside pressure dominates with expanding activity."
            }

        return {
            "label": "range",
            "confidence": 0.5,
            "signals": signals,
            "narrative": "Market remains range-bound without directional conviction."
        }

    finally:
        conn.close()


# =====================================================
# UPSERT
# =====================================================

def store_regime_memory(user_id: int):

    regime = detect_regime(user_id)

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO regime_memory
                (user_id, date, regime_label, confidence, signals_json, narrative)
                VALUES (%s, CURRENT_DATE, %s, %s, %s, %s)
                ON CONFLICT (user_id, date)
                DO UPDATE SET
                    regime_label = EXCLUDED.regime_label,
                    confidence = EXCLUDED.confidence,
                    signals_json = EXCLUDED.signals_json,
                    narrative = EXCLUDED.narrative;
            """, (
                user_id,
                regime["label"],
                regime["confidence"],
                json.dumps(regime["signals"]),
                regime["narrative"]
            ))

        conn.commit()

        logger.info("✅ Regime memory stored: %s", regime["label"])

    finally:
        conn.close()
