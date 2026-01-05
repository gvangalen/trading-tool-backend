import logging
from typing import Optional, Dict, Any, List
from datetime import date

from backend.utils.db import get_db_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# =====================================================
# 🧠 SHARED AGENT CONTEXT BUILDER
# =====================================================
def build_agent_context(
    user_id: int,
    category: str,
    current_score: Optional[float],
    current_items: Optional[List[dict]] = None,
    lookback_days: int = 1,
) -> Dict[str, Any]:
    """
    Bouwt uniforme AI-context voor alle agents.

    Parameters:
    - user_id: gebruiker
    - category: 'macro' | 'technical' | 'market' | ...
    - current_score: huidige gemiddelde score
    - current_items: indicatoren / inputs van vandaag
    - lookback_days: hoeveel dagen terug (default = 1)

    Retourneert:
    - today
    - history (1 of meerdere dagen)
    - delta (vs meest recente historische dag)
    """

    conn = get_db_connection()
    history: List[Dict[str, Any]] = []

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    date,
                    avg_score,
                    trend,
                    bias,
                    risk,
                    summary,
                    top_signals
                FROM ai_category_insights
                WHERE user_id = %s
                  AND category = %s
                  AND date < CURRENT_DATE
                ORDER BY date DESC
                LIMIT %s;
            """, (user_id, category, lookback_days))

            rows = cur.fetchall()

            for r in rows:
                history.append({
                    "date": r[0].isoformat() if r[0] else None,
                    "avg_score": float(r[1]) if r[1] is not None else None,
                    "trend": r[2],
                    "bias": r[3],
                    "risk": r[4],
                    "summary": r[5],
                    "top_signals": r[6],
                })

    except Exception:
        logger.error("❌ build_agent_context DB error", exc_info=True)
    finally:
        conn.close()

    previous = history[0] if history else None
    prev_score = previous["avg_score"] if previous else None

    score_change = (
        round(current_score - prev_score, 2)
        if current_score is not None and prev_score is not None
        else None
    )

    direction = (
        "verbetering" if score_change and score_change > 0
        else "verslechtering" if score_change and score_change < 0
        else "onveranderd"
    )

    context = {
        "today": {
            "avg_score": current_score,
            "items": current_items or [],
        },
        "history": history,  # 1 of meerdere dagen
        "delta": {
            "score_change": score_change,
            "direction": direction,
        },
    }

    logger.info(
        "🧠 Agent context built | user=%s | category=%s | delta=%s",
        user_id,
        category,
        score_change,
    )

    return context
