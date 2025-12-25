import logging
from typing import Dict, Any, Optional

from backend.utils.db import get_db_connection

# =========================================================
# ⚙️ Logging
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =========================================================
# 🧩 Naam-aliases (ALLEEN macro/technical – GEEN market!)
# =========================================================
NAME_ALIASES = {
    "fear_and_greed_index": "fear_greed_index",
    "fear_greed": "fear_greed_index",
    "sandp500": "sp500",
    "s&p500": "sp500",
    "s&p_500": "sp500",
    "sp_500": "sp500",
}

# =========================================================
# 🧠 Normalisatie
# =========================================================
def normalize_indicator_name(name: str) -> str:
    normalized = (
        name.lower()
        .replace("&", "and")
        .replace("s&p", "sp")
        .replace(" ", "_")
        .replace("-", "_")
        .strip()
    )
    return NAME_ALIASES.get(normalized, normalized)

# =========================================================
# 🎯 Score-regel ophalen (DB-driven)
# =========================================================
def get_score_rule_from_db(
    category: str,
    indicator_name: str,
    value: float
) -> Optional[dict]:

    table_map = {
        "technical": "technical_indicator_rules",
        "macro": "macro_indicator_rules",
        "market": "market_indicator_rules",
    }

    table = table_map.get(category)
    if not table:
        logger.error(f"❌ Onbekende category: {category}")
        return None

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return None

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM {table}
                WHERE LOWER(indicator) = LOWER(%s)
                ORDER BY range_min ASC
            """, (indicator_name,))
            rules = cur.fetchall()

        if not rules:
            logger.warning(
                f"⚠️ Geen scoreregels voor {indicator_name} ({category})"
            )
            return None

        for r in rules:
            if r[0] <= value <= r[1]:
                return {
                    "score": r[2],
                    "trend": r[3],
                    "interpretation": r[4],
                    "action": r[5],
                }

        logger.warning(
            f"⚠️ Waarde {value} valt buiten ranges voor {indicator_name}"
        )
        return None

    except Exception:
        logger.exception(
            f"❌ Fout bij ophalen scoreregels ({indicator_name})"
        )
        return None

    finally:
        conn.close()

# =========================================================
# 🔢 SCORE ENGINE (DEFINITIEF)
# =========================================================
def generate_scores_db(
    category: str,
    data: Optional[Dict[str, float]] = None,
    user_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Centrale score-engine.

    REGELS:
    - MARKET:
        • gebruikt ALTIJD meegegeven data (bv. volume % afwijking)
        • haalt GEEN eigen market_data op als data is gezet
    - MACRO / TECHNICAL:
        • data=None → data uit DB op basis van user_id
    """

    # =====================================================
    # MARKET (GLOBAAL — GEEN user_id)
    # =====================================================
    if category == "market":
        if data is None:
            logger.warning(
                "⚠️ generate_scores_db(market) zonder data aangeroepen — skip"
            )
            return {"scores": {}, "total_score": 10}

    # =====================================================
    # MACRO / TECHNICAL (USER-SPECIFIEK)
    # =====================================================
    elif data is None:
        if user_id is None:
            raise ValueError(
                "❌ user_id verplicht voor macro/technical"
            )

        conn = get_db_connection()
        if not conn:
            return {"scores": {}, "total_score": 10}

        try:
            with conn.cursor() as cur:

                if category == "macro":
                    cur.execute("""
                        SELECT DISTINCT ON (name) name, value
                        FROM macro_data
                        WHERE user_id=%s
                        ORDER BY name, timestamp DESC
                    """, (user_id,))
                    rows = cur.fetchall()
                    data = {
                        normalize_indicator_name(r[0]): float(r[1])
                        for r in rows
                        if r[1] is not None
                    }

                elif category == "technical":
                    cur.execute("""
                        SELECT DISTINCT ON (indicator) indicator, value
                        FROM technical_indicators
                        WHERE user_id=%s
                        ORDER BY indicator, timestamp DESC
                    """, (user_id,))
                    rows = cur.fetchall()
                    data = {
                        normalize_indicator_name(r[0]): float(r[1])
                        for r in rows
                        if r[1] is not None
                    }
        finally:
            conn.close()

    # =====================================================
    # GEEN DATA → MINIMUM SCORE
    # =====================================================
    if not data:
        return {"scores": {}, "total_score": 10}

    # =====================================================
    # SCORE BEREKENING
    # =====================================================
    scores: Dict[str, Any] = {}
    total_score = 0
    count = 0

    for indicator, value in data.items():
        rule = get_score_rule_from_db(
            category=category,
            indicator_name=indicator,
            value=value
        )

        if not rule:
            logger.warning(
                f"⚠️ Geen scoreregel match voor {indicator} "
                f"(value={value}, category={category})"
            )
            continue

        score = int(rule["score"])

        scores[indicator] = {
            "value": value,
            "score": score,
            "trend": rule["trend"],
            "interpretation": rule["interpretation"],
            "action": rule["action"],
        }

        total_score += score
        count += 1

    avg_score = round(total_score / count) if count else 10

    return {
        "scores": scores,
        "total_score": avg_score
    }

# =========================================================
# 🔗 DASHBOARD COMBINED SCORES
# =========================================================
def get_scores_for_symbol(
    user_id: int,
    include_metadata: bool = False
) -> Dict[str, Any]:

    conn = get_db_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    macro_score,
                    macro_interpretation,
                    macro_top_contributors,

                    technical_score,
                    technical_interpretation,
                    technical_top_contributors,

                    market_score,
                    market_interpretation,
                    market_top_contributors,

                    setup_score
                FROM daily_scores
                WHERE user_id = %s
                  AND report_date = CURRENT_DATE
                LIMIT 1
            """, (user_id,))

            row = cur.fetchone()

        if not row:
            return {}

        result = {
            "macro_score": row[0],
            "macro_interpretation": row[1],
            "macro_top_contributors": row[2] or [],

            "technical_score": row[3],
            "technical_interpretation": row[4],
            "technical_top_contributors": row[5] or [],

            # 🔥 DIT WAS HET PROBLEEM
            "market_score": row[6],
            "market_interpretation": row[7],
            "market_top_contributors": row[8] or [],

            "setup_score": row[9],
        }

        return result

    finally:
        conn.close()
