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
# 🧩 Naam-aliases (alleen macro/technical, GEEN market!)
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

        return None

    except Exception:
        logger.exception(
            f"❌ Fout bij ophalen scoreregels ({indicator_name})"
        )
        return None

    finally:
        conn.close()

# =========================================================
# 📊 MARKET helpers (GEEN user_id)
# =========================================================
def load_market_rule_indicators(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT indicator
            FROM market_indicator_rules
        """)
        return [r[0] for r in cur.fetchall()]

def load_latest_market_snapshot(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT price, volume, change_24h
            FROM market_data
            WHERE symbol = 'BTC'
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        return cur.fetchone()

def extract_market_data(rule_indicators, snapshot):
    """
    ENIGE market-indicatoren die bestaan:
    - change_24h
    - volume (volume-afwijking %)
    """
    data = {}

    if not snapshot:
        return data

    price, volume, change_24h = snapshot

    if "change_24h" in rule_indicators and change_24h is not None:
        data["change_24h"] = float(change_24h)

    if "volume" in rule_indicators and volume is not None:
        data["volume"] = float(volume)

    return data

# =========================================================
# 🔢 SCORE ENGINE
# =========================================================
def generate_scores_db(
    category: str,
    data: Optional[Dict[str, float]] = None,
    user_id: Optional[int] = None
) -> Dict[str, Any]:

    # -----------------------------------------------------
    # MARKET (globaal, GEEN user_id)
    # -----------------------------------------------------
    if category == "market":
        conn = get_db_connection()
        if not conn:
            return {"scores": {}, "total_score": 10}

        try:
            rule_indicators = load_market_rule_indicators(conn)
            snapshot = load_latest_market_snapshot(conn)
            data = extract_market_data(rule_indicators, snapshot)
        finally:
            conn.close()

    # -----------------------------------------------------
    # MACRO / TECHNICAL (user-specific)
    # -----------------------------------------------------
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
                    }
        finally:
            conn.close()

    # -----------------------------------------------------
    # Geen data → minimale score
    # -----------------------------------------------------
    if not data:
        return {"scores": {}, "total_score": 10}

    scores = {}
    total = 0
    count = 0

    for indicator, value in data.items():
        rule = get_score_rule_from_db(category, indicator, value)
        if not rule:
            continue

        scores[indicator] = {
            "value": value,
            "score": rule["score"],
            "trend": rule["trend"],
            "interpretation": rule["interpretation"],
            "action": rule["action"],
        }

        total += rule["score"]
        count += 1

    avg = round(total / count) if count else 10

    return {
        "scores": scores,
        "total_score": avg
    }

# =========================================================
# 🔗 DASHBOARD COMBINED SCORES
# =========================================================
def get_scores_for_symbol(
    user_id: int,
    include_metadata: bool = False
) -> Dict[str, Any]:

    macro = generate_scores_db("macro", user_id=user_id)
    tech = generate_scores_db("technical", user_id=user_id)
    market = generate_scores_db("market")

    macro_score = macro["total_score"]
    tech_score = tech["total_score"]
    market_score = market["total_score"]

    setup_score = round((macro_score + tech_score) / 2)

    result = {
        "macro_score": macro_score,
        "technical_score": tech_score,
        "market_score": market_score,
        "setup_score": setup_score,
    }

    if include_metadata:
        def top(scores):
            return sorted(
                scores.get("scores", {}).items(),
                key=lambda x: x[1]["score"],
                reverse=True
            )[:3]

        result.update({
            "macro_top_contributors": [i[0] for i in top(macro)],
            "technical_top_contributors": [i[0] for i in top(tech)],
            "market_top_contributors": [i[0] for i in top(market)],
        })

    return result
