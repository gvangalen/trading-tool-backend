import logging
from typing import Dict, Any, Optional
from backend.utils.db import get_db_connection

# =========================================================
# ⚙️ Logging setup
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =========================================================
# 🧩 Bekende naam-aliases
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
# 🧠 Normalisatie functie
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
# ✅ Universele database-functie
# =========================================================
def get_score_rule_from_db(category: str, indicator_name: str, value: float) -> Optional[dict]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ get_score_rule_from_db(): geen DB-verbinding")
        return None

    table_map = {
        "technical": "technical_indicator_rules",
        "macro": "macro_indicator_rules",
        "market": "market_indicator_rules",
    }
    table = table_map.get(category)

    normalized = normalize_indicator_name(indicator_name)

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM {table}
                WHERE LOWER(REPLACE(REPLACE(REPLACE(indicator, '&', 'and'), ' ', '_'), '-', '_')) = %s
                ORDER BY range_min ASC;
            """, (normalized,))
            rules = cur.fetchall()

        if not rules:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor '{normalized}' ({category})")
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

    except Exception as e:
        logger.error(f"❌ get_score_rule_from_db({indicator_name}): {e}", exc_info=True)
        return None

    finally:
        conn.close()


# =========================================================
# 🔥 MARKET: Nieuwe dynamische mapping
# =========================================================
def load_market_indicators(conn):
    """Laad welke indicatoren actief zijn volgens de DB."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT indicator FROM market_indicator_rules;")
        return [r[0] for r in cur.fetchall()]


def load_market_raw_data(conn):
    """Laadt prijs/volume/change voor BTC uit market_data + laatste OHLC uit 7d."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT price, volume, change_24h
            FROM market_data
            WHERE symbol = 'BTC'
            ORDER BY timestamp DESC
            LIMIT 1;
        """)
        snapshot = cur.fetchone()

        cur.execute("""
            SELECT open, high, low, close, change
            FROM market_data_7d
            WHERE symbol = 'BTC'
            ORDER BY date DESC
            LIMIT 1;
        """)
        ohlc = cur.fetchone()

    return snapshot, ohlc


def extract_market_data(rule_indicators, snapshot, ohlc):
    """Bouwt data dict op op basis van welke indicators actief zijn."""

    data = {}

    # Snapshot -> price, volume, change_24h
    if snapshot:
        price = float(snapshot[0])
        volume = float(snapshot[1])
        change_24h = float(snapshot[2])
    else:
        price = volume = change_24h = None

    # OHLC -> open, high, low, close, daily change
    if ohlc:
        o, h, l, c, ch = map(float, ohlc)
    else:
        o = h = l = c = ch = None

    # 1️⃣ btc_change_24h
    if "btc_change_24h" in rule_indicators and change_24h is not None:
        data["btc_change_24h"] = change_24h

    # 2️⃣ price_trend (uit 7d daily % change)
    if "price_trend" in rule_indicators and ch is not None:
        data["price_trend"] = ch

    # 3️⃣ volatility = (high - low) / close * 100
    if "volatility" in rule_indicators and c and h and l:
        data["volatility"] = ((h - l) / c) * 100

    # 4️⃣ volume_strength = volume USD
    if "volume_strength" in rule_indicators and volume is not None:
        data["volume_strength"] = volume

    return data


# =========================================================
# 🔢 Dynamische scoregenerator
# =========================================================
def generate_scores_db(category: str, data: Optional[Dict[str, float]] = None) -> Dict[str, Any]:

    if category == "market":
        conn = get_db_connection()
        if not conn:
            return {"scores": {}, "total_score": 0}

        try:
            rule_indicators = load_market_indicators(conn)
            snapshot, ohlc = load_market_raw_data(conn)
            data = extract_market_data(rule_indicators, snapshot, ohlc)

        finally:
            conn.close()

    # Technical / Macro auto-load
    if data is None:
        data = {}
        conn = get_db_connection()
        if not conn:
            return {"scores": {}, "total_score": 0}

        try:
            with conn.cursor() as cur:
                if category == "macro":
                    cur.execute("""
                        SELECT DISTINCT ON (name) name, value
                        FROM macro_data
                        ORDER BY name, timestamp DESC;
                    """)
                    rows = cur.fetchall()
                    data = {normalize_indicator_name(r[0]): float(r[1]) for r in rows}

                elif category == "technical":
                    cur.execute("""
                        SELECT DISTINCT ON (indicator) indicator, value
                        FROM technical_indicators
                        ORDER BY indicator, timestamp DESC;
                    """)
                    rows = cur.fetchall()
                    data = {normalize_indicator_name(r[0]): float(r[1]) for r in rows}

        finally:
            conn.close()

    # Geen data?
    if not data:
        return {"scores": {}, "total_score": 0}

    # Scores berekenen
    scores = {}
    total_score = 0
    count = 0

    for indicator, value in data.items():
        rule = get_score_rule_from_db(category, indicator, value)
        if not rule:
            continue

        score = int(rule["score"])
        scores[indicator] = {
            "value": value,
            "score": score,
            "trend": rule.get("trend", ""),
            "interpretation": rule.get("interpretation", ""),
            "action": rule.get("action", ""),
        }

        total_score += score
        count += 1

    avg_score = round(total_score / count) if count else 10

    return {"scores": scores, "total_score": avg_score}


# =========================================================
# 🔗 Combined scores
# =========================================================
def get_scores_for_symbol(include_metadata: bool = False) -> Dict[str, Any]:

    conn = get_db_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cur:

            # --- Macro ---
            cur.execute("""
                SELECT DISTINCT ON (name) name, value
                FROM macro_data
                ORDER BY name, timestamp DESC;
            """)
            macro_data = {normalize_indicator_name(r[0]): float(r[1]) for r in cur.fetchall()}

            # --- Technical ---
            cur.execute("""
                SELECT DISTINCT ON (indicator) indicator, value
                FROM technical_indicators
                ORDER BY indicator, timestamp DESC;
            """)
            technical_data = {normalize_indicator_name(r[0]): float(r[1]) for r in cur.fetchall()}

        macro_scores = generate_scores_db("macro", macro_data)
        tech_scores = generate_scores_db("technical", technical_data)
        market_scores = generate_scores_db("market")

        macro_avg = macro_scores["total_score"]
        tech_avg = tech_scores["total_score"]
        market_avg = market_scores["total_score"]

        setup_score = round((macro_avg + tech_avg) / 2)

        result = {
            "macro_score": macro_avg,
            "technical_score": tech_avg,
            "market_score": market_avg,
            "setup_score": setup_score,
        }

        if include_metadata:

            def top(scores_dict):
                if "scores" not in scores_dict:
                    return []
                return sorted(
                    scores_dict["scores"].items(),
                    key=lambda x: x[1]["score"],
                    reverse
