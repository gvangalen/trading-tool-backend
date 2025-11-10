import logging
from typing import Dict, Any, Optional
from backend.utils.db import get_db_connection

# =========================================================
# ⚙️ Logging setup
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =========================================================
# 🔁 Mapping voor market-indicatornamen
# =========================================================
MARKET_INDICATOR_MAP = {
    "price": "btc_price",
    "change_24h": "btc_change_24h",
    "volume": "btc_volume",
}

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
    if normalized in NAME_ALIASES:
        normalized = NAME_ALIASES[normalized]
    return normalized


# =========================================================
# ✅ Universele database-functie voor scoreregels
# =========================================================
def get_score_rule_from_db(category: str, indicator_name: str, value: float) -> Optional[dict]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding in get_score_rule_from_db()")
        return None

    table_map = {
        "technical": "technical_indicator_rules",
        "macro": "macro_indicator_rules",
        "market": "technical_indicator_rules",  # market gebruikt dezelfde structuur
    }
    table = table_map.get(category)
    if not table:
        logger.error(f"⚠️ Ongeldige categorie: {category}")
        return None

    normalized = normalize_indicator_name(indicator_name)
    mapped_name = normalized
    if category == "market":
        mapped_name = MARKET_INDICATOR_MAP.get(indicator_name, indicator_name)
        mapped_name = normalize_indicator_name(mapped_name)
        if mapped_name != indicator_name:
            logger.debug(f"🔁 Indicator '{indicator_name}' gemapt naar '{mapped_name}'")

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT range_min, range_max, score, trend, interpretation, action
                FROM {table}
                WHERE LOWER(REPLACE(REPLACE(REPLACE(indicator, '&', 'and'), ' ', '_'), '-', '_')) = %s
                ORDER BY range_min ASC
            """, (mapped_name,))
            rules = cur.fetchall()

        if not rules:
            logger.warning(f"⚠️ Geen scoreregels gevonden voor {mapped_name} ({category})")
            return None

        for r in rules:
            if r[0] <= value <= r[1]:
                return {
                    "score": r[2],
                    "trend": r[3],
                    "interpretation": r[4],
                    "action": r[5],
                }

        logger.info(f"ℹ️ Waarde {value} valt buiten alle ranges voor {mapped_name}")
        return None

    except Exception as e:
        logger.error(f"❌ Fout in get_score_rule_from_db({indicator_name}): {e}", exc_info=True)
        return None
    finally:
        conn.close()


# =========================================================
# ✅ Dynamische scoregenerator (macro, technical, market)
# =========================================================
def generate_scores_db(category: str, data: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    if data is None:
        data = {}
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Geen DB-verbinding in generate_scores_db(auto-mode)")
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
                    data = {normalize_indicator_name(r[0]): float(r[1]) for r in rows if r[1] is not None}

                elif category == "market":
                    cur.execute("""
                        SELECT DISTINCT ON (symbol)
                            price, volume, change_24h
                        FROM market_data
                        WHERE symbol = 'BTC'
                        ORDER BY symbol, timestamp DESC;
                    """)
                    row = cur.fetchone()
                    if row:
                        data = {
                            "price": float(row[0]) if row[0] else None,
                            "volume": float(row[1]) if row[1] else None,
                            "change_24h": float(row[2]) if row[2] else None,
                        }

                else:
                    cur.execute("""
                        SELECT DISTINCT ON (indicator) indicator, value
                        FROM technical_indicators
                        ORDER BY indicator, timestamp DESC;
                    """)
                    rows = cur.fetchall()
                    data = {normalize_indicator_name(r[0]): float(r[1]) for r in rows if r[1] is not None}

        except Exception as e:
            logger.error(f"❌ Fout bij automatisch ophalen data ({category}): {e}", exc_info=True)
        finally:
            conn.close()

    if not data:
        logger.warning(f"⚠️ Geen inputdata gevonden voor categorie {category}")
        return {"scores": {}, "total_score": 0}

    scores = {}
    total_score = 0
    count = 0

    for indicator, value in data.items():
        if value is None:
            continue

        result = get_score_rule_from_db(category, indicator, float(value))
        if not result:
            continue

        score = round(result.get("score", 10))
        scores[indicator] = {
            "value": value,
            "score": score,
            "trend": result.get("trend", "–"),
            "interpretation": result.get("interpretation", ""),
            "action": result.get("action", ""),
        }
        total_score += score
        count += 1

    avg_score = round(total_score / count) if count else 10
    logger.info(f"✅ {count} geldige {category}-indicatoren gescoord (gemiddelde: {avg_score})")

    return {"scores": scores, "total_score": avg_score}


# =========================================================
# ✅ Samengestelde scoreberekening (voor dashboard/rapport)
# =========================================================
def get_scores_for_symbol(include_metadata: bool = False) -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding in get_scores_for_symbol()")
        return {}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON (name) name, value
                FROM macro_data
                ORDER BY name, timestamp DESC;
            """)
            macro_data = {normalize_indicator_name(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}

            cur.execute("""
                SELECT DISTINCT ON (indicator) indicator, value
                FROM technical_indicators
                ORDER BY indicator, timestamp DESC;
            """)
            technical_data = {normalize_indicator_name(r[0]): float(r[1]) for r in cur.fetchall() if r[1] is not None}

        market_scores = generate_scores_db("market")
        macro_scores = generate_scores_db("macro", macro_data)
        tech_scores = generate_scores_db("technical", technical_data)

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
            def extract_top(scores_dict):
                if not scores_dict.get("scores"):
                    return []
                return sorted(scores_dict["scores"].items(), key=lambda x: x[1]["score"], reverse=True)[:3]

            result.update({
                "macro_top_contributors": [i[0] for i in extract_top(macro_scores)],
                "technical_top_contributors": [i[0] for i in extract_top(tech_scores)],
                "market_top_contributors": [i[0] for i in extract_top(market_scores)],
                "macro_interpretation": "Macro-data uit database",
                "technical_interpretation": "Technische data uit database",
                "market_interpretation": "Marktdata uit market_data-tabel (prijs, volume, change_24h)",
            })

        logger.info(f"✅ DB-scores berekend: {result}")
        return result

    except Exception as e:
        logger.error(f"❌ Fout bij get_scores_for_symbol(): {e}", exc_info=True)
        return {}
    finally:
        conn.close()


# =========================================================
# ✅ Compatibiliteit
# =========================================================
def calculate_macro_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("macro", data)

def calculate_technical_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("technical", data)

def calculate_market_scores(data: Dict[str, float]) -> Dict[str, Any]:
    return generate_scores_db("market", data)


# =========================================================
# ✅ Backwards compatibiliteit voor macro_interpreter
# =========================================================
def calculate_score_from_rules(value: float, rules: list[dict]) -> dict:
    """
    Vindt de juiste scoreregel op basis van 'value' en een lijst met regels.
    Retourneert score, trend, interpretatie, actie.
    """
    try:
        for r in rules:
            if r["range_min"] <= value <= r["range_max"]:
                return {
                    "score": r["score"],
                    "trend": r["trend"],
                    "interpretation": r["interpretation"],
                    "action": r["action"],
                }

        # 🟡 Fallback als waarde niet in een bereik valt
        return {
            "score": 50,
            "trend": "Neutraal",
            "interpretation": f"Waarde {value} valt buiten alle gedefinieerde ranges.",
            "action": "Geen directe actie vereist.",
        }

    except Exception as e:
        logger.error(f"❌ Fout in calculate_score_from_rules(): {e}", exc_info=True)
        return {
            "score": 50,
            "trend": "Onbekend",
            "interpretation": "Fout tijdens scoreberekening.",
            "action": "Controleer regels of waarden.",
        }
