import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional

from backend.utils.db import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent


# =========================================================
# ✅ Config Loader
# =========================================================
def load_config(relative_path: str) -> Dict[str, Any]:
    full_path = BASE_DIR / relative_path
    try:
        with open(full_path, "r") as f:
            config = json.load(f)
        logger.info(f"✅ Config loaded from {relative_path}")
        return config
    except Exception as e:
        logger.error(f"❌ Failed to load config ({relative_path}): {e}")
        return {}


# =========================================================
# ✅ Basis scorefunctie
# =========================================================
def calculate_score(value: Optional[float], thresholds: list, positive: bool = True) -> Optional[int]:
    """➤ Basis scorefunctie met minimale waarde van 10 (nooit 0)."""
    if value is None:
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None

    if len(thresholds) != 3:
        thresholds = [0, 50, 100]

    if positive:
        if value >= thresholds[2]:
            return 100
        elif value >= thresholds[1]:
            return 75
        elif value >= thresholds[0]:
            return 50
        else:
            return 25
    else:
        if value <= thresholds[0]:
            return 100
        elif value <= thresholds[1]:
            return 75
        elif value <= thresholds[2]:
            return 50
        else:
            return 25


# =========================================================
# ✅ Score + interpretatie per indicator
# =========================================================
def calculate_score_from_config(value: float, config: dict) -> dict:
    scoring = config.get("scoring", {})
    positive = config.get("positive", True)
    fallback = {
        "score": 10,
        "trend": "Onbekend",
        "interpretation": "Geen geldige waarde ontvangen.",
        "action": config.get("action", "")
    }

    if not scoring or value is None:
        return fallback

    try:
        matched_score = None
        for range_key, details in scoring.items():
            if "+" in range_key:
                lower = float(range_key.replace("+", ""))
                if value >= lower:
                    matched_score = details
            elif "-" in range_key:
                parts = range_key.split("-")
                if len(parts) == 2:
                    lower = float(parts[0])
                    upper = float(parts[1])
                    if lower <= value < upper:
                        matched_score = details
            if matched_score:
                break

        if not matched_score:
            return fallback

        # ➖ Keer score om als 'positive' False is
        if not positive and "score" in matched_score:
            score = matched_score["score"]
            if isinstance(score, (int, float)):
                matched_score = matched_score.copy()
                matched_score["score"] = 100 - score + 10  # spiegeling met minimum van 10
                matched_score["score"] = max(10, min(100, matched_score["score"]))  # clamp 10–100

        return matched_score

    except Exception as e:
        logger.warning(f"⚠️ Fout bij score interpretatie: {e}")
        return fallback

# =========================================================
# ✅ Universele scoregenerator met afronding
# =========================================================
def generate_scores(data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    scores = {}
    total = 0
    count = 0

    data = {k.lower(): v for k, v in data.items()}
    config = {k.lower(): v for k, v in config.items()}

    for name, conf in config.items():
        value = data.get(name)
        result = calculate_score_from_config(value, conf)

        score = result["score"]
        if isinstance(score, (int, float)):
            score = round(score)

        scores[name] = {
            "value": value,
            "score": score,
            "trend": result["trend"],
            "interpretation": result["interpretation"],
            "action": result["action"],
            "thresholds": conf.get("thresholds"),
            "positive": conf.get("positive", True)
        }

        if isinstance(score, (int, float)):
            total += score
            count += 1

    avg_score = round(total / count) if count else 10
    logger.info(f"✅ {count} geldige indicatoren gescoord (gemiddelde: {avg_score})")
    return {"scores": scores, "total_score": avg_score}


# =========================================================
# ✅ Per-type scoregenerator
# =========================================================
def calculate_macro_scores(data: Dict[str, float], config: Dict[str, Any]) -> Dict[str, Any]:
    return generate_scores(data, config)

def calculate_technical_scores(data: Dict[str, float], config: Dict[str, Any]) -> Dict[str, Any]:
    return generate_scores(data, config)

def calculate_market_scores(data: Dict[str, float], config: Dict[str, Any]) -> Dict[str, Any]:
    return generate_scores(data, config)


# =========================================================
# ✅ Setup Matching (ongewijzigd)
# =========================================================
def match_setups_to_score(setups: list, total_score: float) -> list:
    return [s for s in setups if s.get("min_score", 0) <= total_score <= s.get("max_score", 100)]

def find_best_matching_setup(setups: list, total_score: float) -> Optional[dict]:
    best_setup = None
    smallest_diff = float("inf")
    for setup in setups:
        min_score = setup.get("min_score", 0)
        max_score = setup.get("max_score", 100)
        if min_score <= total_score <= max_score:
            center = (min_score + max_score) / 2
            diff = abs(center - total_score)
            if diff < smallest_diff:
                smallest_diff = diff
                best_setup = setup
    return best_setup


# =========================================================
# ✅ Scores ophalen uit database + totaal berekenen
# =========================================================
def get_scores_for_symbol(include_metadata: bool = False) -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding voor get_scores_for_symbol")
        return {}

    try:
        with conn.cursor() as cur:
            # === Macro data ===
            cur.execute("""
                SELECT name, value FROM macro_data
                WHERE timestamp = (SELECT MAX(timestamp) FROM macro_data)
            """)
            macro_rows = cur.fetchall()
            macro_data = {name: float(value) for name, value in macro_rows if value is not None}

            # === Technische data ===
            cur.execute("""
                SELECT DISTINCT ON (indicator) indicator, value
                FROM technical_indicators
                ORDER BY indicator, timestamp DESC
            """)
            tech_rows = cur.fetchall()
            tech_data = {}
            for indicator, value in tech_rows:
                if value is None:
                    logger.warning(f"⚠️ Indicator '{indicator}' heeft geen waarde (None) — wordt overgeslagen")
                    continue
                try:
                    tech_data[indicator.lower()] = float(value)
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️ Ongeldige waarde voor '{indicator}': {value} ({e})")

            # === Marktdata ===
            cur.execute("""
                SELECT price, volume, change_24h FROM market_data
                ORDER BY timestamp DESC LIMIT 1
            """)
            market_row = cur.fetchone()
            market_data = {}
            if market_row:
                market_data = {
                    "price": float(market_row[0] or 0),
                    "volume": float(market_row[1] or 0),
                    "change_24h": float(market_row[2] or 0)
                }

            # === Configs laden ===
            macro_conf_full = load_config("config/macro_indicators_config.json")
            tech_conf = load_config("config/technical_indicators_config.json")
            market_conf_full = load_config("config/market_indicators_config.json")

            macro_conf = macro_conf_full.get("indicators", {})
            market_conf = market_conf_full.get("indicators", {})

            # === Scores berekenen ===
            macro_scores = calculate_macro_scores(macro_data, macro_conf)
            tech_scores = calculate_technical_scores(tech_data, tech_conf.get("indicators", {}))
            market_scores = calculate_market_scores(market_data, market_conf)

            macro_avg = round(macro_scores["total_score"])
            tech_avg = round(tech_scores["total_score"])
            market_avg = round(market_scores["total_score"])
            setup_score = round((macro_avg + tech_avg) / 2)

            result = {
                "macro_score": macro_avg,
                "technical_score": tech_avg,
                "market_score": market_avg,
                "setup_score": setup_score
            }

            # ➕ Voeg uitleg en top_contributors toe als include_metadata=True
            if include_metadata:
                def extract_top_contributors(scores_dict):
                    return sorted(
                        scores_dict["scores"].items(),
                        key=lambda x: x[1]["score"],
                        reverse=True
                    )[:3]

                result.update({
                    "macro_interpretation": macro_conf_full.get("interpretation", "–"),
                    "technical_interpretation": tech_conf.get("interpretation", "–"),
                    "setup_interpretation": "Gebaseerd op macro + technische score gemiddelde",

                    "macro_top_contributors": [i[0] for i in extract_top_contributors(macro_scores)],
                    "technical_top_contributors": [i[0] for i in extract_top_contributors(tech_scores)],
                    "setup_top_contributors": [],
                })

            logger.info(f"✅ Scores berekend: {result}")
            return result

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en berekenen van scores: {e}", exc_info=True)
        return {}
    finally:
        conn.close()


# =========================================================
# ✅ Dashboard Score Berekening (ook afgerond)
# =========================================================
def get_dashboard_scores(macro_data, technical_data, setups):
    macro_scores = [d["score"] for d in macro_data if isinstance(d.get("score"), (int, float))]
    macro_score = round(sum(macro_scores) / len(macro_scores)) if macro_scores else 10

    used_scores = [v["score"] for v in technical_data.values()]
    total_possible = len(used_scores) * 100
    technical_score = round((sum(used_scores) / total_possible) * 100) if total_possible else 10

    setup_score = round(len(setups) * 10) if setups else 10

    return {
        "macro": macro_score,
        "technical": technical_score,
        "setup": setup_score
    }


# =========================================================
# ✅ Actieve setups ophalen met uitleg en actie
# =========================================================
def get_active_setups_with_info(conn):
    """
    Haalt actieve setups op uit daily_setup_scores met hun uitleg en actie.
    Retourneert lijst van dicts met volledige setup-info.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.name, s.explanation, s.action, s.dynamic_investment, ds.score
                FROM daily_setup_scores ds
                JOIN setups s ON s.id = ds.setup_id
                WHERE ds.is_active = true
                ORDER BY ds.score DESC;
            """)
            rows = cur.fetchall()

            return [
                {
                    "name": row[0],
                    "explanation": row[1],
                    "action": row[2],
                    "dynamic_investment": row[3],
                    "score": row[4],
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"❌ Fout bij ophalen actieve setups: {e}")
        return []

def save_setup_score(setup_id: int, score: int, explanation: str = ""):
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding in save_setup_score")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_setup_scores (setup_id, date, score, explanation)
                VALUES (%s, CURRENT_DATE, %s, %s)
                ON CONFLICT (setup_id, date) DO NOTHING;
            """, (setup_id, score, explanation))
            conn.commit()
            logger.info(f"✅ Setup-score opgeslagen voor setup_id={setup_id}")
    except Exception as e:
        logger.error(f"❌ Fout bij opslaan van setup-score: {e}", exc_info=True)
    finally:
        conn.close()
