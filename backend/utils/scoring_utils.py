import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional

from backend.utils.db import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent


# ✅ Config loader (met auto-fix voor foutieve stringwaarden)
def load_config(relative_path: str) -> Dict[str, Any]:
    full_path = BASE_DIR / relative_path
    try:
        with open(full_path, "r") as f:
            config = json.load(f)
        logger.info(f"✅ Config loaded from {relative_path}")

        # 🧠 Auto-fix: strings die eigenlijk JSON zijn decoderen
        for k, v in list(config.items()):
            if isinstance(v, str):
                try:
                    config[k] = json.loads(v)
                    logger.warning(f"⚠️ Auto-converted string config for '{k}' in {relative_path}")
                except json.JSONDecodeError:
                    logger.error(f"❌ Kon string config niet converteren voor '{k}': {v}")

        return config

    except Exception as e:
        logger.error(f"❌ Failed to load config ({relative_path}): {e}")
        return {}


# ✅ Score calculator (met None-ondersteuning)
def calculate_score(value: Optional[float], thresholds: list, positive: bool = True) -> Optional[int]:
    if value is None:
        logger.debug("⚠️ Geen waarde ontvangen voor scoreberekening → None")
        return None
    try:
        value = float(value)
    except (ValueError, TypeError):
        logger.warning(f"⚠️ Ongeldige waarde ({value}) → None")
        return None

    if positive:
        if value > thresholds[2]:
            return 2
        elif value > thresholds[1]:
            return 1
        elif value > thresholds[0]:
            return -1
        else:
            return -2
    else:
        if value < thresholds[0]:
            return 2
        elif value < thresholds[1]:
            return 1
        elif value < thresholds[2]:
            return -1
        else:
            return -2


# ✅ Score generator (met string-fix)
def generate_scores(data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    scores = {}
    total = 0
    count = 0

    for name, conf in config.items():
        # 🧩 Fix: als conf een string is, probeer te converteren naar dict
        if isinstance(conf, str):
            logger.warning(f"⚠️ Config voor '{name}' is string i.p.v. dict → converteren...")
            try:
                conf = json.loads(conf)
            except json.JSONDecodeError:
                logger.error(f"❌ Kan string niet decoden voor indicator '{name}': {conf}")
                continue  # skip deze indicator

        if not isinstance(conf, dict):
            logger.error(f"❌ Ongeldige config voor '{name}': verwacht dict maar kreeg {type(conf)}")
            continue

        value = data.get(name)
        thresholds = conf.get("thresholds", [0, 50, 100])
        positive = conf.get("positive", True)

        score = calculate_score(value, thresholds, positive)
        scores[name] = {
            "value": value,
            "score": score,
            "thresholds": thresholds,
            "positive": positive
        }

        if isinstance(score, (int, float)):
            total += score
            count += 1

    avg_score = round(total / count, 2) if count else None
    logger.info(f"✅ {count} geldige indicatoren gescoord (gemiddelde: {avg_score})")
    return {"scores": scores, "total_score": avg_score}


# ✅ Alle actuele scores ophalen (macro / technisch / markt)
def get_scores_for_symbol(symbol: str = "BTC") -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding voor get_scores_for_symbol")
        return {}

    try:
        with conn.cursor() as cur:
            # 1️⃣ Macro ophalen
            cur.execute("""
                SELECT name, value FROM macro_data
                WHERE timestamp = (SELECT MAX(timestamp) FROM macro_data)
            """)
            macro_rows = cur.fetchall()
            macro_data = {name: float(value) for name, value in macro_rows}

            # 2️⃣ Technische indicators ophalen
            cur.execute("""
                SELECT indicator, value FROM technical_indicators
                WHERE symbol = %s AND timestamp = (
                    SELECT MAX(timestamp) FROM technical_indicators WHERE symbol = %s
                )
            """, (symbol, symbol))
            tech_rows = cur.fetchall()
            tech_data = {indicator: float(value) for indicator, value in tech_rows}

            # 3️⃣ Market data ophalen
            cur.execute("""
                SELECT price, volume, change_24h FROM market_data
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
            market_row = cur.fetchone()
            market_data = {}
            if market_row:
                market_data = {
                    "price": float(market_row[0]),
                    "volume": float(market_row[1]),
                    "change_24h": float(market_row[2]),
                }

            # 4️⃣ Configs laden
            macro_conf = load_config("config/macro_indicators_config.json")
            tech_conf = load_config("config/technical_indicators_config.json")
            market_conf = load_config("config/market_data_config.json")

            # 5️⃣ Scoreberekening
            macro_scores = generate_scores(macro_data, macro_conf)
            tech_scores = generate_scores(tech_data, tech_conf)
            market_scores = generate_scores(market_data, market_conf)

            # 6️⃣ Berekeningen met None-handling
            macro_avg = macro_scores["total_score"] or 0
            tech_avg = tech_scores["total_score"] or 0
            market_avg = market_scores["total_score"] or 0
            setup_score = round((macro_avg + tech_avg) / 2, 2) if (macro_avg or tech_avg) else None

            return {
                "macro_score": macro_scores["total_score"],
                "technical_score": tech_scores["total_score"],
                "market_score": market_scores["total_score"],
                "sentiment_score": None,  # nog niet geïmplementeerd
                "setup_score": setup_score
            }

    except Exception as e:
        logger.error(f"❌ Fout bij ophalen en berekenen van scores: {e}", exc_info=True)
        return {}

    finally:
        conn.close()


# ✅ Gecombineerde score ophalen uit setup_scores-tabel (fallback bij rapportage)
def calculate_combined_score(symbol: str = "BTC") -> Dict[str, Any]:
    conn = get_db_connection()
    if not conn:
        logger.error("❌ COMB01: Geen databaseverbinding.")
        return {"symbol": symbol, "error": "Geen databaseverbinding", "total_score": None}

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT macro_score, technical_score, sentiment_score
                FROM setup_scores
                WHERE symbol = %s
                ORDER BY created_at DESC  -- ✅ timestamp vervangen door created_at
                LIMIT 1
            """, (symbol,))
            row = cur.fetchone()

        if not row:
            logger.warning(f"⚠️ COMB02: Geen scoregegevens gevonden voor {symbol}")
            return {"symbol": symbol, "error": "Geen scoregegevens", "total_score": None}

        def safe_float(x):
            try:
                return float(x) if x is not None else None
            except (ValueError, TypeError):
                return None

        macro = safe_float(row[0])
        technical = safe_float(row[1])
        sentiment = safe_float(row[2])

        valid_scores = [s for s in [macro, technical, sentiment] if s is not None]
        total = round(sum(valid_scores) / len(valid_scores), 2) if valid_scores else None

        logger.info(f"✅ COMB04: Totale score voor {symbol} = {total}")
        return {
            "symbol": symbol,
            "macro_score": macro,
            "technical_score": technical,
            "sentiment_score": sentiment,
            "total_score": total
        }

    except Exception as e:
        logger.error(f"❌ COMB05: Fout bij scoreberekening voor {symbol}: {e}", exc_info=True)
        return {"symbol": symbol, "error": str(e), "total_score": None}

    finally:
        conn.close()


# ✅ Testfunctie
def test_scoring_utils():
    test_data = {
        "fear_greed_index": 77,
        "btc_dominance": 52.1,
        "dxy": 104.3,
        "rsi": 63,
        "volume": 8000000000,
        "ma_200": 71000,
        "price": 73000,
        "change_24h": 1.2
    }

    macro_conf = load_config("config/macro_indicators_config.json")
    tech_conf = load_config("config/technical_indicators_config.json")
    market_conf = load_config("config/market_data_config.json")

    macro_scores = generate_scores(test_data, macro_conf)
    tech_scores = generate_scores(test_data, tech_conf)
    market_scores = generate_scores(test_data, market_conf)

    print("\n✅ Macro Scores:\n", json.dumps(macro_scores, indent=2))
    print("\n✅ Technical Scores:\n", json.dumps(tech_scores, indent=2))
    print("\n✅ Market Scores:\n", json.dumps(market_scores, indent=2))
    print("\n✅ Live DB Scores:\n", json.dumps(get_scores_for_symbol("BTC"), indent=2))
    print("\n✅ Combined Score (setup_scores):\n", json.dumps(calculate_combined_score("BTC"), indent=2))


if __name__ == "__main__":
    test_scoring_utils()
