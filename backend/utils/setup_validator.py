import logging
import json
import numexpr as ne
from backend.utils.db import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def evaluate_condition(condition, data):
    try:
        return bool(ne.evaluate(condition, local_dict=data))
    except Exception as e:
        logger.error(f"❌ Fout bij evaluatie conditie '{condition}': {e}")
        return False


def validate_setups(asset="BTC"):
    logger.info(f"🔍 Start setup validatie voor {asset}")
    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen databaseverbinding")
        return []

    try:
        with conn.cursor() as cur:
            # Laatste macrodata ophalen
            cur.execute("""
                SELECT DISTINCT ON (name) name, value
                FROM macro_data
                ORDER BY name, timestamp DESC
            """)
            macro_raw = cur.fetchall()
            macro_data = {name: float(value) for name, value in macro_raw if value is not None}

            # Laatste technische data ophalen
            cur.execute("""
                SELECT DISTINCT ON (symbol) symbol, rsi, volume, ma_200
                FROM technical_data
                WHERE symbol = %s
                ORDER BY symbol, timestamp DESC
            """, (asset,))
            technical = cur.fetchone()
            technical_data = {
                "rsi": float(technical[1]) if technical and technical[1] else None,
                "volume": float(technical[2]) if technical and technical[2] else None,
                "ma_200": float(technical[3]) if technical and technical[3] else None,
            }

            # Laatste marketdata ophalen
            cur.execute("""
                SELECT DISTINCT ON (symbol) symbol, price, change_24h
                FROM market_data
                WHERE symbol = %s
                ORDER BY symbol, timestamp DESC
            """, (asset,))
            market = cur.fetchone()
            market_data = {
                "price": float(market[1]) if market and market[1] else None,
                "change_24h": float(market[2]) if market and market[2] else None,
            }

            # Setups ophalen
            cur.execute("""
                SELECT id, name, score_logic
                FROM setups
                WHERE symbol = %s
            """, (asset,))
            setups = cur.fetchall()

        results = []

        for setup_id, name, conditions_json in setups:
            try:
                conditions_dict = json.loads(conditions_json)
            except Exception as e:
                logger.error(f"❌ JSON-fout in setup '{name}': {e}")
                continue

            # Fallback voor ontbrekende categorieën
            for cat in ["macro", "technical", "market"]:
                if cat not in conditions_dict:
                    conditions_dict[cat] = []

            combined_data = {**macro_data, **technical_data, **market_data}
            category_scores = {}
            total_passed = 0
            total_conditions = 0
            failed_conditions = []

            for category, conditions in conditions_dict.items():
                passed = 0
                for cond in conditions:
                    if evaluate_condition(cond, combined_data):
                        passed += 1
                    else:
                        failed_conditions.append(f"{category.upper()}: {cond}")
                score = round((passed / len(conditions)) * 10, 1) if conditions else 0
                category_scores[category] = {"passed": passed, "total": len(conditions), "score": score}
                total_passed += passed
                total_conditions += len(conditions)

            overall_score = round((total_passed / total_conditions) * 10, 1) if total_conditions else 0
            is_active = overall_score >= 7

            results.append({
                "setup_id": setup_id,
                "name": name,
                "asset": asset,
                "active": is_active,
                "score": overall_score,
                "score_breakdown": category_scores,
                "failed_conditions": failed_conditions
            })

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_setup_scores (setup_id, date, score, is_active, explanation)
                    VALUES (%s, CURRENT_DATE, %s, %s, %s)
                    ON CONFLICT (setup_id, date) DO UPDATE
                    SET score = EXCLUDED.score,
                        is_active = EXCLUDED.is_active,
                        explanation = EXCLUDED.explanation
                """, (
                    setup_id,
                    int(overall_score),
                    is_active,
                    json.dumps(failed_conditions)
                ))

        # ✅ Eén gezamenlijke commit
        conn.commit()
        logger.info(f"✅ Setup validatie voltooid voor {asset} ({len(results)} setups)")
        return results

    except Exception as e:
        logger.error(f"❌ Fout bij setup validatie: {e}", exc_info=True)
        return []

    finally:
        conn.close()
