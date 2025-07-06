import logging
import json
import csv
import io
from flask import Blueprint, request, jsonify, Response
from utils.db import get_db_connection

strategy_api = Blueprint("strategy_api", __name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ✅ Strategie opslaan
@strategy_api.route('/strategies', methods=['POST'])
def save_strategy():
    try:
        data = request.get_json()
        strategy_data = {
            "setup_id": data.get("setup_id"),
            "setup_name": data.get("setup_name"),
            "type": data.get("type"),
            "explanation": data.get("explanation"),
            "score": data.get("score"),
            "entry": data.get("entry"),
            "targets": data.get("targets"),
            "stop_loss": data.get("stop_loss"),
            "risk_reward": data.get("risk_reward"),
            "asset": data.get("asset"),
            "timeframe": data.get("timeframe"),
            "tags": data.get("tags", []),
            "favorite": data.get("favorite", False),
            "origin": data.get("origin", "Manual"),
            "ai_reason": data.get("ai_reason", "")
        }

        required_fields = ["setup_id", "setup_name", "asset", "timeframe", "entry", "targets", "stop_loss"]
        for field in required_fields:
            if not strategy_data.get(field):
                return jsonify({"error": f"'{field}' is required."}), 400

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM strategies
                WHERE data->>'setup_id' = %s;
            """, (str(strategy_data["setup_id"]),))
            if cur.fetchone():
                return jsonify({"error": "Strategy already exists"}), 409

        keywords = ["breakout", "scalp", "swing", "reversal"]
        found_tags = [k for k in keywords if k in (strategy_data["setup_name"] + strategy_data["explanation"]).lower()]
        strategy_data["tags"] = list(set(strategy_data["tags"] + found_tags))

        with conn.cursor() as cur:
            cur.execute("INSERT INTO strategies (data, created_at) VALUES (%s::jsonb, NOW()) RETURNING id;", (json.dumps(strategy_data),))
            strategy_id = cur.fetchone()[0]
            conn.commit()

        return jsonify({"message": "Strategy saved", "id": strategy_id}), 201
    except Exception as e:
        logger.error(f"❌ Error saving strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Strategie bijwerken
@strategy_api.route('/strategies/<int:strategy_id>', methods=['PUT'])
def update_strategy(strategy_id):
    try:
        data = request.get_json()
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Strategy not found"}), 404

            strategy_data = row[0]
            strategy_data.update(data)

            cur.execute("UPDATE strategies SET data = %s WHERE id = %s", (json.dumps(strategy_data), strategy_id))
            conn.commit()
        return jsonify({"message": "Strategy updated"}), 200
    except Exception as e:
        logger.error(f"❌ Error updating strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Strategie verwijderen
@strategy_api.route('/strategies/<int:strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM strategies WHERE id = %s", (strategy_id,))
            conn.commit()
        return jsonify({"message": "Strategy deleted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ✅ Toggle favorite
@strategy_api.route('/strategies/<int:strategy_id>/favorite', methods=['PATCH'])
def toggle_favorite(strategy_id):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "Strategy not found"}), 404

            strategy = row[0]
            strategy["favorite"] = not strategy.get("favorite", False)

            cur.execute("UPDATE strategies SET data = %s WHERE id = %s", (json.dumps(strategy), strategy_id))
            conn.commit()
        return jsonify({"message": "Favorite toggled", "favorite": strategy["favorite"]}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ✅ Strategie filteren
@strategy_api.route('/strategies/filter', methods=['GET'])
def filter_strategies():
    try:
        asset = request.args.get("asset")
        timeframe = request.args.get("timeframe")
        tag = request.args.get("tag")
        min_score = request.args.get("min_score", type=float)

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM strategies")
            rows = cur.fetchall()

        filtered = []
        for row in rows:
            id_, strategy = row
            if asset and strategy.get("asset") != asset:
                continue
            if timeframe and strategy.get("timeframe") != timeframe:
                continue
            if tag and tag not in strategy.get("tags", []):
                continue
            if min_score is not None and (float(strategy.get("score", 0)) < min_score):
                continue
            strategy["id"] = id_
            filtered.append(strategy)

        return jsonify(filtered), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ✅ AI-strategie genereren (logica apart)
def generate_strategy_logic(setup_id, conn, overwrite=True):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM setups WHERE id = %s", (setup_id,))
            row = cur.fetchone()
            if not row:
                return {"error": "Setup not found"}, 404

            setup_id, setup = row
            asset = setup.get("asset")
            timeframe = setup.get("timeframe")
            setup_name = setup.get("name")

            strategy = {
                "setup_id": setup_id,
                "setup_name": setup_name,
                "asset": asset,
                "timeframe": timeframe,
                "type": "AI-Generated",
                "explanation": f"Strategie gegenereerd op basis van setup '{setup_name}'",
                "ai_reason": "Op basis van technische en macrodata is deze strategie voorgesteld",
                "entry": "100.00",
                "targets": ["110.00", "120.00"],
                "stop_loss": "95.00",
                "risk_reward": "2.0",
                "score": 7.5,
                "tags": ["ai", "auto"],
                "favorite": False,
                "origin": "AI"
            }

            cur.execute("SELECT id FROM strategies WHERE data->>'setup_id' = %s", (str(setup_id),))
            existing = cur.fetchone()

            if existing:
                if overwrite:
                    cur.execute("UPDATE strategies SET data = %s WHERE id = %s", (json.dumps(strategy), existing[0]))
                    conn.commit()
                    return {"message": "Strategy updated", "id": existing[0]}, 200
                else:
                    return {"error": "Strategy already exists", "id": existing[0]}, 409

            cur.execute("INSERT INTO strategies (data, created_at) VALUES (%s::jsonb, NOW()) RETURNING id;",
                        (json.dumps(strategy),))
            strategy_id = cur.fetchone()[0]
            conn.commit()
            return {"message": "Strategy generated", "id": strategy_id}, 201
    except Exception as e:
        return {"error": str(e)}, 500


# ✅ Genereer AI-strategie voor 1 setup
@strategy_api.route('/strategies/generate/<int:setup_id>', methods=['POST'])
def generate_strategy_for_setup(setup_id):
    conn = None
    try:
        overwrite = request.json.get('overwrite', True)
        conn = get_db_connection()
        result, status = generate_strategy_logic(setup_id, conn, overwrite)
        return jsonify(result), status
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


# ✅ Genereer AI-strategieën voor alle setups
@strategy_api.route('/strategies/generate_all', methods=['POST'])
def generate_all_strategies():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM setups")
            setup_ids = [row[0] for row in cur.fetchall()]

        results = []
        for setup_id in setup_ids:
            result, _ = generate_strategy_logic(setup_id, conn, overwrite=True)
            results.append(result)

        return jsonify({"results": results}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ✅ CSV export (met extra kolommen)
@strategy_api.route('/strategies/export', methods=['GET'])
def export_strategies():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data, created_at FROM strategies")
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Asset", "Timeframe", "Setup", "Score", "Entry", "Stop Loss", "Origin", "Created"])

        for row in rows:
            s = row[1]
            writer.writerow([
                row[0],
                s.get("asset"),
                s.get("timeframe"),
                s.get("setup_name"),
                s.get("score"),
                s.get("entry"),
                s.get("stop_loss"),
                s.get("origin"),
                row[2].strftime("%Y-%m-%d %H:%M:%S")
            ])

        output.seek(0)
        return Response(output.getvalue(), mimetype="text/csv", headers={
            "Content-Disposition": "attachment; filename=strategies.csv"
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
