import logging
import json
import csv
import io
from flask import Blueprint, request, jsonify, Response
from utils.db import get_db_connection

strategy_api = Blueprint("strategy_api", __name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ✅ Strategy opslaan
@strategy_api.route('/strategies', methods=['POST'])
def save_strategy():
    try:
        data = request.get_json()
        strategy_data = {
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

        required_fields = ["setup_name", "asset", "timeframe", "entry", "targets", "stop_loss"]
        for field in required_fields:
            if not strategy_data.get(field):
                return jsonify({"error": f"'{field}' is required."}), 400

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM strategies
                WHERE data->>'setup_name' = %s AND data->>'asset' = %s AND data->>'timeframe' = %s;
            """, (strategy_data["setup_name"], strategy_data["asset"], strategy_data["timeframe"]))
            if cur.fetchone():
                return jsonify({"error": "Strategy with this setup/asset/timeframe already exists."}), 409

        keywords = ["breakout", "scalp", "swing", "reversal"]
        found_tags = [k for k in keywords if k in (strategy_data["setup_name"] + strategy_data["explanation"]).lower()]
        strategy_data["tags"] = list(set(strategy_data["tags"] + found_tags))

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO strategies (data, created_at)
                VALUES (%s::jsonb, NOW()) RETURNING id;
            """, (json.dumps(strategy_data),))
            strategy_id = cur.fetchone()[0]
            conn.commit()

        logger.info(f"✅ Strategy saved (ID: {strategy_id})")
        return jsonify({"message": "Strategy saved successfully", "id": strategy_id}), 201

    except Exception as e:
        logger.error(f"❌ Error saving strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Strategy bijwerken
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

        return jsonify({"message": "Strategy updated successfully"}), 200
    except Exception as e:
        logger.error(f"❌ Error updating strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Strategy verwijderen
@strategy_api.route('/strategies/<int:strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM strategies WHERE id = %s", (strategy_id,))
            conn.commit()
        return jsonify({"message": "Strategy deleted successfully"}), 200
    except Exception as e:
        logger.error(f"❌ Error deleting strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Favorite toggle
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
        logger.error(f"❌ Error toggling favorite: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Filterstrategieën
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
            if min_score and (float(strategy.get("score", 0)) < min_score):
                continue
            strategy["id"] = id_
            filtered.append(strategy)

        return jsonify(filtered), 200
    except Exception as e:
        logger.error(f"❌ Error filtering strategies: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Strategy-uitleg ophalen
@strategy_api.route('/strategies/<int:strategy_id>/explanation', methods=['GET'])
def get_explanation(strategy_id):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies WHERE id = %s", (strategy_id,))
            row = cur.fetchone()

        if not row:
            return jsonify({"error": "Strategy not found"}), 404

        data = row[0]
        return jsonify({
            "setup_name": data.get("setup_name"),
            "explanation": data.get("explanation"),
            "ai_reason": data.get("ai_reason")
        }), 200

    except Exception as e:
        logger.error(f"❌ Error getting explanation: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Summary endpoint
@strategy_api.route('/strategies/summary', methods=['GET'])
def strategy_summary():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies")
            rows = cur.fetchall()

        summary = {"total": 0, "assets": {}}
        for row in rows:
            data = row[0]
            asset = data.get("asset")
            score = data.get("score", 0)
            favorite = data.get("favorite", False)

            summary["total"] += 1
            if asset not in summary["assets"]:
                summary["assets"][asset] = {"count": 0, "score_sum": 0, "favorites": 0}

            summary["assets"][asset]["count"] += 1
            summary["assets"][asset]["score_sum"] += score
            if favorite:
                summary["assets"][asset]["favorites"] += 1

        for asset in summary["assets"]:
            count = summary["assets"][asset]["count"]
            score_sum = summary["assets"][asset]["score_sum"]
            summary["assets"][asset]["avg_score"] = round(score_sum / count, 2) if count else 0

        return jsonify(summary), 200

    except Exception as e:
        logger.error(f"❌ Error in strategy summary: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Scorematrix (asset × timeframe)
@strategy_api.route('/strategies/score_matrix', methods=['GET'])
def score_matrix():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM strategies")
            rows = cur.fetchall()

        matrix = {}
        for row in rows:
            data = row[0]
            asset = data.get("asset")
            timeframe = data.get("timeframe")
            score = data.get("score", 0)

            if asset not in matrix:
                matrix[asset] = {}
            if timeframe not in matrix[asset]:
                matrix[asset][timeframe] = []

            matrix[asset][timeframe].append(score)

        for asset in matrix:
            for timeframe in matrix[asset]:
                scores = matrix[asset][timeframe]
                matrix[asset][timeframe] = round(sum(scores) / len(scores), 2) if scores else 0

        return jsonify(matrix), 200

    except Exception as e:
        logger.error(f"❌ Error in score matrix: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ CSV export
@strategy_api.route('/strategies/export', methods=['GET'])
def export_strategies():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data, created_at FROM strategies")
            rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        header = ["ID", "Asset", "Timeframe", "Setup Name", "Score", "Favorite", "Created At"]
        writer.writerow(header)

        for row in rows:
            strategy = row[1]
            writer.writerow([
                row[0],
                strategy.get("asset"),
                strategy.get("timeframe"),
                strategy.get("setup_name"),
                strategy.get("score"),
                strategy.get("favorite"),
                row[2].strftime("%Y-%m-%d %H:%M:%S")
            ])

        output.seek(0)
        return Response(output.getvalue(), mimetype="text/csv", headers={
            "Content-Disposition": "attachment; filename=strategies_export.csv"
        })

    except Exception as e:
        logger.error(f"❌ Error exporting strategies: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Genereer AI-strategie voor 1 setup
@strategy_api.route('/strategy/generate/<int:setup_id>', methods=['POST'])
def generate_strategy_for_setup(setup_id):
    try:
        overwrite = request.json.get('overwrite', True)
        conn = get_db_connection()

        with conn.cursor() as cur:
            cur.execute("SELECT id, data FROM setups WHERE id = %s", (setup_id,))
            row = cur.fetchone()

        if not row:
            return jsonify({"error": "Setup not found"}), 404

        setup_id, setup = row
        asset = setup.get("asset")
        timeframe = setup.get("timeframe")
        setup_name = setup.get("name")

        strategy = {
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

        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM strategies
                WHERE data->>'setup_name' = %s AND data->>'asset' = %s AND data->>'timeframe' = %s;
            """, (setup_name, asset, timeframe))
            existing = cur.fetchone()

        if existing:
            if overwrite:
                with conn.cursor() as cur:
                    cur.execute("UPDATE strategies SET data = %s WHERE id = %s",
                                (json.dumps(strategy), existing[0]))
                    conn.commit()
                return jsonify({"message": "Strategy updated", "id": existing[0]}), 200
            else:
                return jsonify({"error": "Strategy already exists", "id": existing[0]}), 409

        with conn.cursor() as cur:
            cur.execute("INSERT INTO strategies (data, created_at) VALUES (%s::jsonb, NOW()) RETURNING id;",
                        (json.dumps(strategy),))
            strategy_id = cur.fetchone()[0]
            conn.commit()

        return jsonify({"message": "Strategy generated", "id": strategy_id}), 201

    except Exception as e:
        logger.error(f"❌ Error generating strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Genereer AI-strategieën voor alle setups
@strategy_api.route('/strategy/generate_all', methods=['POST'])
def generate_all_strategies():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM setups")
            setup_ids = [row[0] for row in cur.fetchall()]

        results = []
        for setup_id in setup_ids:
            with strategy_api.test_request_context(json={"overwrite": True}):
                response = generate_strategy_for_setup(setup_id)
                results.append(response.get_json())

        return jsonify({"results": results}), 200

    except Exception as e:
        logger.error(f"❌ Error generating all strategies: {e}")
        return jsonify({"error": str(e)}), 500
