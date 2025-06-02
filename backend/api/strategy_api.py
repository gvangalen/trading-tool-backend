# ✅ strategy_api.py — Extended version with summary, score matrix, explanation and export

import logging
import json
from flask import Blueprint, request, jsonify, Response
from utils.db import get_db_connection
from celery.result import AsyncResult
from tasks import generate_strategy_for_setup, generate_strategies_automatically
from celery_worker import celery
import csv
import io

strategy_api = Blueprint("strategy_api", __name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Save strategy
@strategy_api.route('/api/strategies', methods=['POST'])
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


# ✅ Update, Delete, Get (same as before, skipped here for brevity)

# ✅ Strategy summary endpoint
@strategy_api.route('/api/strategies/summary', methods=['GET'])
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


# ✅ Score matrix (asset × timeframe)
@strategy_api.route('/api/strategies/score_matrix', methods=['GET'])
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


# ✅ Explanation endpoint
@strategy_api.route('/api/strategies/<int:strategy_id>/explanation', methods=['GET'])
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


# ✅ Export strategies as CSV
@strategy_api.route('/api/strategies/export', methods=['GET'])
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
