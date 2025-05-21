# ✅ strategy_api.py (Fully English Version)

import logging
import json
from flask import Blueprint, request, jsonify
from utils.db import get_db_connection   # correct
from celery.result import AsyncResult
from tasks import generate_strategy_for_setup, generate_strategies_automatically
from celery_worker import celery

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


# ✅ Update strategy
@strategy_api.route('/api/strategies/<int:strategy_id>', methods=['PUT'])
def update_strategy(strategy_id):
    try:
        data = request.get_json()

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategies SET data = %s::jsonb
                WHERE id = %s RETURNING id;
            """, (json.dumps(data), strategy_id))
            updated = cur.fetchone()
            conn.commit()

        if not updated:
            return jsonify({"error": "Strategy not found."}), 404

        return jsonify({"message": f"Strategy {strategy_id} updated."}), 200

    except Exception as e:
        logger.error(f"❌ Error updating strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Delete strategy
@strategy_api.route('/api/strategies/<int:strategy_id>', methods=['DELETE'])
def delete_strategy(strategy_id):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM strategies WHERE id = %s RETURNING id;", (strategy_id,))
            deleted = cur.fetchone()
            conn.commit()

        if not deleted:
            return jsonify({"error": "Strategy not found."}), 404

        return jsonify({"message": f"Strategy {strategy_id} deleted."}), 200

    except Exception as e:
        logger.error(f"❌ Error deleting strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Get strategies
@strategy_api.route('/api/strategies', methods=['GET'])
def get_strategies():
    asset = request.args.get("asset")
    timeframe = request.args.get("timeframe")
    sort = request.args.get("sort", "recent")

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, data, created_at FROM strategies")
            rows = cur.fetchall()

        strategies = []
        for row in rows:
            item = row[1]
            item["id"] = row[0]
            item["created_at"] = str(row[2])

            if asset and item.get("asset") != asset:
                continue
            if timeframe and item.get("timeframe") != timeframe:
                continue

            strategies.append(item)

        if sort == "score":
            strategies.sort(key=lambda x: x.get("score", 0), reverse=True)
        elif sort == "favorite":
            strategies.sort(key=lambda x: x.get("favorite", False), reverse=True)
        else:
            strategies.sort(key=lambda x: x.get("created_at"), reverse=True)

        return jsonify({"strategies": strategies}), 200

    except Exception as e:
        logger.error(f"❌ Error fetching strategies: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Generate strategy for one setup
@strategy_api.route("/api/strategy/generate/<int:setup_id>", methods=["POST"])
def generate_strategy_for_single_setup(setup_id):
    try:
        data = request.get_json() or {}
        overwrite = data.get("overwrite", False)
        task = generate_strategy_for_setup.delay(setup_id, overwrite=overwrite)
        return jsonify({"message": "Strategy generation started", "task_id": task.id}), 202
    except Exception as e:
        logger.error(f"❌ Error generating strategy: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Bulk generate strategies
@strategy_api.route("/api/strategy/generate_all", methods=["POST"])
def generate_all_strategies():
    try:
        task = generate_strategies_automatically.delay()
        return jsonify({"message": "Strategy generation started", "task_id": task.id}), 202
    except Exception as e:
        logger.error(f"❌ Error bulk generating strategies: {e}")
        return jsonify({"error": str(e)}), 500


# ✅ Check Celery task status
@strategy_api.route("/api/task_status/<task_id>", methods=["GET"])
def check_task_status(task_id):
    try:
        result = AsyncResult(task_id, app=celery)
        return jsonify({
            "task_id": task_id,
            "status": result.status,
            "result": result.result if result.successful() else None
        }), 200
    except Exception as e:
        logger.error(f"❌ Error checking task status: {e}")
        return jsonify({"error": str(e)}), 500
