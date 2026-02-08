# backend/services/execution_service.py

from backend.engine.decision_engine import decide_amount


def compute_execution_amount(setup: dict, scores: dict) -> float:
    """
    Enige plek waar investeringsbedrag wordt bepaald.
    """

    amount = decide_amount(setup, scores)

    # Optionele pauze-condities
    pause = setup.get("pause_conditions")
    if pause:
        for key, condition in pause.items():
            score = scores.get(key)
            if score is None:
                continue

            if "gt" in condition and score > condition["gt"]:
                return 0.0

            if "lt" in condition and score < condition["lt"]:
                return 0.0

    return amount
