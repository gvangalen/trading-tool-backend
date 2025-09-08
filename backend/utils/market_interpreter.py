# ✅ backend/utils/market_interpreter.py

def interpret_market_indicator(name, value, config):
    try:
        thresholds = config.get("thresholds", [])
        explanation = config.get("explanation", "")
        action = config.get("action", "")
        positive = config.get("positive", True)

        # Interpretatie van score (0–100 schaal)
        score = 50
        if thresholds:
            if len(thresholds) == 3:
                low, mid, high = thresholds
                if value <= low:
                    score = 20
                elif value <= mid:
                    score = 50
                elif value <= high:
                    score = 75
                else:
                    score = 90
        if not positive:
            score = 100 - score

        # Advies op basis van score
        if score >= 75:
            advies = "Sterk positief"
        elif score >= 60:
            advies = "Positief"
        elif score >= 40:
            advies = "Neutraal"
        elif score >= 25:
            advies = "Negatief"
        else:
            advies = "Sterk negatief"

        return {
            "name": name,
            "value": value,
            "score": score,
            "advies": advies,
            "explanation": explanation,
            "action": action
        }

    except Exception as e:
        return {
            "name": name,
            "value": value,
            "score": None,
            "advies": f"⚠️ Error: {e}",
            "explanation": "",
            "action": ""
        }
