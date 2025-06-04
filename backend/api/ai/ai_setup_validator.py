def is_valid_setup(setup: dict) -> bool:
    """
    Evalueert of een setup voldoende data bevat om AI te gebruiken.
    """

    required_fields = ["name", "symbol", "trend", "timeframe", "indicators"]
    for field in required_fields:
        if not setup.get(field):
            return False

    if not isinstance(setup.get("macro_score"), (int, float)):
        return False
    if not isinstance(setup.get("technical_score"), (int, float)):
        return False
    if not isinstance(setup.get("sentiment_score"), (int, float)):
        return False

    return True
