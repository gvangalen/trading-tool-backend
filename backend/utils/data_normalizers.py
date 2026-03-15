import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


# ============================================================
# STRING NORMALIZER
# ============================================================

def normalize_string(value: Any, default: str = "") -> str:
    """
    Zorgt dat een waarde altijd een string wordt.

    Gebruik voor:
    - symbol
    - timeframe
    - explanation
    - tags (single)

    """
    try:
        if value is None:
            return default

        if isinstance(value, str):
            return value.strip()

        return str(value)

    except Exception as e:
        logger.warning(f"[normalize_string] fout: {e}")
        return default


# ============================================================
# NUMBER NORMALIZER
# ============================================================

def normalize_number(value: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Zorgt dat een waarde altijd een float wordt.

    Gebruik voor:
    - entry
    - stop_loss
    - indicator values
    - scores

    """
    try:
        if value is None:
            return default

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):

            v = value.replace(",", "").strip()

            if v == "":
                return default

            return float(v)

        return default

    except Exception as e:
        logger.warning(f"[normalize_number] fout voor waarde {value}: {e}")
        return default


# ============================================================
# ARRAY NORMALIZER
# ============================================================

def normalize_array(value: Any) -> List[Any]:
    """
    Zorgt dat een veld altijd een array/list wordt.

    Input voorbeelden:
    "a,b,c"
    ["a","b","c"]
    None

    Output:
    ["a","b","c"]
    """

    try:

        if value is None:
            return []

        if isinstance(value, list):
            return [v for v in value if v is not None]

        if isinstance(value, str):

            items = [v.strip() for v in value.split(",") if v.strip()]

            return items

        return [value]

    except Exception as e:
        logger.warning(f"[normalize_array] fout: {e}")
        return []


# ============================================================
# TARGET NORMALIZER (TRADING)
# ============================================================

def normalize_targets(value: Any) -> List[float]:
    """
    Zorgt dat strategy targets altijd een float array zijn.

    Ondersteunt:
    "75000,80000,85000"
    [75000,80000]
    [{"price":75000},{"price":80000}]
    """

    try:

        if value is None:
            return []

        targets: List[float] = []

        if isinstance(value, list):

            for v in value:

                if isinstance(v, dict):
                    price = (
                        v.get("price")
                        or v.get("value")
                        or v.get("target")
                    )

                    n = normalize_number(price)

                    if n is not None:
                        targets.append(n)

                else:

                    n = normalize_number(v)

                    if n is not None:
                        targets.append(n)

            return targets

        if isinstance(value, str):

            parts = value.split(",")

            for p in parts:

                n = normalize_number(p)

                if n is not None:
                    targets.append(n)

            return targets

        # fallback single value

        n = normalize_number(value)

        if n is not None:
            return [n]

        return []

    except Exception as e:
        logger.warning(f"[normalize_targets] fout voor value={value}: {e}")
        return []
