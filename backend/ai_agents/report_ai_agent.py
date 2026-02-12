import logging
import json
import re
from difflib import SequenceMatcher
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt_text
from backend.ai_core.system_prompt_builder import build_system_prompt


# =====================================================
# Logging
# =====================================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =====================================================
# REPORT AGENT ROLE
# =====================================================
REPORT_TASK = """
Je bent een senior Bitcoin market strategist.

Je schrijft GEEN daily snapshot.
Je UPDATE een bestaand marktregime.

PRIMAIRE TAAK:
- Detecteer het huidige marktregime
- Bepaal of het regime intact blijft, verdiept of kantelt
- Analyseer veranderingen binnen dat kader
- Bouw voort op het vorige rapport

HERSTART HET MARKTVERHAAL NOOIT.

Markten bewegen in regimes — niet per dag.

DENKKADER (intern toepassen, niet benoemen):
CURRENT_REGIME
REGIME_DIRECTION
REGIME_STRENGTH
RISK_ENVIRONMENT

FOCUS:
- regime continuïteit
- structureel vs reactief
- signaalconvergentie
- positioneel risico

Schrijf alsof een portfolio manager dit leest om exposure te bepalen.

GEEN:
- storytelling
- educatie
- indicator uitleg
- opsommingen
- herhaling van data
- prijslevels (behalve spot)

Elke sectie moet voortbouwen op dezelfde centrale markthypothese.
Nooit opnieuw definiëren.
"""

# =====================================================
# Helpers
# =====================================================
SYSTEM_PROMPT = build_system_prompt(
    agent="report",
    task=REPORT_TASK,
)


def to_float(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except Exception:
        return None


def _flatten_text(obj) -> List[str]:
    out: List[str] = []
    if obj is None:
        return out

    if isinstance(obj, str):
        t = obj.strip()
        if t:
            out.append(t)
        return out

    if isinstance(obj, dict):
        for v in obj.values():
            out.extend(_flatten_text(v))
        return out

    if isinstance(obj, list):
        for v in obj:
            out.extend(_flatten_text(v))
        return out

    return out

def get_regime_memory(user_id: int):

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT regime_label, confidence, signals_json, narrative
                FROM regime_memory
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 1;
            """, (user_id,))

            row = cur.fetchone()

            if not row:
                return None

            return {
                "label": row[0],
                "confidence": float(row[1]) if row[1] else None,
                "signals": row[2],
                "narrative": row[3],
            }

    finally:
        conn.close()


def _safe_json(obj):
    """
    JSON-safe serialization (Decimal/date/datetime-safe).
    """
    from datetime import date, datetime

    if obj is None:
        return None
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_json(v) for v in obj]
    return obj


# =====================================================
# Delta helpers (today vs previous report_date)
# =====================================================


def _get_latest_market_row_for_date(cur, report_date) -> Optional[Tuple]:
    """
    Haal de meest recente market_data snapshot voor een bepaalde datum.
    Verwacht: (price, change_24h, volume)
    """
    cur.execute(
        """
        SELECT price, change_24h, volume
        FROM market_data
        WHERE DATE(timestamp) = %s
        ORDER BY timestamp DESC
        LIMIT 1;
        """,
        (report_date,),
    )
    return cur.fetchone()


def get_daily_deltas(user_id: int) -> Dict[str, Any]:
    """
    Berekent veranderingen t.o.v. de vorige beschikbare report_date.
    Deze deltas zijn analytische brandstof en reduceren herhaling,
    omdat elke sectie begint vanuit verandering (of juist het uitblijven daarvan).

    Output keys:
    - macro_delta, technical_delta, market_delta, setup_delta
    - price_delta, change_delta, volume_delta
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Pak laatste 2 dagen scores (beschikbaar voor user)
            cur.execute(
                """
                SELECT report_date, macro_score, technical_score, market_score, setup_score
                FROM daily_scores
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 2;
                """,
                (user_id,),
            )
            rows = cur.fetchall()

            if not rows or len(rows) < 2:
                return {}

            today_date, today_macro, today_tech, today_market, today_setup = rows[0]
            prev_date, prev_macro, prev_tech, prev_market, prev_setup = rows[1]

            # Markt snapshots per datum (laatste snapshot die dag)
            today_m = _get_latest_market_row_for_date(cur, today_date)
            prev_m = _get_latest_market_row_for_date(cur, prev_date)

            # Deltas scores
            macro_delta = to_float(today_macro) - to_float(prev_macro) if (today_macro is not None and prev_macro is not None) else None
            technical_delta = to_float(today_tech) - to_float(prev_tech) if (today_tech is not None and prev_tech is not None) else None
            market_delta = to_float(today_market) - to_float(prev_market) if (today_market is not None and prev_market is not None) else None
            setup_delta = to_float(today_setup) - to_float(prev_setup) if (today_setup is not None and prev_setup is not None) else None

            # Deltas market (price/change/volume)
            price_delta = None
            change_delta = None
            volume_delta = None

            if today_m and prev_m:
                t_price, t_change, t_vol = today_m
                p_price, p_change, p_vol = prev_m

                if t_price is not None and p_price is not None:
                    price_delta = to_float(t_price) - to_float(p_price)
                if t_change is not None and p_change is not None:
                    change_delta = to_float(t_change) - to_float(p_change)
                if t_vol is not None and p_vol is not None:
                    volume_delta = to_float(t_vol) - to_float(p_vol)

            return {
                "macro_delta": macro_delta,
                "technical_delta": technical_delta,
                "market_delta": market_delta,
                "setup_delta": setup_delta,
                "price_delta": price_delta,
                "change_delta": change_delta,
                "volume_delta": volume_delta,
                "today_date": today_date.isoformat() if today_date else None,
                "prev_date": prev_date.isoformat() if prev_date else None,
            }
    finally:
        conn.close()


# =====================================================
# BOT DAILY SNAPSHOT (BACKEND = TRUTH)
# =====================================================
def get_bot_daily_snapshot(user_id: int) -> Dict[str, Any]:
    """
    Leest de botbeslissing van vandaag.

    CONTRACT (frontend + report + pdf):
    {
      bot_name: str,
      action: "buy" | "sell" | "hold",
      confidence: float | str | None,
      amount_eur: float | None,
      setup_match: str | None,
      reason: str | None
    }

    BELANGRIJK:
    - Deze functie retourneert ALTIJD een dict
    - HOLD is een geldige, bewuste beslissing
    - setup_match is ALTIJD string of None (NOOIT object)
    """

    conn = get_db_connection()
    if not conn:
        return {
            "bot_name": "Bot",
            "action": "hold",
            "confidence": None,
            "amount_eur": None,
            "setup_match": None,
            "reason": "Geen databaseverbinding — bot snapshot niet beschikbaar.",
        }

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  b.name AS bot_name,
                  d.action,
                  d.confidence,
                  d.amount_eur,
                  d.scores_json,
                  d.reason_json
                FROM bot_decisions d
                JOIN bot_configs b ON b.id = d.bot_id
                WHERE d.user_id = %s
                  AND d.decision_date = CURRENT_DATE
                ORDER BY d.updated_at DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            row = cur.fetchone()

        # ─────────────────────────────────────────────
        # Geen bot decision vandaag → expliciete HOLD
        # ─────────────────────────────────────────────
        if not row:
            return {
                "bot_name": "Bot",
                "action": "hold",
                "confidence": None,
                "amount_eur": None,
                "setup_match": None,
                "reason": "Geen botbeslissing vandaag — drempels of voorwaarden niet gehaald.",
            }

        bot_name, action, confidence, amount_eur, scores_json, reason_json = row

        # ─────────────────────────────────────────────
        # Action normaliseren
        # ─────────────────────────────────────────────
        normalized_action = (action or "hold").lower()
        if normalized_action not in ("buy", "sell", "hold"):
            normalized_action = "hold"

        # ─────────────────────────────────────────────
        # scores_json veilig parsen
        # ─────────────────────────────────────────────
        if scores_json is None:
            scores_json = {}
        elif isinstance(scores_json, str):
            try:
                scores_json = json.loads(scores_json)
            except Exception:
                scores_json = {}

        # ─────────────────────────────────────────────
        # setup_match NORMALISEREN → STRING
        # ─────────────────────────────────────────────
        raw_match = scores_json.get("setup_match")
        setup_match = None

        if isinstance(raw_match, dict):
            setup_match = raw_match.get("name") or raw_match.get("label") or raw_match.get("id")
        elif isinstance(raw_match, list):
            setup_match = ", ".join(
                str(x.get("name") if isinstance(x, dict) and x.get("name") else x) for x in raw_match
            )
        elif isinstance(raw_match, (str, int, float)):
            setup_match = str(raw_match)

        # ─────────────────────────────────────────────
        # reason_json → nette tekst
        # ─────────────────────────────────────────────
        reason_text = None

        if reason_json is not None:
            if isinstance(reason_json, str):
                try:
                    parsed = json.loads(reason_json)
                    reason_json = parsed
                except Exception:
                    reason_text = reason_json

            if reason_text is None:
                if isinstance(reason_json, list):
                    reason_text = "; ".join(str(x) for x in reason_json if str(x).strip())
                elif isinstance(reason_json, dict):
                    if "reason" in reason_json:
                        reason_text = str(reason_json["reason"])
                    elif "reasons" in reason_json and isinstance(reason_json["reasons"], list):
                        reason_text = "; ".join(str(x) for x in reason_json["reasons"] if str(x).strip())
                    else:
                        reason_text = str(reason_json)

        if normalized_action == "hold" and not reason_text:
            reason_text = "Geen trade: voorwaarden of risicodrempels niet gehaald."

        # ─────────────────────────────────────────────
        # amount / confidence veilig
        # ─────────────────────────────────────────────
        amount_val = None
        try:
            if amount_eur is not None:
                amount_val = float(amount_eur)
        except Exception:
            amount_val = None

        conf_val = confidence
        try:
            if isinstance(confidence, str) and confidence.strip().replace(".", "", 1).isdigit():
                conf_val = float(confidence)
        except Exception:
            conf_val = confidence

        return {
            "bot_name": bot_name or "Bot",
            "action": normalized_action,
            "confidence": conf_val,
            "amount_eur": amount_val,
            "setup_match": setup_match,
            "reason": reason_text,
        }

    finally:
        conn.close()


# =====================================================
# Text generation (AI) + defensive parsing
# =====================================================
def generate_text(prompt: str, fallback: str) -> str:
    """
    Verantwoordelijk voor:
    - AI-call
    - opschonen output
    - JSON-defensieve parsing
    GEEN deduplicatie (doen we hogerop)
    """

    # 🔥 PRO safeguard — voorkomt silent context explosions
    if len(prompt) > 12000:
        logger.warning("⚠️ Large AI prompt detected (%s chars)", len(prompt))

    raw = ask_gpt_text(prompt, system_role=SYSTEM_PROMPT)

    if not raw:
        logger.warning("⚠️ AI gaf lege response — fallback gebruikt.")
        return fallback

    # 1️⃣ Strip code fences / markdown
    text = raw.replace("```json", "").replace("```", "").strip()

    # 2️⃣ Normale tekst → direct terug
    if not text.lstrip().startswith("{"):
        return text if len(text) > 5 else fallback

    # 3️⃣ Defensieve JSON-parse (voor wanneer model zich niet aan instructies houdt)
    try:
        parsed = json.loads(text)
        parts = _flatten_text(parsed)

        blacklist = {
            "GO",
            "NO-GO",
            "STATUS",
            "RISICO",
            "IMPACT",
            "ACTIE",
            "ONVOLDOENDE DATA",
            "CONDITIONAL",
        }

        cleaned = [
            p for p in parts
            if p.strip() and p.strip().upper() not in blacklist
        ]

        if cleaned:
            return "\n\n".join(cleaned)

        if parts:
            return "\n\n".join(parts)

    except Exception as e:
        logger.warning("⚠️ JSON parsing mislukt — ruwe tekst gebruikt. Error=%s", e)

    return text if len(text) > 5 else fallback


# =====================================================
# Repetition control (cross-section deduplication)
# =====================================================
def _normalize_sentence(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)
    return s


def _is_too_similar(a: str, b: str, threshold: float = 0.82) -> bool:
    return SequenceMatcher(None, a, b).ratio() >= threshold


def reduce_repetition(text: str, seen: List[str]) -> str:
    """
    Verwijdert zinnen die semantisch te sterk lijken
    op eerder geschreven zinnen in andere secties.
    """
    sentences = re.split(r"(?<=[.!?])\s+", text)
    output: List[str] = []

    for s in sentences:
        norm = _normalize_sentence(s)

        if not norm or len(norm) < 20:
            continue

        if any(_is_too_similar(norm, prev) for prev in seen):
            continue

        output.append(s)
        seen.append(norm)

    return " ".join(output)


# =====================================================
# SCORES & MARKET
# =====================================================


def get_daily_scores(user_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT macro_score, technical_score, market_score, setup_score
                FROM daily_scores
                WHERE user_id = %s
                ORDER BY report_date DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            row = cur.fetchone()

        return {
            "macro_score": to_float(row[0]) if row else None,
            "technical_score": to_float(row[1]) if row else None,
            "market_score": to_float(row[2]) if row else None,
            "setup_score": to_float(row[3]) if row else None,
        }
    finally:
        conn.close()


def get_market_snapshot() -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT price, change_24h, volume
                FROM market_data
                ORDER BY timestamp DESC
                LIMIT 1;
                """
            )
            row = cur.fetchone()

        return {
            "price": to_float(row[0]) if row else None,
            "change_24h": to_float(row[1]) if row else None,
            "volume": to_float(row[2]) if row else None,
        }
    finally:
        conn.close()


def _indicator_list(cur, sql, user_id):
    cur.execute(sql, (user_id,))
    rows = cur.fetchall()
    return [
        {
            "indicator": r[0],
            "value": to_float(r[1]),
            "score": to_float(r[2]),
            "interpretation": r[3],
        }
        for r in rows
    ]


# =====================================================
# INDICATOR HIGHLIGHTS (UNIFORM STRUCTUUR – GEEN DUPLICATEN)
# =====================================================
def get_market_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            return _indicator_list(
                cur,
                """
                SELECT DISTINCT ON (name)
                    name,
                    value,
                    score,
                    interpretation
                FROM market_data_indicators
                WHERE user_id = %s
                  AND score IS NOT NULL
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY name, timestamp DESC
                LIMIT 5;
                """,
                user_id,
            )
    finally:
        conn.close()


def get_macro_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            return _indicator_list(
                cur,
                """
                SELECT DISTINCT ON (name)
                    name,
                    value,
                    score,
                    COALESCE(interpretation, action)
                FROM macro_data
                WHERE user_id = %s
                  AND score IS NOT NULL
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY name, timestamp DESC
                LIMIT 5;
                """,
                user_id,
            )
    finally:
        conn.close()


def get_technical_indicator_highlights(user_id: int) -> List[dict]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            return _indicator_list(
                cur,
                """
                SELECT DISTINCT ON (indicator)
                    indicator,
                    value,
                    score,
                    COALESCE(uitleg, advies)
                FROM technical_indicators
                WHERE user_id = %s
                  AND score IS NOT NULL
                  AND DATE(timestamp) = CURRENT_DATE
                ORDER BY indicator, timestamp DESC
                LIMIT 5;
                """,
                user_id,
            )
    finally:
        conn.close()


# =====================================================
# SETUP SNAPSHOT
# =====================================================
def get_setup_snapshot(user_id: int) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.id, s.name, s.symbol, s.timeframe, d.score
                FROM daily_setup_scores d
                JOIN setups s ON s.id = d.setup_id
                WHERE d.user_id = %s
                ORDER BY d.report_date DESC, d.is_best DESC, d.score DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            best = cur.fetchone()

            cur.execute(
                """
                SELECT s.id, s.name, d.score
                FROM daily_setup_scores d
                JOIN setups s ON s.id = d.setup_id
                WHERE d.user_id = %s
                ORDER BY d.report_date DESC, d.score DESC
                LIMIT 5;
                """,
                (user_id,),
            )
            rows = cur.fetchall()

        if not best:
            return {}

        return {
            "best_setup": {
                "id": best[0],
                "name": best[1],
                "symbol": best[2],
                "timeframe": best[3],
                "score": to_float(best[4]),
            },
            "top_setups": [{"id": r[0], "name": r[1], "score": to_float(r[2])} for r in rows],
        }
    finally:
        conn.close()


# =====================================================
# STRATEGY SNAPSHOT
# =====================================================
def get_active_strategy_snapshot(user_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.name, s.symbol, s.timeframe,
                    a.entry, a.targets, a.stop_loss,
                    a.adjustment_reason, a.confidence_score
                FROM active_strategy_snapshot a
                JOIN setups s ON s.id = a.setup_id
                WHERE a.user_id = %s
                ORDER BY a.snapshot_date DESC, a.created_at DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            row = cur.fetchone()

        if not row:
            return None

        return {
            "setup_name": row[0],
            "symbol": row[1],
            "timeframe": row[2],
            "entry": to_float(row[3]),
            "targets": row[4],
            "stop_loss": to_float(row[5]),
            "adjustment_reason": row[6],
            "confidence_score": to_float(row[7]),
        }
    finally:
        conn.close()


# =====================================================
# PROMPTS (REPORT AGENT 2.0 — SAMENHANG & VERKLARING)
# =====================================================
def p_exec() -> str:
    return """
Formuleer één centrale markthypothese voor vandaag.

Verplicht:
- Begin met wat er veranderde t.o.v. de vorige dag (of benoem expliciet dat het beeld gelijk bleef)
- Geef de belangrijkste oorzaak/driver die uit de data volgt
- Maak duidelijk of dit een structurele verschuiving lijkt of een reactieve beweging

Schrijf als één analytisch openingsverhaal.
Geen opsommingen, geen labels, geen herhaling van dezelfde zinstructuren.
""".strip()


def p_market() -> str:
    return """
Analyseer de marktbeweging van vandaag.

Verplicht:
- Start met de verandering t.o.v. gisteren (prijs/volume/market score)
- Verklaar waarom de market score bewoog of juist niet
- Leg uit wat volume zegt over de kwaliteit van de beweging
- Eindig met een kort oordeel over duurzaamheid (zonder prijsniveaus)

Geen lijstjes, geen herhaling van cijfers zonder causaliteit.
""".strip()


def p_macro() -> str:
    return """
Analyseer de macro-omgeving van vandaag.

Verplicht:
- Benoem welke macro-krachten dominant bleven en wat dat betekent voor het speelveld
- Leg uit waarom macro-indicatoren meebewegen of juist NIET meebewegen met de koers
- Maak de spanning concreet tussen veiligheid (Bitcoin) en risicobereidheid (market context)

Geen macro-boekjesuitleg, alleen interpretatie van de aangeleverde data.
""".strip()


def p_technical() -> str:
    return """
Analyseer de technische structuur van vandaag.

Verplicht:
- Leg uit of techniek bevestigt, achterblijft of tegenwerkt t.o.v. de beweging
- Noem welke signalen betrouwbaarheid ONDERMIJNEN of juist VERSTERKEN (alleen uit data)
- Beschrijf of dit herstel, consolidatie of ruis is, en waarom

Geen klassieke TA-uitleg, geen indicator-definities, geen prijsniveaus.
""".strip()


def p_setup(best_setup: Optional[Dict[str, Any]]) -> str:
    if not best_setup:
        return """
Er is vandaag geen setup die voldoende aansluit bij de huidige marktomstandigheden.

Verplicht:
- Leg uit waarom setups nu niet passen (koppel aan scorecombinatie + indicatorcontext)
- Benoem wat er in de data zou moeten veranderen voordat setups weer logisch worden

Geen aannames buiten de data.
""".strip()

    return f"""
De best scorende setup vandaag is "{best_setup.get('name')}" op timeframe {best_setup.get('timeframe')} (score {best_setup.get('score')}).

Verplicht:
- Verklaar waarom deze setup relatief beter scoort dan de rest (koppel aan actuele context)
- Beoordeel of de omstandigheden deze setup ondersteunen of slechts tolereren
- Maak duidelijk of dit iets is om actief te gebruiken of vooral te monitoren (zonder trade-instructies)

Geen herhaling van de setup-naam in elke zin.
""".strip()


def p_strategy(active_strategy: Optional[Dict[str, Any]]) -> str:
    if not active_strategy:
        return """
Er is momenteel geen actieve strategie.

Verplicht:
- Leg uit waarom de huidige scorecombinatie geen strategie rechtvaardigt
- Benoem welke voorwaarden in de data eerst moeten verbeteren/verslechteren voordat een strategie logisch wordt

Geen hypothetische trades, geen prijsniveaus.
""".strip()

    return """
Er is een actieve strategie aanwezig.

Verplicht:
- Plaats de strategie in de huidige macro-, market- en technische context
- Benoem de belangrijkste aannames die vandaag waar moeten blijven
- Beoordeel of de strategie robuust blijft of fragieler wordt (zonder aanpassingen voor te schrijven)

Geen herhaling van entries/targets/stop; die staan elders.
""".strip()


def p_bot_strategy(bot_snapshot: Dict[str, Any]) -> str:
    if bot_snapshot.get("action") == "hold":
        return """
De bot heeft vandaag bewust geen trade geplaatst.

Verplicht:
- Leg uit welke voorwaarden/drempels uit de botdata onvoldoende waren
- Plaats dit in de bredere context: waarom terughoudendheid vandaag logisch was
- Benoem wat er in de data moet veranderen voordat actie logisch wordt (algemeen, niet als tradeplan)

Gebruik uitsluitend de aangeleverde botdata. Geen aannames. Geen nieuwe beslissingen.
""".strip()

    return """
Er is vandaag een botbeslissing genomen.

BELANGRIJK:
- De feitelijke botactie, confidence en bedragen worden elders getoond
- Herhaal of parafraseer deze NIET

Verplicht:
- Geef context waarom de beslissing logisch is binnen de scorecombinatie
- Benadruk discipline/drempels als reden, niet emotie of aannames
- Koppel aan marktkader (zonder prijsniveaus)

Gebruik uitsluitend de aangeleverde botdata. Geen aannames. Geen nieuwe beslissingen.
""".strip()


def p_outlook() -> str:
    return """
Schrijf een scenario-vooruitblik voor de komende 24-48 uur.

Verplicht:
- Benoem welke factoren bevestiging vereisen
- Benoem welke signalen een regime-shift zouden aangeven
- Houd het conditioneel (als/dan), zonder prijsniveaus

Geen opsommingen, één doorlopend stuk tekst.
""".strip()


# =====================================================
# MAIN BUILDER — REPORT AGENT 2.0 (SAFE + CONTEXT-AWARE)
# =====================================================
from backend.ai_core.regime_memory import get_regime_memory
from backend.ai_core.transition_detector import compute_transition_detector


def generate_daily_report_sections(user_id: int) -> Dict[str, Any]:

    # -------------------------------------------------
    # 1) Basis data
    # -------------------------------------------------
    scores = get_daily_scores(user_id)
    market = get_market_snapshot()

    market_ind = get_market_indicator_highlights(user_id)
    macro_ind = get_macro_indicator_highlights(user_id)
    tech_ind = get_technical_indicator_highlights(user_id)

    setup_snapshot = get_setup_snapshot(user_id)
    best_setup = setup_snapshot.get("best_setup")
    active_strategy = get_active_strategy_snapshot(user_id)
    bot_snapshot = get_bot_daily_snapshot(user_id)

    deltas = get_daily_deltas(user_id)

    # -------------------------------------------------
    # 🔥 NEW — REGIME + TRANSITION
    # -------------------------------------------------
    regime = get_regime_memory(user_id)

    transition = None
    if regime and regime.get("signals"):
        transition = regime["signals"].get("transition")

    # fallback (should rarely happen)
    if not transition:
        transition = compute_transition_detector(user_id)

    # -------------------------------------------------
    # 2) Extra context
    # -------------------------------------------------
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT report_date, executive_summary, market_analysis,
                       macro_context, technical_analysis,
                       setup_validation, strategy_implication, outlook
                FROM daily_reports
                WHERE user_id = %s
                  AND report_date < CURRENT_DATE
                ORDER BY report_date DESC
                LIMIT 1;
                """,
                (user_id,),
            )
            prev_report = cur.fetchone()

            cur.execute(
                """
                SELECT category, avg_score, trend, bias, risk, summary
                FROM ai_category_insights
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 5;
                """,
                (user_id,),
            )
            ai_insights = cur.fetchall()

            cur.execute(
                """
                SELECT category, indicator, ai_score, comment, recommendation
                FROM ai_reflections
                WHERE user_id = %s
                ORDER BY date DESC
                LIMIT 5;
                """,
                (user_id,),
            )
            ai_reflections = cur.fetchall()

    finally:
        conn.close()

    # -------------------------------------------------
    # 🔥 MEGA CONTEXT — THIS IS THE BRAIN
    # -------------------------------------------------
    context_blob = f"""
YOU ARE NOT WRITING A DAILY REPORT.
YOU ARE UPDATING A LIVE MARKET MODEL.

Markets move in regimes.
Transitions matter more than daily moves.

==================================================
PRIMARY REGIME
==================================================

{json.dumps(_safe_json(regime), ensure_ascii=False)}

Do NOT redefine this unless transition risk forces you.

==================================================
TRANSITION LAYER — HIGHEST SHORT TERM SIGNAL
==================================================

Transition risk: {transition.get("transition_risk")}
Primary flag: {transition.get("primary_flag")}
Narrative: {transition.get("narrative")}

Signals:
{json.dumps(_safe_json(transition.get("signals")), ensure_ascii=False)}

INTERPRETATION RULES:

If transition_risk >= 70:
→ Write like late-cycle risk is building.

If transition_risk >= 60:
→ Emphasize fragility even if price is stable.

If transition_risk <= 40:
→ Treat the regime as structurally supported.

Never ignore this layer.

==================================================
PREVIOUS REPORT — CONTINUITY ENGINE
==================================================

{json.dumps(_safe_json(prev_report), ensure_ascii=False)}

You are CONTINUING this document.

NOT rewriting it.

==================================================
WHAT CHANGED
==================================================

Macro delta: {deltas.get("macro_delta")}
Market delta: {deltas.get("market_delta")}
Technical delta: {deltas.get("technical_delta")}
Setup delta: {deltas.get("setup_delta")}

Price delta: {deltas.get("price_delta")}
Volume delta: {deltas.get("volume_delta")}

Interpret changes ONLY through the regime + transition lens.

==================================================
LIVE STATE
==================================================

Price: {market.get("price")}
24h change: {market.get("change_24h")}
Volume: {market.get("volume")}

Scores:

Macro: {scores.get("macro_score")}
Market: {scores.get("market_score")}
Technical: {scores.get("technical_score")}
Setup: {scores.get("setup_score")}

==================================================
INDICATOR CLUSTERS
==================================================

MARKET:
{json.dumps(_safe_json(market_ind), ensure_ascii=False)}

MACRO:
{json.dumps(_safe_json(macro_ind), ensure_ascii=False)}

TECHNICAL:
{json.dumps(_safe_json(tech_ind), ensure_ascii=False)}

==================================================
POSITIONING CONTEXT
==================================================

Best setup:
{json.dumps(_safe_json(best_setup), ensure_ascii=False)}

Active strategy:
{json.dumps(_safe_json(active_strategy), ensure_ascii=False)}

Bot decision:
{json.dumps(_safe_json(bot_snapshot), ensure_ascii=False)}

==================================================
AI META
==================================================

Insights:
{json.dumps(_safe_json(ai_insights), ensure_ascii=False)}

Reflections:
{json.dumps(_safe_json(ai_reflections), ensure_ascii=False)}

==================================================
COGNITIVE FRAMEWORK
==================================================

Internally determine:

CURRENT_REGIME  
REGIME_DIRECTION  
REGIME_MATURITY  
TRANSITION_RISK  
PARTICIPATION_QUALITY  
RISK_ASYMMETRY  

Write ALL sections from ONE hypothesis.

Every paragraph must feel like a continuation.

Never reset the story.

==================================================
HARD RULES
==================================================

- Do NOT summarize data
- Do NOT repeat the regime
- Do NOT narrate
- Avoid identical sentence openings
- Focus on positioning risk

Price levels forbidden except spot.
"""

    # -------------------------------------------------
    # 4) Generate
    # -------------------------------------------------
    seen_sentences: List[str] = []

    executive_summary = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_exec(), "Regime intact."),
        seen_sentences,
    )

    market_analysis = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_market(), "Market steady."),
        seen_sentences,
    )

    macro_context = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_macro(), "Macro unchanged."),
        seen_sentences,
    )

    technical_analysis = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_technical(), "Technicals neutral."),
        seen_sentences,
    )

    setup_validation = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_setup(best_setup), "Setups selective."),
        seen_sentences,
    )

    strategy_implication = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_strategy(active_strategy), "Strategy stable."),
        seen_sentences,
    )

    bot_strategy = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_bot_strategy(bot_snapshot), "Bot inactive."),
        seen_sentences,
    )

    outlook = reduce_repetition(
        generate_text(context_blob + "\n\n" + p_outlook(), "Await confirmation."),
        seen_sentences,
    )

    # -------------------------------------------------
    # RESULT
    # -------------------------------------------------
    result = {
        "executive_summary": executive_summary,
        "market_analysis": market_analysis,
        "macro_context": macro_context,
        "technical_analysis": technical_analysis,
        "setup_validation": setup_validation,
        "strategy_implication": strategy_implication,
        "bot_strategy": bot_strategy,
        "bot_snapshot": bot_snapshot,
        "outlook": outlook,
        "price": market.get("price"),
        "change_24h": market.get("change_24h"),
        "volume": market.get("volume"),
        "macro_score": scores.get("macro_score"),
        "technical_score": scores.get("technical_score"),
        "market_score": scores.get("market_score"),
        "setup_score": scores.get("setup_score"),
        "market_indicator_highlights": market_ind,
        "macro_indicator_highlights": macro_ind,
        "technical_indicator_highlights": tech_ind,
        "best_setup": best_setup,
        "top_setups": setup_snapshot.get("top_setups", []),
        "active_strategy": active_strategy,
        "deltas": deltas,
        "transition": transition,   # 🔥 NEW (great for debugging & UI later)
    }

    logger.info("✅ Report agent WITH TRANSITION OK")
    return result
