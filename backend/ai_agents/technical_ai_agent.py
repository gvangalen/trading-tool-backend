import logging
import json

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt
from backend.utils.scoring_utils import normalize_indicator_name
from backend.ai_core.system_prompt_builder import build_system_prompt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ======================================================
# 📊 TECHNICAL AI AGENT — IDENTIEK AAN MACRO (MET 1 FIX)
# ======================================================
def run_technical_agent(user_id: int):
    """
    Genereert technical AI insights voor één user.

    Schrijft:
    - ai_category_insights (technical)
    - ai_reflections (technical)

    ✔ Geen scoring
    ✔ Geen berekeningen
    ✔ Alleen laatste snapshot per indicator
    """

    if user_id is None:
        raise ValueError("❌ Technical AI Agent vereist een user_id")

    logger.info(f"📊 [Technical-Agent] Start — user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding")
        return

    try:
        # =========================================================
        # 1️⃣ TECHNICAL DATA — LAATSTE SNAPSHOT PER INDICATOR
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ti.indicator, ti.value, ti.score, ti.advies, ti.uitleg, ti.timestamp
                FROM technical_indicators ti
                JOIN indicators i
                  ON i.name = ti.indicator
                 AND i.category = 'technical'
                 AND i.active = TRUE
                WHERE ti.user_id = %s
                ORDER BY ti.indicator, ti.timestamp DESC;
            """, (user_id,))
            rows = cur.fetchall()

        if not rows:
            logger.info(f"ℹ️ Geen technische data voor user_id={user_id}")
            return

        latest = {}
        for name, value, score, advies, uitleg, ts in rows:
            key = normalize_indicator_name(name)
            if key not in latest:
                latest[key] = {
                    "indicator": key,
                    "value": float(value) if value is not None else None,
                    "score": float(score) if score is not None else None,
                    "advies": advies or "",
                    "uitleg": uitleg or "",
                    "timestamp": ts.isoformat() if ts else None,
                }

        technical_items = list(latest.values())
        avg_score = round(
            sum(i["score"] for i in technical_items if i["score"] is not None)
            / len(technical_items),
            2
        )

        # =========================================================
        # 2️⃣ AI TECHNICAL ANALYSE
        # =========================================================
        payload = {
            "technical_items": technical_items,
            "avg_score": avg_score,
        }

        technical_task = """
Analyseer de beschikbare technische indicatoren voor Bitcoin.

Belangrijk:
- Gebruik uitsluitend de aangeleverde indicatoren
- Ook bij weinig data moet je een duidelijke analyse geven
- Vermijd lege antwoorden of 'onvoldoende data'

Geef altijd:
- trend (bullish / bearish / neutraal)
- bias (positief / negatief / neutraal)
- risico (laag / gemiddeld / hoog)
- momentum (sterk / neutraal / zwak)
- korte samenvatting (minstens 1 zin)
- belangrijkste technische signalen (minstens 1 punt)

Antwoord uitsluitend in geldige JSON.
"""

        system_prompt = build_system_prompt(
            agent="technical",
            task=technical_task
        )

        raw_ai_context = ask_gpt(
            prompt=json.dumps(payload, ensure_ascii=False, indent=2),
            system_role=system_prompt
        )

        if not isinstance(raw_ai_context, dict):
            raise ValueError("❌ Technical AI response geen geldige JSON")

        # =========================================================
        # 🔧 ENIGE FIX: GENESTE AI-OUTPUT NORMALISEREN
        # =========================================================
        if "technical_analysis" in raw_ai_context:
            ta = raw_ai_context.get("technical_analysis", {})
            indicators = ta.get("indicators", {})

            bullets = []
            bias_votes = []
            risk_votes = []

            for _, d in indicators.items():
                if not isinstance(d, dict):
                    continue

                bullets.append(
                    d.get("uitleg")
                    or d.get("interpretation")
                    or "Technisch signaal actief"
                )

                bias_votes.append(d.get("bias", "").lower())
                risk_votes.append(d.get("risico", "").lower())

            ai_context = {
                "trend": "bearish" if any("bear" in b for b in bias_votes) else "neutraal",
                "bias": "negatief" if any("neg" in b for b in bias_votes) else "neutraal",
                "risk": "hoog" if any("hoog" in r for r in risk_votes) else "gemiddeld",
                "momentum": "zwak",
                "summary": bullets[0] if bullets else "Technische signalen zijn gemengd.",
                "top_signals": bullets[:5],
            }
        else:
            ai_context = {
                "trend": raw_ai_context.get("trend", ""),
                "bias": raw_ai_context.get("bias", ""),
                "risk": raw_ai_context.get("risk") or raw_ai_context.get("risico", ""),
                "momentum": raw_ai_context.get("momentum", ""),
                "summary": raw_ai_context.get("summary") or raw_ai_context.get("samenvatting", ""),
                "top_signals": raw_ai_context.get("top_signals", []),
            }

        # =========================================================
        # 3️⃣ OPSLAAN AI_CATEGORY_INSIGHTS
        # =========================================================
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ai_category_insights
                    (category, user_id, avg_score, trend, bias, risk, summary, top_signals)
                VALUES ('technical', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, category, date)
                DO UPDATE SET
                    avg_score = EXCLUDED.avg_score,
                    trend = EXCLUDED.trend,
                    bias = EXCLUDED.bias,
                    risk = EXCLUDED.risk,
                    summary = EXCLUDED.summary,
                    top_signals = EXCLUDED.top_signals,
                    created_at = NOW();
            """, (
                user_id,
                avg_score,
                ai_context["trend"],
                ai_context["bias"],
                ai_context["risk"],
                ai_context["summary"],
                json.dumps(ai_context["top_signals"]),
            ))

        conn.commit()
        logger.info(f"✅ [Technical-Agent] Voltooid voor user_id={user_id}")

    except Exception:
        conn.rollback()
        logger.error("❌ [Technical-Agent] Fout", exc_info=True)

    finally:
        conn.close()
