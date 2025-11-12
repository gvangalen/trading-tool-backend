import json
import logging
import traceback
from datetime import datetime, timedelta

from celery import shared_task

from backend.utils.db import get_db_connection
from backend.utils.openai_client import ask_gpt

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CATEGORIES = ["macro", "market", "technical", "setup", "strategy"]

# -----------------------------
# Helpers
# -----------------------------
def _safe_json(obj, fallback="{}"):
    try:
        return json.loads(obj) if isinstance(obj, str) else obj
    except Exception:
        return json.loads(fallback)

def _fetch_today_insights(conn):
    """
    Haalt de AI-category-insights van vandaag op voor alle CATEGORIES.
    Valt desnoods terug op laatste 2 dagen voor ontbrekende categorieën.
    """
    insights = {}
    today = datetime.utcnow().date()
    lookback = [today, today - timedelta(days=1), today - timedelta(days=2)]

    with conn.cursor() as cur:
        for cat in CATEGORIES:
            found = None
            for d in lookback:
                cur.execute("""
                    SELECT category, COALESCE(avg_score, 0) AS avg_score, trend, bias, risk, summary, top_signals
                    FROM ai_category_insights
                    WHERE category = %s AND date = %s
                    LIMIT 1;
                """, (cat, d))
                row = cur.fetchone()
                if row:
                    found = {
                        "category": row[0],
                        "avg_score": int(row[1]) if row[1] is not None else None,
                        "trend": row[2],
                        "bias": row[3],
                        "risk": row[4],
                        "summary": row[5] or "",
                        "top_signals": _safe_json(row[6] or "[]", fallback="[]"),
                        "date": str(d),
                    }
                    break
            if found:
                insights[cat] = found

    return insights

def _fetch_numeric_scores(conn):
    """
    Haalt ruwe (DB) scores op voor vergelijking/alignment.
    - daily_scores: macro_score, market_score, technical_score, setup_score (laatste)
    - ai_reflections: avg(ai_score), avg(compliance) per categorie
    """
    numeric = {
        "daily_scores": {},
        "ai_reflections": {},
    }

    with conn.cursor() as cur:
        # daily_scores (laatste record)
        cur.execute("""
            SELECT macro_score, market_score, technical_score, setup_score
            FROM daily_scores
            ORDER BY report_date DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        if row:
            numeric["daily_scores"] = {
                "macro": row[0],
                "market": row[1],
                "technical": row[2],
                "setup": row[3],
                # strategy heeft geen ruwe score in daily_scores
            }

        # ai_reflections gem. ai_score & compliance per category voor vandaag
        cur.execute("""
            SELECT category,
                   ROUND(AVG(COALESCE(ai_score, 0))::numeric, 1) AS avg_ai_score,
                   ROUND(AVG(COALESCE(compliance, 0))::numeric, 1) AS avg_compliance
            FROM ai_reflections
            WHERE date = CURRENT_DATE
            GROUP BY category;
        """)
        rows = cur.fetchall() or []
        for r in rows:
            numeric["ai_reflections"][r[0]] = {
                "avg_ai_score": float(r[1]) if r[1] is not None else None,
                "avg_compliance": float(r[2]) if r[2] is not None else None,
            }

    return numeric

def _build_ai_prompt(insights, numeric):
    """
    Bouwt een compact maar rijk prompt voor de Score-Orchestrator.
    """
    def fmt_ins(cat):
        i = insights.get(cat)
        if not i: return f"[{cat.upper()}] – geen data"
        sigs = i.get("top_signals", [])
        sig_txt = ", ".join(sigs) if isinstance(sigs, list) else str(sigs)
        return (
            f"[{cat.upper()}] score≈{i.get('avg_score')} | trend={i.get('trend')} | bias={i.get('bias')} | risk={i.get('risk')}\n"
            f"  summary: {i.get('summary')}\n"
            f"  signals: {sig_txt}"
        )

    blocks = [fmt_ins(cat) for cat in CATEGORIES]

    # numeric block
    ds = numeric.get("daily_scores", {})
    refs = numeric.get("ai_reflections", {})
    ref_lines = []
    for cat in CATEGORIES:
        r = refs.get(cat, {})
        ref_lines.append(
            f"{cat}: ai_score={r.get('avg_ai_score','–')}, compliance={r.get('avg_compliance','–')}"
        )
    numeric_block = (
        "Raw numeric context:\n"
        f"- daily_scores: {ds}\n"
        f"- ai_reflections: {', '.join(ref_lines)}"
    )

    return f"""
Jij bent een **orchestrator AI** voor trading. Je combineert meerdere domein-agents
(macro, market, technical, setup, strategy) tot één coherent oordeel.
Je mag **geen regels in de brondata wijzigen**; je reflecteert, weegt en controleert consistentie.

Hieronder de samenvattingen per domein:
{chr(10).join(blocks)}

{numeric_block}

Geef je antwoord **uitsluitend** als JSON (geen extra tekst) met de volgende structuur:

{{
  "master_trend": "bullish|bearish|neutraal",
  "master_bias": "risk-on|risk-off|gemengd",
  "master_risk": "laag|gemiddeld|hoog",
  "master_score": 72,               // 0-100, afgerond op gehele
  "weights": {{
    "macro": 0.25, "market": 0.25, "technical": 0.25, "setup": 0.15, "strategy": 0.10
  }},
  "alignment_score": 0-100,         // hoe consistent zijn de domeinen met elkaar?
  "data_warnings": [ "string", ... ], // afwijkingen, ontbrekende categorieën, sterke conflicts
  "summary": "max 3 zinnen met overkoepelende interpretatie.",
  "outlook": "korte verwachting (1 zin).",
  "domains": {{
    "macro":    {{ "score": <int|null>, "trend": "...", "bias": "...", "risk": "..." }},
    "market":   {{ "score": <int|null>, "trend": "...", "bias": "...", "risk": "..." }},
    "technical":{{ "score": <int|null>, "trend": "...", "bias": "...", "risk": "..." }},
    "setup":    {{ "score": <int|null>, "trend": "...", "bias": "...", "risk": "..." }},
    "strategy": {{ "score": <int|null>, "trend": "...", "bias": "...", "risk": "..." }}
  }}
}}

Regels:
- Weeg domeinen **data-gedreven**: verhoog gewicht voor domeinen met hoge compliance/kwaliteit; verlaag bij missende of inconsistente data.
- master_score is **niet** simpel gemiddelde; het is jouw gewogen, consistente oordeel.
- Vul ontbrekende domeinen netjes met null/\"–\" en voeg uitleg toe in data_warnings.
- Houd het antwoord strikt aan bovenstaande JSON-structuur (sleutels incl. volgorde zijn niet kritisch, maar namen wel).
"""

def _store_master_result(conn, ai_json):
    """
    Slaat de master-score op in ai_category_insights (category='score').
    Extra metadata (weights, alignment_score, data_warnings, domains, outlook)
    gaat in 'top_signals' als JSON.
    """
    # parse & normalize
    try:
        data = json.loads(ai_json) if isinstance(ai_json, str) else ai_json
    except Exception:
        logger.warning("⚠️ AI-response kon niet als JSON gelezen worden. Sla ruwe tekst op in summary.")
        data = {
            "master_trend": None,
            "master_bias": None,
            "master_risk": None,
            "master_score": None,
            "summary": str(ai_json)[:500],
            "outlook": None,
            "weights": {},
            "alignment_score": None,
            "data_warnings": [],
            "domains": {},
        }

    master_score   = data.get("master_score")
    master_trend   = data.get("master_trend")
    master_bias    = data.get("master_bias")
    master_risk    = data.get("master_risk")
    summary        = data.get("summary") or ""
    outlook        = data.get("outlook")
    weights        = data.get("weights", {})
    alignment      = data.get("alignment_score")
    warnings       = data.get("data_warnings", [])
    domains        = data.get("domains", {})

    # We bundelen extra velden in top_signals JSON (zodat we geen migratie nodig hebben)
    meta = {
        "outlook": outlook,
        "weights": weights,
        "alignment_score": alignment,
        "data_warnings": warnings,
        "domains": domains,
    }

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ai_category_insights
                (category, avg_score, trend, bias, risk, summary, top_signals)
            VALUES ('score', %s, %s, %s, %s, %s, %s)
            ON CONFLICT (category, date) DO UPDATE SET
                avg_score = EXCLUDED.avg_score,
                trend = EXCLUDED.trend,
                bias = EXCLUDED.bias,
                risk = EXCLUDED.risk,
                summary = EXCLUDED.summary,
                top_signals = EXCLUDED.top_signals,
                created_at = NOW();
        """, (
            int(round(master_score)) if isinstance(master_score, (int, float)) else None,
            master_trend,
            master_bias,
            master_risk,
            summary,
            json.dumps(meta, ensure_ascii=False),
        ))

# -----------------------------
# Celery Task
# -----------------------------
@shared_task(name="backend.ai_agents.score_ai_agent.generate_master_score")
def generate_master_score():
    """
    Combineert macro/market/technical/setup/strategy insights + numerieke context,
    laat AI gewogen oordeel en alignment bepalen, en slaat resultaat op als
    category='score' in ai_category_insights.
    """
    logger.info("🧮 Start Score AI Agent (Master Orchestrator)...")

    conn = get_db_connection()
    if not conn:
        logger.error("❌ Geen DB-verbinding.")
        return

    try:
        insights = _fetch_today_insights(conn)
        if not insights:
            logger.warning("⚠️ Geen category insights gevonden (macro/market/technical/setup/strategy).")
        numeric = _fetch_numeric_scores(conn)

        prompt = _build_ai_prompt(insights, numeric)
        ai_response = ask_gpt(prompt)

        if not ai_response:
            logger.warning("⚠️ Geen AI-response ontvangen van orchestrator.")
            return

        logger.info("🧠 Master Orchestrator antwoord ontvangen; opslaan...")
        _store_master_result(conn, ai_response)
        conn.commit()
        logger.info("✅ AI Master Score opgeslagen (ai_category_insights, category='score').")

    except Exception:
        logger.error("❌ Fout in Score AI Agent:")
        logger.error(traceback.format_exc())
    finally:
        conn.close()
