"""
Central AI system prompt builder.

Zorgt voor:
- Consistente AI-rol
- Geen dubbele identity/style
- Multi-agent schaalbaarheid
- Automatische style selectie
"""

from backend.ai_core.ai_identity import AI_IDENTITY
from backend.ai_core.ai_constraints import AI_CONSTRAINTS
from backend.ai_core.ai_style import AI_STYLE
from backend.ai_core.ai_style_report import AI_STYLE_REPORT


# =====================================================
# 🔥 STYLE MAPPING (SUPER BELANGRIJK)
# =====================================================
STYLE_MAP = {
    # alle report agents
    "report": AI_STYLE_REPORT,

    # future proof:
    # "strategy": AI_STYLE_STRATEGY,
    # "bot": AI_STYLE_BOT,
}


# =====================================================
# 🧠 SYSTEM PROMPT BUILDER
# =====================================================
def build_system_prompt(
    task: str,
    agent: str = "general",
    style_override: str | None = None,
) -> str:
    """
    Bouwt een complete system prompt voor OpenAI.
    """

    if not task or not isinstance(task, str):
        raise ValueError("AI task description is verplicht en moet een string zijn.")

    # normalize (voorkomt bugs)
    agent = agent.lower()

    # -------------------------------------------------
    # 🔥 STYLE SELECTIE (SCHAALBAAR)
    # -------------------------------------------------
    if isinstance(style_override, str) and style_override.strip():

        style_block = style_override.strip()

    else:
        style_block = next(
            (
                style
                for key, style in STYLE_MAP.items()
                if key in agent   # <<< MAGIC LINE
            ),
            AI_STYLE,  # fallback
        ).strip()

    # -------------------------------------------------
    # 🧱 PROMPT BUILD
    # -------------------------------------------------
    prompt = f"""
{AI_IDENTITY.strip()}

{AI_CONSTRAINTS.strip()}

{style_block}

------------------------------
AGENT TYPE
------------------------------
{agent.upper()}

------------------------------
SPECIFIEKE TAAK
------------------------------
{task.strip()}
""".strip()

    return prompt
