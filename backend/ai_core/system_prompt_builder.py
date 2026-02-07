"""
Central AI system prompt builder.

Deze helper zorgt voor:
- Consistente AI-rol over de hele applicatie
- Geen herhaling van identity / constraints / style
- Schaalbaarheid naar meerdere agents (report, strategy, bot, etc.)
- Automatische style-selectie per agent
- Optionele style-override per agent (non-breaking)
"""

from backend.ai_core.ai_identity import AI_IDENTITY
from backend.ai_core.ai_constraints import AI_CONSTRAINTS
from backend.ai_core.ai_style import AI_STYLE
from backend.ai_core.ai_style_report import AI_STYLE_REPORT


def build_system_prompt(
    task: str,
    agent: str = "general",
    style_override: str | None = None,
) -> str:
    """
    Bouwt een complete system prompt voor OpenAI.

    Parameters:
    - task (str): de specifieke taak van de agent
    - agent (str): type agent (report, strategy, bot, etc.)
    - style_override (str | None):
        Optionele stijl-instructies die de automatische stijl vervangen.

    Returns:
    - str: volledige system prompt
    """

    if not task or not isinstance(task, str):
        raise ValueError("AI task description is verplicht en moet een string zijn.")

    # -------------------------------------------------
    # 🧠 Style selectie (auto + override)
    # -------------------------------------------------
    if isinstance(style_override, str) and style_override.strip():
        style_block = style_override.strip()
    elif agent == "report":
        style_block = AI_STYLE_REPORT.strip()
    else:
        style_block = AI_STYLE.strip()

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
