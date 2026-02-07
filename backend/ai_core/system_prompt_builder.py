"""
Central AI system prompt builder.

Deze helper zorgt voor:
- Consistente AI-rol over de hele applicatie
- Geen herhaling van identity / constraints / style
- Schaalbaarheid naar meerdere agents (report, strategy, bot, etc.)
- Optionele style-override per agent (non-breaking)
"""

from backend.ai_core.ai_identity import AI_IDENTITY
from backend.ai_core.ai_constraints import AI_CONSTRAINTS
from backend.ai_core.ai_style import AI_STYLE


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
        Optionele stijl-instructies die AI_STYLE vervangen.
        Indien None → standaard AI_STYLE.

    Returns:
    - str: volledige system prompt
    """

    if not task or not isinstance(task, str):
        raise ValueError("AI task description is verplicht en moet een string zijn.")

    # -------------------------------------------------
    # 🧠 Style selectie (default of override)
    # -------------------------------------------------
    style_block = (
        style_override.strip()
        if isinstance(style_override, str) and style_override.strip()
        else AI_STYLE.strip()
    )

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
