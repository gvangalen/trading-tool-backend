"""
Central AI system prompt builder.

Deze helper zorgt voor:
- Consistente AI-rol over de hele applicatie
- Geen herhaling van identity / constraints / style
- Schaalbaarheid naar meerdere agents (report, strategy, bot, etc.)
"""

from backend.ai_core.ai_identity import AI_IDENTITY
from backend.ai_core.ai_constraints import AI_CONSTRAINTS
from backend.ai_core.ai_style import AI_STYLE


def build_system_prompt(task: str) -> str:
    """
    Bouwt een complete system prompt voor OpenAI.

    Parameters:
    - task (str): de specifieke taak van de agent (bijv. report, strategie, validatie)

    Returns:
    - str: volledige system prompt
    """

    if not task or not isinstance(task, str):
        raise ValueError("AI task description is verplicht en moet een string zijn.")

    prompt = f"""
{AI_IDENTITY.strip()}

{AI_CONSTRAINTS.strip()}

{AI_STYLE.strip()}

------------------------------
SPECIFIEKE TAAK
------------------------------
{task.strip()}
""".strip()

    return prompt
