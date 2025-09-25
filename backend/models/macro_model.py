from pydantic import BaseModel
from datetime import datetime

class MacroIndicator(BaseModel):
    name: str                 # Bijvoorbeeld "S&P500"
    symbol: str              # Bijvoorbeeld "^GSPC"
    source: str              # "yahoo", "alpha_vantage", etc.
    value: float
    score: int
    trend: str               # Bijvoorbeeld "Sterk", "Zwak", etc.
    interpretation: str      # Bijvoorbeeld "Dalend", "Sterke stijging"
    explanation: str         # Komt uit config
    action: str              # Komt uit config
    correlation: str         # "positief"/"negatief"
    category: str            # "macro", "sentiment", etc.
    link: str                # Link naar chart of bron
    timestamp: datetime
