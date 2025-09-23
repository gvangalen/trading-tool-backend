from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class TechnicalIndicator(BaseModel):
    symbol: str
    indicator: str
    value: float
    score: int
    advies: str
    uitleg: str
    timestamp: datetime
