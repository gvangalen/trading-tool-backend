from pydantic import BaseModel
from datetime import datetime, date

class TechnicalIndicator(BaseModel):
    symbol: str
    indicator: str
    value: float
    score: int
    advies: str
    uitleg: str
    timestamp: datetime
    date: date  
