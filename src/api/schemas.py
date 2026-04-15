from pydantic import BaseModel
from datetime import date

class ForecastPoint(BaseModel):
    city: str
    forecast_date: date
    target_date: date
    horizon: int
    predicted: float
    actual: float | None

class HistoryPoint(BaseModel):
    date: date
    city: str
    pm10: float | None

class QueryResponse(BaseModel):
    answer: str