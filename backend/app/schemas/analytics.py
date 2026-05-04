from pydantic import BaseModel, Field


class PredictQuery(BaseModel):
    device_id: int
    horizon_hours: int = Field(default=6, ge=1, le=168)


class PredictResponse(BaseModel):
    device_id: int
    horizon_hours: int
    predictions: list[float]


class AnomalyRequest(BaseModel):
    device_id: int | None = None
    consumptions: list[float] | None = None


class AnomalyOut(BaseModel):
    index: int
    consumption: float
    score: float


class AnomalyResponse(BaseModel):
    anomalies: list[AnomalyOut]


class OptimizationSuggestionsResponse(BaseModel):
    suggestions: list[str]
