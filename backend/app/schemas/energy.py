from datetime import datetime

from pydantic import BaseModel, Field


class EnergyReadingCreate(BaseModel):
    consumption: float = Field(ge=0)


class EnergyReadingOut(BaseModel):
    id: int
    device_id: int
    timestamp: datetime
    consumption: float

    model_config = {"from_attributes": True}
