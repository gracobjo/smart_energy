from datetime import datetime

from pydantic import BaseModel


class AlertOut(BaseModel):
    id: int
    device_id: int
    type: str
    message: str
    created_at: datetime

    model_config = {"from_attributes": True}
