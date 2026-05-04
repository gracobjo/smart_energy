from pydantic import BaseModel, Field


class DeviceCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    location: str = Field(default="", max_length=512)


class DeviceUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    location: str | None = Field(default=None, max_length=512)


class DeviceOut(BaseModel):
    id: int
    name: str
    location: str
    user_id: int

    model_config = {"from_attributes": True}
