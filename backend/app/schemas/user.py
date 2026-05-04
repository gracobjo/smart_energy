from pydantic import BaseModel, EmailStr, Field

from app.models import UserRole


class UserOut(BaseModel):
    id: int
    email: EmailStr
    role: UserRole
    tenant_id: int | None = None

    model_config = {"from_attributes": True}


class UserCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    role: UserRole = UserRole.user
    tenant_id: int | None = None


class UserUpdate(BaseModel):
    role: UserRole | None = None
    tenant_id: int | None = None
