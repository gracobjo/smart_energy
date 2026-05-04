from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Tenant, User, UserRole
from app.schemas.auth import LoginRequest, RegisterRequest, Token
from app.schemas.user import UserOut
from app.security import create_access_token, hash_password, verify_password

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserOut, status_code=status.HTTP_201_CREATED)
def register(body: RegisterRequest, db: Annotated[Session, Depends(get_db)]) -> User:
    exists = db.scalar(select(User).where(User.email == body.email))
    if exists:
        raise HTTPException(status_code=400, detail="Email ya registrado")

    n_users = db.scalar(select(func.count()).select_from(User)) or 0
    role = UserRole.admin if n_users == 0 else UserRole.user

    tenant_id = None
    if body.tenant_name:
        t = db.scalar(select(Tenant).where(Tenant.name == body.tenant_name))
        if not t:
            t = Tenant(name=body.tenant_name)
            db.add(t)
            db.flush()
        tenant_id = t.id

    user = User(
        email=body.email,
        hashed_password=hash_password(body.password),
        role=role,
        tenant_id=tenant_id,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.post("/login", response_model=Token)
def login(body: LoginRequest, db: Annotated[Session, Depends(get_db)]) -> Token:
    user = db.scalar(select(User).where(User.email == body.email))
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciales incorrectas")
    token = create_access_token(user.id, extra={"role": user.role.value})
    return Token(access_token=token)
