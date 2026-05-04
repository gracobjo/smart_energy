from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.database import get_db
from app.models import Device, EnergyReading, User, UserRole
from app.schemas.energy import EnergyReadingCreate, EnergyReadingOut

router = APIRouter(prefix="/energy", tags=["energy"])


def _device_access(user: User, device: Device | None) -> bool:
    if not device:
        return False
    if user.role == UserRole.admin:
        return True
    return device.user_id == user.id


@router.get("/readings", response_model=list[EnergyReadingOut])
def list_readings(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    device_id: int = Query(...),
    limit: int = Query(default=100, ge=1, le=2000),
) -> list[EnergyReading]:
    device = db.get(Device, device_id)
    if not _device_access(current, device):
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    rows = db.scalars(
        select(EnergyReading)
        .where(EnergyReading.device_id == device_id)
        .order_by(EnergyReading.id.desc())
        .limit(limit)
    ).all()
    return list(reversed(rows))


@router.post("/readings", response_model=EnergyReadingOut, status_code=status.HTTP_201_CREATED)
def create_reading(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    device_id: int = Query(..., description="ID del dispositivo"),
    body: EnergyReadingCreate = Body(...),
) -> EnergyReading:
    device = db.get(Device, device_id)
    if not _device_access(current, device):
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    r = EnergyReading(device_id=device_id, consumption=body.consumption)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r
