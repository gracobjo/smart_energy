from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.database import get_db
from app.models import Device, User, UserRole
from app.schemas.device import DeviceCreate, DeviceOut, DeviceUpdate

router = APIRouter(prefix="/devices", tags=["devices"])


def _can_access_device(user: User, device: Device) -> bool:
    if user.role == UserRole.admin:
        return True
    return device.user_id == user.id


@router.get("", response_model=list[DeviceOut])
def list_devices(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
) -> list[Device]:
    if current.role == UserRole.admin:
        return list(db.scalars(select(Device).order_by(Device.id)).all())
    return list(db.scalars(select(Device).where(Device.user_id == current.id).order_by(Device.id)).all())


@router.post("", response_model=DeviceOut, status_code=status.HTTP_201_CREATED)
def create_device(
    body: DeviceCreate,
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
) -> Device:
    d = Device(name=body.name, location=body.location, user_id=current.id)
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


@router.get("/{device_id}", response_model=DeviceOut)
def get_device(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    device_id: int,
) -> Device:
    d = db.get(Device, device_id)
    if not d or not _can_access_device(current, d):
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    return d


@router.patch("/{device_id}", response_model=DeviceOut)
def update_device(
    device_id: int,
    body: DeviceUpdate,
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
) -> Device:
    d = db.get(Device, device_id)
    if not d or not _can_access_device(current, d):
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    if current.role != UserRole.admin and d.user_id != current.id:
        raise HTTPException(status_code=403, detail="No permitido")
    if body.name is not None:
        d.name = body.name
    if body.location is not None:
        d.location = body.location
    db.commit()
    db.refresh(d)
    return d


@router.delete("/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_device(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    device_id: int,
) -> None:
    d = db.get(Device, device_id)
    if not d:
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    if current.role != UserRole.admin and d.user_id != current.id:
        raise HTTPException(status_code=403, detail="No permitido")
    db.delete(d)
    db.commit()
