from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.database import get_db
from app.models import Alert, Device, User, UserRole
from app.schemas.alert import AlertOut

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertOut])
def list_alerts(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    device_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> list[Alert]:
    q = select(Alert).order_by(Alert.id.desc()).limit(limit)
    if device_id is not None:
        device = db.get(Device, device_id)
        if not device:
            raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
        if current.role != UserRole.admin and device.user_id != current.id:
            raise HTTPException(status_code=403, detail="No permitido")
        q = select(Alert).where(Alert.device_id == device_id).order_by(Alert.id.desc()).limit(limit)
    elif current.role != UserRole.admin:
        ids = db.scalars(select(Device.id).where(Device.user_id == current.id)).all()
        if not ids:
            return []
        q = select(Alert).where(Alert.device_id.in_(ids)).order_by(Alert.id.desc()).limit(limit)
    return list(db.scalars(q).all())
