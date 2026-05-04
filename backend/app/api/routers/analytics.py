from statistics import mean
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_current_user
from app.database import get_db
from app.models import Device, EnergyReading, User, UserRole
from app.schemas.analytics import (
    AnomalyOut,
    AnomalyRequest,
    AnomalyResponse,
    OptimizationSuggestionsResponse,
    PredictResponse,
)
from ai.anomalies import detect_anomalies
from ai.prediction import predict_consumption, train_consumption_model

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _access_device(user: User, device: Device | None) -> bool:
    if not device:
        return False
    if user.role == UserRole.admin:
        return True
    return device.user_id == user.id


@router.get("/predict", response_model=PredictResponse)
def predict(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    device_id: int = Query(...),
    horizon_hours: int = Query(default=6, ge=1, le=168),
) -> PredictResponse:
    device = db.get(Device, device_id)
    if not _access_device(current, device):
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    consumptions = list(
        db.scalars(
            select(EnergyReading.consumption)
            .where(EnergyReading.device_id == device_id)
            .order_by(EnergyReading.id.asc())
            .limit(500)
        ).all()
    )
    if len(consumptions) < 8:
        raise HTTPException(
            status_code=400,
            detail="Se necesitan al menos ~8 lecturas históricas para entrenar el modelo",
        )
    model = train_consumption_model(consumptions)
    if model is None:
        raise HTTPException(status_code=400, detail="No se pudo entrenar el modelo con los datos actuales")
    preds = predict_consumption(model, consumptions, horizon_hours)
    return PredictResponse(device_id=device_id, horizon_hours=horizon_hours, predictions=preds)


@router.post("/anomalies", response_model=AnomalyResponse)
def anomalies(
    body: AnomalyRequest,
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
) -> AnomalyResponse:
    consumptions = body.consumptions
    if body.device_id is not None:
        device = db.get(Device, body.device_id)
        if not _access_device(current, device):
            raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
        consumptions = list(
            db.scalars(
                select(EnergyReading.consumption)
                .where(EnergyReading.device_id == body.device_id)
                .order_by(EnergyReading.id.desc())
                .limit(200)
            ).all()
        )
        consumptions = list(reversed(consumptions))
    if not consumptions or len(consumptions) < 4:
        raise HTTPException(status_code=400, detail="Proporcione consumptions o device_id con histórico suficiente")
    raw = detect_anomalies(consumptions)
    return AnomalyResponse(anomalies=[AnomalyOut(index=i, consumption=c, score=s) for i, c, s in raw])


@router.get("/suggestions", response_model=OptimizationSuggestionsResponse)
def suggestions(
    current: Annotated[User, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_db)],
    device_id: int = Query(...),
) -> OptimizationSuggestionsResponse:
    device = db.get(Device, device_id)
    if not _access_device(current, device):
        raise HTTPException(status_code=404, detail="Dispositivo no encontrado")
    vals = list(
        db.scalars(
            select(EnergyReading.consumption)
            .where(EnergyReading.device_id == device_id)
            .order_by(EnergyReading.id.desc())
            .limit(48)
        ).all()
    )
    if not vals:
        return OptimizationSuggestionsResponse(suggestions=["Aún no hay lecturas para este dispositivo."])
    m = mean(vals)
    tips: list[str] = []
    if m > 8:
        tips.append("Consumo medio alto: revisar equipos en stand-by y horarios punta.")
    elif m < 1:
        tips.append("Consumo muy bajo: verificar que el medidor esté online.")
    else:
        tips.append("Consumo estable: mantener monitorización y comparar semana a semana.")
    if max(vals) - min(vals) > 6:
        tips.append("Gran variación entre lecturas: conviene descomponer por franja horaria.")
    tips.append("Desplazar cargas flexibles a valle reduce coste y estrés de red.")
    return OptimizationSuggestionsResponse(suggestions=tips)
