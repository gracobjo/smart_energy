"""
Simula dispositivos IoT que publican lecturas periódicamente.
Complementa el pipeline legacy (Kafka/Cassandra) sin sustituirlo en el MVP.
"""
from __future__ import annotations

import asyncio
import logging
import random
from statistics import mean, pstdev

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import SessionLocal
from app.models import Alert, Device, EnergyReading
from app.services.notifications import send_email_sync, send_telegram
from app.websocket_manager import manager

log = logging.getLogger(__name__)

_stop = asyncio.Event()


def _tick_sync() -> tuple[list[dict], list[str]]:
    """Inserta lecturas simuladas; devuelve payloads WS y mensajes Telegram."""
    payloads: list[dict] = []
    telegram_msgs: list[str] = []
    db: Session = SessionLocal()
    try:
        devices = list(db.scalars(select(Device)).all())
        if not devices:
            return [], []

        for device in devices:
            hist = list(
                db.scalars(
                    select(EnergyReading.consumption)
                    .where(EnergyReading.device_id == device.id)
                    .order_by(EnergyReading.id.desc())
                    .limit(40)
                ).all()
            )

            base = 2.0 + (device.id % 5) * 0.3
            noise = random.gauss(0, 0.4)
            consumption = max(0.05, base + noise)
            if random.random() < 0.04:
                consumption += random.uniform(4, 12)

            reading = EnergyReading(device_id=device.id, consumption=consumption)
            db.add(reading)
            db.flush()

            payloads.append(
                {
                    "type": "energy_reading",
                    "device_id": device.id,
                    "consumption": consumption,
                    "reading_id": reading.id,
                }
            )

            if len(hist) >= 8:
                m = mean(hist)
                try:
                    std = pstdev(hist)
                except Exception:
                    std = 0.0
                if std > 1e-6 and consumption > m + 3 * std:
                    msg = (
                        f"Consumo anómalo: {consumption:.2f} kWh (media reciente {m:.2f}) "
                        f"en dispositivo «{device.name}»"
                    )
                    db.add(
                        Alert(
                            device_id=device.id,
                            type="spike",
                            message=msg,
                        )
                    )
                    telegram_msgs.append(f"[SmartEnergy] {msg}")

        db.commit()
        for p in payloads:
            r = db.get(EnergyReading, p["reading_id"])
            if r and r.timestamp:
                p["timestamp"] = r.timestamp.isoformat()
        return payloads, telegram_msgs
    except Exception as e:
        log.exception("IoT simulator tick: %s", e)
        db.rollback()
        return [], []
    finally:
        db.close()


async def _sim_loop() -> None:
    interval = get_settings().iot_sim_interval_seconds
    while not _stop.is_set():
        payloads, tg_msgs = await asyncio.to_thread(_tick_sync)
        for p in payloads:
            await manager.broadcast_json(p)
        for m in tg_msgs:
            await send_telegram(m)
            await asyncio.to_thread(send_email_sync, "Alerta energía", m)
        try:
            await asyncio.wait_for(_stop.wait(), timeout=interval)
        except TimeoutError:
            continue


_task: asyncio.Task | None = None


def start_iot_simulator() -> None:
    global _task
    _stop.clear()
    if _task is None or _task.done():
        _task = asyncio.create_task(_sim_loop())
        log.info("Simulador IoT iniciado (intervalo %.1fs)", get_settings().iot_sim_interval_seconds)


def stop_iot_simulator() -> None:
    _stop.set()
    log.info("Simulador IoT detenido")
