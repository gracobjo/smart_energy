from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Permite `import ai` en local sin PYTHONPATH (Docker ya define PYTHONPATH=/app).
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError as PydanticValidationError

from app.api.routers import alerts, analytics, auth, devices, energy, users
from app.config import get_settings
from app.database import Base, SessionLocal, engine
from app.iot_simulator import start_iot_simulator, stop_iot_simulator
from app.logging_config import setup_logging
from app.models import User
from app.security import decode_token
from app.websocket_manager import manager

log = logging.getLogger(__name__)


def _apply_schema() -> None:
    """Alembic en arranque (recomendado) o create_all si está desactivado."""
    settings = get_settings()
    if settings.run_alembic_on_startup:
        from alembic import command
        from alembic.config import Config

        ini_path = Path(__file__).resolve().parent.parent / "alembic.ini"
        cfg = Config(str(ini_path))
        command.upgrade(cfg, "head")
        log.info("Migraciones Alembic aplicadas (head)")
    else:
        Base.metadata.create_all(bind=engine)
        log.warning("Alembic desactivado: usando create_all (solo desarrollo)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    _apply_schema()
    start_iot_simulator()
    log.info("Aplicación iniciada")
    yield
    stop_iot_simulator()
    log.info("Aplicación detenida")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)

    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.exception_handler(PydanticValidationError)
    async def pydantic_handler(_: Request, exc: PydanticValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(Exception)
    async def generic_handler(_: Request, exc: Exception) -> JSONResponse:
        log.exception("Error no controlado: %s", exc)
        return JSONResponse(
            status_code=500,
            content={"detail": "Error interno"},
        )

    app.include_router(auth.router)
    app.include_router(users.router)
    app.include_router(devices.router)
    app.include_router(energy.router)
    app.include_router(analytics.router)
    app.include_router(alerts.router)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/ws/energy")
    async def ws_energy(websocket: WebSocket, token: str = Query(..., description="JWT (mismo valor que access_token)")) -> None:
        payload = decode_token(token)
        if not payload or not payload.get("sub"):
            await websocket.close(code=1008)
            return
        try:
            uid = int(payload["sub"])
        except (TypeError, ValueError):
            await websocket.close(code=1008)
            return
        db = SessionLocal()
        try:
            user = db.get(User, uid)
            if not user:
                await websocket.close(code=1008)
                return
        finally:
            db.close()

        await manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            await manager.disconnect(websocket)

    return app


app = create_app()
