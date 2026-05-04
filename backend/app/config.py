from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Smart Energy API"
    debug: bool = False
    database_url: str = "postgresql+psycopg2://smartenergy:smartenergy@localhost:5432/smartenergy"

    jwt_secret: str = "change-me-in-production-use-long-random-secret"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24

    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"

    iot_sim_interval_seconds: float = 3.0

    # Notificaciones opcionales (bonus)
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    alert_email_from: str | None = None
    alert_email_to: str | None = None

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # Migraciones: True = `alembic upgrade head` al arrancar; False = solo create_all (no recomendado en prod)
    run_alembic_on_startup: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
