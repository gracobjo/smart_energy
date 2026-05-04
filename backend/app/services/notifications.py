import logging
import smtplib
from email.message import EmailMessage

import httpx

from app.config import get_settings

log = logging.getLogger(__name__)


async def send_telegram(text: str) -> bool:
    s = get_settings()
    if not s.telegram_bot_token or not s.telegram_chat_id:
        return False
    url = f"https://api.telegram.org/bot{s.telegram_bot_token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                url,
                json={"chat_id": s.telegram_chat_id, "text": text[:3500]},
            )
        if r.status_code != 200:
            log.warning("Telegram API %s: %s", r.status_code, r.text)
            return False
        return True
    except Exception as e:
        log.warning("Telegram error: %s", e)
        return False


def send_email_sync(subject: str, body: str) -> bool:
    s = get_settings()
    if not all([s.smtp_host, s.smtp_user, s.smtp_password, s.alert_email_from, s.alert_email_to]):
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = s.alert_email_from
    msg["To"] = s.alert_email_to
    msg.set_content(body)
    try:
        with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=15) as smtp:
            smtp.starttls()
            smtp.login(s.smtp_user, s.smtp_password)
            smtp.send_message(msg)
        return True
    except Exception as e:
        log.warning("SMTP error: %s", e)
        return False
