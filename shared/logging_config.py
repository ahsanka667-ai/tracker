"""
shared/logging_config.py — Shared logging setup for all services.

Logs to both stdout (so systemd/docker logs still work) AND a rotating
file under LOG_DIR, so you have history to look back at after a crash
without needing journalctl/docker logs to still have the buffer.

v5:
  * `log_event(logger, "EVENT_CODE", **fields)` — the stable, greppable
    structured-log contract (spec §43): CLICK_CREATED, ATTRIBUTION_RESOLVED,
    META_EVENT_QUEUED, …
  * A redaction filter is attached to every handler we own, so a Meta access
    token, bot token, or MTProto session string can never be written to disk
    even if some library logs a raw request.
  * Optional JSON lines (LOG_JSON=true) for shipping to a log aggregator.
"""
from __future__ import annotations

import json
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any

from shared.config import get_settings

_CONFIGURED = False


class RedactionFilter(logging.Filter):
    """Scrub credential-shaped strings out of every record."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            from shared.security import redact
            msg = record.getMessage()
            red = redact(msg)
            if red != msg:
                record.msg = red
                record.args = ()
        except Exception:  # pragma: no cover - logging must never explode
            pass
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key in ("event", "fields"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def setup_logging(service_name: str) -> None:
    global _CONFIGURED
    settings = get_settings()
    os.makedirs(settings.LOG_DIR, exist_ok=True)

    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    if _CONFIGURED:  # avoid duplicate handlers if called more than once
        return
    _CONFIGURED = True

    if settings.LOG_JSON:
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(RedactionFilter())
    root.addHandler(console)

    # Rotating file handler — 5MB per file, keep 5 backups (~25MB max per service)
    log_path = os.path.join(settings.LOG_DIR, f"{service_name}.log")
    file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(RedactionFilter())
    root.addHandler(file_handler)

    # Telethon is chatty at INFO and its connection logs include peer ids; keep
    # it at WARNING unless someone is explicitly debugging (LOG_LEVEL=DEBUG).
    for noisy in ("telethon", "asyncio", "aiohttp.access", "httpx", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.DEBUG if level <= logging.DEBUG else logging.WARNING)


def log_event(logger: logging.Logger, event: str, *, level: int = logging.INFO,
              **fields: Any) -> None:
    """
    Emit a structured, greppable event line:

        log_event(logger, "META_EVENT_QUEUED", meta_event_id=42, event_name="Lead")

    Values are run through the redactor first, because `fields` is where
    people are tempted to drop a payload dict.
    """
    try:
        from shared.security import redact_obj
        safe = redact_obj(fields)
    except Exception:  # pragma: no cover - defensive
        safe = {k: str(v)[:120] for k, v in fields.items()}
    if getattr(get_settings(), "LOG_JSON", False):
        logger.log(level, event, extra={"event": event, "fields": safe})
    else:
        rendered = " ".join(f"{k}={v}" for k, v in safe.items())
        logger.log(level, "%s %s", event, rendered)
