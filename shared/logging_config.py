"""
shared/logging_config.py — Shared logging setup for all 3 services.

Logs to both stdout (so systemd/docker logs still work) AND a rotating
file under LOG_DIR, so you have history to look back at after a crash
without needing journalctl/docker logs to still have the buffer.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

from shared.config import get_settings


def setup_logging(service_name: str):
    settings = get_settings()
    os.makedirs(settings.LOG_DIR, exist_ok=True)

    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = logging.Formatter(fmt)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers if setup_logging is called more than once
    if root.handlers:
        return

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    # Rotating file handler — 5MB per file, keep 5 backups (~25MB max per service)
    log_path = os.path.join(settings.LOG_DIR, f"{service_name}.log")
    file_handler = RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=5)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
