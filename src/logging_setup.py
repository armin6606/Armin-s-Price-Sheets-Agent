"""Logging configuration for Price Sheet Bot.

Sets up dual logging: console + file + JSONL structured events.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


JSONL_PATH = "./logs/price_sheet_bot.jsonl"
LOG_PATH = "./logs/price_sheet_bot.log"


class JsonlHandler(logging.Handler):
    """Writes structured JSON lines to a log file."""

    def __init__(self, path: str):
        super().__init__()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path

    def emit(self, record):
        try:
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if hasattr(record, "event_data"):
                entry["data"] = record.event_data
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, default=str) + "\n")
        except Exception:
            self.handleError(record)


def setup_logging(log_dir: str = "./logs", level: str = "INFO") -> logging.Logger:
    """Configure root logger with console, file, and JSONL handlers."""
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger("price_sheet_bot")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Prevent duplicate handlers on re-init
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    log_file = os.path.join(log_dir, "price_sheet_bot.log")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # JSONL handler
    jsonl_file = os.path.join(log_dir, "price_sheet_bot.jsonl")
    jh = JsonlHandler(jsonl_file)
    jh.setLevel(logging.DEBUG)
    logger.addHandler(jh)

    return logger


def log_event(logger: logging.Logger, level: str, message: str, **data):
    """Log a structured event with extra data fields."""
    record = logger.makeRecord(
        name=logger.name,
        level=getattr(logging, level.upper(), logging.INFO),
        fn="",
        lno=0,
        msg=message,
        args=(),
        exc_info=None,
    )
    record.event_data = data
    logger.handle(record)
