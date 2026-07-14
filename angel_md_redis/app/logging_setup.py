"""Shared logging setup for angel_md_redis runners."""

from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path


def setup_logger(name: str) -> logging.Logger:
    """
    Configure a named logger that writes to console and (optionally) to:
      logs/<YYYY-MM-DD>/<name>.log

    Env:
      LOG_LEVEL   - DEBUG / INFO / WARNING / ERROR (default INFO)
      LOG_DIR     - base log directory (default: logs)
      LOG_TO_FILE - 1/0 write FileHandler (default 1). Set 0 under run_all.sh
                    because nohup already redirects stdout into the same file.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    to_file = os.getenv("LOG_TO_FILE", "1").strip() not in ("0", "false", "False", "no")

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.handlers:
        logger.setLevel(level)
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    log_path = None
    if to_file:
        log_dir = Path(os.getenv("LOG_DIR", "logs")) / date.today().isoformat()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{name}.log"
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    logger.info(
        "logger ready name=%s level=%s file=%s",
        name,
        level_name,
        log_path if log_path else "(stdout only)",
    )
    return logger
