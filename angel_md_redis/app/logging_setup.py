"""Shared logging setup for angel_md_redis runners."""

from __future__ import annotations

import logging
import os
import sys
from datetime import date
from pathlib import Path


def setup_logger(name: str) -> logging.Logger:
    """
    Configure a named logger that writes to console and (optionally) to:
      logs/<YYYY-MM-DD>/<name>.log

    Env:
      LOG_LEVEL   - DEBUG / INFO / WARNING / ERROR (default INFO)
      LOG_DIR     - base log directory (default: logs)
      LOG_TO_FILE - 1/0 write FileHandler. If unset, FileHandler is on only
                    when stdout is a TTY. Set 0 under run_all.sh / run_all.ps1
                    because those already redirect stdout into the log file.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    # Under run_all, stdout is already redirected to logs/<date>/<name>.log.
    # Default FileHandler off when stdout is not a TTY so we do not lock the
    # same file twice on Windows. Explicit LOG_TO_FILE still wins.
    log_to_file_env = os.getenv("LOG_TO_FILE")
    if log_to_file_env is None:
        to_file = sys.stdout.isatty()
    else:
        to_file = log_to_file_env.strip() not in ("0", "false", "False", "no")

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

    # stdout (not stderr) so run_all.ps1 RedirectStandardError stays real errors
    # instead of filling *.err.log with INFO EMITs.
    sh = logging.StreamHandler(sys.stdout)
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
