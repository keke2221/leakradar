"""Logging helpers with run context."""

from __future__ import annotations

import logging
import sys
from typing import Optional

_LOGGER: Optional[logging.Logger] = None


def get_logger(run_id: Optional[str] = None) -> logging.Logger:
    global _LOGGER
    if _LOGGER:
        return _LOGGER

    logger = logging.getLogger("leakradar")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.handlers = [handler]
    if run_id:
        logger = logging.LoggerAdapter(logger, {"run_id": run_id})  # type: ignore
    _LOGGER = logger  # type: ignore
    return logger
