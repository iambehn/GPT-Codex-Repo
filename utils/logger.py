"""
Logging configuration for the clipbot pipeline.

All pipeline modules call get_logger(__name__) to get a named logger.
Logs are written to both stdout and logs/pipeline.log.
Log level defaults to INFO; set LOG_LEVEL env var to override (e.g. DEBUG).
"""

import logging
import os
import sys
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    """Return a named logger configured for the clipbot pipeline.

    On first call the root 'clipbot' logger is set up with a stdout handler
    and a file handler writing to logs/pipeline.log. Subsequent calls return
    a child logger that inherits those handlers.

    Args:
        name: Typically __name__ of the calling module.

    Returns:
        A configured logging.Logger instance.
    """
    root_logger = logging.getLogger("clipbot")

    if not root_logger.handlers:
        level_name = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        root_logger.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Stdout handler
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        root_logger.addHandler(stdout_handler)

        # File handler — logs/pipeline.log
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "pipeline.log")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    return logging.getLogger(f"clipbot.{name}")
