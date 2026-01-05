"""
Logging Configuration Module.
Sets up structured logging for the arbitrage bot.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import LoggingConfig


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output."""

    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'

    def format(self, record: logging.LogRecord) -> str:
        # Add color to levelname
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(config: LoggingConfig) -> logging.Logger:
    """
    Set up logging for the application.

    Args:
        config: Logging configuration.

    Returns:
        The root logger.
    """
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Console handler with colors
    if config.console_logging:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, config.level.upper()))
        console_formatter = ColoredFormatter(config.format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # File handler
    if config.log_file:
        log_path = Path(config.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(config.log_file)
        file_handler.setLevel(logging.DEBUG)  # Log everything to file
        file_formatter = logging.Formatter(config.format)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("web3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    root_logger.info(f"Logging initialized at {config.level} level")

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a module.

    Args:
        name: Logger name (usually __name__).

    Returns:
        Logger instance.
    """
    return logging.getLogger(name)
