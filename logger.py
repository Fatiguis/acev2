"""
Logging Configuration Module.
Sets up structured logging for the arbitrage bot.

Per Grok Round 5: Added async-compatible logging using QueueHandler to prevent
blocking the event loop during HF trading operations. File I/O now happens in
a background thread via QueueListener.
"""

import logging
import logging.handlers
import sys
import queue
import atexit
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import LoggingConfig

# Log rotation settings (prevent disk full on Render)
MAX_LOG_SIZE_MB = 50  # Max 50MB per log file
BACKUP_COUNT = 5  # Keep 5 backup files (250MB total max)

# Global queue listener for async logging cleanup
_queue_listener: Optional[logging.handlers.QueueListener] = None


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

    # Per Grok Round 23: Rate limit patterns to filter (demote 429s to DEBUG)
    # GitHub: py-clob floods logs with 429s - masks real issues like approval failures
    RATE_LIMIT_PATTERNS = ['429', 'rate limit', 'too many requests', 'throttl']

    def format(self, record: logging.LogRecord) -> str:
        # Per Grok Round 23: Demote rate limit messages to DEBUG level
        # Prevents log flooding during high-volume trading
        msg_lower = str(record.msg).lower() if record.msg else ""
        if any(pattern in msg_lower for pattern in self.RATE_LIMIT_PATTERNS):
            if record.levelno > logging.DEBUG:
                record.levelno = logging.DEBUG
                record.levelname = 'DEBUG'

        # Add color to levelname
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(config: LoggingConfig, use_async: bool = True) -> logging.Logger:
    """
    Set up logging for the application.

    Per Grok Round 5: Added async-compatible logging option using QueueHandler.
    When use_async=True, file I/O happens in a background thread to prevent
    blocking the event loop during HF trading.

    Args:
        config: Logging configuration.
        use_async: If True, use QueueHandler for non-blocking file logging.

    Returns:
        The root logger.
    """
    global _queue_listener

    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Stop any existing queue listener
    if _queue_listener is not None:
        _queue_listener.stop()
        _queue_listener = None

    # Console handler with colors (always direct - console is fast)
    if config.console_logging:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, config.level.upper()))
        console_formatter = ColoredFormatter(config.format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # File handler with rotation (prevent disk full on Render)
    if config.log_file:
        log_path = Path(config.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Create the actual file handler
        file_handler = logging.handlers.RotatingFileHandler(
            config.log_file,
            maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,  # Convert MB to bytes
            backupCount=BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)  # Log everything to file
        file_formatter = logging.Formatter(config.format)
        file_handler.setFormatter(file_formatter)

        if use_async:
            # Per Grok Round 5: Use QueueHandler for non-blocking file I/O
            # Logs go to queue, QueueListener writes to file in background thread
            log_queue = queue.Queue(-1)  # Unbounded queue
            queue_handler = logging.handlers.QueueHandler(log_queue)
            queue_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(queue_handler)

            # Start listener in background thread
            _queue_listener = logging.handlers.QueueListener(
                log_queue,
                file_handler,
                respect_handler_level=True
            )
            _queue_listener.start()

            # Per Grok Round 18: Removed atexit - unreliable in forked/multithreaded apps
            # Cleanup should be called explicitly via supervisor shutdown using cleanup_logging()
            # atexit may not fire on supervisor kill, orphaning listener thread

            root_logger.info(
                f"Async logging enabled: queue-based file I/O, "
                f"max {MAX_LOG_SIZE_MB}MB per file, {BACKUP_COUNT} backups"
            )
        else:
            # Direct file handler (blocking - for CLI scripts)
            root_logger.addHandler(file_handler)
            root_logger.info(f"Log rotation enabled: max {MAX_LOG_SIZE_MB}MB per file, {BACKUP_COUNT} backups")

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("web3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    root_logger.info(f"Logging initialized at {config.level} level")

    return root_logger


def _cleanup_queue_listener():
    """Clean up the queue listener on exit."""
    global _queue_listener
    if _queue_listener is not None:
        _queue_listener.stop()
        _queue_listener = None


def cleanup_logging():
    """
    Per Grok Round 18/23: Explicit cleanup for supervisor shutdown with flush.

    Call this in supervisor cleanup_coro to ensure log flush before exit.
    atexit is unreliable in forked/multithreaded apps and may not fire
    on supervisor kill, orphaning listener thread and losing forensic data.

    Per Grok Round 23: Added explicit queue flush to prevent data loss.
    GitHub: Threads need explicit stop - rn1's 13K trades need audit-proof logs.
    """
    global _queue_listener

    # Per Grok Round 23: Flush queue before stopping listener
    # Ensures all pending log messages are written (forensic data for audits)
    if _queue_listener is not None:
        try:
            # Signal queue to flush by enqueueing sentinel
            # QueueListener.stop() calls queue.put_nowait(self._sentinel)
            _queue_listener.stop()
            _queue_listener = None
            logging.getLogger().info("Log queue flushed and listener stopped")
        except Exception as e:
            # Last-resort stderr logging if logger broken
            import sys
            print(f"[CLEANUP] Log flush error: {e}", file=sys.stderr)

    logging.shutdown()


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a module.

    Args:
        name: Logger name (usually __name__).

    Returns:
        Logger instance.
    """
    return logging.getLogger(name)
