#!/usr/bin/env python3
"""Shared logging configuration with colored output for ETL CLI tools."""
from __future__ import annotations
import logging
import os
import sys


def _supports_ansi() -> bool:
    """Detect if the terminal supports ANSI escape codes.
    
    Returns:
        True if ANSI codes are supported, False otherwise.
    """
    # Check for explicit NO_COLOR environment variable (https://no-color.org/)
    if os.environ.get("NO_COLOR"):
        return False
    # Check for explicit FORCE_COLOR environment variable
    if os.environ.get("FORCE_COLOR"):
        return True
    # Docker containers typically support ANSI even without TTY
    # Check common Docker indicators
    if os.path.exists("/.dockerenv") or os.environ.get("DOCKER_CONTAINER"):
        return True
    # Check if stdout is a TTY
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return False
    # Windows: Check for Windows Terminal or ConEmu (support ANSI)
    # or if ANSICON is set
    if sys.platform == "win32":
        return (
            os.environ.get("WT_SESSION")  # Windows Terminal
            or os.environ.get("ANSICON")  # ANSICON
            or os.environ.get("ConEmuANSI") == "ON"  # ConEmu
            or "TERM" in os.environ  # Git Bash, Cygwin
        )
    # Unix-like systems generally support ANSI
    return True


# Determine ANSI support once at module load
_ANSI_SUPPORTED = _supports_ansi()


class LogColors:
    """ANSI color codes for colorized logging output.
    
    Colors are automatically disabled on terminals that don't support ANSI.
    Set NO_COLOR=1 to disable colors, or FORCE_COLOR=1 to force enable.
    """
    RESET = '\033[0m' if _ANSI_SUPPORTED else ''
    BOLD = '\033[1m' if _ANSI_SUPPORTED else ''
    # Log level colors
    DEBUG = '\033[36m' if _ANSI_SUPPORTED else ''      # Cyan
    INFO = '\033[32m' if _ANSI_SUPPORTED else ''       # Green
    WARNING = '\033[33m' if _ANSI_SUPPORTED else ''    # Yellow
    ERROR = '\033[31m' if _ANSI_SUPPORTED else ''      # Red
    CRITICAL = '\033[35m' if _ANSI_SUPPORTED else ''   # Magenta
    # Component colors
    MODULE = '\033[94m' if _ANSI_SUPPORTED else ''     # Blue

class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output for different log levels."""
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors based on level.
        Args:
            record: Log record to format.

        Returns:
            Formatted string with ANSI color codes.
        """
        # Color mapping for log levels
        level_colors = {
            'DEBUG': LogColors.DEBUG,
            'INFO': LogColors.INFO,
            'WARNING': LogColors.WARNING,
            'ERROR': LogColors.ERROR,
            'CRITICAL': LogColors.CRITICAL,
        }
        # Get color for this level
        level_color = level_colors.get(record.levelname, LogColors.RESET)
        # Format: [LEVEL] module - message
        colored_levelname = f"{level_color}{LogColors.BOLD}[{record.levelname}]{LogColors.RESET}"
        colored_module = f"{LogColors.MODULE}{record.name}{LogColors.RESET}"
        # Create colored message
        formatted = f"{colored_levelname} {colored_module} - {record.getMessage()}"
        return formatted

def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging with colored output for ETL CLI tools.

    This function sets up:
    - Colored console output with custom format
    - INFO level by default (configurable)
    - Suppression of noisy Spark/Py4J logs (WARNING+ only)

    Args:
        level: Logging level (default: logging.INFO).

    Example:
        >>> from pv_lakehouse.etl.utils.logging_config import configure_logging
        >>> configure_logging()  # For Bronze/Silver/Gold CLI
    """
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()
    # Console handler with colored format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    # Use colored formatter
    formatter = ColoredFormatter()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    # Suppress noisy Spark/Py4J logs - only show WARNING and above
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("org.apache.spark").setLevel(logging.WARNING)
    logging.getLogger("org.sparkproject").setLevel(logging.WARNING)


__all__ = ["LogColors", "ColoredFormatter", "configure_logging"]
