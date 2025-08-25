"""
Utility functions for the churn pipeline.

This module provides helper functions for configuring loggers across
different stages of the pipeline.  Logging is centralised here so
that all scripts share the same formatting and log destination.

The `get_logger` function returns a configured logger with a file
handler writing to the `data/logs` directory and a stream handler
writing to standard output.  Log level can be passed on demand.
"""
import logging
import os
from datetime import datetime


def get_logger(name: str, log_dir: str, level: int = logging.INFO) -> logging.Logger:
    """Create and return a logger that writes to a file and stdout.

    Args:
        name: Name of the logger (often __name__).
        log_dir: Directory where log files should be saved.
        level: Logging level.

    Returns:
        Configured logger instance.
    """
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    # Avoid adding duplicate handlers if logger already configured
    if not logger.handlers:
        # File handler with timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(log_dir, f"{name}_{timestamp}.log")
        fh = logging.FileHandler(file_path)
        fh.setLevel(level)
        # Stream handler
        sh = logging.StreamHandler()
        sh.setLevel(level)
        # Formatter
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh.setFormatter(formatter)
        sh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger