"""
@Author : Evan Cillie
@LastEdit : 06/21/26
@Purpose : Set up project logging
"""

import logging
from pathlib import Path


def setup_logger(name: str, log_name: str = "pipeline") -> logging.Logger:
    """
    Creates and returns a logger that writes to both the console and a log file.

    Args:
        name: Name of the logger, usually __name__.
        log_name: Name of the log file without .log.

    Returns:
        Configured logger.
    """

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_dir / f"{log_name}.log", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
