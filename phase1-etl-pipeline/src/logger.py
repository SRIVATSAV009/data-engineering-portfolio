"""
Structured logging for the Weather ETL Pipeline.

Why structured logging?
- Every log line has consistent fields: timestamp, level, component, message
- In production you ship these logs to Datadog/CloudWatch and query them
- 'print()' is for scripts. Logging is for pipelines.
"""

import logging
import sys
from datetime import datetime


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger for any pipeline component.
    Usage: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
