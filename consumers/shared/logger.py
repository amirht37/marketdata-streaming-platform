

import logging
import os
from logging.handlers import TimedRotatingFileHandler

from pythonjsonlogger import jsonlogger


LOG_ROOT = "/app/logs/consumers"
os.makedirs(LOG_ROOT,exist_ok=True)

formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(lineno)d %(funcName)s %(message)s",
    rename_fields={
        "asctime": "timestamp",
        "levelname": "level",
        "name": "logger",
        "lineno": "line",
        "funcName": "function",
        "message": "message",
    },
)


def get_logger(
    consumer_name: str,
    *,
    to_console: bool = False,
) -> logging.Logger:

    # /app/logs/consumers/trades
    log_dir = os.path.join(LOG_ROOT, consumer_name)
    os.makedirs(log_dir, exist_ok=True)

    # /app/logs/consumers/trades/trades.log
    log_path = os.path.join(
        log_dir,
        f"{consumer_name}.log",
    )

    # Get/create logger
    logger = logging.getLogger(consumer_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Prevent duplicate handlers if get_logger()
    # gets called more than once.
    if not logger.handlers:

        # File handler
        file_handler = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Optional console handler
        if to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    return logger
