import logging
import os
from pythonjsonlogger import jsonlogger
from logging.handlers import TimedRotatingFileHandler

LOG_DIR = "/app/logs/producers"
os.makedirs(LOG_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# JSON formatter
# ─────────────────────────────────────────────
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(lineno)d %(funcName)s %(message)s",
    rename_fields={
        "asctime": "timestamp",
        "levelname": "level",
        "name": "logger",
        "lineno": "line",
        "funcName": "function",
        "message": "message",
    }
)

# ─────────────────────────────────────────────
# Factory: component logger (console + file)
# ─────────────────────────────────────────────
def create_component_logger(
    name: str,
    *,
    to_console: bool = False,
    subdir: str | None = None,
) -> logging.Logger:
    """
    Main logger for a pipeline component.
    e.g. create_component_logger("parser", to_console=True)
    """
    log_dir = os.path.join(LOG_DIR, subdir or name)
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{name}.log")

    component_logger = logging.getLogger(name)
    component_logger.setLevel(logging.INFO)
    component_logger.propagate = False  # avoid duplicate logs

    if not component_logger.handlers:
        file_handler = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        component_logger.addHandler(file_handler)

        if to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            component_logger.addHandler(console_handler)

    return component_logger


# ─────────────────────────────────────────────
# Factory: domain logger (file only, same as before)
# ─────────────────────────────────────────────
def create_domain_logger(name: str) -> logging.Logger:
    path = os.path.join(LOG_DIR, name)
    os.makedirs(path, exist_ok=True)
    log_path = os.path.join(path, f"{name}.log")

    domain_logger = logging.getLogger(name)
    domain_logger.setLevel(logging.INFO)
    domain_logger.propagate = False

    if not domain_logger.handlers:
        handler = TimedRotatingFileHandler(
            log_path,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
        )
        handler.setFormatter(formatter)
        domain_logger.addHandler(handler)

    return domain_logger


# ─────────────────────────────────────────────
# WS / service logger (existing pattern)
# ─────────────────────────────────────────────
logger = logging.getLogger("service")
logger.setLevel(logging.INFO)
logger.propagate = False

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    ws_path = os.path.join(LOG_DIR, "wslogs")
    os.makedirs(ws_path, exist_ok=True)
    ws_handler = TimedRotatingFileHandler(
        os.path.join(ws_path, "ws.log"),
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8",
    )
    ws_handler.setFormatter(formatter)
    logger.addHandler(ws_handler)

logger.info("Main logger initialized")

# ─────────────────────────────────────────────
# Domain loggers (market data streams)
# ─────────────────────────────────────────────
log_orderbook = create_domain_logger("orderbook")
log_trades    = create_domain_logger("trades")
log_candles   = create_domain_logger("candles")
log_stats     = create_domain_logger("stats")

# ─────────────────────────────────────────────
# Pipeline component loggers
# ─────────────────────────────────────────────
log_parser     = create_component_logger("parser",     to_console=True)
log_serializer = create_component_logger("serializer", to_console=True)
log_sender     = create_component_logger("sender",     to_console=True)
log_main = create_component_logger("main",to_console=True)