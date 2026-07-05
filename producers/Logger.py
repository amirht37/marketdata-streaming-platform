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
    "%(asctime)s %(levelname)s %(name)s %(message)s",
    rename_fields={
        "asctime": "timestamp",
        "levelname": "level",
        "name": "logger"
    }
)

# ─────────────────────────────────────────────
# MAIN LOGGER → console + ws.log
# ─────────────────────────────────────────────
logger = logging.getLogger("service")
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# WS file handler
ws_log_path = os.path.join(LOG_DIR, "ws.log")
ws_handler = TimedRotatingFileHandler(
    ws_log_path,
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8"
)
ws_handler.setFormatter(formatter)
logger.addHandler(ws_handler)

logger.info("Main logger initialized")


# ─────────────────────────────────────────────
# DOMAIN LOGGERS → rotating file handlers
# ─────────────────────────────────────────────
def create_domain_logger(name: str):
    log_path = os.path.join(LOG_DIR, f"{name}.log")

    domain_logger = logging.getLogger(name)
    domain_logger.setLevel(logging.INFO)

    handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8"
    )
    handler.setFormatter(formatter)
    domain_logger.addHandler(handler)

    return domain_logger


log_orderbook = create_domain_logger("orderbook")
log_trades = create_domain_logger("trades")
log_candles = create_domain_logger("candles")
log_stats = create_domain_logger("stats")
