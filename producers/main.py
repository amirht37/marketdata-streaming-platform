import asyncio

from prometheus_client import start_http_server

from Logger import log_main
from parser import parse
from serializer import init_serializers, serialize
from sender import init_sender, send, poll_sender, flush_sender
from ws_manager import ws_manager
from ws_manager import (
    build_orderbook_channels,
    build_trade_channels,
    build_stats_channels,
    build_candle_channels,
)


async def handle_message(msg):
    record, domain = parse(msg)
    if record is None:
        return

    payload = serialize(record, domain)
    if payload is None:
        return

    ok = send(payload, domain, key=record.get("symbol"))
    if not ok:
        log_main.error(
            "pipeline_send_failed",
            extra={"domain": domain, "symbol": record.get("symbol")},
        )


async def poll_loop():
    """Background task: drain Kafka delivery callbacks."""
    while True:
        poll_sender(0)
        await asyncio.sleep(0.05)


async def main():
    log_main.info("producer_starting")

    init_serializers()
    init_sender()
    start_http_server(9200)

    log_main.info(
        "producer_initialized",
        extra={"metrics_port": 9200},
    )

    tasks = [
        ws_manager("ORDERBOOKS", build_orderbook_channels()),
        ws_manager("TRADES", build_trade_channels()),
        ws_manager("STATS", build_stats_channels()),
        ws_manager("CANDLES", build_candle_channels()),
    ]

    async def runner(gen):
        async for msg in gen:
            await handle_message(msg)

    try:
        await asyncio.gather(
            poll_loop(),
            *(runner(t) for t in tasks),
        )
    finally:
        flush_sender()
        log_main.info("producer_shutdown")


if __name__ == "__main__":
    asyncio.run(main())