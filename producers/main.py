import asyncio
from parser import parse
from sender import send
from ws_manager import ws_manager
from ws_manager import (
    build_orderbook_channels,
    build_trade_channels,
    build_stats_channels,
    build_candle_channels
)

async def handle_message(msg):
    record, domain = parse(msg)
    if record:
        send(record, domain)
        print(f"[OK] {domain} → {record['symbol']}")

async def main():
    tasks = [
        ws_manager("ORDERBOOKS", build_orderbook_channels()),
        ws_manager("TRADES", build_trade_channels()),
        ws_manager("STATS", build_stats_channels()),
        ws_manager("CANDLES", build_candle_channels()),
    ]

    # run all ws managers concurrently
    async def runner(gen):
        async for msg in gen:
            await handle_message(msg)

    await asyncio.gather(*(runner(t) for t in tasks))

if __name__ == "__main__":
    asyncio.run(main())
