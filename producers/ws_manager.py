import websockets
import asyncio
import json
from Logger import logger

WS_URL = "wss://ws.nobitex.ir/connection/websocket"

COINS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "SOLUSDT", "DOGEUSDT", "TRXUSDT", "LTCUSDT"
]

RESOLUTIONS = ['1', '5', '15', '30', '60', '120', '240', '360', '720']


def build_orderbook_channels():
    return [f"public:orderbook-{c}" for c in COINS]


def build_trade_channels():
    return [f"public:trades-{c}" for c in COINS]


def build_stats_channels():
    return [f"public:market-stats-{c}" for c in COINS]


def build_candle_channels():
    channels = []
    for c in COINS:
        for r in RESOLUTIONS:
            channels.append(f"public:candle-{c}-{r}")
    return channels


def decode_multi_json(raw):
    decoder = json.JSONDecoder()
    raw = raw.strip()
    results = []

    while raw:
        try:
            obj, idx = decoder.raw_decode(raw)
            results.append(obj)
            raw = raw[idx:].lstrip()
        except Exception:
            break

    return results


async def ws_manager(domain_name, channels):
    """
    WebSocket manager that:
    - connects to Nobitex WS
    - subscribes to channels
    - receives raw messages
    - decodes multi-JSON
    - yields each valid push message to main.py
    """
    while True:
        try:
            logger.info(f"[{domain_name}] connecting...")

            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                ping_timeout=None
            ) as ws:

                # handshake
                await ws.send(json.dumps({"connect": {}, "id": 1}))
                logger.info(f"[{domain_name}] handshake ok")

                # subscribe
                sub_id = 2
                for ch in channels:
                    await ws.send(json.dumps({
                        "id": sub_id,
                        "subscribe": {"channel": ch}
                    }))
                    logger.info(f"[{domain_name}] subscribed {ch}")
                    sub_id += 1
                    await asyncio.sleep(0.05)

                # receive messages
                while True:
                    raw = await ws.recv()
                    if not raw or raw.strip() == "":
                        continue

                    messages = decode_multi_json(raw)
                    logger.debug(f"[{domain_name}] decoded {len(messages)} messages")

                    for msg in messages:
                        if not isinstance(msg, dict):
                            continue
                        if "push" not in msg:
                            continue

                        # THIS IS THE IMPORTANT PART
                        yield msg

        except Exception as e:
            logger.warning(f"[{domain_name}] WS error: {e}")
            await asyncio.sleep(2)
