import websockets
import asyncio
import json
from Logger import logger
import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server

WS_URL = "wss://ws.nobitex.ir/connection/websocket"

COINS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "SOLUSDT", "DOGEUSDT", "TRXUSDT", "LTCUSDT"
]

RESOLUTIONS = ['1', '5', '15', '30', '60', '120', '240', '360', '720']


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
# Start this once at process boot (e.g. in main.py): start_http_server(9100)

WS_CONNECTION_UP = Gauge(
    "ws_connection_up",
    "1 if the websocket connection is currently established, else 0",
    ["domain"],
)

WS_RECONNECTS_TOTAL = Counter(
    "ws_reconnects_total",
    "Number of times the websocket has (re)connected, by trigger reason",
    ["domain", "reason"],  # reason: initial | exception | watchdog_timeout
)

WS_HANDSHAKE_DURATION = Histogram(
    "ws_handshake_duration_seconds",
    "Time from opening the socket to finishing channel subscription",
    ["domain"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

WS_MESSAGES_RECEIVED_TOTAL = Counter(
    "ws_messages_received_total",
    "Number of push messages received, bucketed by channel type",
    ["domain", "channel_type"],  # orderbook | trades | candle | market-stats | other
)

WS_CHANNEL_SILENCE_SECONDS = Gauge(
    "ws_channel_silence_seconds",
    "Seconds since the last message was seen on a channel",
    ["domain", "channel_type", "coin"],
)

WS_WATCHDOG_RECONNECTS_TOTAL = Counter(
    "ws_watchdog_reconnects_total",
    "Number of full reconnects triggered by the silence watchdog, by channel type",
    ["domain", "channel_type", "coin"],
)

WS_DECODE_ERRORS_TOTAL = Counter(
    "ws_decode_errors_total",
    "Number of raw frames that failed JSON decoding",
    ["domain"],
)


def _channel_type(channel: str) -> str:
    # channel looks like "public:orderbook-BTCUSDT" or "public:candle-BTCUSDT-60"
    body = channel.split(":", 1)[-1]
    return body.split("-", 1)[0] if body else "other"


def _channel_coin(channel: str) -> str:
    parts = channel.split("-")
    return parts[1] if len(parts) > 1 else "unknown"


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


def decode_multi_json(raw, domain_name="unknown"):
    decoder = json.JSONDecoder()
    raw = raw.strip()
    results = []

    while raw:
        try:
            obj, idx = decoder.raw_decode(raw)
            results.append(obj)
            raw = raw[idx:].lstrip()
        except Exception:
            WS_DECODE_ERRORS_TOTAL.labels(domain=domain_name).inc()
            break

    return results


async def ws_manager(domain_name, channels):
    channel_last_ts = {ch: time.time() for ch in channels}
    first_attempt = True

    while True:
        try:
            logger.info(f"[{domain_name}] connecting...")
            connect_started_at = time.time()

            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                ping_timeout=None
            ) as ws:
                await ws.send(json.dumps({"connect": {}, "id": 1}))
                await ws.recv()
                logger.info(f"[{domain_name}] handshake ok")

                sub_id = 2
                for ch in channels:
                    await ws.send(json.dumps({
                        "id": sub_id,
                        "subscribe": {"channel": ch}
                    }))
                    sub_id += 1
                    await asyncio.sleep(0.05)

                logger.info(f"[{domain_name}] subscribed {len(channels)} channels")

                WS_CONNECTION_UP.labels(domain=domain_name).set(1)
                WS_RECONNECTS_TOTAL.labels(
                    domain=domain_name,
                    reason="initial" if first_attempt else "exception",
                ).inc()
                first_attempt = False
                WS_HANDSHAKE_DURATION.labels(domain=domain_name).observe(
                    time.time() - connect_started_at
                )

                # reset silence clocks on every fresh connection
                now = time.time()
                for ch in channels:
                    channel_last_ts[ch] = now

                # watchdog runs as background task
                # doesn't interfere with yield
                watchdog = asyncio.create_task(
                    _channel_watchdog(ws, domain_name, channels, channel_last_ts)
                )

                try:
                    async for raw in ws:
                        if not raw:
                            continue

                        if raw.strip() == "{}":
                            await ws.send("{}")
                            continue

                        messages = decode_multi_json(raw, domain_name)

                        for msg in messages:
                            if not isinstance(msg, dict):
                                continue
                            if "push" not in msg:
                                continue

                            # update per-channel timestamp
                            channel = msg["push"].get("channel", "")
                            if channel in channel_last_ts:
                                channel_last_ts[channel] = time.time()

                            WS_MESSAGES_RECEIVED_TOTAL.labels(
                                domain=domain_name,
                                channel_type=_channel_type(channel),
                            ).inc()

                            yield msg

                finally:
                    watchdog.cancel()
                    await asyncio.gather(watchdog, return_exceptions=True)
                    WS_CONNECTION_UP.labels(domain=domain_name).set(0)

        except Exception as e:
            WS_CONNECTION_UP.labels(domain=domain_name).set(0)
            logger.warning(f"[{domain_name}] error: {e} — reconnecting in 2s")
            await asyncio.sleep(2)


def get_timeout(channel: str) -> int:
    if "candle" in channel:
        resolution = int(channel.split("-")[-1])
        # give 3x the resolution as timeout minimum
        return max(resolution * 3 * 60, 120)
    if "orderbook" in channel:
        return 30   # orderbook is always active
    if "trades" in channel:
        return 60   # trades can be quiet on low volume coins
    if "market-stats" in channel:
        return 120  # market stats updates infrequently
    return 60


async def _channel_watchdog(ws, domain_name, channels, channel_last_ts):
    while True:
        await asyncio.sleep(30)
        now = time.time()

        for ch in channels:
            silence = now - channel_last_ts.get(ch, 0)
            timeout = get_timeout(ch)

            WS_CHANNEL_SILENCE_SECONDS.labels(
                domain=domain_name,
                channel_type=_channel_type(ch),
                coin=_channel_coin(ch),
            ).set(silence)

            if silence > timeout:
                logger.warning(f"[{domain_name}] {ch} silent {int(silence)}s → full reconnect")
                WS_WATCHDOG_RECONNECTS_TOTAL.labels(
                    domain=domain_name,
                    channel_type=_channel_type(ch),
                    coin=_channel_coin(ch),
                ).inc()
                await ws.close()
                return
