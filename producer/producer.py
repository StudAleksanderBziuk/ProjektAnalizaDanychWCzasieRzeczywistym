"""
BTC Producer — publiczne API Binance (ZERO kluczy API)
======================================================
Oficjalne publiczne URL-e Binance (bez logowania, bez konta):
  REST:   https://data-api.binance.vision/api/v3/klines
  WS:     wss://data-stream.binance.vision:443/ws/btcusdt@...

Topiki Kafka:
  btc.prices  – każdy trade tick (co ~100ms)
  btc.ohlcv   – zamknięte świece 1m
"""

import json
import os
import time
import threading

import requests
import websocket
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# ── Oficjalne publiczne URL-e Binance (nie wymagają klucza API) ────────
BASE_REST   = "https://data-api.binance.vision/api/v3"
WS_BASE     = "wss://data-stream.binance.vision:443/ws"
SYMBOL      = "BTCUSDT"
SYMBOL_LC   = SYMBOL.lower()

TRADE_URL   = f"{WS_BASE}/{SYMBOL_LC}@trade"
KLINE_URL   = f"{WS_BASE}/{SYMBOL_LC}@kline_1m"


# ── Kafka Producer z retry ─────────────────────────────────────────────
def make_producer(retries: int = 20, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[PRODUCER] Połączono z Kafka ({KAFKA_BOOTSTRAP})", flush=True)
            return p
        except NoBrokersAvailable:
            print(f"[PRODUCER] Kafka nie gotowa – próba {attempt}/{retries}, retry za {delay}s", flush=True)
            time.sleep(delay)
    raise RuntimeError("Nie można połączyć z Kafka po wszystkich próbach")


producer = make_producer()


# ══════════════════════════════════════════════════════════════════════
# 1. REST — historyczne świece przy starcie (ostatnie 100 świec 1m)
# ══════════════════════════════════════════════════════════════════════

def fetch_historical_klines(symbol: str = SYMBOL,
                             interval: str = "1m",
                             limit: int = 100):
    """
    Pobiera historyczne świece przez REST (data-api.binance.vision)
    i wysyła do topiku btc.ohlcv — bez żadnego klucza API.
    """
    url    = f"{BASE_REST}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        klines = resp.json()
    except Exception as e:
        print(f"[REST] Błąd pobierania klines: {e}", flush=True)
        return

    for k in klines:
        payload = {
            "symbol":     symbol,
            "open":       float(k[1]),
            "high":       float(k[2]),
            "low":        float(k[3]),
            "close":      float(k[4]),
            "volume":     float(k[5]),
            "trades":     int(k[8]),
            "open_time":  k[0],
            "close_time": k[6],
        }
        producer.send("btc.ohlcv", value=payload)

    print(f"[REST] Wysłano {len(klines)} historycznych świec → btc.ohlcv", flush=True)


# ══════════════════════════════════════════════════════════════════════
# 2. WebSocket — live ticki → btc.prices
# ══════════════════════════════════════════════════════════════════════

def on_trade(ws, raw):
    msg = json.loads(raw)
    payload = {
        "symbol":      msg["s"],
        "price":       float(msg["p"]),
        "quantity":    float(msg["q"]),
        "timestamp":   msg["T"],
        "buyer_maker": msg["m"],   # True = sprzedaż, False = kupno
    }
    producer.send("btc.prices", value=payload)
    print(f"[TRADE]  {payload['price']:.2f} USDT  qty={payload['quantity']:.6f}", flush=True)


# ══════════════════════════════════════════════════════════════════════
# 3. WebSocket — zamknięte świece 1m → btc.ohlcv
# ══════════════════════════════════════════════════════════════════════

def on_kline(ws, raw):
    msg = json.loads(raw)
    k = msg["k"]
    if not k["x"]:   # x=True tylko gdy świeca jest ZAMKNIĘTA
        return
    payload = {
        "symbol":     k["s"],
        "open":       float(k["o"]),
        "high":       float(k["h"]),
        "low":        float(k["l"]),
        "close":      float(k["c"]),
        "volume":     float(k["v"]),
        "trades":     int(k["n"]),
        "open_time":  k["t"],
        "close_time": k["T"],
    }
    producer.send("btc.ohlcv", value=payload)
    print(
        f"[KLINE]  O={payload['open']:.2f}  H={payload['high']:.2f}  "
        f"L={payload['low']:.2f}  C={payload['close']:.2f}  "
        f"Vol={payload['volume']:.3f}  Trades={payload['trades']}",
        flush=True,
    )


def on_error(ws, error):
    print(f"[WS ERROR] {error}", flush=True)


def on_close(ws, *_):
    print("[WS] Rozłączono – ponowne połączenie za 5s…", flush=True)
    time.sleep(5)


def run_stream(url: str, handler):
    """Pętla WebSocket z automatycznym reconnect."""
    while True:
        ws = websocket.WebSocketApp(
            url,
            on_message=handler,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
        time.sleep(5)


# ── Main ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("[PRODUCER] Start — publiczne API Binance (bez klucza)", flush=True)
    print(f"[PRODUCER] REST:  {BASE_REST}", flush=True)
    print(f"[PRODUCER] WS:    {WS_BASE}", flush=True)

    # 1. Historyczne świece na start
    fetch_historical_klines(limit=100)

    # 2. Live streamy w osobnych wątkach
    t1 = threading.Thread(target=run_stream, args=(TRADE_URL, on_trade),  daemon=True, name="trade-ws")
    t2 = threading.Thread(target=run_stream, args=(KLINE_URL, on_kline), daemon=True, name="kline-ws")
    t1.start()
    t2.start()
    t1.join()
    t2.join()
