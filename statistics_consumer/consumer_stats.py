import csv
import json
import os
import statistics
import time
from collections import deque
from datetime import datetime

import requests
from kafka import KafkaConsumer


TOPIC = "btc.prices"
BOOTSTRAP_SERVERS = "localhost:9092"

WINDOW_SECONDS = 60
PRINT_INTERVAL_SECONDS = 5
FEAR_GREED_REFRESH_SECONDS = 3600

CSV_FILE = "btc_statistics.csv"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="franek-btc-analytics-3",
    api_version=(2, 8, 0)
)

trades = deque()

ath_price = None
ath_time = None
atl_price = None
atl_time = None

largest_trades = []
largest_price_moves = []
previous_price = None

fear_greed = {
    "value": None,
    "classification": None,
    "timestamp": None
}
last_fear_greed_fetch = 0.0
last_print_time = 0.0


def get_fear_greed_index() -> dict:
    try:
        response = requests.get(
            "https://api.alternative.me/fng/",
            timeout=10
        )
        response.raise_for_status()
        data = response.json()["data"][0]

        return {
            "value": int(data["value"]),
            "classification": data["value_classification"],
            "timestamp": datetime.fromtimestamp(
                int(data["timestamp"])
            ).isoformat(timespec="seconds")
        }

    except Exception as error:
        print(f"Nie udało się pobrać Fear & Greed Index: {error}")
        return fear_greed


def ensure_csv_exists() -> None:
    if os.path.exists(CSV_FILE):
        return

    with open(CSV_FILE, mode="w", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "generated_at",
                "window_seconds",
                "trade_count",
                "average_price",
                "weighted_average_price",
                "min_price",
                "max_price",
                "price_range",
                "price_std",
                "total_volume",
                "percentage_change",
                "ath_price",
                "ath_time",
                "atl_price",
                "atl_time",
                "largest_trade_quantity",
                "largest_trade_value",
                "largest_price_move",
                "fear_greed_value",
                "fear_greed_classification",
                "fear_greed_timestamp"
            ]
        )
        writer.writeheader()


def save_to_csv(stats: dict) -> None:
    with open(CSV_FILE, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=stats.keys())
        writer.writerow(stats)


def remove_old_trades(current_time: float) -> None:
    threshold = current_time - WINDOW_SECONDS

    while trades and trades[0]["timestamp_seconds"] < threshold:
        trades.popleft()


def update_ath_atl(price: float, timestamp_seconds: float) -> None:
    global ath_price, ath_time, atl_price, atl_time

    event_time = datetime.fromtimestamp(timestamp_seconds).isoformat(timespec="seconds")

    if ath_price is None or price > ath_price:
        ath_price = price
        ath_time = event_time

    if atl_price is None or price < atl_price:
        atl_price = price
        atl_time = event_time


def update_rankings(trade: dict) -> None:
    global previous_price

    trade_value = trade["price"] * trade["quantity"]

    largest_trades.append({
        "price": trade["price"],
        "quantity": trade["quantity"],
        "trade_value": trade_value
    })

    largest_trades.sort(key=lambda x: x["trade_value"], reverse=True)
    del largest_trades[5:]

    if previous_price is not None:
        move = trade["price"] - previous_price

        largest_price_moves.append({
            "previous_price": previous_price,
            "current_price": trade["price"],
            "move": move,
            "abs_move": abs(move)
        })

        largest_price_moves.sort(key=lambda x: x["abs_move"], reverse=True)
        del largest_price_moves[5:]

    previous_price = trade["price"]


def calculate_statistics() -> dict | None:
    if not trades:
        return None

    prices = [trade["price"] for trade in trades]
    quantities = [trade["quantity"] for trade in trades]

    total_volume = sum(quantities)

    weighted_average_price = (
        sum(price * quantity for price, quantity in zip(prices, quantities))
        / total_volume
        if total_volume > 0
        else None
    )

    percentage_change = (
        (prices[-1] - prices[0]) / prices[0] * 100
        if prices[0] != 0
        else None
    )

    largest_trade = largest_trades[0] if largest_trades else {}
    largest_move = largest_price_moves[0] if largest_price_moves else {}

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window_seconds": WINDOW_SECONDS,
        "trade_count": len(prices),
        "average_price": statistics.fmean(prices),
        "weighted_average_price": weighted_average_price,
        "min_price": min(prices),
        "max_price": max(prices),
        "price_range": max(prices) - min(prices),
        "price_std": statistics.pstdev(prices) if len(prices) > 1 else 0.0,
        "total_volume": total_volume,
        "percentage_change": percentage_change,
        "ath_price": ath_price,
        "ath_time": ath_time,
        "atl_price": atl_price,
        "atl_time": atl_time,
        "largest_trade_quantity": largest_trade.get("quantity"),
        "largest_trade_value": largest_trade.get("trade_value"),
        "largest_price_move": largest_move.get("move"),
        "fear_greed_value": fear_greed["value"],
        "fear_greed_classification": fear_greed["classification"],
        "fear_greed_timestamp": fear_greed["timestamp"]
    }


def print_rankings() -> None:
    print("\nTOP 5 największych transakcji:")
    for i, trade in enumerate(largest_trades, start=1):
        print(
            f"{i}. value={trade['trade_value']:.2f} USDT, "
            f"price={trade['price']:.2f}, qty={trade['quantity']:.6f}"
        )

    print("\nTOP 5 największych zmian ceny:")
    for i, move in enumerate(largest_price_moves, start=1):
        print(
            f"{i}. move={move['move']:.2f} USDT, "
            f"from={move['previous_price']:.2f}, to={move['current_price']:.2f}"
        )


ensure_csv_exists()

print(f"Nasłuchiwanie topicu: {TOPIC}")
print(f"Eksport CSV: {CSV_FILE}")

for message in consumer:
    print(f"DEBUG otrzymano: {message.value['price']}", flush=True)
    now = time.time()

    if now - last_fear_greed_fetch >= FEAR_GREED_REFRESH_SECONDS:
        fear_greed = get_fear_greed_index()
        last_fear_greed_fetch = now

    raw_trade = message.value

    trade = {
        "timestamp_seconds": float(raw_trade["timestamp"]) / 1000,
        "price": float(raw_trade["price"]),
        "quantity": float(raw_trade["quantity"])
    }

    trades.append(trade)
    update_ath_atl(trade["price"], trade["timestamp_seconds"])
    update_rankings(trade)

    remove_old_trades(now)

    if now - last_print_time >= PRINT_INTERVAL_SECONDS:
        stats = calculate_statistics()

        if stats is not None:
            print("\nSTATYSTYKI:")
            print(json.dumps(stats, indent=2))
            print_rankings()
            save_to_csv(stats)

        last_print_time = now
