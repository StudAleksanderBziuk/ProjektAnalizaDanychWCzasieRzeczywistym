"""
Kafka → Redis bridge
Czyta btc.prices z Kafki i zapisuje aktualną cenę do Redisa.
FastAPI odczytuje tę wartość przez endpoint /price/{coin}.
"""
import json
import os
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import redis

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_HOST      = os.getenv("REDIS_HOST", "localhost")


def make_consumer(retries=20, delay=5):
    for attempt in range(1, retries + 1):
        try:
            c = KafkaConsumer(
                "btc.prices",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode()),
                auto_offset_reset="latest",
                group_id="redis-writer"
            )
            print(f"[REDIS-BRIDGE] Kafka connected", flush=True)
            return c
        except NoBrokersAvailable:
            print(f"[REDIS-BRIDGE] Kafka not ready – {attempt}/{retries}, retry in {delay}s", flush=True)
            time.sleep(delay)
    raise RuntimeError("Cannot connect to Kafka")


r        = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
consumer = make_consumer()

print(f"[REDIS-BRIDGE] Zapisuję ceny BTC do Redisa ({REDIS_HOST})...", flush=True)

for msg in consumer:
    d = msg.value
    r.setex(f"price:{d['symbol']}", 10, str(d["price"]))
    print(f"[REDIS-BRIDGE] price:{d['symbol']} = {d['price']}", flush=True)
