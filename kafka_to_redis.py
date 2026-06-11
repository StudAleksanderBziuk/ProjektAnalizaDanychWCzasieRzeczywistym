from kafka import KafkaConsumer
import redis
import json

consumer = KafkaConsumer(
    "btc.prices",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode()),
    auto_offset_reset="latest",
    group_id="redis-writer"
)

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
print("Zapisuję ceny BTC do Redisa...")

for msg in consumer:
    d = msg.value
    r.setex(f"price:{d['symbol']}", 10, str(d["price"]))
    print(f"Redis: price:BTCUSDT = {d['price']}")