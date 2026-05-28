## Wymagania

Zainstaluj **Docker Desktop**:
- Windows / Mac: https://www.docker.com/products/docker-desktop/
- Linux: https://docs.docker.com/engine/install/

## Uruchomienie

```bash
git clone https://github.com/StudAleksanderBziuk/ProjektAnalizaDanychWCzasieRzeczywistym.git
cd btc-kafka
docker compose up --build
```

Pierwsze uruchomienie pobiera obrazy (~1.5 GB) — kilka minut.  
Kolejne starty: `docker compose up`

## Sprawdzenie czy działa

**Logi w terminalu** — powinny lecieć co kilka sekund:
```
btc_producer | [TRADE]  104823.50 USDT  qty=0.000041
btc_producer | [TRADE]  104821.20 USDT  qty=0.000180
btc_producer | [KLINE]  O=104800.00  H=104850.00  L=104790.00  C=104823.50
```

**Kafka UI** → http://localhost:8080 → Topics → btc.prices → Messages

## Topiki Kafka (dla reszty grupy)

### btc.prices — każdy trade (~co 100ms)
```json
{
  "symbol":      "BTCUSDT",
  "price":       104823.50,
  "quantity":    0.000041,
  "timestamp":   1748095200000,
  "buyer_maker": false
}
```

### btc.ohlcv — zamknięta świeca 1m + 100 historycznych na start
```json
{
  "symbol":     "BTCUSDT",
  "open":       104800.00,
  "high":       104850.00,
  "low":        104790.00,
  "close":      104823.50,
  "volume":     1.243,
  "trades":     312,
  "open_time":  1748095140000,
  "close_time": 1748095199999
}
```

### btc.alerts — 

## Jak podłączyć się jako Consumer (dla reszty grupy)

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "btc.prices",                 # lub "btc.ohlcv"
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="osoba-X"            # każda osoba daje unikalne ID
)

for msg in consumer:
    d = msg.value
    print(d["price"])
```

## Zatrzymanie

```bash
docker compose down        # zatrzymuje
docker compose down -v     # zatrzymuje + czyści dane Kafka
```

## Struktura

```
btc-kafka/
├── docker-compose.yml
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
└── notebooks/
    └── BTC_Analysis.ipynb
```
