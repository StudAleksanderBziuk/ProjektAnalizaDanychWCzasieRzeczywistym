# CryptoWatch — Analiza Danych BTC w Czasie Rzeczywistym

System do analizy cen Bitcoina w czasie rzeczywistym oparty na Apache Kafka, Binance API, FastAPI i PostgreSQL.

## Architektura

```
Binance WebSocket
      │
      ▼
  Producer (Docker)
      │
      ▼
Apache Kafka ──────────────────────────────────┐
   btc.prices                             btc.ohlcv
      │                                        │
      ├──► kafka-to-redis (Docker) ──► Redis   │
      │         │                              │
      │         ▼                              │
      │    FastAPI /price/{coin}               │
      │                                        │
      └──► statistics_consumer (lokalnie) ◄───┘
                │
                ▼
         btc_statistics.csv
```

## Wymagania

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.11+
- pip

## Uruchomienie

### 1. Sklonuj repozytorium

```bash
git clone <repo-url>
cd ProjektAnalizaDanychWCzasieRzeczywistym
```

### 2. Uruchom cały stack Dockerem

```bash
docker compose up --build
```

> Pierwsze uruchomienie pobiera obrazy (~2 GB) — zajmuje kilka minut.
> Kolejne starty: `docker compose up`

Poczekaj aż zobaczysz w logach:
```
btc_producer | [PRODUCER] Połączono z Kafka
btc_producer | [TRADE]  104823.50 USDT  qty=0.000041
```

### 3. Uruchom statistics consumer (lokalnie)

W osobnym terminalu:

```bash
pip install kafka-python requests
python3 statistics_consumer/consumer_stats.py
```

Statystyki pojawią się po ~60 sekundach (wypełnienie okna czasowego).

## Serwisy po uruchomieniu

| Serwis | Adres | Opis |
|---|---|---|
| FastAPI | http://localhost:8000/docs | REST API + Swagger UI |
| Kafka UI | http://localhost:8080 | Przeglądarka topików Kafka |
| PostgreSQL | localhost:5432 | Baza danych |
| Redis | localhost:6379 | Cache cen live |

## Topiki Kafka

| Topik | Zawartość | Częstotliwość |
|---|---|---|
| `btc.prices` | Każdy trade tick | ~co 100ms |
| `btc.ohlcv` | Zamknięte świece 1m | co minutę |
| `btc.alerts` | Wykryte anomalie cenowe | przy skoku |

## API Endpoints

### POST /register
```json
{
  "email": "user@example.com",
  "password": "haslo123"
}
```

### POST /login
```json
{
  "email": "user@example.com",
  "password": "haslo123"
}
```
Zwraca `access_token` JWT.

### GET /price/{coin}
```
GET /price/BTCUSDT
```
Zwraca aktualną cenę z Redis cache (odświeżana co ~10s).

## Format danych Kafka (dla innych modułów)

### btc.prices
```json
{
  "symbol": "BTCUSDT",
  "price": 104823.50,
  "quantity": 0.000041,
  "timestamp": 1748095200000,
  "buyer_maker": false
}
```

### btc.ohlcv
```json
{
  "symbol": "BTCUSDT",
  "open": 104800.00,
  "high": 104850.00,
  "low": 104790.00,
  "close": 104823.50,
  "volume": 1.243,
  "trades": 312,
  "open_time": 1748095140000,
  "close_time": 1748095199999
}
```

## Połączenie z bazą danych

**Terminal (psql):**
```bash
docker exec -it projektanalizadanychwczasierzeczywistym-db-1 psql -U postgres -d cryptowatch
```

**DBeaver / pgAdmin:**
- Host: `localhost`
- Port: `5432`
- Database: `cryptowatch`
- Username: `postgres`
- Password: `twojehaslo`

## Statistics Consumer

Skrypt `statistics_consumer/consumer_stats.py` czyta z topiku `btc.prices` i co 5 sekund:
- wyświetla statystyki (średnia, min, max, wolumen, ATH, ATL)
- pobiera Fear & Greed Index z alternative.me
- zapisuje dane do `btc_statistics.csv`

Plik CSV można podłączyć bezpośrednio do Power BI.

**Ważne:** upewnij się że w pliku `consumer_stats.py` jest:
```python
BOOTSTRAP_SERVERS = "localhost:9092"
```

### System Alertów (Discord Webhooks)

Skrypt `notebooks/System alertów.ipynb` działa w czasie rzeczywistym i nasłuchuje topiku `btc.prices`. 
Pozwala na dynamiczne dodawanie reguł cenowych (np. spadek poniżej lub wzrost powyżej zadanej kwoty) do lokalnej bazy danych. Gdy nadchodząca z Kafki cena spełni warunek zapisany w bazie, system automatycznie generuje i wysyła powiadomienie na dedykowany kanał Discord przy pomocy Webhooka.

**Uruchomienie:**
Otwórz plik `System alertów.ipynb` w Jupyter Notebook i wykonaj wszystkie komórki. Zmiana parametrów w komórce z `AlertRule` pozwala na dodawanie nowych progów w locie.

## Zatrzymanie

```bash
docker compose down          # zatrzymuje kontenery
docker compose down -v       # zatrzymuje + czyści dane Kafka i bazy
```

## Znane problemy i rozwiązania

| Problem | Rozwiązanie |
|---|---|
| `command not found: docker-compose` | Użyj `docker compose` (bez myślnika) |
| `error getting credentials` | Uruchom: `echo '{}' > ~/.docker/config.json` |
| `NoBrokersAvailable` w statistics_consumer | Upewnij się że `BOOTSTRAP_SERVERS = "localhost:9092"` |
| Port 8000 zajęty | Sprawdź `lsof -i :8000` i zakończ zbędne procesy |
| 500 error przy /register | Sprawdź czy `bcrypt==4.0.1` jest w requirements.txt |

## Struktura projektu

```
ProjektAnalizaDanychWCzasieRzeczywistym/
├── docker-compose.yml          # cały stack
├── Dockerfile.api              # obraz FastAPI
├── main.py                     # FastAPI endpointy
├── models.py                   # modele SQLAlchemy
├── database.py                 # połączenie z PostgreSQL
├── auth.py                     # JWT + bcrypt
├── requirements.txt            # zależności FastAPI
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             # Binance WS → Kafka
├── kafka_to_redis/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── kafka_to_redis.py       # Kafka → Redis bridge
├── statistics_consumer/
│   ├── consumer_stats.py       # statystyki → CSV
│   └── README_stats_consumer.md
└── notebooks/
        ├── BTC_Analysis.ipynb      # analiza w Jupyter
        └── System alertów.ipynb    # alerty na Discord
```

## Zespół

| Osoba | Moduł |
|---|---|
| Osoba 1 | Producer — Binance API + Kafka |
| Osoba 2 | Kafka Consumer + Pandas/NumPy |
| Osoba 3 | System alertów |
| Osoba 4 | Backend API + Baza danych + Auth |
| Osoba 5 | Analityki + Statistics Consumer + Power BI |
| Osoba 6 | DevOps + Docker + Prezentacja |
