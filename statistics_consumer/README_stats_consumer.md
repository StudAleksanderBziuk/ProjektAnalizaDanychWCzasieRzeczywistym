# Moduł Statistics Consumer

Moduł `statistics_consumer` odpowiada za analizę danych przesyłanych przez Apache Kafka w czasie rzeczywistym. Odczytuje dane z topicu `btc.prices`, oblicza statystyki dla ostatnich 60 sekund transakcji oraz zapisuje wyniki do pliku CSV.

## Funkcjonalności

* Średnia cena BTC z ostatnich 60 sekund
* Średnia cena ważona wolumenem
* Minimalna i maksymalna cena
* Odchylenie standardowe ceny
* Łączny wolumen transakcji
* Zmiana procentowa ceny
* ATH (All Time High od uruchomienia aplikacji)
* ATL (All Time Low od uruchomienia aplikacji)
* Ranking 5 największych transakcji
* Ranking 5 największych zmian ceny
* Fear & Greed Index
* Eksport wyników do pliku `btc_statistics.csv`

## Wymagania

* Python 3.10+
* Uruchomiona infrastruktura Docker z Kafka
* Dostęp do topicu `btc.prices`

## Uruchomienie projektu

Najpierw uruchom infrastrukturę Kafka:

```bash
docker compose up -d
```

Sprawdź status kontenerów:

```bash
docker compose ps
```

Powinny działać co najmniej:

```text
zookeeper
kafka
kafka-ui
producer
```

## Utworzenie środowiska Python

Przejdź do katalogu projektu:

```bash
cd statistics_consumer
```

Utwórz środowisko wirtualne:

### Linux (bash / zsh)

```bash
python3 -m venv btc_stats_env
source btc_stats_env/bin/activate
```

### Linux (fish)

```fish
python3 -m venv btc_stats_env
source btc_stats_env/bin/activate.fish
```

### Windows

```powershell
python -m venv btc_stats_env
btc_stats_env\Scripts\activate
```

## Instalacja zależności

```bash
pip install kafka-python requests
```

## Uruchomienie modułu

```bash
python btc_analytics.py
```

Po uruchomieniu aplikacja rozpocznie nasłuchiwanie wiadomości z topicu:

```text
btc.prices
```

Przykładowy komunikat startowy:

```text
Nasłuchiwanie topicu: btc.prices
Eksport CSV: btc_statistics.csv
```

## Wyniki

Co 5 sekund aplikacja wyświetla statystyki dla ostatnich 60 sekund danych:

```json
{
  "trade_count": 412,
  "average_price": 104823.81,
  "weighted_average_price": 104824.12,
  "min_price": 104810.20,
  "max_price": 104835.70,
  "price_std": 4.32,
  "total_volume": 0.38421,
  "percentage_change": 0.0124
}
```

Dodatkowo prezentowane są:

* TOP 5 największych transakcji
* TOP 5 największych zmian ceny
* aktualny ATH
* aktualny ATL
* Fear & Greed Index

## Eksport CSV

Statystyki są automatycznie dopisywane do pliku:

```text
btc_statistics.csv
```

Plik można później wykorzystać w:

* Excel
* LibreOffice Calc
* Power BI
* Python Notebook
* Tableau

## Zatrzymanie aplikacji

W terminalu:

```text
CTRL + C
```

## Zatrzymanie infrastruktury Kafka

W katalogu głównym projektu:

```bash
docker compose down
```
