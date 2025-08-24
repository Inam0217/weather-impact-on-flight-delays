# Weather Impact on Flight Delays (ETL → Airflow → Spark)

A two-phase portfolio project:
1) **Baseline ETL + Airflow**: ingest flights (Kaggle CSV) and weather (OpenWeather API) into MySQL/Postgres, then build a joined table.
2) **Spark Upgrade**: scale the same pipeline with PySpark and compute aggregations.

## Repository Structure
```
weather_flight_delay/
├─ airflow/
│  └─ dags/
│     └─ flight_weather_dag.py
├─ configs/
│  ├─ db.example.env
│  ├─ openweather.env.example
│  └─ airports.csv
├─ data/
│  ├─ raw/         # Put Kaggle flights CSV here (e.g., flights_2018.csv)
│  └─ processed/   # Cleaned artifacts
├─ etl/
│  ├─ common/
│  │  ├─ db.py
│  │  └─ utils.py
│  ├─ flights/
│  │  └─ extract_transform_load.py
│  └─ weather/
│     └─ extract_transform_load.py
├─ spark/
│  └─ jobs/
│     └─ join_and_agg.py
├─ requirements.txt
└─ README.md
```

## Quickstart

### 1) Python environment
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp configs/db.example.env .env
cp configs/openweather.env.example .env.openweather
```

### 2) Configure credentials
Edit **.env** to point to your DB, and **.env.openweather** to set the API key.

### 3) Add data
- Put a Kaggle flights CSV into `data/raw/` (e.g., `data/raw/flights_2018.csv`).
- `configs/airports.csv` contains airport-code → lat/lon mappings used for weather lookups. Add more as needed.

### 4) Run ETL locally (baseline)
```bash
python etl/flights/extract_transform_load.py --csv data/raw/flights_2018.csv
python etl/weather/extract_transform_load.py --date 2018-01-01 --airport JFK
```
> The weather script supports repeated runs per day/airport. Add a simple loop or use Airflow to orchestrate.

### 5) Airflow (optional at this stage)
Use `airflow/dags/flight_weather_dag.py` as a reference DAG that calls these scripts. (You can plug this into your existing Docker-based Airflow setup.)

### 6) Spark upgrade
Use `spark/jobs/join_and_agg.py` to read from MySQL via JDBC and produce joined/aggregated outputs.

---

## Portfolio Notes
- Start with Baseline ETL to get results quickly.
- Then show scalability with the Spark job and add two or three meaningful aggregations:
  - Delay minutes vs. precipitation/wind bins
  - Weather sensitivity by airport
  - Seasonal effect on average delay
