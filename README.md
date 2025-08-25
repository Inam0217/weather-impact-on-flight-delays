# Weather Impact on Flight Delays

Quantify how hourly **weather conditions** affect **flight departure delays**. This portfolio project builds two small datasets (Flights + Hourly Weather), loads them into MySQL, and creates a join view to analyze delay impact by condition and airport. Optional folders are provided for Airflow and Spark extensions.

---

## Quickstart (Clone & Run)

> Pick the commands for your OS/shell.

### Windows (PowerShell)
```powershell
python -m venv .venv
.\.venv\Scriptsctivate
pip install -r requirements.txt

# If using env files (optional)
# Copy-Item configs\db.example.env .env

mysql -u root -p < sql\schema_airline_db.sql
mysql -u root -p < sql\schema_weather_db.sql

python etllights\extract_transform_load.py
python etl\weather\extract_transform_load.py

mysql -u root -p -e "source sql/create_view_v_flight_weather.sql"
```

### Windows (Git Bash / CMD)
```bash
python -m venv .venv
# Git Bash:
source .venv/Scripts/activate
# CMD alternative:
# .\.venv\Scriptsctivate
pip install -r requirements.txt

# cp configs/db.example.env .env  # optional

mysql -u root -p < sql/schema_airline_db.sql
mysql -u root -p < sql/schema_weather_db.sql

python etl/flights/extract_transform_load.py
python etl/weather/extract_transform_load.py

mysql -u root -p -e "source sql/create_view_v_flight_weather.sql"
```

### Linux / macOS (bash/zsh)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# cp configs/db.example.env .env  # optional

mysql -u root -p < sql/schema_airline_db.sql
mysql -u root -p < sql/schema_weather_db.sql

python etl/flights/extract_transform_load.py
python etl/weather/extract_transform_load.py

mysql -u root -p -e "source sql/create_view_v_flight_weather.sql"
```

---

## Data Sources

### Flights
- **What:** CSV with at least:
  - `origin` (IATA code, e.g., BOS)
  - `sched_dep_time` (ISO timestamp, e.g., `2015-12-31 13:45:00`)
  - `dep_delay_min` (integer minutes; negative allowed)
  - *(optional)* `flight_id` or primary key
- **Where:** Place under `data/flights/` (e.g., `data/flights/flights_2015_sample.csv`).
- **Timezone:** Timestamps should be **UTC** or converted to the **same timezone** used by weather (see below). The SQL join rounds to the **hour**.

### Weather (Hourly)
- **What:** Hourly weather per airport/station with at least:
  - `airport` (IATA code matching `origin`)
  - `ts_hour` (`YYYY-MM-DD HH:00:00`; top of hour)
  - `weather_main` (e.g., Clear, Rain, Snow)
  - `rain_1h_mm`, `snow_1h_mm`, `wind_speed_ms`, `pressure_hpa`
- **Where:** Place under `data/weather/` (e.g., `data/weather/BOS_2015_hourly.csv`).
- **Timezone:** `ts_hour` must align with the **same timezone** as `sched_dep_time` after hour-rounding.

### Reproducibility
- This repo is **data-source agnostic**: use any flights + hourly weather datasets as long as they match the required columns.
- After loading both tables, run:  
  `mysql -u root -p -e "source sql/create_view_v_flight_weather.sql"`  
  to create the join view `v_flight_weather`.

---

## Project Structure

```
.
├─ etl/
│  ├─ flights/                  # Scripts to ingest/transform flights CSVs
│  └─ weather/                  # Scripts to ingest/transform hourly weather CSVs
├─ sql/
│  ├─ schema_airline_db.sql     # Table(s) for flights
│  ├─ schema_weather_db.sql     # Table(s) for hourly weather
│  └─ create_view_v_flight_weather.sql  # Join view
├─ configs/                     # Optional env/config files
├─ data/
│  ├─ flights/                  # Place your flights CSVs here
│  └─ weather/                  # Place your hourly weather CSVs here
├─ airflow/                     # (Optional) DAG scaffolding
├─ spark/                       # (Optional) Spark job scaffolding
└─ README.md
```

---

## How It Works

1. **Ingest flights** CSV → MySQL `airline_db` (example name).
2. **Ingest hourly weather** CSV → MySQL `weather_db`.
3. **Create a View** `v_flight_weather` joining by:
   - `origin` ⇄ `airport`
   - `DATE_FORMAT(sched_dep_time, '%Y-%m-%d %H:00:00')` ⇄ `ts_hour`  
   *(Ensure both are aligned to the same timezone before the join.)*

---

## Example Queries

Average departure delay by `weather_main`:
```sql
SELECT weather_main,
       COUNT(*) AS n_flights,
       ROUND(AVG(dep_delay_min), 2) AS avg_dep_delay_min
FROM v_flight_weather
GROUP BY weather_main
ORDER BY avg_dep_delay_min DESC;
```

Worst hours at a specific airport (e.g., BOS):
```sql
SELECT DATE_FORMAT(sched_dep_time, '%Y-%m-%d %H:00:00') AS dep_hour,
       ROUND(AVG(dep_delay_min), 2) AS avg_dep_delay_min,
       COUNT(*) AS n_flights,
       MAX(weather_main) AS weather
FROM v_flight_weather
WHERE origin = 'BOS'
GROUP BY dep_hour
ORDER BY avg_dep_delay_min DESC
LIMIT 10;
```

**Recommended indexes** for speed:
```sql
-- On weather_hourly (or your weather table name)
CREATE INDEX idx_weather_airport_hour ON weather_hourly (airport, ts_hour);

-- On flights
CREATE INDEX idx_flights_sched_hour ON flights (sched_dep_time);
```

---

## Configuration & Environment

- If you keep credentials in env files, copy `configs/db.example.env` to `.env` and fill variables.
- Update MySQL connection settings in your ETL scripts (host, port, user, password, db).

---

## Troubleshooting

- **No rows in the view?** Check that:
  - `origin` IATA codes match `airport` codes.
  - `sched_dep_time` hour-rounding matches `ts_hour` exactly.
  - Both timestamps are in the same timezone (prefer UTC).

- **Import errors in Python:** Pin versions in `requirements.txt` (e.g. `pandas==2.2.2`, `mysql-connector-python==9.0.0`). Then re-run `pip install -r requirements.txt` inside the activated venv.

- **MySQL client not found:** Install MySQL or use a GUI (MySQL Workbench) to run the `.sql` files manually.

---

## Roadmap (Optional Enhancements)

- **Analytics polish:** Add a small KPI table + one chart screenshot to README.
- **Engineering polish:** Wire scripts into an Airflow DAG for a daily run.
- **Scale:** Optional Spark job to process larger time ranges into Parquet.

---

## License

MIT (or your preferred license).
