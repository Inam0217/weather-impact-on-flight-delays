# Weather Impact on Flight Delays ✈️🌦️

**Goal:** Analyze how weather conditions impact flight delays by integrating flight data with hourly weather data.

---

## 🔹 Tech Stack
- **Python** (ETL scripts)
- **MySQL** (data storage)
- **Spark** (batch processing, joins)
- (Optional) Airflow (orchestration)

---

## 🔹 Project Flow
```mermaid
flowchart LR
A[Flights CSVs] -->|ETL| B[(airline_db.flights)]
C[Weather API/CSV] -->|ETL| D[(weather_db.weather_hourly)]
B --> E[v_flight_weather]
D --> E
E --> F[Analysis / Insights]

🔹 How to Run

Setup environment

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp configs/db.example.env .env


Setup MySQL schemas

Run sql/schema_airline_db.sql

Run sql/schema_weather_db.sql

Run ETL scripts

python etl/flights/extract_transform_load.py
python etl/weather/extract_transform_load.py


Create integrated view

-- sql/create_view_v_flight_weather.sql

🔹 Output

View: v_flight_weather

Example: Flights joined with weather conditions (rain_1h_mm, snow_1h_mm, wind_speed_ms, etc.)

🔹 Future Enhancements

Airport-wise dashboards
Delay KPIs per weather condition
Airflow DAG orchestration
Spark optimization for big data