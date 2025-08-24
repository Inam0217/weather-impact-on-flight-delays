import os
import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "pass"),
    "database": os.getenv("DB_NAME", "airline_db"),
}

_pool = None

def get_conn():
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(pool_name="wf_pool", pool_size=5, **DB_CONFIG)
    return _pool.get_connection()

def init_schema():
    ddl = """
    CREATE TABLE IF NOT EXISTS flights (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        flight_date DATE,
        airline VARCHAR(8),
        flight_number VARCHAR(16),
        origin VARCHAR(8),
        dest VARCHAR(8),
        sched_dep_time DATETIME,
        dep_delay_mins INT,
        sched_arr_time DATETIME,
        arr_delay_mins INT
    );
    CREATE TABLE IF NOT EXISTS weather_hourly (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        airport VARCHAR(8),
        ts_hour DATETIME,
        temp_c FLOAT,
        wind_speed_ms FLOAT,
        wind_deg FLOAT,
        visibility_m FLOAT,
        humidity FLOAT,
        pressure_hpa FLOAT,
        weather_main VARCHAR(32),
        weather_desc VARCHAR(64),
        rain_1h_mm FLOAT,
        snow_1h_mm FLOAT,
        UNIQUE KEY uniq_airport_hour (airport, ts_hour)
    );
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
        conn.commit()
    finally:
        conn.close()
