# etl/weather/extract_transform_load.py — Meteostat-backed (with NaN handling, date-range + multi-airports)

import argparse
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector
from meteostat import Hourly, Point
from pytz import timezone

from etl.common.db import get_conn, init_schema

COCO_MAP = {
    0: ("Clear", "Clear sky"),
    1: ("Mainly clear", "Mainly clear"),
    2: ("Partly cloudy", "Partly cloudy"),
    3: ("Overcast", "Overcast"),
    4: ("Fog", "Fog"),
    5: ("Depositing rime fog", "Rime fog"),
    6: ("Light drizzle", "Light drizzle"),
    7: ("Drizzle", "Drizzle"),
    8: ("Dense drizzle", "Dense drizzle"),
    9: ("Light freezing drizzle", "Light freezing drizzle"),
    10: ("Freezing drizzle", "Freezing drizzle"),
    11: ("Dense freezing drizzle", "Dense freezing drizzle"),
    12: ("Slight rain", "Slight rain"),
    13: ("Rain", "Rain"),
    14: ("Heavy rain", "Heavy rain"),
    15: ("Light freezing rain", "Light freezing rain"),
    16: ("Freezing rain", "Freezing rain"),
    17: ("Heavy freezing rain", "Heavy freezing rain"),
    18: ("Slight snow", "Slight snow"),
    19: ("Snow", "Snow"),
    20: ("Heavy snow", "Heavy snow"),
    21: ("Snow grains", "Snow grains"),
    22: ("Slight rain showers", "Slight rain showers"),
    23: ("Rain showers", "Rain showers"),
    24: ("Violent rain showers", "Violent rain showers"),
    25: ("Slight snow showers", "Slight snow showers"),
    26: ("Snow showers", "Snow showers"),
    27: ("Thunderstorm", "Thunderstorm"),
    28: ("Thunderstorm with slight hail", "Thunderstorm with slight hail"),
    29: ("Thunderstorm with hail", "Thunderstorm with hail"),
}


def kmh_to_ms(x):
    try:
        return float(x) / 3.6 if pd.notna(x) else None
    except Exception:
        return None


def safe_val(v):
    if pd.isna(v) or v is pd.NaT:
        return None
    return v


def load_airports(config_csv: str = "configs/airports.csv"):
    df = pd.read_csv(config_csv, comment="#", header=0)
    df.columns = [c.strip() for c in df.columns]

    need = {"IATA", "Lat", "Lon", "Timezone"}
    missing = need - set(df.columns)
    if missing:
        raise SystemExit(f"airports.csv missing columns: {missing}. Have: {list(df.columns)}")

    out = {}
    for _, r in df.iterrows():
        code = str(r["IATA"]).strip().upper()
        out[code] = (float(r["Lat"]), float(r["Lon"]), str(r["Timezone"]).strip())
    return out


def fetch_hourly_for_day_meteostat(lat: float, lon: float, tz: str, date_str: str) -> pd.DataFrame:
    """
    Fetch one local day of hourly weather from Meteostat and return a clean DataFrame
    with tz-naive local hourly timestamps. Handles duplicate hours and avoids index
    alignment issues by assigning numpy arrays.
    """
    day = datetime.fromisoformat(date_str)
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    point = Point(lat, lon)
    df = Hourly(point, start, end).fetch()

    # Empty day → return schema with no rows
    if df.empty:
        return pd.DataFrame(columns=[
            "ts_hour", "temp_c", "wind_speed_ms", "wind_deg", "visibility_m",
            "humidity", "pressure_hpa", "weather_main", "weather_desc",
            "rain_1h_mm", "snow_1h_mm"
        ])

    # 1) Remove duplicate timestamps (can happen with source merges)
    df = df.loc[~df.index.duplicated(keep="first")]

    # 2) Normalize timezone: UTC -> local tz -> tz-naive
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    local_tz = timezone(tz)
    df.index = df.index.tz_convert(local_tz)
    df.index = df.index.tz_localize(None)

    # 3) Build output using numpy to avoid index alignment reindex errors
    out = pd.DataFrame()
    out["ts_hour"] = df.index
    out["temp_c"] = df["temp"].to_numpy() if "temp" in df.columns else None
    out["wind_speed_ms"] = (df["wspd"].to_numpy() / 3.6) if "wspd" in df.columns else None
    out["wind_deg"] = df["wdir"].to_numpy() if "wdir" in df.columns else None
    out["visibility_m"] = None
    out["humidity"] = None
    out["pressure_hpa"] = df["pres"].to_numpy() if "pres" in df.columns else None

    if "coco" in df.columns:
        coco_np = df["coco"].to_numpy()
        out["weather_main"] = [
            COCO_MAP.get(int(x), (None, None))[0] if pd.notna(x) else None
            for x in coco_np
        ]
        out["weather_desc"] = [
            COCO_MAP.get(int(x), (None, None))[1] if pd.notna(x) else None
            for x in coco_np
        ]
    else:
        out["weather_main"] = None
        out["weather_desc"] = None

    out["rain_1h_mm"] = df["prcp"].to_numpy() if "prcp" in df.columns else None
    out["snow_1h_mm"] = df["snow"].to_numpy() if "snow" in df.columns else None

    return out

def _batched(iterable, size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def load_weather(df: pd.DataFrame, airport_code: str, batch_size: int = 2000) -> int:
    if df.empty:
        return 0

    rows_iter = df[[
        "ts_hour","temp_c","wind_speed_ms","wind_deg","visibility_m",
        "humidity","pressure_hpa","weather_main","weather_desc","rain_1h_mm","snow_1h_mm"
    ]].itertuples(index=False, name=None)

    sql = (
        """
        INSERT INTO weather_hourly
        (airport, ts_hour, temp_c, wind_speed_ms, wind_deg, visibility_m,
         humidity, pressure_hpa, weather_main, weather_desc, rain_1h_mm, snow_1h_mm)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
          temp_c=VALUES(temp_c),
          wind_speed_ms=VALUES(wind_speed_ms),
          wind_deg=VALUES(wind_deg),
          visibility_m=VALUES(visibility_m),
          humidity=VALUES(humidity),
          pressure_hpa=VALUES(pressure_hpa),
          weather_main=VALUES(weather_main),
          weather_desc=VALUES(weather_desc),
          rain_1h_mm=VALUES(rain_1h_mm),
          snow_1h_mm=VALUES(snow_1h_mm)
        """
    )

    inserted = 0
    conn = get_conn()
    try:
        cur = conn.cursor()
        for chunk in _batched(rows_iter, batch_size):
            values = [(airport_code, *[safe_val(v) for v in row]) for row in chunk]
            cur.executemany(sql, values)
            conn.commit()
            inserted += len(values)
        return inserted
    except mysql.connector.Error as e:
        raise RuntimeError(f"MySQL error while inserting weather rows (inserted={inserted}): {e}") from e
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Single date YYYY-MM-DD (local date at airport)")
    parser.add_argument("--start-date", help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--airport", help="Single IATA code, e.g., JFK")
    parser.add_argument("--airports", help="Comma-separated IATA list, e.g., ATL,ORD,JFK")
    parser.add_argument("--batch-size", type=int, default=2000, help="MySQL insert batch size")
    args = parser.parse_args()

    init_schema()

    airports_map = load_airports()

    # Build airport codes list
    codes = []
    if args.airports:
        codes.extend([c.strip().upper() for c in args.airports.split(",") if c.strip()])
    if args.airport:
        codes.append(args.airport.strip().upper())
    # de-duplicate
    codes = list(dict.fromkeys(codes))

    if not codes:
        raise SystemExit("Must provide --airport or --airports")

    missing = [c for c in codes if c not in airports_map]
    if missing:
        raise SystemExit(f"These airports are not in configs/airports.csv: {missing}")

    # Determine date list
    if args.date:
        dates = [args.date]
    elif args.start_date and args.end_date:
        start = datetime.fromisoformat(args.start_date)
        end = datetime.fromisoformat(args.end_date)
        dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end - start).days + 1)]
    else:
        raise SystemExit("Must provide either --date or both --start-date and --end-date")

    grand_total = 0
    for code in codes:
        lat, lon, tz = airports_map[code]
        total = 0
        for d in dates:
            df = fetch_hourly_for_day_meteostat(lat, lon, tz, d)
            count = load_weather(df, code, batch_size=args.batch_size)
            print(f"Ingested/updated {count} weather rows for {code} on {d}.")
            total += count
            grand_total += count
        print(f"Total rows ingested/updated for {code}: {total}")

    print(f"Grand total across all airports: {grand_total}")


if __name__ == "__main__":
    main()
