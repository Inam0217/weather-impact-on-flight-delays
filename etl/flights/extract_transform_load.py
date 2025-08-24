# etl/flights/extract_transform_load.py (updated)
# Compatible with Kaggle 2015 Flights schema
# - Handles HHMM = 2400 (rolls to next day)
# - Handles overnight arrivals (arrival HHMM < departure HHMM)
# - Adds --limit to load a small sample quickly
# - Uses chunked/batch inserts to avoid MySQL timeouts

import argparse
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector

from etl.common.db import get_conn, init_schema

# -------------------- Helpers --------------------

def normalize_hhmm(x):
    """Return (hhmm_str, add_day_flag). 2400 -> ('0000', True). Handles NaN/strings.
    """
    try:
        i = int(x)
    except Exception:
        return "0000", False
    if i == 2400:
        return "0000", True
    s = str(i).zfill(4)
    return s, False


def make_dt(date_str: str, hhmm_str: str) -> datetime:
    return datetime.strptime(
        f"{date_str} {hhmm_str[:2]}:{hhmm_str[2:]}:00", "%Y-%m-%d %H:%M:%S"
    )


def to_int_series(s: pd.Series, default: int = 0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default).astype(int)


# -------------------- Core ETL --------------------

def transform(raw: pd.DataFrame) -> pd.DataFrame:
    """Transforms Kaggle Flights (2015) schema into internal schema.

    Expects columns:
      YEAR, MONTH, DAY, AIRLINE, FLIGHT_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT,
      SCHEDULED_DEPARTURE (HHMM), SCHEDULED_ARRIVAL (HHMM), DEPARTURE_DELAY, ARRIVAL_DELAY
    """
    r = raw.copy()

    required = [
        "YEAR",
        "MONTH",
        "DAY",
        "AIRLINE",
        "FLIGHT_NUMBER",
        "ORIGIN_AIRPORT",
        "DESTINATION_AIRPORT",
        "SCHEDULED_DEPARTURE",
        "SCHEDULED_ARRIVAL",
        "DEPARTURE_DELAY",
        "ARRIVAL_DELAY",
    ]
    missing = [c for c in required if c not in r.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}\nCSV Columns: {list(r.columns)}")

    # Standardize our internal names
    r.rename(
        columns={
            "AIRLINE": "airline",
            "FLIGHT_NUMBER": "flight_number",
            "ORIGIN_AIRPORT": "origin",
            "DESTINATION_AIRPORT": "dest",
            "SCHEDULED_DEPARTURE": "crs_dep",
            "SCHEDULED_ARRIVAL": "crs_arr",
            "DEPARTURE_DELAY": "dep_delay_mins",
            "ARRIVAL_DELAY": "arr_delay_mins",
        },
        inplace=True,
    )

    # Build flight_date = YYYY-MM-DD
    r["flight_date"] = (
        pd.to_datetime(
            r[["YEAR", "MONTH", "DAY"]].rename(
                columns={"YEAR": "year", "MONTH": "month", "DAY": "day"}
            ),
            errors="coerce",
        ).dt.strftime("%Y-%m-%d")
    )

    # Normalize HHMM and build scheduled datetimes
    dep_parts = [normalize_hhmm(v) for v in r["crs_dep"]]
    dep_hhmm = [p[0] for p in dep_parts]
    dep_add_day = [p[1] for p in dep_parts]
    dep_dt = [
        make_dt(d, h) + (timedelta(days=1) if add else timedelta(0))
        for d, h, add in zip(r["flight_date"], dep_hhmm, dep_add_day)
    ]

    arr_parts = [normalize_hhmm(v) for v in r["crs_arr"]]
    arr_hhmm = [p[0] for p in arr_parts]
    arr_add_day_2400 = [p[1] for p in arr_parts]

    # Overnight roll: if arrival HHMM < departure HHMM, arrival is next day
    dep_int = [int(h) for h in dep_hhmm]
    arr_int = [int(h) for h in arr_hhmm]
    arr_roll = [a < d for a, d in zip(arr_int, dep_int)]

    r["sched_dep_time"] = dep_dt
    r["sched_arr_time"] = [
        make_dt(d, h)
        + (timedelta(days=1) if (roll or add2400) else timedelta(0))
        for d, h, roll, add2400 in zip(
            r["flight_date"], arr_hhmm, arr_roll, arr_add_day_2400
        )
    ]

    # Clean numeric delays
    r["dep_delay_mins"] = to_int_series(r["dep_delay_mins"], default=0)
    r["arr_delay_mins"] = to_int_series(r["arr_delay_mins"], default=0)

    # Final selection in DB column order
    keep = [
        "flight_date",
        "airline",
        "flight_number",
        "origin",
        "dest",
        "sched_dep_time",
        "dep_delay_mins",
        "sched_arr_time",
        "arr_delay_mins",
    ]
    return r[keep]


def _batched(iterable, size):
    """Yield lists of up to `size` items from an iterator."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def load_flights(df: pd.DataFrame, batch_size: int = 5000) -> int:
    if df.empty:
        return 0

    # Build iterable of tuples in the exact SQL order
    rows_iter = df[
        [
            "flight_date",
            "airline",
            "flight_number",
            "origin",
            "dest",
            "sched_dep_time",
            "dep_delay_mins",
            "sched_arr_time",
            "arr_delay_mins",
        ]
    ].itertuples(index=False, name=None)

    inserted = 0
    sql = (
        """
        INSERT INTO flights
        (flight_date, airline, flight_number, origin, dest,
         sched_dep_time, dep_delay_mins, sched_arr_time, arr_delay_mins)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    )

    conn = get_conn()
    try:
        cur = conn.cursor()
        for chunk in _batched(rows_iter, batch_size):
            cur.executemany(sql, chunk)
            conn.commit()
            inserted += len(chunk)
        return inserted
    except mysql.connector.Error as e:
        # Surface a clear message and re-raise for visibility
        raise RuntimeError(f"MySQL error while inserting rows (inserted={inserted}): {e}") from e
    finally:
        conn.close()


# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to Kaggle flights CSV")
    parser.add_argument(
        "--limit", type=int, default=None, help="Only read first N rows from CSV (faster testing)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=5000, help="Insert batch size for MySQL"
    )
    args = parser.parse_args()

    # Initialize DB schema/tables if needed
    init_schema()

    # Read CSV
    read_kwargs = {"low_memory": False}
    if args.limit:
        read_kwargs["nrows"] = args.limit
    raw = pd.read_csv(args.csv, **read_kwargs)

    df = transform(raw)
    n = load_flights(df, batch_size=args.batch_size)
    print(f"Ingested {n} flight rows.")


if __name__ == "__main__":
    main()
