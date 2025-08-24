from datetime import datetime
from typing import Optional

def to_hour(dt_str: str) -> str:
    """Truncate an ISO-like datetime string to the hour (YYYY-MM-DD HH:00:00)."""
    dt = datetime.fromisoformat(dt_str.replace("Z","").replace("T"," "))
    return dt.strftime("%Y-%m-%d %H:00:00")

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default
