from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def now_et(tz_name: str) -> datetime:
    return datetime.now(ZoneInfo(tz_name))

def to_et(dt: datetime, tz_name: str) -> datetime:
    # treat naive as ET already
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt.astimezone(ZoneInfo(tz_name))

def week_bounds_et(anchor: datetime) -> tuple[datetime, datetime]:
    # Monday 00:00 -> next Monday 00:00
    start = anchor - timedelta(days=anchor.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=7)
    return start, end
