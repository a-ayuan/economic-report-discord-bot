from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def now_et() -> datetime:
    return datetime.now(tz=ZoneInfo("America/New_York"))

def start_of_week_et(dt: datetime) -> datetime:
    # Monday 00:00 ET
    d0 = dt.astimezone(ZoneInfo("America/New_York"))
    return (d0 - timedelta(days=d0.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

def end_of_week_et(dt: datetime) -> datetime:
    sow = start_of_week_et(dt)
    return sow + timedelta(days=7)
