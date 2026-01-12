from datetime import datetime
from zoneinfo import ZoneInfo

def now_tz(tz_name: str) -> datetime:
    return datetime.now(tz=ZoneInfo(tz_name))

def to_tz(dt: datetime, tz_name: str) -> datetime:
    return dt.astimezone(ZoneInfo(tz_name))
