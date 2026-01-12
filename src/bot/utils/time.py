from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

def now_tz(tz_name: str) -> datetime:
    return datetime.now(tz=ZoneInfo(tz_name))

def to_tz(dt: datetime, tz_name: str) -> datetime:
    return dt.astimezone(ZoneInfo(tz_name))

def week_bounds_ny(now_ny: datetime) -> tuple[datetime, datetime]:
    """
    Returns [Mon 00:00, next Mon 00:00) in America/New_York.
    """
    if now_ny.tzinfo is None:
        now_ny = now_ny.replace(tzinfo=NY_TZ)

    # Monday = 0
    start = (now_ny - timedelta(days=now_ny.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=7)
    return start, end
