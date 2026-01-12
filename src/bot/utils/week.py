from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def week_window(dt: datetime, tz_name: str) -> tuple[datetime, datetime]:
    """
    Returns (start_of_week, end_of_week) in tz, where week starts Monday 00:00:00
    and ends Sunday 23:59:59.
    """
    tz = ZoneInfo(tz_name)
    local = dt.astimezone(tz)

    # Monday=0 ... Sunday=6
    start = (local - timedelta(days=local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end = (start + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end
