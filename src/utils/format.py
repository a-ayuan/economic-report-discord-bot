from datetime import datetime

def fmt_dt(dt: datetime) -> str:
    # 8:30 AM ET
    return dt.strftime("%-I:%M %p ET")

def fmt_value(v: str | None) -> str:
    return v if (v is not None and str(v).strip() != "") else "N/A"
