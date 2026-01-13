from datetime import datetime
from typing import Iterable

from src.models import EconomicEvent

def fmt_dt(dt: datetime) -> str:
    # dt should be timezone-aware ET
    return dt.strftime("%a %m/%d %I:%M %p ET")

def fmt_value(v: str | None) -> str:
    return v if (v is not None and str(v).strip() != "") else "N/A"

def format_release_line(e: EconomicEvent) -> str:
    r = e.release
    return (
        f"**{e.name}** ({fmt_dt(e.scheduled_time_et)})\n"
        f"Forecast: {fmt_value(r.forecast)} | Previous: {fmt_value(r.previous)} | Actual: {fmt_value(r.actual)}"
    )

def format_week_summary(events: Iterable[EconomicEvent], title: str) -> str:
    lines = [f"**{title}**"]
    for e in sorted(events, key=lambda x: x.scheduled_time_et):
        status = e.status
        lines.append(
            f"- {fmt_dt(e.scheduled_time_et)} — {e.name} "
            f"[{status}] "
            f"(F:{fmt_value(e.release.forecast)} P:{fmt_value(e.release.previous)} A:{fmt_value(e.release.actual)})"
        )
    if len(lines) == 1:
        lines.append("_None_")
    return "\n".join(lines)
