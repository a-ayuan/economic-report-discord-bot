from datetime import datetime
from typing import Iterable

from src.models import EconomicEvent

def fmt_dt(dt: datetime) -> str:
    # dt should be timezone-aware ET
    return dt.strftime("%a %m/%d %I:%M %p ET")

def fmt_value(v: str | None) -> str:
    return v if (v is not None and str(v).strip() != "") else "N/A"

def _clip(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: max(0, n - 1)] + "…"

def format_week_summary(events: Iterable[EconomicEvent], title: str) -> str:
    evs = sorted(list(events), key=lambda x: x.scheduled_time_et)

    lines: list[str] = [f"**{title}**"]
    if not evs:
        lines.append("```text\nNone\n```")
        return "\n".join(lines)

    out: list[str] = ["```text"]
    for e in evs:
        r = e.release
        name = _clip(e.name, 60)  # allows wrap on mobile without being absurdly long
        out.append(f"{fmt_dt(e.scheduled_time_et)}  [{e.status}]")
        out.append(f"{name}")
        out.append(
            f"F: {fmt_value(r.forecast)} | P: {fmt_value(r.previous)} | A: {fmt_value(r.actual)}"
        )
        out.append("")  # blank line between events

    # remove trailing blank line
    if out and out[-1] == "":
        out.pop()

    out.append("```")
    lines.append("\n".join(out))
    return "\n".join(lines)

def format_release_line(e: EconomicEvent) -> str:
    r = e.release
    return (
        f"**{e.name}** ({fmt_dt(e.scheduled_time_et)})\n"
        f"Forecast: {fmt_value(r.forecast)} | Previous: {fmt_value(r.previous)} | Actual: {fmt_value(r.actual)}"
    )