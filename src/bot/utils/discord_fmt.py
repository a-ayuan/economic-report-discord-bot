from __future__ import annotations

from collections import defaultdict

from src.bot.providers.myfxbook import CalendarEvent

DISCORD_MAX_LEN = 2000

def format_week_calendar(events: list[CalendarEvent]) -> str:
    if not events:
        return "**This week:** no matching events found."

    by_day: dict[str, list[CalendarEvent]] = defaultdict(list)
    for ev in events:
        day_name = ev.dt_ny.strftime("%A")
        by_day[day_name].append(ev)

    # Fixed weekday order
    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    lines: list[str] = ["**This Week (High impact, USD):**"]
    for day in order:
        day_events = by_day.get(day, [])
        if not day_events:
            lines.append(f"\n**{day}:** no news")
            continue

        lines.append(f"\n**{day}:**")
        for ev in day_events:
            t = ev.dt_ny.strftime("%-I:%M %p ET") if hasattr(ev.dt_ny, "strftime") else ""
            actual = ev.actual if ev.actual else "PENDING"
            forecast = ev.forecast if ev.forecast else "N/A"
            prev = ev.previous if ev.previous else "N/A"

            lines.append(
                f"- **{t}**: {ev.title}; "
                f"Actual: **{actual}**; Forecast: **{forecast}**; Previous: **{prev}**"
            )

    # Discord safety: keep under 2000 chars. If needed, trim.
    msg = "\n".join(lines)
    if len(msg) <= 1900:
        return msg

    # If too long, keep only first N events per day until fits
    trimmed: list[str] = ["**This Week (High impact, USD):** (trimmed)"]
    for day in order:
        day_events = by_day.get(day, [])
        if not day_events:
            trimmed.append(f"\n**{day}:** no news")
            continue
        trimmed.append(f"\n**{day}:**")
        for ev in day_events[:6]:
            t = ev.dt_ny.strftime("%-I:%M %p ET")
            actual = ev.actual if ev.actual else "PENDING"
            forecast = ev.forecast if ev.forecast else "N/A"
            prev = ev.previous if ev.previous else "N/A"
            trimmed.append(
                f"- **{t}**: {ev.title}; Actual: **{actual}**; Forecast: **{forecast}**; Previous: **{prev}**"
            )
    return "\n".join(trimmed)

def chunk_message(text: str, max_len: int = DISCORD_MAX_LEN) -> list[str]:
    """
    Split on paragraph boundaries first, then lines, to stay within Discord limit.
    """
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    def flush():
        nonlocal current, current_len
        if current:
            chunks.append("\n".join(current).strip())
            current = []
            current_len = 0

    for line in text.split("\n"):
        add_len = len(line) + 1
        if current_len + add_len > max_len:
            flush()
        current.append(line)
        current_len += add_len

    flush()
    return [c for c in chunks if c]
