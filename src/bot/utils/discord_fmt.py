from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo

from src.bot.models import EconEvent

DISCORD_MAX_LEN = 2000

def _val_or_pending(actual: str | None) -> str:
    if actual is None or actual.strip() == "" or actual.strip().lower() in {"-", "n/a"}:
        return "PENDING"
    return actual.strip()

def _val_or_na(v: str | None) -> str:
    if v is None or v.strip() == "" or v.strip().lower() in {"-", "pending"}:
        return "N/A"
    return v.strip()

def format_week_calendar(events: Iterable[EconEvent], tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    grouped: dict[str, list[EconEvent]] = defaultdict(list)

    # group by weekday name
    for e in events:
        wd = e.release_dt.astimezone(tz).strftime("%A")
        grouped[wd].append(e)

    # order weekdays
    ordered_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    # sort within day by time
    for day in ordered_days:
        grouped[day].sort(key=lambda x: x.release_dt)

    lines: list[str] = []
    lines.append("**This Week (USD, High Impact)**")

    for day in ordered_days:
        day_events = grouped.get(day, [])
        lines.append(f"\n__**{day}**__")
        if not day_events:
            lines.append("- no news")
            continue

        for e in day_events:
            t = e.release_dt.astimezone(tz).strftime("%-I:%M %p %Z")
            actual = _val_or_pending(e.actual)
            forecast = _val_or_na(e.forecast)
            previous = _val_or_na(e.previous)
            lines.append(
                f"- **{t}**: {e.title}; Actual: `{actual}`; Forecast: `{forecast}`; Previous: `{previous}`"
            )

    return "\n".join(lines).strip()

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
