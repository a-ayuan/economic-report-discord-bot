from datetime import datetime
from typing import Iterable

from datetime import datetime
from typing import Iterable

import discord

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

def _status_emoji(status: str) -> str:
    s = (status or "").lower()
    if s in {"released", "actual", "done", "completed"}:
        return "✅"
    if s in {"pending", "scheduled", "upcoming"}:
        return "⏳"
    if s in {"delayed", "postponed"}:
        return "⚠️"
    if s in {"canceled", "cancelled"}:
        return "🛑"
    return "•"

def build_week_embeds(
    released_events: Iterable[EconomicEvent],
    pending_events: Iterable[EconomicEvent],
    *,
    title_prefix: str = "This week",
) -> list[discord.Embed]:
    released = sorted(list(released_events), key=lambda x: x.scheduled_time_et)
    pending = sorted(list(pending_events), key=lambda x: x.scheduled_time_et)

    embeds: list[discord.Embed] = []

    # Released embed (green)
    e1 = discord.Embed(
        title=f"{title_prefix} • Released",
        description="Reports that have already printed.",
        color=discord.Color.green(),
    )
    if not released:
        e1.add_field(name="None", value="No released events in range.", inline=False)
    else:
        for ev in released[:25]:
            r = ev.release
            name = _clip(ev.name, 90)
            status = ev.status or "released"
            line1 = f"{fmt_dt(ev.scheduled_time_et)}  {_status_emoji(status)} **{status}**"
            line2 = f"F: {fmt_value(getattr(r, 'forecast', None))} | P: {fmt_value(getattr(r, 'previous', None))} | A: {fmt_value(getattr(r, 'actual', None))}"
            e1.add_field(name=name, value=f"{line1}\n{line2}", inline=False)

        if len(released) > 25:
            e1.set_footer(text=f"Showing 25/{len(released)} released events (Discord embed field limit).")

    embeds.append(e1)

    # Pending embed (orange)
    e2 = discord.Embed(
        title=f"{title_prefix} • Pending",
        description="Upcoming reports that have not printed yet.",
        color=discord.Color.orange(),
    )
    if not pending:
        e2.add_field(name="None", value="No pending events in range.", inline=False)
    else:
        for ev in pending[:25]:
            r = ev.release
            name = _clip(ev.name, 90)
            status = ev.status or "pending"
            line1 = f"{fmt_dt(ev.scheduled_time_et)}  {_status_emoji(status)} **{status}**"
            # Pending often has no actual yet; still show it as N/A
            line2 = f"F: {fmt_value(getattr(r, 'forecast', None))} | P: {fmt_value(getattr(r, 'previous', None))} | A: {fmt_value(getattr(r, 'actual', None))}"
            e2.add_field(name=name, value=f"{line1}\n{line2}", inline=False)

        if len(pending) > 25:
            e2.set_footer(text=f"Showing 25/{len(pending)} pending events (Discord embed field limit).")

    embeds.append(e2)

    return embeds

def format_release_line(e: EconomicEvent) -> str:
    r = e.release
    return (
        f"**{e.name}** ({fmt_dt(e.scheduled_time_et)})\n"
        f"Forecast: {fmt_value(r.forecast)} | Previous: {fmt_value(r.previous)} | Actual: {fmt_value(r.actual)}"
    )