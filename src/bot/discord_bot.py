import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Awaitable, Callable, Optional

import discord
from discord.ext import commands

from ..storage.models import EconEvent
from ..utils.format import fmt_dt, fmt_value

log = logging.getLogger("bot.discord")

class CalendarCog(commands.Cog):
    def __init__(self, bot: "EconDiscordBot"):
        self.bot = bot

    @commands.command(name="calendar")
    async def calendar(self, ctx: commands.Context):
        # Only respond in the configured command channel
        if ctx.channel.id != self.bot.command_channel_id:
            return

        # Force a live refresh so we can test that the endpoints are being hit
        try:
            await ctx.send("Refreshing calendar from official sources (BLS / BEA / Census)...")
            events = await self.bot.refresh_week_events(trigger="command")
            await ctx.send(f"Refresh complete. Loaded **{len(events)}** configured events for this week.")
        except Exception as e:
            log.exception("!calendar refresh failed: %s", e)
            await ctx.send(f"Refresh failed: `{type(e).__name__}: {e}`")
            return

        events = self.bot._week_events
        if not events:
            await ctx.send("No events found after refresh. Check your matching hints in `src/config/events.yaml`.")
            return

        by_day: dict[str, list[EconEvent]] = defaultdict(list)
        tz = ZoneInfo("America/New_York")

        for e in events:
            day = e.release_dt.astimezone(tz).strftime("%A %b %-d")
            by_day[day].append(e)

        # Sort day keys by their actual date (robust across month boundaries)
        def day_sort_key(day_label: str) -> datetime:
            # day_label format: "Monday Jan 12"
            now_year = datetime.now(tz=tz).year
            return datetime.strptime(f"{day_label} {now_year}", "%A %b %d %Y").replace(tzinfo=tz)

        lines: list[str] = ["**This week's USD high-impact calendar**"]
        for day in sorted(by_day.keys(), key=day_sort_key):
            lines.append(f"\n__{day}__")
            for e in sorted(by_day[day], key=lambda x: x.release_dt):
                lines.append(
                    f"- {fmt_dt(e.release_dt)}: **{e.title}** | "
                    f"F: {fmt_value(e.forecast)} | "
                    f"P: {fmt_value(e.previous)} | "
                    f"A: {fmt_value(e.actual)} | "
                    f"{e.status}"
                )

        await ctx.send("\n".join(lines))

class EconDiscordBot(commands.Bot):
    """
    Discord bot wrapper. `refresh_hook` is injected from main.py and must return a fresh list[EconEvent]
    after scraping the official calendars and updating cache/scheduler.
    """

    def __init__(self, *, command_channel_id: int, announce_channel_id: int):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

        self.command_channel_id = command_channel_id
        self.announce_channel_id = announce_channel_id

        self._week_events: list[EconEvent] = []

        self._refresh_hook: Optional[Callable[[], Awaitable[list[EconEvent]]]] = None
        self._refresh_lock = asyncio.Lock()
        self._last_refresh_at: Optional[datetime] = None

    async def setup_hook(self) -> None:
        await self.add_cog(CalendarCog(self))
        log.info("CalendarCog registered")

    def set_week_events(self, events: list[EconEvent]) -> None:
        self._week_events = events

    def set_refresh_hook(self, hook: Callable[[], Awaitable[list[EconEvent]]]) -> None:
        self._refresh_hook = hook

    async def refresh_week_events(self, *, trigger: str = "unknown") -> list[EconEvent]:
        """
        Forces a live refresh via injected hook. Protects against concurrent refresh calls.
        """
        if self._refresh_hook is None:
            raise RuntimeError("Refresh hook not set. main.py must call bot.set_refresh_hook(...).")

        async with self._refresh_lock:
            log.info("Refreshing week events (trigger=%s)...", trigger)
            events = await self._refresh_hook()
            self._last_refresh_at = datetime.now(tz=ZoneInfo("America/New_York"))
            self.set_week_events(events)
            log.info("Refresh done: %d events", len(events))
            return events

    async def post_release(self, event: EconEvent) -> None:
        ch = self.get_channel(self.announce_channel_id)
        if ch is None:
            log.warning("Announce channel not found: %s", self.announce_channel_id)
            return

        msg = (
            f"**{event.title}**\n"
            f"Time: {fmt_dt(event.release_dt)}\n"
            f"Forecast: {fmt_value(event.forecast)}\n"
            f"Previous: {fmt_value(event.previous)}\n"
            f"Actual: {fmt_value(event.actual)}\n"
            f"Status: {event.status}"
        )
        await ch.send(msg)
