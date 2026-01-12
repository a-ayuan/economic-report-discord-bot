import logging
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands

from ..storage.models import EconEvent
from ..utils.format import fmt_dt, fmt_value

log = logging.getLogger("bot.discord")

class EconDiscordBot(commands.Bot):
    def __init__(self, *, command_channel_id: int, announce_channel_id: int):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

        self.command_channel_id = command_channel_id
        self.announce_channel_id = announce_channel_id

        self._week_events: list[EconEvent] = []

        self.add_command(self.calendar)

    def set_week_events(self, events: list[EconEvent]) -> None:
        self._week_events = events

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

    @commands.command(name="calendar")
    async def calendar(self, ctx: commands.Context):
        if ctx.channel.id != self.command_channel_id:
            return

        if not self._week_events:
            await ctx.send("No cached calendar yet. Try again soon.")
            return

        by_day: dict[str, list[EconEvent]] = defaultdict(list)
        tz = ZoneInfo("America/New_York")
        for e in self._week_events:
            day = e.release_dt.astimezone(tz).strftime("%A %b %-d")
            by_day[day].append(e)

        lines: list[str] = ["**This week's USD high-impact calendar**"]
        for day in sorted(by_day.keys(), key=lambda k: datetime.strptime(k, "%A %b %d")):
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
