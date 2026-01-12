import logging
from datetime import timedelta

import discord
from discord.ext import commands

from src.bot.cache import CacheState
from src.bot.config import AppSettings
from src.bot.providers import FXStreetProvider, ForexFactoryProvider
from src.bot.providers.base import EconProvider
from src.bot.scheduler import EventScheduler
from src.bot.utils.discord_fmt import chunk_message, format_week_calendar
from src.bot.utils.time import now_tz
from src.bot.utils.week import week_window

log = logging.getLogger(__name__)

class EconDiscordBot(commands.Bot):
    def __init__(self, *, settings: AppSettings) -> None:
        intents = discord.Intents.default(); intents.message_content = True
        # If you later want message-content parsing for more advanced commands,
        # you may need Message Content intent enabled in Discord Developer Portal.
        super().__init__(command_prefix="!", intents=intents)

        self.settings = settings

        self.cache = CacheState.load(settings.cache_path)

        self.provider = self._make_provider(settings)

        cfg = settings.raw_config
        self.currency = cfg.get("currency", "USD")
        self.impact = cfg.get("impact", "high")
        self.allowlist = cfg.get("keyword_allowlist", []) or []
        self.blocklist = cfg.get("keyword_blocklist", []) or []

        self.template = (cfg.get("discord", {}) or {}).get(
            "message_template",
            "**{title}** | Forecast: `{forecast}` | Previous: `{previous}` | Actual: `{actual}`",
        )

        self.filtered_provider = _FilteredProvider(
            base=self.provider,
            currency=self.currency,
            impact=self.impact,
            allowlist=self.allowlist,
            blocklist=self.blocklist,
        )

        self.scheduler = EventScheduler(
            tz_name=settings.tz,
            provider=self.filtered_provider,
            cache=self.cache,
            cache_path=settings.cache_path,
            on_event_released=self._post_release_event,
            polling_cfg=(cfg.get("polling", {}) or {}),
            refresh_cfg=(cfg.get("calendar_refresh", {}) or {}),
        )

        # Register commands
        self._register_commands()

    def _make_provider(self, settings: AppSettings) -> EconProvider:
        provider_name = (settings.raw_config.get("provider") or "fxstreet").lower()
        if provider_name == "forexfactory":
            return ForexFactoryProvider(user_agent=settings.user_agent, tz_name=settings.tz)
        return FXStreetProvider(user_agent=settings.user_agent, tz_name=settings.tz)

    def _register_commands(self) -> None:
        @self.command(name="calendar")
        async def calendar_cmd(ctx: commands.Context) -> None:
            """
            Fetch and print current week's calendar (USD + high impact filtered).
            """
            try:
                now = now_tz(self.settings.tz)
                start, end = week_window(now, self.settings.tz)

                # Fetch this week's events (already filtered)
                events = self.filtered_provider.fetch_calendar(start, end)

                text = format_week_calendar(events, self.settings.tz)
                for part in chunk_message(text):
                    await self._safe_send(ctx, part)

            except Exception as ex:
                log.exception("!calendar failed: %s", ex)
                await self._safe_send(ctx, f"Failed to fetch calendar: `{type(ex).__name__}`")

    async def on_ready(self) -> None:
        log.info("Logged in as %s", self.user)
        self.scheduler.start()

    async def _post_release_event(self, event) -> None:
        """
        Scheduled posting to the primary channel (DISCORD_CHANNEL_ID).
        """
        channel = self.get_channel(self.settings.discord_channel_id)
        if channel is None:
            log.error("Primary channel not found: %s", self.settings.discord_channel_id)
            return

        forecast = event.forecast or "(n/a)"
        previous = event.previous or "(n/a)"
        actual = event.actual or "(n/a)"

        msg = self.template.format(
            title=event.title,
            forecast=forecast,
            previous=previous,
            actual=actual,
        )

        await channel.send(msg)

    async def _safe_send(self, ctx: commands.Context, text: str) -> None:
        """
        Prefer replying in the command channel. If not possible, fallback to COMMAND_CHANNEL_ID.
        """
        # Try the invoking channel first
        try:
            if ctx.channel is not None:
                await ctx.channel.send(text)
                return
        except Exception:
            pass

        # Fallback
        if self.settings.command_channel_id is None:
            return

        ch = self.get_channel(self.settings.command_channel_id)
        if ch is None:
            log.error("COMMAND_CHANNEL_ID channel not found: %s", self.settings.command_channel_id)
            return
        try:
            await ch.send(text)
        except Exception as ex:
            log.error("Failed to send fallback command output: %s", ex)

class _FilteredProvider(EconProvider):
    def __init__(self, *, base: EconProvider, currency: str, impact: str, allowlist, blocklist) -> None:
        self.base = base
        self.currency = currency
        self.impact = impact
        self.allowlist = allowlist
        self.blocklist = blocklist

    def fetch_calendar(self, start, end):
        events = self.base.fetch_calendar(start, end)
        return EconProvider.apply_filters(
            events,
            currency=self.currency,
            impact=self.impact,
            allowlist=self.allowlist,
            blocklist=self.blocklist,
        )

    def fetch_event_update(self, event):
        return self.base.fetch_event_update(event)