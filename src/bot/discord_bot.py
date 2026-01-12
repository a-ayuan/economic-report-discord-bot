from __future__ import annotations

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import discord

from bot.cache import CacheState
from bot.providers import FXStreetProvider, ForexFactoryProvider
from bot.providers.base import EconProvider
from bot.scheduler import EventScheduler
from bot.config import AppSettings

log = logging.getLogger(__name__)

class EconDiscordBot(discord.Client):
    def __init__(self, *, settings: AppSettings) -> None:
        intents = discord.Intents.default()
        super().__init__(intents=intents)

        self.settings = settings
        self.tz = ZoneInfo(settings.tz)

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

        self.scheduler = EventScheduler(
            tz_name=settings.tz,
            provider=_FilteredProvider(
                base=self.provider,
                currency=self.currency,
                impact=self.impact,
                allowlist=self.allowlist,
                blocklist=self.blocklist,
            ),
            cache=self.cache,
            cache_path=settings.cache_path,
            on_event_released=self._post_event,
            polling_cfg=(cfg.get("polling", {}) or {}),
            refresh_cfg=(cfg.get("calendar_refresh", {}) or {}),
        )

    def _make_provider(self, settings: AppSettings) -> EconProvider:
        provider_name = (settings.raw_config.get("provider") or "fxstreet").lower()
        if provider_name == "forexfactory":
            return ForexFactoryProvider(user_agent=settings.user_agent, tz_name=settings.tz)
        return FXStreetProvider(user_agent=settings.user_agent, tz_name=settings.tz)

    async def on_ready(self) -> None:
        log.info("Logged in as %s", self.user)
        self.scheduler.start()

    async def _post_event(self, event) -> None:
        channel = self.get_channel(self.settings.discord_channel_id)
        if channel is None:
            log.error("Channel not found: %s", self.settings.discord_channel_id)
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

class _FilteredProvider(EconProvider):
    def __init__(self, *, base: EconProvider, currency: str, impact: str, allowlist, blocklist) -> None:
        self.base = base
        self.currency = currency
        self.impact = impact
        self.allowlist = allowlist
        self.blocklist = blocklist

    def fetch_calendar(self, start: datetime, end: datetime):
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
