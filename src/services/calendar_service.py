import logging
from datetime import datetime

from src.models import EconomicEvent
from src.providers.base import Provider

log = logging.getLogger("calendar_service")

class CalendarService:
    def __init__(self, providers: list[Provider], post_only_configured_sources: bool):
        self.providers = providers
        self.post_only_configured_sources = post_only_configured_sources

    async def build(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        events: list[EconomicEvent] = []

        for p in self.providers:
            try:
                evs = await p.build_calendar(start_et, end_et)
            except Exception as e:
                log.exception("Provider %s build_calendar failed: %s", getattr(p, "name", "?"), e)
                continue
            events.extend(evs)

        if self.post_only_configured_sources:
            events = [e for e in events if e.provider_configured]

        for e in events:
            if getattr(e, "release", None) is None:
                continue
            if e.status in ("released", "disabled"):
                continue
            if e.release.previous is not None:
                continue

            provider = next((p for p in self.providers if p.name == e.provider), None)
            if provider is None:
                continue

            try:
                await provider.prefill_previous(e)
            except Exception as ex:
                log.exception("prefill_previous failed for %s (%s): %s", e.name, e.event_id, ex)

        return sorted(events, key=lambda x: x.scheduled_time_et)
