import logging
from datetime import datetime

from src.models import EconomicEvent

log = logging.getLogger("calendar_service")

class CalendarService:
    def __init__(self, providers: list, post_only_configured_sources: bool):
        self.providers = providers
        self.post_only_configured_sources = post_only_configured_sources

    async def build(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        all_events: list[EconomicEvent] = []
        for p in self.providers:
            try:
                evs = await p.build_calendar(start_et, end_et)
                all_events.extend(evs)
            except Exception as e:
                log.exception("Provider %s failed build_calendar: %s", getattr(p, "name", "unknown"), e)

        # Dedupe by event_id
        seen = set()
        deduped: list[EconomicEvent] = []
        for e in all_events:
            if e.event_id in seen:
                continue
            seen.add(e.event_id)
            if self.post_only_configured_sources and not e.provider_configured:
                # keep it in calendar for summary, but mark disabled
                e.status = "disabled"
            deduped.append(e)

        return sorted(deduped, key=lambda x: x.scheduled_time_et)
