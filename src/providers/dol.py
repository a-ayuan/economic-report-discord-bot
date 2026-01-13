import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.dol")

# Official UI weekly claims PDF endpoint :contentReference[oaicite:11]{index=11}
DOL_UI_PDF = "https://www.dol.gov/ui/data.pdf"

class DOLProvider(Provider):
    name = "DOL"

    def __init__(self, http: HttpClient, tz_name: str):
        self.http = http
        self.tz_name = tz_name

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        # Weekly claims are generally Thursdays 8:30 AM ET.
        # We schedule Thursdays inside the window.
        events: list[EconomicEvent] = []
        cur = start_et.replace(hour=0, minute=0, second=0, microsecond=0)
        while cur < end_et:
            if cur.weekday() == 3:  # Thu (Mon=0)
                dt = cur.replace(hour=8, minute=30, second=0, microsecond=0)
                if start_et <= dt < end_et:
                    stamp = dt.isoformat()
                    events.append(
                        EconomicEvent(
                            event_id=safe_event_id("dol", "Unemployment Claims", stamp),
                            name="Unemployment Claims",
                            country="US",
                            currency="USD",
                            scheduled_time_et=dt,
                            provider=self.name,
                            provider_configured=True,
                            group_key=f"claims:{stamp}",
                        )
                    )
            cur += timedelta(days=1)
        return events

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        # Minimal: confirm the PDF is reachable and mark released once scheduled time passes.
        # Parsing the PDF for the headline figure is possible, but PDF parsing is brittle.
        # Start with "released (link available)" and upgrade to PDF parsing later if you want exact values.
        if datetime.now(ZoneInfo(self.tz_name)) < event.scheduled_time_et:
            return event

        _ = await self.http.get_bytes(DOL_UI_PDF)  # raises if not available
        event.status = "released"
        event.release = ReleaseData(
            actual=None,
            previous=None,
            forecast=None,
            updated_at=datetime.now(ZoneInfo(self.tz_name)),
            source_url=DOL_UI_PDF,
        )
        return event
