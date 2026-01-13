import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.fed")

# Official FOMC meeting calendars page :contentReference[oaicite:12]{index=12}
FOMC_CAL_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

def _et(dt: datetime, tz_name: str) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt.astimezone(ZoneInfo(tz_name))

class FedProvider(Provider):
    name = "FED"

    def __init__(self, http: HttpClient, tz_name: str):
        self.http = http
        self.tz_name = tz_name

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        html = await self.http.get_text(FOMC_CAL_URL)
        soup = BeautifulSoup(html, "html.parser")

        events: list[EconomicEvent] = []

        # The page contains yearly calendars with meeting date ranges and links.
        # We'll create events for: Federal Funds Rate, FOMC Statement, FOMC Press Conference, FOMC Meeting Minutes.
        # Press conference is not always; minutes released ~3 weeks later (per page text). :contentReference[oaicite:13]{index=13}

        text = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        # Lightweight parse: find patterns like "January 27-28" and later "Press Conference"
        # This is intentionally conservative; you can harden to parse the calendar table structure.
        for ln in lines:
            if "Press Conference" in ln:
                # We'll not schedule exact without robust DOM parsing; leave disabled until hardened.
                continue

        # For now: schedule nothing automatically; still supports posting when you manually add events in cache.
        return events

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        # For Fed events, you usually fetch the statement/minutes URL once published.
        # This skeleton marks them missing unless you implement exact link discovery.
        event.status = "missing"
        event.release = ReleaseData(source_url=FOMC_CAL_URL)
        return event
