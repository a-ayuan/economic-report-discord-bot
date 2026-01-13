import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.holidays")

# Official Federal Reserve System holiday schedule :contentReference[oaicite:14]{index=14}
FRB_HOLIDAYS_URL = "https://www.frbservices.org/about/holiday-schedules"

def _et(dt: datetime, tz_name: str) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt.astimezone(ZoneInfo(tz_name))

class HolidaysProvider(Provider):
    name = "HOLIDAYS"

    def __init__(self, http: HttpClient, tz_name: str):
        self.http = http
        self.tz_name = tz_name

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        html = await self.http.get_text(FRB_HOLIDAYS_URL)
        soup = BeautifulSoup(html, "html.parser")

        # Parse dates from page text (conservative)
        # If FRB changes structure, just cache + adjust.
        text = soup.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        events: list[EconomicEvent] = []
        for ln in lines:
            # Many holiday pages include "January 1, 2026 — New Year’s Day"
            if "—" in ln:
                left, right = ln.split("—", 1)
                left = left.strip()
                name = right.strip()
                try:
                    dt = datetime.strptime(left, "%B %d, %Y")
                except Exception:
                    continue
                dt_et = _et(dt.replace(hour=0, minute=0, second=0, microsecond=0), self.tz_name)
                if start_et <= dt_et < end_et:
                    stamp = dt_et.isoformat()
                    events.append(
                        EconomicEvent(
                            event_id=safe_event_id("holiday", f"Bank Holiday: {name}", stamp),
                            name=f"All Bank Holidays: {name}",
                            country="US",
                            currency="USD",
                            scheduled_time_et=dt_et,
                            provider=self.name,
                            provider_configured=True,
                            group_key=f"holiday:{stamp}",
                        )
                    )

        return events

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        # Holidays don't "release" values.
        event.status = "released"
        event.release = ReleaseData(source_url=FRB_HOLIDAYS_URL, updated_at=datetime.now(ZoneInfo(self.tz_name)))
        return event
