import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.bea")

# Official BEA release schedule :contentReference[oaicite:9]{index=9}
BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"

def _et(dt: datetime, tz_name: str) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt.astimezone(ZoneInfo(tz_name))

class BEAProvider(Provider):
    name = "BEA"

    def __init__(self, http: HttpClient, tz_name: str):
        self.http = http
        self.tz_name = tz_name

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        html = await self.http.get_text(BEA_SCHEDULE_URL)
        soup = BeautifulSoup(html, "html.parser")

        events: list[EconomicEvent] = []

        # The BEA schedule is a page with release entries; simplest: scan text blocks
        # and look for GDP / Personal Income and Outlays (PCE) lines with dates/times.
        text = soup.get_text("\n", strip=True)

        # Lightweight heuristics (BEA page can change); we still cache and refresh every 30 min.
        # You can harden this by parsing specific DOM blocks if BEA changes layout.
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        # We'll create placeholder events for GDP releases by detecting "Gross Domestic Product"
        # Time is often 8:30 AM ET; if not parsable, skip.
        for i, ln in enumerate(lines):
            if "Gross Domestic Product" in ln:
                # try to find a date nearby (previous line often "January 22" etc.)
                date_line = lines[i - 1] if i > 0 else ""
                time_line = lines[i]  # sometimes includes time elsewhere; fallback 8:30

                dt = self._parse_bea_date(date_line, default_hour=8, default_minute=30)
                if not dt:
                    continue
                dt_et = _et(dt, self.tz_name)
                if not (start_et <= dt_et < end_et):
                    continue

                stamp = dt_et.isoformat()
                events.append(
                    EconomicEvent(
                        event_id=safe_event_id("bea", "Advance GDP q/q", stamp),
                        name="Advance GDP q/q",
                        country="US",
                        currency="USD",
                        scheduled_time_et=dt_et,
                        provider=self.name,
                        provider_configured=True,
                        group_key=f"gdp:{stamp}",
                    )
                )
                events.append(
                    EconomicEvent(
                        event_id=safe_event_id("bea", "Final GDP q/q", stamp),
                        name="Final GDP q/q",
                        country="US",
                        currency="USD",
                        scheduled_time_et=dt_et,
                        provider=self.name,
                        provider_configured=True,
                        group_key=f"gdp:{stamp}",
                    )
                )

            if "Personal Income and Outlays" in ln:
                dt = self._parse_bea_date(lines[i - 1] if i > 0 else "", default_hour=8, default_minute=30)
                if not dt:
                    continue
                dt_et = _et(dt, self.tz_name)
                if not (start_et <= dt_et < end_et):
                    continue
                stamp = dt_et.isoformat()
                events.append(
                    EconomicEvent(
                        event_id=safe_event_id("bea", "Core PCE Price Index m/m", stamp),
                        name="Core PCE Price Index m/m",
                        country="US",
                        currency="USD",
                        scheduled_time_et=dt_et,
                        provider=self.name,
                        provider_configured=False,  # needs BEA API series mapping or HTML parsing
                        group_key=f"pce:{stamp}",
                    )
                )

        return events

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        # For BEA, you typically want either BEA API or parse the specific release page.
        # This skeleton leaves it disabled until you choose a "no-delay" mechanism you trust.
        if not event.provider_configured:
            event.status = "disabled"
            event.release = ReleaseData(forecast=None, previous=None, actual=None, source_url=BEA_SCHEDULE_URL)
            return event

        # Mark missing until you implement BEA API / release parsing
        event.status = "missing"
        event.release = ReleaseData(forecast=None, previous=None, actual=None, source_url=BEA_SCHEDULE_URL)
        return event

    @staticmethod
    def _parse_bea_date(date_line: str, default_hour: int, default_minute: int) -> datetime | None:
        # Example BEA "January 22" (year may be elsewhere); use current year as fallback.
        # If BEA page includes year lines, refine here.
        if not date_line:
            return None
        try:
            parts = date_line.split()
            if len(parts) < 2:
                return None
            month = parts[0]
            day = int(parts[1].replace(",", ""))
            year = datetime.now().year
            return datetime.strptime(f"{month} {day} {year} {default_hour}:{default_minute}", "%B %d %Y %H:%M")
        except Exception:
            return None
