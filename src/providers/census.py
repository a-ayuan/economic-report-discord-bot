import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.census")

# Census time series API docs (official) :contentReference[oaicite:10]{index=10}
CENSUS_TS_DOC = "https://api.census.gov/data/timeseries.html"

class CensusProvider(Provider):
    name = "CENSUS"

    def __init__(self, http: HttpClient, tz_name: str):
        self.http = http
        self.tz_name = tz_name

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        # Census release calendar varies; without scraping an official "Economic Indicators Release Schedule"
        # page robustly, we create a conservative placeholder: do NOT schedule automatically.
        # Instead, you can add a schedule scraper later.
        #
        # To keep the bot usable, we add NO auto-scheduled Census events by default.
        return []

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        event.status = "disabled"
        event.release = ReleaseData(source_url=CENSUS_TS_DOC)
        return event

    @staticmethod
    def default_retail_sales_events(dt_et: datetime) -> list[EconomicEvent]:
        stamp = dt_et.isoformat()
        group = f"retail:{stamp}"
        return [
            EconomicEvent(
                event_id=safe_event_id("census", "Retail Sales m/m", stamp),
                name="Retail Sales m/m",
                country="US",
                currency="USD",
                scheduled_time_et=dt_et,
                provider="CENSUS",
                provider_configured=False,
                group_key=group,
            ),
            EconomicEvent(
                event_id=safe_event_id("census", "Core Retail Sales m/m", stamp),
                name="Core Retail Sales m/m",
                country="US",
                currency="USD",
                scheduled_time_et=dt_et,
                provider="CENSUS",
                provider_configured=False,
                group_key=group,
            ),
        ]
