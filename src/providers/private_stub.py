from datetime import datetime

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider

PRIVATE_EVENTS = [
    "ADP Non-Farm Employment Change",
    "ISM Services PMI",
    "ISM Manufacturing PMI",
    "Flash Manufacturing PMI",
    "Flash Services PMI",
    "Prelim UoM Consumer Sentiment",
    "Prelim UoM Inflation Expectations",
]

class PrivateStubProvider(Provider):
    name = "PRIVATE_STUB"

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        # We do not schedule these without an approved, ToS-compliant source.
        return []

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        event.status = "disabled"
        event.release = ReleaseData(
            actual=None,
            previous=None,
            forecast=None,
            source_url=None,
        )
        return event
