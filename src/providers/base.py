from abc import ABC, abstractmethod
from datetime import datetime

from src.models import EconomicEvent

class Provider(ABC):
    name: str

    @abstractmethod
    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        """
        Return scheduled events in [start_et, end_et).
        Times must be ET tz-aware.
        """
        raise NotImplementedError

    @abstractmethod
    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        """
        If released, fill event.release.actual/previous/(forecast) and set status.
        If not yet released, keep scheduled.
        """
        raise NotImplementedError
