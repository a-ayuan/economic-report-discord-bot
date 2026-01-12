from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterable

from bot.models import EconEvent

class EconProvider(ABC):
    @abstractmethod
    def fetch_calendar(self, start: datetime, end: datetime) -> list[EconEvent]:
        """Return upcoming events between [start, end]."""

    @abstractmethod
    def fetch_event_update(self, event: EconEvent) -> EconEvent:
        """Return event with updated forecast/previous/actual fields (when available)."""

    @staticmethod
    def apply_filters(
        events: Iterable[EconEvent],
        *,
        currency: str,
        impact: str,
        allowlist: list[str],
        blocklist: list[str],
    ) -> list[EconEvent]:
        out: list[EconEvent] = []
        allow = [x.lower() for x in allowlist or []]
        block = [x.lower() for x in blocklist or []]

        for e in events:
            if e.currency.upper() != currency.upper():
                continue
            if impact and e.impact != impact:
                continue

            title_l = e.title.lower()

            if block and any(b in title_l for b in block):
                continue
            if allow and not any(a in title_l for a in allow):
                continue

            out.append(e)
        return out
