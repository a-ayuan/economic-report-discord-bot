from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class EconEvent:
    # stable id to dedupe across restarts/provider refreshes
    event_id: str

    title: str
    currency: str

    # scheduled release time in tz-aware datetime
    release_dt: datetime

    impact: str  # "high" | "medium" | "low" (provider-mapped)

    # values may be missing until release
    forecast: Optional[str] = None
    previous: Optional[str] = None
    actual: Optional[str] = None
