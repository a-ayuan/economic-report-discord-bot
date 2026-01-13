from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

EventStatus = Literal["scheduled", "released", "missing", "disabled"]

@dataclass
class ReleaseData:
    actual: str | None = None
    previous: str | None = None
    forecast: str | None = None  # typically N/A from official sources
    unit: str | None = None
    updated_at: datetime | None = None
    source_url: str | None = None

@dataclass
class EconomicEvent:
    # Stable identifier used for cache + dedupe
    event_id: str

    name: str
    country: str  # "US"
    currency: str  # "USD"

    scheduled_time_et: datetime

    provider: str  # e.g., "BLS", "BEA", "DOL", "FED", "CENSUS", etc.
    provider_configured: bool

    status: EventStatus = "scheduled"
    release: ReleaseData = field(default_factory=ReleaseData)

    # How to group related releases (e.g., CPI m/m + CPI y/y at same timestamp)
    group_key: str | None = None
