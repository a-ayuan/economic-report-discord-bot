from dataclasses import dataclass
from datetime import datetime

@dataclass
class EconEvent:
    id: str
    source: str
    title: str
    release_dt: datetime
    url: str | None = None

    forecast: str | None = None
    previous: str | None = None
    actual: str | None = None

    status: str = "upcoming"  # upcoming|released|missing
