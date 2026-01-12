import logging
import time
from typing import Optional

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from ..storage.models import EconEvent

log = logging.getLogger("services.release")

def fetch_release_values(session: requests.Session, event: EconEvent) -> tuple[Optional[str], Optional[str]]:
    """
    Return (actual, previous).
    NOTE: Forecast is not official; kept in config or N/A.

    For MVP we leave this unimplemented per release type, because each agency/release page differs.
    You should implement per-event parsers (CPI, Empsit, PCE, GDP, etc.) as you expand coverage.
    """
    # Placeholder: implement event-specific logic.
    # Example strategy:
    # - map event.id -> official release URL
    # - GET the page once (not per second)
    # - parse actual/previous from the HTML table
    return (None, None)

def poll_for_release(
    session: requests.Session,
    event: EconEvent,
    *,
    phase1_seconds: int,
    phase1_interval: int,
    phase2_seconds: int,
    phase2_interval: int,
    phase3_seconds: int,
    phase3_interval: int,
) -> EconEvent:
    """
    Polite near-release polling with backoff.
    """
    def _try_update(e: EconEvent) -> bool:
        actual, previous = fetch_release_values(session, e)
        if actual is not None or previous is not None:
            e.actual = actual
            e.previous = previous
            e.status = "released" if actual is not None else "missing"
            return True
        return False

    # Phase 1
    t_end = time.time() + phase1_seconds
    while time.time() < t_end:
        if _try_update(event):
            return event
        time.sleep(phase1_interval)

    # Phase 2
    t_end = time.time() + phase2_seconds
    while time.time() < t_end:
        if _try_update(event):
            return event
        time.sleep(phase2_interval)

    # Phase 3
    t_end = time.time() + phase3_seconds
    while time.time() < t_end:
        if _try_update(event):
            return event
        time.sleep(phase3_interval)

    event.status = "missing"
    return event
