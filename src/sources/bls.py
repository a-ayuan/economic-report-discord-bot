import logging
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import requests

from ..utils.http import http_get_text

log = logging.getLogger("sources.bls")

BLS_SCHEDULE_URL = "https://www.bls.gov/schedule/"

@dataclass(frozen=True)
class BlsScheduleItem:
    title: str
    release_dt: datetime

def fetch_bls_schedule(session: requests.Session) -> list[BlsScheduleItem]:
    """
    Parses https://www.bls.gov/schedule/ (selected releases list).
    This is a lightweight calendar source and should be polled infrequently.
    """
    html = http_get_text(session, BLS_SCHEDULE_URL)
    soup = BeautifulSoup(html, "html.parser")

    # The schedule page is a table-like layout; we keep parsing flexible.
    rows = soup.select("table tbody tr")
    items: list[BlsScheduleItem] = []
    tz = ZoneInfo("America/New_York")

    for tr in rows:
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        # Common layout: Date | Release | Time
        if len(tds) < 3:
            continue
        date_txt, rel_txt, time_txt = tds[0], tds[1], tds[2]
        if not rel_txt or "AM" not in time_txt and "PM" not in time_txt:
            continue
        try:
            # Example: "January 13" (year may be implied on page)
            # We'll infer year as current year if missing.
            year = datetime.now(tz=tz).year
            dt = datetime.strptime(f"{date_txt} {year} {time_txt}", "%B %d %Y %I:%M %p")
            dt = dt.replace(tzinfo=tz)
            items.append(BlsScheduleItem(title=rel_txt, release_dt=dt))
        except Exception:
            continue

    log.info("BLS schedule parsed %d items", len(items))
    return items
