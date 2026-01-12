from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import requests

from ..utils.http import http_get_text

log = logging.getLogger("sources.census")

CENSUS_CAL_URL = "https://www.census.gov/economic-indicators/calendar-listview.html"

@dataclass(frozen=True)
class CensusScheduleItem:
    title: str
    release_dt: datetime

def fetch_census_schedule(session: requests.Session) -> list[CensusScheduleItem]:
    html = http_get_text(session, CENSUS_CAL_URL)
    soup = BeautifulSoup(html, "html.parser")
    tz = ZoneInfo("America/New_York")

    items: list[CensusScheduleItem] = []

    # The list view has rows with columns including Release Date and Time.
    rows = soup.select("table tbody tr")
    for tr in rows:
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        # Common: [Indicator, Release Date, Time, Reference Period, ...]
        if len(tds) < 3:
            continue
        title, date_txt, time_txt = tds[0], tds[1], tds[2]
        if not title or not time_txt:
            continue
        try:
            dt = datetime.strptime(f"{date_txt} {time_txt}", "%B %d, %Y %I:%M %p")
            dt = dt.replace(tzinfo=tz)
            items.append(CensusScheduleItem(title=title, release_dt=dt))
        except Exception:
            continue

    log.info("Census schedule parsed %d items", len(items))
    return items
