import logging
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import requests

from ..utils.http import http_get_text

log = logging.getLogger("sources.bea")

BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"

@dataclass(frozen=True)
class BeaScheduleItem:
    title: str
    release_dt: datetime

def fetch_bea_schedule(session: requests.Session) -> list[BeaScheduleItem]:
    html = http_get_text(session, BEA_SCHEDULE_URL)
    soup = BeautifulSoup(html, "html.parser")
    tz = ZoneInfo("America/New_York")

    items: list[BeaScheduleItem] = []

    # Rows often appear as a table in the "Upcoming Releases" tab content.
    rows = soup.select("table tbody tr")
    for tr in rows:
        tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
        if len(tds) < 3:
            continue
        date_txt, time_txt, title = tds[0], tds[1], tds[2]
        if not title:
            continue
        try:
            dt = datetime.strptime(f"{date_txt} {time_txt}", "%B %d %I:%M %p")
            dt = dt.replace(year=datetime.now(tz=tz).year, tzinfo=tz)
            items.append(BeaScheduleItem(title=title, release_dt=dt))
        except Exception:
            continue

    log.info("BEA schedule parsed %d items", len(items))
    return items
