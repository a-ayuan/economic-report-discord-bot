import hashlib
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from src.bot.models import EconEvent
from src.bot.providers.base import EconProvider
from src.bot.utils.http import get_session

log = logging.getLogger(__name__)


class FXStreetProvider(EconProvider):
    """
    Scrapes FXStreet US calendar page.

    NOTE: This is HTML scraping and may break if FXStreet changes markup.
    """

    BASE_URL = "https://www.fxstreet.com/economic-calendar/united-states"

    def __init__(self, *, user_agent: str, tz_name: str) -> None:
        self.sess = get_session(user_agent)
        self.tz = ZoneInfo(tz_name)

    def fetch_calendar(self, start: datetime, end: datetime) -> list[EconEvent]:
        # FXStreet page is “live”; for a starter we scrape and then filter by date window.
        r = self.sess.get(self.BASE_URL, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        # FXStreet markup changes; this parser is intentionally defensive.
        # Strategy: find rows/items with a time + title + impact + previous/consensus/actual
        events: list[EconEvent] = []

        # Heuristic: look for elements that resemble “actual/consensus/previous” columns
        # Many calendar UIs render a table; if this breaks, switch to another provider adapter.
        rows = soup.select("tr") or []
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue

            text_cells = [td.get_text(" ", strip=True) for td in tds]
            joined = " | ".join(text_cells).lower()
            if "usd" not in joined:
                continue

            # Best-effort extraction
            time_str = text_cells[0]
            title = text_cells[1]
            currency = "USD"

            # impact often shown with colored bars; fallback by keywords
            impact = "high" if ("high" in joined or "red" in joined) else "high"

            forecast = _pick_cell(text_cells, ["consensus", "forecast"], default=None)
            previous = _pick_cell(text_cells, ["previous"], default=None)
            actual = _pick_cell(text_cells, ["actual"], default=None)

            release_dt = _parse_release_datetime(time_str, start, self.tz)
            if release_dt is None:
                continue

            if release_dt < start.astimezone(self.tz) or release_dt > end.astimezone(self.tz):
                continue

            event_id = _stable_id("fxstreet", title, currency, release_dt.isoformat())
            events.append(
                EconEvent(
                    event_id=event_id,
                    title=title,
                    currency=currency,
                    release_dt=release_dt,
                    impact=impact,
                    forecast=forecast,
                    previous=previous,
                    actual=actual,
                )
            )

        log.info("FXStreet: parsed %d events in window", len(events))
        return events

    def fetch_event_update(self, event: EconEvent) -> EconEvent:
        # For simplicity: re-scrape and find the matching event_id
        start = event.release_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(hour=23, minute=59, second=59)
        day_events = self.fetch_calendar(start, end)
        for e in day_events:
            if e.event_id == event.event_id:
                return e
        return event

def _stable_id(provider: str, title: str, currency: str, dt_iso: str) -> str:
    h = hashlib.sha256(f"{provider}|{title}|{currency}|{dt_iso}".encode("utf-8")).hexdigest()
    return f"{provider}_{h[:20]}"

def _pick_cell(cells: list[str], keywords: list[str], default=None):
    # This is a fallback; FXStreet table labels may not be present in-row.
    # If you want higher accuracy, refine selectors to the actual columns.
    return default

def _parse_release_datetime(time_str: str, reference_day: datetime, tz: ZoneInfo):
    """
    Best-effort parse time like '08:30' or '8:30' as on the reference_day date in tz.
    """
    s = (time_str or "").strip().lower()
    if not s:
        return None
    # Some calendars use 'All Day' or '-' for unscheduled
    if "all" in s or s in {"-", "n/a"}:
        return None

    # normalize
    s = s.replace("am", "").replace("pm", "").strip()
    parts = s.split(":")
    if len(parts) != 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except ValueError:
        return None

    return reference_day.astimezone(tz).replace(hour=hh, minute=mm, second=0, microsecond=0)
