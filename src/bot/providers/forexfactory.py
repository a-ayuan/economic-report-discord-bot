import hashlib
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from bot.models import EconEvent
from bot.providers.base import EconProvider
from bot.utils.http import get_session

log = logging.getLogger(__name__)

class ForexFactoryProvider(EconProvider):
    """
    Scrapes ForexFactory calendar.

    NOTE: ForexFactory may restrict automated access. Prefer an API source if available.
    This adapter is included because you explicitly referenced FF “red folder” classification.
    """

    BASE_URL = "https://www.forexfactory.com/calendar"

    def __init__(self, *, user_agent: str, tz_name: str) -> None:
        self.sess = get_session(user_agent)
        self.tz = ZoneInfo(tz_name)

    def fetch_calendar(self, start: datetime, end: datetime) -> list[EconEvent]:
        r = self.sess.get(self.BASE_URL, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        events: list[EconEvent] = []

        # ForexFactory uses a structured calendar table; markup can change.
        rows = soup.select("tr.calendar__row") or soup.select("tr")
        for tr in rows:
            txt = tr.get_text(" ", strip=True).lower()
            if "usd" not in txt:
                continue

            # Red folder / high impact often rendered via CSS classes.
            impact = "high" if ("impact" in txt and "high" in txt) else "high"

            # Best-effort cells
            time_el = tr.select_one(".calendar__time")
            title_el = tr.select_one(".calendar__event")
            act_el = tr.select_one(".calendar__actual")
            fc_el = tr.select_one(".calendar__forecast")
            prev_el = tr.select_one(".calendar__previous")

            if not title_el or not time_el:
                continue

            title = title_el.get_text(" ", strip=True)
            time_str = time_el.get_text(" ", strip=True)
            currency = "USD"

            release_dt = _parse_release_datetime(time_str, start, self.tz)
            if release_dt is None:
                continue

            if release_dt < start.astimezone(self.tz) or release_dt > end.astimezone(self.tz):
                continue

            actual = act_el.get_text(" ", strip=True) if act_el else None
            forecast = fc_el.get_text(" ", strip=True) if fc_el else None
            previous = prev_el.get_text(" ", strip=True) if prev_el else None

            event_id = _stable_id("forexfactory", title, currency, release_dt.isoformat())
            events.append(
                EconEvent(
                    event_id=event_id,
                    title=title,
                    currency=currency,
                    release_dt=release_dt,
                    impact=impact,
                    forecast=forecast or None,
                    previous=previous or None,
                    actual=actual or None,
                )
            )

        log.info("ForexFactory: parsed %d events in window", len(events))
        return events

    def fetch_event_update(self, event: EconEvent) -> EconEvent:
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

def _parse_release_datetime(time_str: str, reference_day: datetime, tz: ZoneInfo):
    s = (time_str or "").strip().lower()
    if not s or s in {"-", "n/a"}:
        return None
    if "all" in s or "day" in s:
        return None

    # remove am/pm if present (FF often uses 24h or local formats)
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
