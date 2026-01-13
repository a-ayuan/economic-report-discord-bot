import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.bls")

# Official BLS schedule pages
BLS_SCHEDULES = {
    "Employment Situation": "https://www.bls.gov/schedule/news_release/empsit.htm",
    "CPI": "https://www.bls.gov/schedule/news_release/cpi.htm",
    "PPI": "https://www.bls.gov/schedule/news_release/ppi.htm",
    "JOLTS": "https://www.bls.gov/schedule/news_release/jolts.htm",
}

BLS_API_V2 = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

SERIES = {
    "CPI_ALL_SA": "CUUR0000SA0",
    "CPI_CORE_SA": "CUUR0000SA0L1E",
    "PPI_FINAL_DEMAND": "WPUFD4",
    "NFP_PAYROLLS": "CES0000000001",
    "UNEMP_RATE": "LNS14000000",
    "AHE": "CES0500000003",
    "JOLTS_OPENINGS": None,  # needs mapping if you want it through BLS API
}

# Accept "08:30 AM", "8:30 a.m. ET", "8:30 AM ET" etc.
TIME_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(a\.m\.|p\.m\.|am|pm)\b", re.IGNORECASE)

# BLS schedule text rows commonly look like:
# "December 2025 Jan. 13, 2026 08:30 AM"
# Some months are "May" (no dot). Some are "Nov." (dot).
SCHEDULE_TEXT_ROW_RE = re.compile(
    r"^(?P<ref_month>[A-Za-z]+\s+\d{4})\s+"
    r"(?P<mon>[A-Za-z]{3,9}\.?)\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4})\s+"
    r"(?P<hh>\d{1,2}):(?P<mm>\d{2})\s*(?P<ampm>AM|PM)$",
    re.IGNORECASE,
)

MONTH_MAP = {
    "jan": 1,
    "jan.": 1,
    "feb": 2,
    "feb.": 2,
    "mar": 3,
    "mar.": 3,
    "apr": 4,
    "apr.": 4,
    "may": 5,
    "jun": 6,
    "jun.": 6,
    "jul": 7,
    "jul.": 7,
    "aug": 8,
    "aug.": 8,
    "sep": 9,
    "sep.": 9,
    "sept": 9,
    "sept.": 9,
    "oct": 10,
    "oct.": 10,
    "nov": 11,
    "nov.": 11,
    "dec": 12,
    "dec.": 12,
}

def _et(dt: datetime, tz_name: str) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo(tz_name))
    return dt.astimezone(ZoneInfo(tz_name))

def _parse_time(s: str) -> tuple[int, int] | None:
    """
    Parse time strings like:
      "08:30 AM", "8:30 a.m. ET", "8:30 AM ET"
    """
    if not s:
        return None
    s2 = s.strip().lower().replace("et", "").strip()
    s2 = s2.replace("a.m.", "am").replace("p.m.", "pm")

    m = TIME_RE.search(s2)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    ampm = m.group(3).lower().replace(".", "")
    if ampm == "pm" and hh != 12:
        hh += 12
    if ampm == "am" and hh == 12:
        hh = 0
    return hh, mm

def _extract_datetimes_from_tables(soup: BeautifulSoup, tz_name: str) -> list[datetime]:
    """
    If the page has a real HTML table, try to extract schedule rows.
    We do NOT assume one fixed header; we just look for rows that contain a parsable date + time.
    """
    out: list[datetime] = []

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            # Try to find a "Release Date" like "Jan. 13, 2026" somewhere in the row
            row_text = [c.get_text(" ", strip=True) for c in cells]
            joined = " | ".join([t for t in row_text if t])

            # Find an embedded "Mon. DD, YYYY"
            m = re.search(r"([A-Za-z]{3,9}\.?)\s+(\d{1,2}),\s+(\d{4})", joined)
            if not m:
                continue

            mon_raw = m.group(1).strip().lower()
            if mon_raw not in MONTH_MAP:
                continue
            mon = MONTH_MAP[mon_raw]
            day = int(m.group(2))
            year = int(m.group(3))

            # Find a time token
            t = _parse_time(joined)
            if t is None:
                continue
            hh, mm = t

            dt = datetime(year, mon, day, hh, mm, tzinfo=ZoneInfo(tz_name))
            out.append(dt)

    # De-dupe
    uniq = sorted(set(out))
    return uniq

def _extract_datetimes_from_text(soup: BeautifulSoup, tz_name: str) -> list[datetime]:
    """
    Parse the plain-text schedule section:
      "Reference Month Release Date Release Time"
      "December 2025 Jan. 09, 2026 08:30 AM"
    """
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Find the schedule header line
    header_idx = -1
    for i, ln in enumerate(lines):
        # BLS pages show: "Reference Month Release Date Release Time"
        if "Reference Month" in ln and "Release Date" in ln and "Release Time" in ln:
            header_idx = i
            break

    if header_idx == -1:
        return []

    out: list[datetime] = []
    for ln in lines[header_idx + 1 :]:
        # Stop once we reach subscription/footer content
        if ln.lower().startswith("subscribe to the bls online calendar"):
            break

        m = SCHEDULE_TEXT_ROW_RE.match(ln)
        if not m:
            # Some pages contain occasional non-row lines; ignore.
            continue

        mon_raw = m.group("mon").strip().lower()
        if mon_raw not in MONTH_MAP:
            continue

        mon = MONTH_MAP[mon_raw]
        day = int(m.group("day"))
        year = int(m.group("year"))
        hh = int(m.group("hh"))
        mm = int(m.group("mm"))
        ampm = m.group("ampm").strip().upper()

        if ampm == "PM" and hh != 12:
            hh += 12
        if ampm == "AM" and hh == 12:
            hh = 0

        out.append(datetime(year, mon, day, hh, mm, tzinfo=ZoneInfo(tz_name)))

    uniq = sorted(set(out))
    return uniq

def _extract_schedule_datetimes(soup: BeautifulSoup, tz_name: str) -> list[datetime]:
    """
    Combined extractor:
      1) Try tables (some BLS pages may be table-based in the future)
      2) Fall back to parsing the schedule section text (current common format)
    """
    dts = _extract_datetimes_from_tables(soup, tz_name)
    if dts:
        return dts
    return _extract_datetimes_from_text(soup, tz_name)

class BLSProvider(Provider):
    name = "BLS"

    def __init__(self, http: HttpClient, tz_name: str, api_key: str | None):
        self.http = http
        self.tz_name = tz_name
        self.api_key = api_key

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        events: list[EconomicEvent] = []

        for key, url in BLS_SCHEDULES.items():
            try:
                html = await self.http.get_text(url)
            except Exception as e:
                log.exception("Failed to fetch BLS schedule page %s: %s", url, e)
                continue

            soup = BeautifulSoup(html, "html.parser")
            sched_times = _extract_schedule_datetimes(soup, self.tz_name)

            if not sched_times:
                log.warning("No schedule rows detected on %s", url)
                continue

            for scheduled in sched_times:
                if not (start_et <= scheduled < end_et):
                    continue

                stamp = scheduled.isoformat()

                if key == "Employment Situation":
                    group = f"empsit:{stamp}"
                    events.extend(
                        [
                            EconomicEvent(
                                event_id=safe_event_id("bls", "Non-Farm Employment Change", stamp),
                                name="Non-Farm Employment Change",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=True,
                                group_key=group,
                            ),
                            EconomicEvent(
                                event_id=safe_event_id("bls", "Unemployment Rate", stamp),
                                name="Unemployment Rate",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=True,
                                group_key=group,
                            ),
                            EconomicEvent(
                                event_id=safe_event_id("bls", "Average Hourly Earnings m/m", stamp),
                                name="Average Hourly Earnings m/m",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=True,
                                group_key=group,
                            ),
                        ]
                    )

                elif key == "CPI":
                    group = f"cpi:{stamp}"
                    events.extend(
                        [
                            EconomicEvent(
                                event_id=safe_event_id("bls", "CPI m/m", stamp),
                                name="CPI m/m",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=True,
                                group_key=group,
                            ),
                            EconomicEvent(
                                event_id=safe_event_id("bls", "CPI y/y", stamp),
                                name="CPI y/y",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=True,
                                group_key=group,
                            ),
                            EconomicEvent(
                                event_id=safe_event_id("bls", "Core CPI m/m", stamp),
                                name="Core CPI m/m",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=True,
                                group_key=group,
                            ),
                        ]
                    )

                elif key == "PPI":
                    group = f"ppi:{stamp}"
                    events.extend(
                        [
                            EconomicEvent(
                                event_id=safe_event_id("bls", "PPI m/m", stamp),
                                name="PPI m/m",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=True,
                                group_key=group,
                            ),
                            EconomicEvent(
                                event_id=safe_event_id("bls", "Core PPI m/m", stamp),
                                name="Core PPI m/m",
                                country="US",
                                currency="USD",
                                scheduled_time_et=scheduled,
                                provider=self.name,
                                provider_configured=False,  # not mapped by default
                                group_key=group,
                            ),
                        ]
                    )

                elif key == "JOLTS":
                    group = f"jolts:{stamp}"
                    events.append(
                        EconomicEvent(
                            event_id=safe_event_id("bls", "JOLTS Job Openings", stamp),
                            name="JOLTS Job Openings",
                            country="US",
                            currency="USD",
                            scheduled_time_et=scheduled,
                            provider=self.name,
                            provider_configured=False,  # needs series id mapping
                            group_key=group,
                        )
                    )

        return sorted(events, key=lambda x: x.scheduled_time_et)

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        if not event.provider_configured:
            event.status = "disabled"
            event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)
            return event

        forecast = None  # not provided by BLS

        if event.name in ("CPI m/m", "CPI y/y"):
            series_id = SERIES["CPI_ALL_SA"]
            return await self._fill_pct_change(
                event,
                series_id,
                kind="yoy" if "y/y" in event.name else "mom",
                forecast=forecast,
            )

        if event.name == "Core CPI m/m":
            series_id = SERIES["CPI_CORE_SA"]
            return await self._fill_pct_change(event, series_id, kind="mom", forecast=forecast)

        if event.name == "PPI m/m":
            series_id = SERIES["PPI_FINAL_DEMAND"]
            return await self._fill_pct_change(event, series_id, kind="mom", forecast=forecast)

        if event.name == "Non-Farm Employment Change":
            series_id = SERIES["NFP_PAYROLLS"]
            return await self._fill_level_change(event, series_id, unit="K", forecast=forecast)

        if event.name == "Unemployment Rate":
            series_id = SERIES["UNEMP_RATE"]
            return await self._fill_latest(event, series_id, unit="%", forecast=forecast)

        if event.name == "Average Hourly Earnings m/m":
            series_id = SERIES["AHE"]
            return await self._fill_pct_change(event, series_id, kind="mom", forecast=forecast)

        event.status = "disabled"
        return event

    async def _bls_post(self, series_id: str) -> dict:
        payload: dict = {"seriesid": [series_id]}
        if self.api_key:
            payload["registrationKey"] = self.api_key
        payload["startyear"] = str(datetime.now().year - 2)
        payload["endyear"] = str(datetime.now().year)
        return await self.http.post_json(BLS_API_V2, payload)

    @staticmethod
    def _extract_latest_points(resp: dict) -> list[tuple[str, float]]:
        series = (resp.get("Results") or {}).get("series") or []
        if not series:
            return []
        data = series[0].get("data") or []
        pts: list[tuple[str, float]] = []
        for row in data:
            year = row.get("year")
            period = row.get("period")
            if not (year and period and str(period).startswith("M")):
                continue
            m = int(str(period)[1:])
            ym = f"{year}-{m:02d}"
            try:
                v = float(row.get("value"))
            except Exception:
                continue
            pts.append((ym, v))
        pts.sort(key=lambda x: x[0])
        return pts

    async def _fill_latest(self, event: EconomicEvent, series_id: str, unit: str, forecast: str | None) -> EconomicEvent:
        resp = await self._bls_post(series_id)
        pts = self._extract_latest_points(resp)
        if len(pts) < 2:
            return event

        prev = pts[-2][1]
        cur = pts[-1][1]

        event.status = "released"
        event.release = ReleaseData(
            actual=f"{cur:.2f}",
            previous=f"{prev:.2f}",
            forecast=forecast,
            unit=unit,
            updated_at=datetime.now(ZoneInfo(self.tz_name)),
            source_url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
        )
        return event

    async def _fill_pct_change(self, event: EconomicEvent, series_id: str, kind: str, forecast: str | None) -> EconomicEvent:
        resp = await self._bls_post(series_id)
        pts = self._extract_latest_points(resp)

        if kind == "mom":
            if len(pts) < 3:
                return event
            prev = pts[-2][1]
            cur = pts[-1][1]
            prevprev = pts[-3][1]
            mom = (cur / prev - 1.0) * 100.0
            prev_mom = (prev / prevprev - 1.0) * 100.0
            event.status = "released"
            event.release = ReleaseData(
                actual=f"{mom:.2f}%",
                previous=f"{prev_mom:.2f}%",
                forecast=forecast,
                unit="%",
                updated_at=datetime.now(ZoneInfo(self.tz_name)),
                source_url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
            )
            return event

        if kind == "yoy":
            if len(pts) < 14:
                return event
            cur = pts[-1][1]
            year_ago = pts[-13][1]
            prev = pts[-2][1]
            prev_year_ago = pts[-14][1]
            yoy = (cur / year_ago - 1.0) * 100.0
            prev_yoy = (prev / prev_year_ago - 1.0) * 100.0
            event.status = "released"
            event.release = ReleaseData(
                actual=f"{yoy:.2f}%",
                previous=f"{prev_yoy:.2f}%",
                forecast=forecast,
                unit="%",
                updated_at=datetime.now(ZoneInfo(self.tz_name)),
                source_url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
            )
            return event

        return event

    async def _fill_level_change(self, event: EconomicEvent, series_id: str, unit: str, forecast: str | None) -> EconomicEvent:
        resp = await self._bls_post(series_id)
        pts = self._extract_latest_points(resp)
        if len(pts) < 3:
            return event
        prev = pts[-2][1]
        cur = pts[-1][1]
        prevprev = pts[-3][1]
        change = cur - prev
        prev_change = prev - prevprev
        event.status = "released"
        event.release = ReleaseData(
            actual=f"{change:.0f}",
            previous=f"{prev_change:.0f}",
            forecast=forecast,
            unit=unit,
            updated_at=datetime.now(ZoneInfo(self.tz_name)),
            source_url="https://api.bls.gov/publicAPI/v2/timeseries/data/",
        )
        return event
