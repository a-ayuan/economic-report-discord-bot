import logging
import re
from datetime import date, datetime
from typing import Optional
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
    # CPI-U (All items)
    "CPI_ALL_NSA": "CUUR0000SA0",   # NOT seasonally adjusted (U)
    "CPI_ALL_SA":  "CUSR0000SA0",   # seasonally adjusted (S)

    # CPI-U (All items less food & energy)
    "CPI_CORE_NSA": "CUUR0000SA0L1E",
    "CPI_CORE_SA":  "CUSR0000SA0L1E",

    "PPI_FINAL_DEMAND": "WPUFD4",
    "NFP_PAYROLLS": "CES0000000001",
    "UNEMP_RATE": "LNS14000000",
    "AHE": "CES0500000003",
    "JOLTS_OPENINGS": None,
}

# Accept "08:30 AM", "8:30 a.m. ET", "8:30 AM ET" etc.
TIME_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(a\.m\.|p\.m\.|am|pm)\b", re.IGNORECASE)

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

def _parse_time(s: str) -> tuple[int, int] | None:
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
    out: list[datetime] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            row_text = [c.get_text(" ", strip=True) for c in cells]
            joined = " | ".join([t for t in row_text if t])

            m = re.search(r"([A-Za-z]{3,9}\.?)\s+(\d{1,2}),\s+(\d{4})", joined)
            if not m:
                continue

            mon_raw = m.group(1).strip().lower()
            mon = MONTH_MAP.get(mon_raw)
            if not mon:
                continue

            t = _parse_time(joined)
            if t is None:
                continue

            day = int(m.group(2))
            year = int(m.group(3))
            hh, mm = t
            out.append(datetime(year, mon, day, hh, mm, tzinfo=ZoneInfo(tz_name)))

    return sorted(set(out))

def _extract_datetimes_from_text(soup: BeautifulSoup, tz_name: str) -> list[datetime]:
    text = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    header_idx = -1
    for i, ln in enumerate(lines):
        if "Reference Month" in ln and "Release Date" in ln and "Release Time" in ln:
            header_idx = i
            break
    if header_idx == -1:
        return []

    out: list[datetime] = []
    for ln in lines[header_idx + 1 :]:
        if ln.lower().startswith("subscribe to the bls online calendar"):
            break

        m = SCHEDULE_TEXT_ROW_RE.match(ln)
        if not m:
            continue

        mon_raw = m.group("mon").strip().lower()
        mon = MONTH_MAP.get(mon_raw)
        if not mon:
            continue

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

    return sorted(set(out))

def _extract_schedule_datetimes(soup: BeautifulSoup, tz_name: str) -> list[datetime]:
    dts = _extract_datetimes_from_tables(soup, tz_name)
    return dts if dts else _extract_datetimes_from_text(soup, tz_name)

def _add_months(d: date, delta_months: int) -> date:
    y = d.year
    m = d.month + delta_months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    return date(y, m, 1)

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
                                provider_configured=False,
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
                            provider_configured=False,
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

        if event.name == "CPI m/m":
            return await self._fill_pct_change(event, SERIES["CPI_ALL_SA"], kind="mom", forecast=forecast)

        if event.name == "CPI y/y":
            return await self._fill_pct_change(event, SERIES["CPI_ALL_NSA"], kind="yoy", forecast=forecast)

        if event.name == "Core CPI m/m":
            return await self._fill_pct_change(event, SERIES["CPI_CORE_SA"], kind="mom", forecast=forecast)

        if event.name == "PPI m/m":
            return await self._fill_pct_change(event, SERIES["PPI_FINAL_DEMAND"], kind="mom", forecast=forecast)

        if event.name == "Non-Farm Employment Change":
            return await self._fill_level_change(event, SERIES["NFP_PAYROLLS"], unit="K", forecast=forecast)

        if event.name == "Unemployment Rate":
            return await self._fill_latest(event, SERIES["UNEMP_RATE"], unit="%", forecast=forecast)

        if event.name == "Average Hourly Earnings m/m":
            return await self._fill_pct_change(event, SERIES["AHE"], kind="mom", forecast=forecast)

        event.status = "disabled"
        return event

    async def _bls_post(self, series_id: str) -> dict:
        payload: dict = {"seriesid": [series_id]}
        if self.api_key:
            payload["registrationKey"] = self.api_key
        payload["startyear"] = str(datetime.now().year - 3)
        payload["endyear"] = str(datetime.now().year)
        return await self.http.post_json(BLS_API_V2, payload)

    @staticmethod
    def _extract_latest_points(resp: dict) -> list[tuple[date, Optional[float]]]:
        series = (resp.get("Results") or {}).get("series") or []
        if not series:
            return []

        data = series[0].get("data") or []
        pts: list[tuple[date, Optional[float]]] = []

        for row in data:
            year = row.get("year")
            period = row.get("period")
            val = row.get("value")

            if not (year and period and isinstance(period, str) and period.startswith("M")):
                continue

            try:
                m = int(period[1:])
            except Exception:
                continue
            if m < 1 or m > 12:
                continue

            d = date(int(year), m, 1)
            if val in (None, "", "-"):
                pts.append((d, None))
                continue

            try:
                v = float(val)
            except Exception:
                pts.append((d, None))
                continue

            pts.append((d, v))

        pts.sort(key=lambda x: x[0])
        return pts

    @staticmethod
    def _points_to_month_map(pts: list[tuple[date, Optional[float]]]) -> dict[date, Optional[float]]:
        return {d: v for d, v in pts}

    @staticmethod
    def _find_last_valid_mom(
        month_map: dict[date, Optional[float]],
        months_sorted: list[date],
        *,
        before: Optional[date] = None,
    ) -> tuple[date, float] | None:
        for end in reversed(months_sorted):
            if before is not None and end >= before:
                continue
            prev = _add_months(end, -1)
            v_end = month_map.get(end)
            v_prev = month_map.get(prev)
            if v_end is None or v_prev is None:
                continue
            mom = (v_end / v_prev - 1.0) * 100.0
            return end, mom
        return None

    @staticmethod
    def _find_last_valid_yoy(
        month_map: dict[date, Optional[float]],
        months_sorted: list[date],
        *,
        before: Optional[date] = None,
    ) -> tuple[date, float] | None:
        for end in reversed(months_sorted):
            if before is not None and end >= before:
                continue
            base = _add_months(end, -12)
            v_end = month_map.get(end)
            v_base = month_map.get(base)
            if v_end is None or v_base is None:
                continue
            yoy = (v_end / v_base - 1.0) * 100.0
            return end, yoy
        return None

    async def _fill_latest(self, event: EconomicEvent, series_id: str, unit: str, forecast: str | None) -> EconomicEvent:
        resp = await self._bls_post(series_id)
        pts = self._extract_latest_points(resp)
        vals = [(d, v) for d, v in pts if v is not None]
        if len(vals) < 2:
            return event

        prev = vals[-2][1]
        cur = vals[-1][1]

        event.status = "released"
        event.release = ReleaseData(
            actual=f"{cur:.1f}",
            previous=f"{prev:.1f}",
            forecast=forecast,
            unit=unit,
            updated_at=datetime.now(ZoneInfo(self.tz_name)),
            source_url=BLS_API_V2,
        )
        return event

    async def _fill_pct_change(self, event: EconomicEvent, series_id: str, kind: str, forecast: str | None) -> EconomicEvent:
        resp = await self._bls_post(series_id)
        pts = self._extract_latest_points(resp)
        month_map = self._points_to_month_map(pts)
        months = sorted(month_map.keys())
        if not months:
            return event

        if kind == "mom":
            last = self._find_last_valid_mom(month_map, months)
            if last is None:
                return event
            end_d, mom = last

            prev_calc = self._find_last_valid_mom(month_map, months, before=end_d)
            prev_str = f"{prev_calc[1]:.1f}%" if prev_calc is not None else None

            latest_calendar = months[-1]
            if end_d != latest_calendar:
                log.warning(
                    "Latest m/m unavailable for %s at %s; falling back to last valid m/m ending %s",
                    series_id,
                    latest_calendar,
                    end_d,
                )

            event.status = "released"
            event.release = ReleaseData(
                actual=f"{mom:.1f}%",
                previous=prev_str,
                forecast=forecast,
                unit="%",
                updated_at=datetime.now(ZoneInfo(self.tz_name)),
                source_url=BLS_API_V2,
            )
            return event

        if kind == "yoy":
            last = self._find_last_valid_yoy(month_map, months)
            if last is None:
                return event
            end_d, yoy = last

            prev_calc = self._find_last_valid_yoy(month_map, months, before=end_d)
            prev_str = f"{prev_calc[1]:.1f}%" if prev_calc is not None else None

            latest_calendar = months[-1]
            if end_d != latest_calendar:
                log.warning(
                    "Latest y/y unavailable for %s at %s; falling back to last valid y/y ending %s",
                    series_id,
                    latest_calendar,
                    end_d,
                )

            event.status = "released"
            event.release = ReleaseData(
                actual=f"{yoy:.1f}%",
                previous=prev_str,
                forecast=forecast,
                unit="%",
                updated_at=datetime.now(ZoneInfo(self.tz_name)),
                source_url=BLS_API_V2,
            )
            return event

        return event

    async def _fill_level_change(self, event: EconomicEvent, series_id: str, unit: str, forecast: str | None) -> EconomicEvent:
        resp = await self._bls_post(series_id)
        pts = self._extract_latest_points(resp)
        vals = [(d, v) for d, v in pts if v is not None]
        if len(vals) < 3:
            return event

        prevprev = vals[-3][1]
        prev = vals[-2][1]
        cur = vals[-1][1]

        change = cur - prev
        prev_change = prev - prevprev

        event.status = "released"
        event.release = ReleaseData(
            actual=f"{change:.0f}",
            previous=f"{prev_change:.0f}",
            forecast=forecast,
            unit=unit,
            updated_at=datetime.now(ZoneInfo(self.tz_name)),
            source_url=BLS_API_V2,
        )
        return event
