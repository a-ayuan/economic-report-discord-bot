# src/providers/census.py
import logging
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.census")

CENSUS_TS_DOC = "https://api.census.gov/data/timeseries.html"
CENSUS_EI_CAL_LIST = "https://www.census.gov/economic-indicators/calendar-listview.html"

CENSUS_RETAIL_RELEASE_SCHEDULE = "https://www.census.gov/retail/release_schedule.html"
CENSUS_MARTS_CURRENT_PDF = "https://www.census.gov/retail/marts/www/marts_current.pdf"

EITS_MRTSADV_BASE = "https://api.census.gov/data/timeseries/eits/mrtsadv"

CAT_TOTAL = "44X72"
CAT_AUTOS = "441"
DATA_TYPE_SALES = "SM"

INDICATOR_NAME = "Advance Monthly Sales for Retail and Food Services"

_A_CODE_RE = re.compile(r"\bA(\d{12})\b")


def _month_key(dt_et: datetime) -> str:
    return f"{dt_et.year:04d}-{dt_et.month:02d}"


def _prev_month_key(yyyy_mm: str) -> str:
    y, m = yyyy_mm.split("-")
    year = int(y)
    month = int(m) - 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year:04d}-{month:02d}"


def _parse_code_dt_from_any(text_or_href: str, tz: ZoneInfo) -> datetime:
    m = _A_CODE_RE.search(text_or_href or "")
    if not m:
        raise ValueError("No AYYYYMMDDHHMM token found")
    return datetime.strptime(m.group(1), "%Y%m%d%H%M").replace(tzinfo=tz)


def _is_truthy_sa(v: str) -> bool:
    s = str(v).strip().lower()
    return s in {"1", "y", "yes", "true", "t", "sa", "s"}


class CensusProvider(Provider):
    name = "CENSUS"

    def __init__(self, http: HttpClient, tz_name: str, api_key: str | None):
        self.http = http
        self.tz_name = tz_name
        self.api_key = api_key
        self._year_cache: dict[tuple[str, str], list[list[str]]] = {}

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        tz = ZoneInfo(self.tz_name)

        if start_et.tzinfo is None:
            start_et = start_et.replace(tzinfo=tz)
        else:
            start_et = start_et.astimezone(tz)

        if end_et.tzinfo is None:
            end_et = end_et.replace(tzinfo=tz)
        else:
            end_et = end_et.astimezone(tz)

        html = await self.http.get_text(CENSUS_EI_CAL_LIST)
        soup = BeautifulSoup(html, "html.parser")

        events: list[EconomicEvent] = []
        seen_stamps: set[str] = set()

        for tr in soup.find_all("tr"):
            row_text = tr.get_text(" ", strip=True)
            if INDICATOR_NAME.lower() not in row_text.lower():
                continue

            if re.search(r"\bTBD\b", row_text, flags=re.IGNORECASE):
                continue

            dt_local: datetime | None = None

            try:
                dt_local = _parse_code_dt_from_any(row_text, tz)
            except Exception:
                pass

            if dt_local is None:
                for a in tr.find_all("a", href=True):
                    try:
                        dt_local = _parse_code_dt_from_any(a.get("href", ""), tz)
                        break
                    except Exception:
                        continue

            if dt_local is None:
                continue

            if not (start_et <= dt_local < end_et):
                continue

            stamp = dt_local.isoformat()
            if stamp in seen_stamps:
                continue
            seen_stamps.add(stamp)

            group = f"census:marts:{stamp}"

            events.append(
                EconomicEvent(
                    event_id=safe_event_id("census", "Retail Sales m/m", stamp),
                    name="Retail Sales m/m",
                    country="US",
                    currency="USD",
                    scheduled_time_et=dt_local,
                    provider="CENSUS",
                    provider_configured=True,
                    group_key=group,
                )
            )
            events.append(
                EconomicEvent(
                    event_id=safe_event_id("census", "Core Retail Sales m/m", stamp),
                    name="Core Retail Sales m/m",
                    country="US",
                    currency="USD",
                    scheduled_time_et=dt_local,
                    provider="CENSUS",
                    provider_configured=True,
                    group_key=group,
                )
            )

        return sorted(events, key=lambda e: e.scheduled_time_et)

    async def prefill_previous(self, event: EconomicEvent) -> EconomicEvent:
        if not event.provider_configured:
            return event

        if getattr(event, "release", None) is None:
            event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)

        if event.release.previous is not None:
            return event

        tz = ZoneInfo(self.tz_name)
        event_time = event.scheduled_time_et
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=tz)

        # Retail Sales "report month" is the month before the release date.
        # Example: January 2026 release -> report month is 2025-12.
        report_month = _prev_month_key(_month_key(event_time))
        prev_month = _prev_month_key(report_month)
        prevprev_month = _prev_month_key(prev_month)

        try:
            if event.name == "Retail Sales m/m":
                prev_mm = await self._compute_mm_change(CAT_TOTAL, prev_month, prevprev_month)
                event.release.previous = f"{prev_mm:.1f}%"
            elif event.name == "Core Retail Sales m/m":
                prev_mm = await self._compute_core_mm_change(prev_month, prevprev_month)
                event.release.previous = f"{prev_mm:.1f}%"

            event.release.unit = "%"
            event.release.source_url = CENSUS_MARTS_CURRENT_PDF
            event.release.updated_at = datetime.now(ZoneInfo(self.tz_name))
        except Exception as e:
            log.exception("Census prefill_previous failed for %s: %s", event.name, e)

        return event

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        tz = ZoneInfo(self.tz_name)
        event_time = event.scheduled_time_et
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=tz)

        # Actual is for the report month (month before release date)
        report_month = _prev_month_key(_month_key(event_time))
        prev_month = _prev_month_key(report_month)
        prevprev_month = _prev_month_key(prev_month)

        try:
            if getattr(event, "release", None) is None:
                event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)

            if event.name == "Retail Sales m/m":
                actual_mm = await self._compute_mm_change(CAT_TOTAL, report_month, prev_month)
                if event.release.previous is None:
                    prev_mm = await self._compute_mm_change(CAT_TOTAL, prev_month, prevprev_month)
                    event.release.previous = f"{prev_mm:.1f}%"
                event.release.actual = f"{actual_mm:.1f}%"

            elif event.name == "Core Retail Sales m/m":
                actual_mm = await self._compute_core_mm_change(report_month, prev_month)
                if event.release.previous is None:
                    prev_mm = await self._compute_core_mm_change(prev_month, prevprev_month)
                    event.release.previous = f"{prev_mm:.1f}%"
                event.release.actual = f"{actual_mm:.1f}%"

            else:
                event.status = "disabled"
                return event

            event.release.forecast = None
            event.release.unit = "%"
            event.release.source_url = CENSUS_MARTS_CURRENT_PDF
            event.release.updated_at = datetime.now(ZoneInfo(self.tz_name))
            event.status = "released"
            return event

        except Exception as e:
            log.exception("Census fetch_release failed for %s: %s", event.name, e)
            event.status = "scheduled"
            event.release = ReleaseData(source_url=CENSUS_RETAIL_RELEASE_SCHEDULE)
            return event

    async def _fetch_year_rows(self, year: str) -> list[list[str]]:
        cache_key = ("mrtsadv", year)
        if cache_key in self._year_cache:
            return self._year_cache[cache_key]

        params = "get=cell_value,seasonally_adj,data_type_code,category_code,time_slot_id,time&for=us:*"
        if self.api_key:
            params += f"&key={self.api_key}"

        # IMPORTANT: Use year-only time (Census examples do this for mrtsadv)
        url = f"{EITS_MRTSADV_BASE}?{params}&time={year}"

        body = (await self.http.get_text(url)) or ""
        body = body.strip()
        if not body:
            # Census can return 204 with empty body when no data yet
            self._year_cache[cache_key] = []
            return []

        # Body is JSON (array of arrays)
        import json

        data = json.loads(body)
        if not data or len(data) < 2:
            self._year_cache[cache_key] = []
            return []

        header = data[0]
        rows = data[1:]

        # Convert to list[str] rows, but keep header separate in caller
        out: list[list[str]] = [header] + rows
        self._year_cache[cache_key] = out
        return out

    async def _fetch_sales_value(self, category_code: str, month_yyyy_mm: str) -> float:
        year, _month = month_yyyy_mm.split("-")
        data = await self._fetch_year_rows(year)
        if not data:
            raise RuntimeError(f"No Census API data returned for year={year}")

        header = data[0]
        rows = data[1:]

        idx_val = header.index("cell_value")
        idx_sa = header.index("seasonally_adj")
        idx_dt = header.index("data_type_code")
        idx_cat = header.index("category_code")
        idx_slot = header.index("time_slot_id")
        idx_time = header.index("time")

        # Filter down to the exact month/category/datatype/monthly slot
        candidates: list[list[str]] = []
        for r in rows:
            if str(r[idx_time]).strip() != month_yyyy_mm:
                continue
            if str(r[idx_slot]).strip().upper() != "M":
                continue
            if str(r[idx_dt]).strip().upper() != DATA_TYPE_SALES:
                continue
            if str(r[idx_cat]).strip().upper() != category_code.upper():
                continue
            candidates.append(r)

        if not candidates:
            raise RuntimeError(f"No matching Census rows for {category_code} {month_yyyy_mm}")

        # Prefer seasonally adjusted rows
        best = None
        for r in candidates:
            if _is_truthy_sa(r[idx_sa]):
                best = r
                break
        if best is None:
            best = candidates[0]

        v = str(best[idx_val]).replace(",", "").strip()
        return float(v)

    async def _compute_mm_change(self, category_code: str, cur_month: str, prev_month: str) -> float:
        cur_val = await self._fetch_sales_value(category_code, cur_month)
        prev_val = await self._fetch_sales_value(category_code, prev_month)
        return self._pct_change(cur_val, prev_val)

    async def _compute_core_mm_change(self, cur_month: str, prev_month: str) -> float:
        total_cur = await self._fetch_sales_value(CAT_TOTAL, cur_month)
        total_prev = await self._fetch_sales_value(CAT_TOTAL, prev_month)

        autos_cur = await self._fetch_sales_value(CAT_AUTOS, cur_month)
        autos_prev = await self._fetch_sales_value(CAT_AUTOS, prev_month)

        core_cur = total_cur - autos_cur
        core_prev = total_prev - autos_prev
        return self._pct_change(core_cur, core_prev)

    @staticmethod
    def _pct_change(cur: float, prev: float) -> float:
        if prev == 0:
            raise ZeroDivisionError("Previous value is zero; cannot compute percent change.")
        return (cur - prev) / prev * 100.0
