import json
import logging
import re
from datetime import datetime
from typing import Optional
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

# Monthly ART survey time series API
EITS_MARTS_BASE = "https://api.census.gov/data/timeseries/eits/marts"

CAT_TOTAL = "44X72"
CAT_AUTOS_PREFIX = "441"

# Not needed
# CAT_GAS_PREFIX = "447"
# CAT_BUILDING_PREFIX = "444"
# CAT_FOOD_SERVICES_PREFIX = "722"

DATA_TYPE_SALES = "SM"
INDICATOR_NAME = "Advance Monthly Sales for Retail and Food Services"

# Calendar list rows include:
#   Release datetime code: AYYYYMMDDHHMM  (12 digits)
#   Period covered code:   AYYYYMM        (6 digits)
_A_RELEASE_DT_RE = re.compile(r"\bA(\d{12})\b")
_A_PERIOD_RE = re.compile(r"\bA(\d{6})(?!\d)\b")

def _prev_month_key(yyyy_mm: str) -> str:
    y, m = yyyy_mm.split("-")
    year = int(y)
    month = int(m) - 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year:04d}-{month:02d}"

def _parse_time_yyyy_mm(v: str) -> Optional[str]:
    s = str(v).strip()
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    return None

def _is_sa(v: str) -> bool:
    return str(v).strip().lower() in {"1", "y", "yes", "true", "t", "sa", "s"}

def _period_code_to_yyyy_mm(code6: str) -> str:
    return f"{code6[0:4]}-{code6[4:6]}"

def _mk_group_key(stamp: str, period_yyyy_mm: str) -> str:
    # Persist period-covered month in the group key so it survives JSON save/load.
    return f"census:marts:{stamp}:p={period_yyyy_mm}"

def _period_from_group_key(group_key: Optional[str]) -> Optional[str]:
    if not group_key:
        return None
    m = re.search(r":p=(\d{4}-\d{2})\b", group_key)
    return m.group(1) if m else None

class CensusProvider(Provider):
    name = "CENSUS"

    def __init__(self, http: HttpClient, tz_name: str, api_key: str | None):
        self.http = http
        self.tz_name = tz_name
        self.api_key = api_key
        self._cache: list[list[str]] | None = None
        self._cache_year_range: tuple[int, int] | None = None

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        tz = ZoneInfo(self.tz_name)
        start_et = start_et.astimezone(tz) if start_et.tzinfo else start_et.replace(tzinfo=tz)
        end_et = end_et.astimezone(tz) if end_et.tzinfo else end_et.replace(tzinfo=tz)

        html = await self.http.get_text(CENSUS_EI_CAL_LIST)
        soup = BeautifulSoup(html, "html.parser")

        events: list[EconomicEvent] = []
        seen_release_stamp: set[str] = set()

        for tr in soup.find_all("tr"):
            txt = tr.get_text(" ", strip=True)
            if INDICATOR_NAME.lower() not in txt.lower():
                continue
            if "TBD" in txt.upper():
                continue

            dt_local: Optional[datetime] = None
            try:
                code12 = _A_RELEASE_DT_RE.search(txt).group(1)
                dt_local = datetime.strptime(code12, "%Y%m%d%H%M").replace(tzinfo=tz)
            except Exception:
                dt_local = None

            if dt_local is None:
                for a in tr.find_all("a", href=True):
                    try:
                        code12 = _A_RELEASE_DT_RE.search(a.get("href", "")).group(1)
                        dt_local = datetime.strptime(code12, "%Y%m%d%H%M").replace(tzinfo=tz)
                        break
                    except Exception:
                        continue

            if dt_local is None or not (start_et <= dt_local < end_et):
                continue

            period_yyyy_mm: Optional[str] = None
            m = _A_PERIOD_RE.search(txt)
            if m:
                period_yyyy_mm = _period_code_to_yyyy_mm(m.group(1))
            else:
                for a in tr.find_all("a", href=True):
                    m2 = _A_PERIOD_RE.search(a.get("href", "") or "")
                    if m2:
                        period_yyyy_mm = _period_code_to_yyyy_mm(m2.group(1))
                        break

            if period_yyyy_mm is None:
                continue

            stamp = dt_local.isoformat()
            if stamp in seen_release_stamp:
                continue
            seen_release_stamp.add(stamp)

            group = _mk_group_key(stamp, period_yyyy_mm)

            events.append(
                EconomicEvent(
                    event_id=safe_event_id("census", "Retail Sales m/m", stamp),
                    name="Retail Sales m/m",
                    country="US",
                    currency="USD",
                    scheduled_time_et=dt_local,
                    provider=self.name,
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
                    provider=self.name,
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
        ref_dt = (
            event.scheduled_time_et.astimezone(tz)
            if event.scheduled_time_et.tzinfo
            else event.scheduled_time_et.replace(tzinfo=tz)
        )

        try:
            cur_month = await self._current_month_for_event(event, ref_dt)
            prev_month = _prev_month_key(cur_month)
            prevprev_month = _prev_month_key(prev_month)

            if event.name == "Retail Sales m/m":
                prev_change = await self._compute_mm_change_total(prev_month, prevprev_month, ref_dt)
            elif event.name == "Core Retail Sales m/m":
                prev_change = await self._compute_core_mm_change(prev_month, prevprev_month, ref_dt)
            else:
                return event

            event.release.previous = f"{prev_change:.1f}%"
            event.release.unit = "%"
            event.release.source_url = CENSUS_MARTS_CURRENT_PDF
            event.release.updated_at = datetime.now(tz)
            return event

        except Exception as e:
            log.exception("prefill_previous failed for %s (%s): %s", event.name, event.event_id, e)
            return event

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        if not event.provider_configured:
            event.status = "disabled"
            event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)
            return event

        if getattr(event, "release", None) is None:
            event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)

        tz = ZoneInfo(self.tz_name)
        ref_dt = (
            event.scheduled_time_et.astimezone(tz)
            if event.scheduled_time_et.tzinfo
            else event.scheduled_time_et.replace(tzinfo=tz)
        )

        try:
            cur_month = await self._current_month_for_event(event, ref_dt)
            prev_month = _prev_month_key(cur_month)
            prevprev_month = _prev_month_key(prev_month)

            if event.release.previous is None:
                if event.name == "Retail Sales m/m":
                    prev_change = await self._compute_mm_change_total(prev_month, prevprev_month, ref_dt)
                elif event.name == "Core Retail Sales m/m":
                    prev_change = await self._compute_core_mm_change(prev_month, prevprev_month, ref_dt)
                else:
                    event.status = "disabled"
                    return event
                event.release.previous = f"{prev_change:.1f}%"

            actual_change: Optional[float] = None
            if event.name == "Retail Sales m/m":
                actual_change = await self._try_compute_mm_change_total(cur_month, prev_month, ref_dt)
            elif event.name == "Core Retail Sales m/m":
                actual_change = await self._try_compute_core_mm_change(cur_month, prev_month, ref_dt)
            else:
                event.status = "disabled"
                return event

            event.release.actual = f"{actual_change:.1f}%" if actual_change is not None else None
            event.release.forecast = None
            event.release.unit = "%"
            event.release.source_url = CENSUS_MARTS_CURRENT_PDF
            event.release.updated_at = datetime.now(tz)

            event.status = "released" if actual_change is not None else "scheduled"
            return event

        except Exception as e:
            log.exception("fetch_release failed for %s (%s): %s", event.name, event.event_id, e)
            event.status = "scheduled"
            event.release.source_url = CENSUS_RETAIL_RELEASE_SCHEDULE
            return event

    async def _current_month_for_event(self, event: EconomicEvent, ref_dt: datetime) -> str:
        period = _period_from_group_key(getattr(event, "group_key", None))
        if period and re.fullmatch(r"\d{4}-\d{2}", period):
            return period

        return await self._latest_available_month(ref_dt)

    async def _load_data(self, ref_dt: datetime) -> list[list[str]]:
        year = ref_dt.year
        start_y = year - 1
        end_y = year + 1

        if self._cache is not None and self._cache_year_range == (start_y, end_y):
            return self._cache

        time_pred = f"time=from+{start_y}+to+{end_y}"
        params = (
            "get=data_type_code,time_slot_id,seasonally_adj,category_code,cell_value,error_data"
            f"&{time_pred}&for=us:*"
        )
        if self.api_key:
            params += f"&key={self.api_key}"

        url = f"{EITS_MARTS_BASE}?{params}"
        body = await self.http.get_text(url)
        data = json.loads(body)

        self._cache = data
        self._cache_year_range = (start_y, end_y)
        return data

    async def _latest_available_month(self, ref_dt: datetime) -> str:
        data = await self._load_data(ref_dt)
        header, rows = data[0], data[1:]

        idx_dt = header.index("data_type_code")
        idx_cat = header.index("category_code")
        idx_val = header.index("cell_value")
        idx_time = header.index("time")

        best: Optional[str] = None
        for r in rows:
            if r[idx_dt] != DATA_TYPE_SALES:
                continue
            if r[idx_cat] != CAT_TOTAL:
                continue
            try:
                float(str(r[idx_val]).replace(",", ""))
            except Exception:
                continue

            t = _parse_time_yyyy_mm(r[idx_time])
            if t and (best is None or t > best):
                best = t

        if best is None:
            raise RuntimeError("No valid Census month found")

        return best

    async def _fetch_sales_value_exact(self, category: str, month: str, ref_dt: datetime) -> float:
        data = await self._load_data(ref_dt)
        header, rows = data[0], data[1:]

        idx_dt = header.index("data_type_code")
        idx_cat = header.index("category_code")
        idx_val = header.index("cell_value")
        idx_sa = header.index("seasonally_adj")
        idx_time = header.index("time")

        candidates: list[list[str]] = []
        for r in rows:
            if r[idx_dt] != DATA_TYPE_SALES:
                continue
            if r[idx_cat] != category:
                continue
            if r[idx_time] != month:
                continue
            try:
                float(str(r[idx_val]).replace(",", ""))
            except Exception:
                continue
            candidates.append(r)

        if not candidates:
            raise RuntimeError(f"No matching Census rows for {category} {month}")

        best = next((r for r in candidates if _is_sa(r[idx_sa])), candidates[0])
        return float(str(best[idx_val]).replace(",", ""))

    async def _fetch_sales_value_prefix_best(self, prefix: str, month: str, ref_dt: datetime) -> float:
        """
        Prefer the aggregate code (e.g. '441') when present; otherwise sum the
        most-granular level available under that prefix (avoids double counting).
        """
        data = await self._load_data(ref_dt)
        header, rows = data[0], data[1:]

        idx_dt = header.index("data_type_code")
        idx_cat = header.index("category_code")
        idx_val = header.index("cell_value")
        idx_sa = header.index("seasonally_adj")
        idx_time = header.index("time")

        exact_candidates: list[list[str]] = []
        for r in rows:
            if r[idx_dt] != DATA_TYPE_SALES:
                continue
            if r[idx_time] != month:
                continue
            if r[idx_cat] != prefix:
                continue
            try:
                float(str(r[idx_val]).replace(",", ""))
            except Exception:
                continue
            exact_candidates.append(r)

        if exact_candidates:
            best = next((r for r in exact_candidates if _is_sa(r[idx_sa])), exact_candidates[0])
            return float(str(best[idx_val]).replace(",", ""))

        by_cat_best: dict[str, tuple[bool, float]] = {}
        min_len: Optional[int] = None

        for r in rows:
            if r[idx_dt] != DATA_TYPE_SALES:
                continue
            if r[idx_time] != month:
                continue

            cat = str(r[idx_cat])
            if not cat.startswith(prefix) or cat == prefix:
                continue
            if cat == CAT_TOTAL:
                continue

            try:
                val = float(str(r[idx_val]).replace(",", ""))
            except Exception:
                continue

            sa = _is_sa(r[idx_sa])

            if min_len is None or len(cat) < min_len:
                min_len = len(cat)
                by_cat_best.clear()

            if len(cat) != min_len:
                continue

            prev = by_cat_best.get(cat)
            if prev is None:
                by_cat_best[cat] = (sa, val)
            else:
                prev_sa, _prev_val = prev
                if (not prev_sa) and sa:
                    by_cat_best[cat] = (True, val)

        if not by_cat_best:
            raise RuntimeError(f"No matching Census rows for prefix={prefix} {month}")

        return sum(v for _sa, v in by_cat_best.values())

    async def _fetch_sales_value_prefix_sum(self, prefix: str, month: str, ref_dt: datetime) -> float:
        data = await self._load_data(ref_dt)
        header, rows = data[0], data[1:]

        idx_dt = header.index("data_type_code")
        idx_cat = header.index("category_code")
        idx_val = header.index("cell_value")
        idx_sa = header.index("seasonally_adj")
        idx_time = header.index("time")

        best_by_cat: dict[str, tuple[bool, float]] = {}

        for r in rows:
            if r[idx_dt] != DATA_TYPE_SALES:
                continue

            cat = r[idx_cat]
            if not str(cat).startswith(prefix):
                continue
            if cat == CAT_TOTAL:
                continue
            if r[idx_time] != month:
                continue

            try:
                val = float(str(r[idx_val]).replace(",", ""))
            except Exception:
                continue

            sa = _is_sa(r[idx_sa])
            prev = best_by_cat.get(cat)
            if prev is None:
                best_by_cat[cat] = (sa, val)
            else:
                prev_sa, _prev_val = prev
                if (not prev_sa) and sa:
                    best_by_cat[cat] = (True, val)

        if not best_by_cat:
            raise RuntimeError(f"No matching Census rows for prefix={prefix} {month}")

        return sum(v for _sa, v in best_by_cat.values())

    async def _compute_mm_change_total(self, cur: str, prev: str, ref_dt: datetime) -> float:
        cur_v = await self._fetch_sales_value_exact(CAT_TOTAL, cur, ref_dt)
        prev_v = await self._fetch_sales_value_exact(CAT_TOTAL, prev, ref_dt)
        if prev_v == 0:
            raise ZeroDivisionError("Previous value is zero")
        return (cur_v - prev_v) / prev_v * 100.0

    async def _compute_core_mm_change(self, cur: str, prev: str, ref_dt: datetime) -> float:
        total_cur = await self._fetch_sales_value_exact(CAT_TOTAL, cur, ref_dt)
        total_prev = await self._fetch_sales_value_exact(CAT_TOTAL, prev, ref_dt)

        autos_cur = await self._fetch_sales_value_prefix_best(CAT_AUTOS_PREFIX, cur, ref_dt)
        autos_prev = await self._fetch_sales_value_prefix_best(CAT_AUTOS_PREFIX, prev, ref_dt)

        core_cur = total_cur - autos_cur
        core_prev = total_prev - autos_prev

        if core_prev == 0:
            raise ZeroDivisionError("Previous core value is zero")
        return (core_cur - core_prev) / core_prev * 100.0

    async def _try_compute_mm_change_total(self, cur: str, prev: str, ref_dt: datetime) -> Optional[float]:
        try:
            return await self._compute_mm_change_total(cur, prev, ref_dt)
        except Exception:
            return None

    async def _try_compute_core_mm_change(self, cur: str, prev: str, ref_dt: datetime) -> Optional[float]:
        try:
            return await self._compute_core_mm_change(cur, prev, ref_dt)
        except Exception:
            return None
