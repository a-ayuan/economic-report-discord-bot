import json
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

EITS_MRTSADV_BASE = "https://api.census.gov/data/timeseries/eits/marts"

CAT_TOTAL = "44X72"
CAT_AUTOS_PREFIX = "441"
CAT_GAS_PREFIX = "447"

DATA_TYPE_SALES = "SM"
INDICATOR_NAME = "Advance Monthly Sales for Retail and Food Services"

_A_CODE_RE = re.compile(r"\bA(\d{12})\b")

def _prev_month_key(yyyy_mm: str) -> str:
    y, m = yyyy_mm.split("-")
    year = int(y)
    month = int(m) - 1
    if month == 0:
        month = 12
        year -= 1
    return f"{year:04d}-{month:02d}"

def _parse_time_yyyy_mm(v: str) -> str | None:
    s = str(v).strip()
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    return None

def _is_sa(v: str) -> bool:
    return str(v).strip().lower() in {"1", "y", "yes", "true", "t", "sa", "s"}

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
        seen: set[str] = set()

        for tr in soup.find_all("tr"):
            txt = tr.get_text(" ", strip=True)
            if INDICATOR_NAME.lower() not in txt.lower():
                continue
            if "TBD" in txt.upper():
                continue

            dt_local: datetime | None = None
            try:
                code = _A_CODE_RE.search(txt).group(1)
                dt_local = datetime.strptime(code, "%Y%m%d%H%M").replace(tzinfo=tz)
            except Exception:
                dt_local = None

            if dt_local is None:
                for a in tr.find_all("a", href=True):
                    try:
                        code = _A_CODE_RE.search(a.get("href", "")).group(1)
                        dt_local = datetime.strptime(code, "%Y%m%d%H%M").replace(tzinfo=tz)
                        break
                    except Exception:
                        continue

            if dt_local is None or not (start_et <= dt_local < end_et):
                continue

            stamp = dt_local.isoformat()
            if stamp in seen:
                continue
            seen.add(stamp)

            group = f"census:marts:{stamp}"

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
            latest_month = await self._latest_available_month(ref_dt)
            prevprev = _prev_month_key(latest_month)

            if event.name == "Retail Sales m/m":
                prev_change = await self._compute_mm_change_total(latest_month, prevprev, ref_dt)
            elif event.name == "Core Retail Sales m/m":
                prev_change = await self._compute_core_mm_change(latest_month, prevprev, ref_dt)
            else:
                return event

            event.release.previous = f"{prev_change:.1f}%"
            event.release.unit = "%"
            event.release.source_url = CENSUS_MARTS_CURRENT_PDF
            event.release.updated_at = datetime.now(tz)
            return event

        except Exception as e:
            log.exception("Census prefill_previous failed for %s (%s): %s", event.name, event.event_id, e)
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
            latest_month = await self._latest_available_month(ref_dt)
            prevprev = _prev_month_key(latest_month)

            if event.release.previous is None:
                if event.name == "Retail Sales m/m":
                    prev_change = await self._compute_mm_change_total(latest_month, prevprev, ref_dt)
                elif event.name == "Core Retail Sales m/m":
                    prev_change = await self._compute_core_mm_change(latest_month, prevprev, ref_dt)
                else:
                    event.status = "disabled"
                    return event
                event.release.previous = f"{prev_change:.1f}%"

            event.release.actual = None
            event.release.forecast = None
            event.release.unit = "%"
            event.release.source_url = CENSUS_MARTS_CURRENT_PDF
            event.release.updated_at = datetime.now(tz)
            event.status = "scheduled"
            return event

        except Exception as e:
            log.exception("Census fetch_release failed for %s (%s): %s", event.name, event.event_id, e)
            event.status = "scheduled"
            event.release.source_url = CENSUS_RETAIL_RELEASE_SCHEDULE
            return event

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

        url = f"{EITS_MRTSADV_BASE}?{params}"
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

        best: str | None = None

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

        autos_cur = await self._fetch_sales_value_prefix_sum(CAT_AUTOS_PREFIX, cur, ref_dt)
        autos_prev = await self._fetch_sales_value_prefix_sum(CAT_AUTOS_PREFIX, prev, ref_dt)

        gas_cur = await self._fetch_sales_value_prefix_sum(CAT_GAS_PREFIX, cur, ref_dt)
        gas_prev = await self._fetch_sales_value_prefix_sum(CAT_GAS_PREFIX, prev, ref_dt)

        core_cur = total_cur - autos_cur - gas_cur
        core_prev = total_prev - autos_prev - gas_prev

        if core_prev == 0:
            raise ZeroDivisionError("Previous core value is zero")
        return (core_cur - core_prev) / core_prev * 100.0
