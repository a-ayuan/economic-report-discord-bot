import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from src.models import EconomicEvent, ReleaseData
from src.providers.base import Provider
from src.utils.http import HttpClient, safe_event_id

log = logging.getLogger("provider.dol")

DOL_UI_PDF = "https://www.dol.gov/ui/data.pdf"

FRED_SERIES_OBS = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES_INITIAL_CLAIMS = "ICSA"

def _week_ending_for_release_dt(release_dt_et: datetime) -> date:
    d = release_dt_et.date()
    if release_dt_et.weekday() == 3:  # Thu
        return d - timedelta(days=5)  # Sat
    return d - timedelta(days=((release_dt_et.weekday() - 5) % 7))

def _fmt_claims_k(v: float) -> str:
    return f"{v / 1000.0:.0f}K"

class DOLProvider(Provider):
    name = "DOL"

    def __init__(self, http: HttpClient, tz_name: str, fred_api_key: str | None = None):
        self.http = http
        self.tz_name = tz_name
        self.fred_api_key = fred_api_key

    async def build_calendar(self, start_et: datetime, end_et: datetime) -> list[EconomicEvent]:
        events: list[EconomicEvent] = []
        cur = start_et.replace(hour=0, minute=0, second=0, microsecond=0)
        while cur < end_et:
            if cur.weekday() == 3:  # Thu (Mon=0)
                dt = cur.replace(hour=8, minute=30, second=0, microsecond=0)
                if start_et <= dt < end_et:
                    stamp = dt.isoformat()
                    events.append(
                        EconomicEvent(
                            event_id=safe_event_id("dol", "Unemployment Claims", stamp),
                            name="Unemployment Claims",
                            country="US",
                            currency="USD",
                            scheduled_time_et=dt,
                            provider=self.name,
                            provider_configured=True,
                            group_key=f"claims:{stamp}",
                        )
                    )
            cur += timedelta(days=1)
        return events

    async def prefill_previous(self, event: EconomicEvent) -> EconomicEvent:
        if not event.provider_configured:
            return event

        if getattr(event, "release", None) is None:
            event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)

        if event.release.previous is not None:
            return event

        try:
            if event.name != "Unemployment Claims":
                return event

            week_end = _week_ending_for_release_dt(event.scheduled_time_et)
            prev_week_end = week_end - timedelta(days=7)

            obs = await self._fred_observations(
                series_id=FRED_SERIES_INITIAL_CLAIMS,
                start=prev_week_end,
                end=week_end,
            )

            prev_val = obs.get(prev_week_end.isoformat())
            if prev_val is None:
                return event

            event.release.previous = _fmt_claims_k(prev_val)
            event.release.unit = "K"
            event.release.source_url = self._fred_source_url(FRED_SERIES_INITIAL_CLAIMS, prev_week_end, week_end)
            event.release.updated_at = datetime.now(ZoneInfo(self.tz_name))
            return event

        except Exception as e:
            log.exception("prefill_previous failed for %s (%s): %s", event.name, event.event_id, e)
            return event

    async def fetch_release(self, event: EconomicEvent) -> EconomicEvent:
        if not event.provider_configured:
            event.status = "disabled"
            event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)
            return event

        if datetime.now(ZoneInfo(self.tz_name)) < event.scheduled_time_et:
            return event

        if getattr(event, "release", None) is None:
            event.release = ReleaseData(actual=None, previous=None, forecast=None, source_url=None)

        try:
            if event.name != "Unemployment Claims":
                event.status = "disabled"
                return event

            week_end = _week_ending_for_release_dt(event.scheduled_time_et)
            prev_week_end = week_end - timedelta(days=7)

            obs = await self._fred_observations(
                series_id=FRED_SERIES_INITIAL_CLAIMS,
                start=prev_week_end,
                end=week_end,
            )

            cur_val = obs.get(week_end.isoformat())
            if cur_val is None:
                return event

            prev_val = obs.get(prev_week_end.isoformat())

            event.status = "released"
            if event.release.previous is None and prev_val is not None:
                event.release.previous = _fmt_claims_k(prev_val)

            event.release.actual = _fmt_claims_k(cur_val)
            event.release.forecast = None
            event.release.unit = "K"
            event.release.updated_at = datetime.now(ZoneInfo(self.tz_name))
            event.release.source_url = self._fred_source_url(FRED_SERIES_INITIAL_CLAIMS, prev_week_end, week_end)
            return event

        except Exception as e:
            log.exception("fetch_release failed for %s (%s): %s", event.name, event.event_id, e)
            return event

    def _fred_source_url(self, series_id: str, start: date, end: date) -> str:
        params = [
            f"series_id={series_id}",
            "file_type=json",
            f"observation_start={start.isoformat()}",
            f"observation_end={end.isoformat()}",
        ]
        if self.fred_api_key:
            params.append("api_key=REDACTED")
        return f"{FRED_SERIES_OBS}?{'&'.join(params)}"

    async def _fred_observations(self, series_id: str, start: date, end: date) -> dict[str, float]:
        params = [
            f"series_id={series_id}",
            "file_type=json",
            f"observation_start={start.isoformat()}",
            f"observation_end={end.isoformat()}",
        ]
        if self.fred_api_key:
            params.append(f"api_key={self.fred_api_key}")

        url = f"{FRED_SERIES_OBS}?{'&'.join(params)}"
        raw = await self.http.get_text(url)
        data = json.loads(raw)

        out: dict[str, float] = {}
        for row in (data.get("observations") or []):
            d = row.get("date")
            v = row.get("value")
            if not d or v in (None, "", "."):
                continue
            try:
                out[d] = float(v)
            except Exception:
                continue
        return out
