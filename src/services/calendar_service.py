import logging
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
import yaml
import requests

from ..storage.models import EconEvent
from ..sources.bls import fetch_bls_schedule
from ..sources.bea import fetch_bea_schedule
from ..sources.census import fetch_census_schedule

log = logging.getLogger("services.calendar")

@dataclass(frozen=True)
class ConfigEvent:
    id: str
    source: str
    title: str
    match_hint: str
    forecast: str | None

def load_config_events(path: str) -> list[ConfigEvent]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    out: list[ConfigEvent] = []
    for e in raw.get("events", []):
        out.append(ConfigEvent(
            id=e["id"],
            source=e["source"],
            title=e["title"],
            match_hint=e.get("match_hint", e["title"]),
            forecast=e.get("forecast"),
        ))
    return out

def _match(title: str, hint: str) -> bool:
    return hint.lower() in title.lower()

def refresh_week_calendar(
    session: requests.Session,
    cfg_events: list[ConfigEvent],
    week_start: datetime,
    week_end: datetime,
) -> list[EconEvent]:
    tz = ZoneInfo("America/New_York")

    bls_items = fetch_bls_schedule(session)
    bea_items = fetch_bea_schedule(session)
    census_items = fetch_census_schedule(session)

    # Source -> list of schedule items (title, dt)
    universe: dict[str, list[tuple[str, datetime]]] = {
        "bls": [(i.title, i.release_dt) for i in bls_items],
        "bea": [(i.title, i.release_dt) for i in bea_items],
        "census": [(i.title, i.release_dt) for i in census_items],
    }

    out: list[EconEvent] = []
    for ce in cfg_events:
        candidates = universe.get(ce.source, [])
        for (t, dt) in candidates:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            if not (week_start <= dt < week_end):
                continue
            if _match(t, ce.match_hint):
                out.append(EconEvent(
                    id=ce.id,
                    source=ce.source,
                    title=ce.title,
                    release_dt=dt,
                    url=None,  # optional: set to known release page if you want
                    forecast=ce.forecast,
                ))
                break

    out.sort(key=lambda e: e.release_dt)
    log.info("Built week calendar with %d configured events", len(out))
    return out
