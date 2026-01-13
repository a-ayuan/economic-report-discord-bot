import json
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from src.models import EconomicEvent, ReleaseData

log = logging.getLogger("cache")

def _dt_to_str(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None

def _dt_from_str(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s)

def save_events(path: Path, events: list[EconomicEvent]) -> None:
    payload: list[dict[str, Any]] = []
    for e in events:
        d = asdict(e)
        d["scheduled_time_et"] = e.scheduled_time_et.isoformat()
        d["release"]["updated_at"] = _dt_to_str(e.release.updated_at)
        payload.append(d)

    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)
    log.info("Saved %d events to %s", len(events), path)

def load_events(path: Path) -> list[EconomicEvent]:
    if not path.exists():
        return []
    raw = json.loads(path.read_text(encoding="utf-8"))
    events: list[EconomicEvent] = []
    for d in raw:
        r = d.get("release") or {}
        release = ReleaseData(
            actual=r.get("actual"),
            previous=r.get("previous"),
            forecast=r.get("forecast"),
            unit=r.get("unit"),
            updated_at=_dt_from_str(r.get("updated_at")),
            source_url=r.get("source_url"),
        )
        events.append(
            EconomicEvent(
                event_id=d["event_id"],
                name=d["name"],
                country=d["country"],
                currency=d["currency"],
                scheduled_time_et=datetime.fromisoformat(d["scheduled_time_et"]),
                provider=d["provider"],
                provider_configured=bool(d.get("provider_configured", False)),
                status=d.get("status", "scheduled"),
                release=release,
                group_key=d.get("group_key"),
            )
        )
    return events
