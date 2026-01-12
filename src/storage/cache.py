import json
import os
from dataclasses import asdict
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from .models import EconEvent

def _json_default(o: Any):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"Not JSON serializable: {type(o)}")

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def save_events(cache_dir: str, events: list[EconEvent]) -> str:
    ensure_dir(cache_dir)
    path = os.path.join(cache_dir, "events.json")
    payload = [asdict(e) for e in events]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, default=_json_default, indent=2)
    return path

def load_events(cache_dir: str) -> list[EconEvent]:
    path = os.path.join(cache_dir, "events.json")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    out: list[EconEvent] = []
    for r in raw:
        r["release_dt"] = datetime.fromisoformat(r["release_dt"]).astimezone(ZoneInfo("America/New_York"))
        out.append(EconEvent(**r))
    return out

def save_state(cache_dir: str, state: dict[str, Any]) -> None:
    ensure_dir(cache_dir)
    path = os.path.join(cache_dir, "state.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def load_state(cache_dir: str) -> dict[str, Any]:
    path = os.path.join(cache_dir, "state.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
