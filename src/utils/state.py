import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

@dataclass
class BotState:
    # Lowest ET datetime allowed to appear in the calendar.
    active_start_et: datetime

    # Deduping across restarts
    posted_release_groups: set[str] = field(default_factory=set)
    posted_missing_groups: set[str] = field(default_factory=set)
    posted_expired_groups: set[str] = field(default_factory=set)

def _monday_start(dt: datetime) -> datetime:
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    start = start - timedelta(days=start.weekday())
    return start

def default_state(tz_name: str) -> BotState:
    now = datetime.now(ZoneInfo(tz_name))
    return BotState(active_start_et=_monday_start(now))

def load_state(path: Path, tz_name: str) -> BotState:
    if not path.exists():
        return default_state(tz_name)

    raw = json.loads(path.read_text(encoding="utf-8"))

    s = raw.get("active_start_et")
    if not s:
        st = default_state(tz_name)
    else:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo(tz_name))
        else:
            dt = dt.astimezone(ZoneInfo(tz_name))
        st = BotState(active_start_et=dt)

    st.posted_release_groups = set(raw.get("posted_release_groups") or [])
    st.posted_missing_groups = set(raw.get("posted_missing_groups") or [])
    st.posted_expired_groups = set(raw.get("posted_expired_groups") or [])
    return st

def save_state(path: Path, state: BotState) -> None:
    payload = {
        "active_start_et": state.active_start_et.isoformat(),
        "posted_release_groups": sorted(state.posted_release_groups),
        "posted_missing_groups": sorted(state.posted_missing_groups),
        "posted_expired_groups": sorted(state.posted_expired_groups),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)

def cleanup_weekly_state(state: BotState) -> None:
    """
    Keep state compact. Since we only care about the active 1-2 week window,
    it's safe to clear dedupe sets on weekly clean.
    """
    state.posted_release_groups.clear()
    state.posted_missing_groups.clear()
    state.posted_expired_groups.clear()
