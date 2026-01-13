import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from src.models import EconomicEvent

log = logging.getLogger("release_watcher")

@dataclass
class PollPlan:
    due: bool
    next_poll_at: datetime | None
    in_burst_window: bool
    burst_deadline: datetime | None
    backoff_seconds: int | None

class ReleaseWatcher:
    """
    - Starting at scheduled_time:
        poll every burst_poll_seconds for burst_window_seconds
    - If still missing after burst window:
        post "missing" (once), then poll with backoff:
          60s -> 120s -> 240s -> ... up to 900s (15 min)
    """

    def __init__(
        self,
        providers_by_name: dict,
        burst_poll_seconds: int,
        burst_window_seconds: int,
        backoff_start_seconds: int,
        backoff_max_seconds: int,
    ):
        self.providers_by_name = providers_by_name
        self.burst_poll_seconds = burst_poll_seconds
        self.burst_window_seconds = burst_window_seconds
        self.backoff_start_seconds = backoff_start_seconds
        self.backoff_max_seconds = backoff_max_seconds

        # event_id -> next poll time
        self._next_poll_at: dict[str, datetime] = {}
        # event_id -> current backoff
        self._backoff: dict[str, int] = {}

    def plan(self, e: EconomicEvent, now_et: datetime) -> PollPlan:
        if e.status in ("released", "disabled"):
            return PollPlan(due=False, next_poll_at=None, in_burst_window=False, burst_deadline=None, backoff_seconds=None)

        if now_et < e.scheduled_time_et:
            # Not due yet
            self._next_poll_at.pop(e.event_id, None)
            self._backoff.pop(e.event_id, None)
            return PollPlan(due=False, next_poll_at=None, in_burst_window=False, burst_deadline=None, backoff_seconds=None)

        burst_deadline = e.scheduled_time_et + timedelta(seconds=self.burst_window_seconds)
        in_burst = now_et <= burst_deadline

        # Initialize next poll if missing
        if e.event_id not in self._next_poll_at:
            self._next_poll_at[e.event_id] = e.scheduled_time_et

        # Determine if we're due right now
        due = now_et >= self._next_poll_at[e.event_id]

        if in_burst:
            # During burst: fixed 5s cadence
            next_at = now_et + timedelta(seconds=self.burst_poll_seconds) if due else self._next_poll_at[e.event_id]
            return PollPlan(
                due=due,
                next_poll_at=next_at,
                in_burst_window=True,
                burst_deadline=burst_deadline,
                backoff_seconds=None,
            )

        # After burst: exponential backoff up to max
        bo = self._backoff.get(e.event_id, self.backoff_start_seconds)
        next_at = now_et + timedelta(seconds=bo) if due else self._next_poll_at[e.event_id]
        return PollPlan(
            due=due,
            next_poll_at=next_at,
            in_burst_window=False,
            burst_deadline=burst_deadline,
            backoff_seconds=bo,
        )

    async def poll_event(self, event: EconomicEvent) -> EconomicEvent:
        provider = self.providers_by_name.get(event.provider)
        if not provider:
            event.status = "disabled"
            return event
        try:
            return await provider.fetch_release(event)
        except Exception as e:
            log.exception("fetch_release failed for %s (%s): %s", event.name, event.event_id, e)
            return event

    async def maybe_poll(self, e: EconomicEvent, now_et: datetime) -> EconomicEvent:
        plan = self.plan(e, now_et)
        if not plan.due:
            return e

        updated = await self.poll_event(e)

        # Update next poll scheduling if still not released
        if updated.status != "released":
            burst_deadline = e.scheduled_time_et + timedelta(seconds=self.burst_window_seconds)
            if now_et <= burst_deadline:
                self._next_poll_at[e.event_id] = now_et + timedelta(seconds=self.burst_poll_seconds)
            else:
                bo = self._backoff.get(e.event_id, self.backoff_start_seconds)
                self._next_poll_at[e.event_id] = now_et + timedelta(seconds=bo)
                self._backoff[e.event_id] = min(bo * 2, self.backoff_max_seconds)
        else:
            # Released: stop polling
            self._next_poll_at.pop(e.event_id, None)
            self._backoff.pop(e.event_id, None)

        return updated

    async def check_due_live_once(self, events: list[EconomicEvent], now_et: datetime) -> list[EconomicEvent]:
        updated: list[EconomicEvent] = []
        for e in events:
            if e.status in ("released", "disabled"):
                updated.append(e)
                continue
            if now_et < e.scheduled_time_et:
                updated.append(e)
                continue
            updated.append(await self.maybe_poll(e, now_et))
        return updated

    @staticmethod
    def groups(events: Iterable[EconomicEvent]) -> dict[str, list[EconomicEvent]]:
        """
        Group by group_key if present, else by event_id.
        Used for posting "missing" and "released" once per group.
        """
        g: dict[str, list[EconomicEvent]] = {}
        for e in events:
            key = e.group_key or e.event_id
            g.setdefault(key, []).append(e)
        return g
