import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from src.models import EconomicEvent

log = logging.getLogger("release_watcher")

@dataclass
class PollPlan:
    due: bool
    next_poll_at: datetime | None
    in_burst_window: bool
    burst_deadline: datetime | None
    backoff_seconds: int | None
    expired_for_day: bool

class ReleaseWatcher:
    """
    Live polling policy per event:

    - Starting at scheduled_time:
        poll every burst_poll_seconds for burst_window_seconds
    - If still missing after burst window:
        poll with backoff (doubling) up to backoff_max_seconds
    - Stop polling at trading-day cutoff (5:00 PM ET) on the scheduled date
    """

    def __init__(
        self,
        providers_by_name: dict,
        burst_poll_seconds: int,
        burst_window_seconds: int,
        backoff_start_seconds: int,
        backoff_max_seconds: int,
        trading_day_cutoff_hour_et: int = 17,  # 5 PM
    ):
        self.providers_by_name = providers_by_name
        self.burst_poll_seconds = burst_poll_seconds
        self.burst_window_seconds = burst_window_seconds
        self.backoff_start_seconds = backoff_start_seconds
        self.backoff_max_seconds = backoff_max_seconds
        self.cutoff_hour = trading_day_cutoff_hour_et

        # event_id -> next poll time
        self._next_poll_at: dict[str, datetime] = {}
        # event_id -> current backoff
        self._backoff: dict[str, int] = {}

    def _cutoff_dt(self, e: EconomicEvent) -> datetime:
        """
        Cutoff is 5:00 PM ET on the same calendar date as the scheduled release.
        """
        s = e.scheduled_time_et
        return s.replace(hour=self.cutoff_hour, minute=0, second=0, microsecond=0)

    def plan(self, e: EconomicEvent, now_et: datetime) -> PollPlan:
        if e.status in ("released", "disabled"):
            return PollPlan(False, None, False, None, None, expired_for_day=False)

        if now_et < e.scheduled_time_et:
            self._next_poll_at.pop(e.event_id, None)
            self._backoff.pop(e.event_id, None)
            return PollPlan(False, None, False, None, None, expired_for_day=False)

        cutoff = self._cutoff_dt(e)
        if now_et >= cutoff:
            # Stop polling after cutoff.
            self._next_poll_at.pop(e.event_id, None)
            self._backoff.pop(e.event_id, None)
            return PollPlan(False, None, False, None, None, expired_for_day=True)

        burst_deadline = e.scheduled_time_et + timedelta(seconds=self.burst_window_seconds)
        in_burst = now_et <= burst_deadline

        if e.event_id not in self._next_poll_at:
            self._next_poll_at[e.event_id] = e.scheduled_time_et

        due = now_et >= self._next_poll_at[e.event_id]

        if in_burst:
            next_at = now_et + timedelta(seconds=self.burst_poll_seconds) if due else self._next_poll_at[e.event_id]
            return PollPlan(
                due=due,
                next_poll_at=next_at,
                in_burst_window=True,
                burst_deadline=burst_deadline,
                backoff_seconds=None,
                expired_for_day=False,
            )

        bo = self._backoff.get(e.event_id, self.backoff_start_seconds)
        next_at = now_et + timedelta(seconds=bo) if due else self._next_poll_at[e.event_id]
        return PollPlan(
            due=due,
            next_poll_at=next_at,
            in_burst_window=False,
            burst_deadline=burst_deadline,
            backoff_seconds=bo,
            expired_for_day=False,
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
        if plan.expired_for_day:
            # main loop will mark as missing at group level
            return e

        if not plan.due:
            return e

        updated = await self.poll_event(e)

        if updated.status != "released":
            cutoff = self._cutoff_dt(e)
            # Don’t schedule beyond cutoff
            if now_et + timedelta(seconds=self.burst_poll_seconds) >= cutoff:
                self._next_poll_at.pop(e.event_id, None)
                self._backoff.pop(e.event_id, None)
                return updated

            burst_deadline = e.scheduled_time_et + timedelta(seconds=self.burst_window_seconds)
            if now_et <= burst_deadline:
                self._next_poll_at[e.event_id] = now_et + timedelta(seconds=self.burst_poll_seconds)
            else:
                bo = self._backoff.get(e.event_id, self.backoff_start_seconds)
                self._next_poll_at[e.event_id] = now_et + timedelta(seconds=bo)
                self._backoff[e.event_id] = min(bo * 2, self.backoff_max_seconds)
        else:
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
    def groups(events) -> dict[str, list[EconomicEvent]]:
        g: dict[str, list[EconomicEvent]] = {}
        for e in events:
            key = e.group_key or e.event_id
            g.setdefault(key, []).append(e)
        return g

    def is_expired_for_day(self, e: EconomicEvent, now_et: datetime) -> bool:
        """
        Helper for main loop: after cutoff (5PM ET on scheduled date), stop and mark missing.
        """
        if e.status in ("released", "disabled"):
            return False
        if now_et < e.scheduled_time_et:
            return False
        return now_et >= self._cutoff_dt(e)
