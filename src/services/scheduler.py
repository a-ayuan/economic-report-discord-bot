import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

import requests

from ..storage.cache import load_state, save_state
from ..storage.models import EconEvent
from .release_service import poll_for_release

log = logging.getLogger("services.scheduler")

class EventScheduler:
    def __init__(self, session: requests.Session, cache_dir: str, retry_cfg: dict):
        self.session = session
        self.cache_dir = cache_dir
        self.retry_cfg = retry_cfg
        self.scheduler = AsyncIOScheduler()
        self.on_event_released = None  # callback(event: EconEvent) -> Awaitable[None]

    def start(self) -> None:
        self.scheduler.start()

    def shutdown(self) -> None:
        self.scheduler.shutdown(wait=False)

    def schedule_events(self, events: list[EconEvent]) -> None:
        state = load_state(self.cache_dir)
        seen = state.get("seen_actual", {})

        for e in events:
            # Avoid scheduling old events too aggressively; still allow for late publications.
            self.scheduler.add_job(
                self._run_event_job,
                trigger=DateTrigger(run_date=e.release_dt),
                args=[e],
                id=f"event_{e.id}_{int(e.release_dt.timestamp())}",
                replace_existing=True,
                misfire_grace_time=3600,
            )
        log.info("Scheduled %d event jobs", len(events))

        state["seen_actual"] = seen
        save_state(self.cache_dir, state)

    async def _run_event_job(self, event: EconEvent) -> None:
        log.info("Running event job: %s (%s)", event.id, event.title)

        loop = asyncio.get_running_loop()
        updated: EconEvent = await loop.run_in_executor(
            None,
            lambda: poll_for_release(
                self.session,
                event,
                **self.retry_cfg,
            ),
        )

        state = load_state(self.cache_dir)
        seen = state.get("seen_actual", {})

        # De-dupe posting
        key = f"{updated.id}:{int(updated.release_dt.timestamp())}"
        fingerprint = f"{updated.actual}|{updated.previous}"
        if seen.get(key) == fingerprint:
            return

        seen[key] = fingerprint
        state["seen_actual"] = seen
        save_state(self.cache_dir, state)

        if self.on_event_released is not None:
            await self.on_event_released(updated)
