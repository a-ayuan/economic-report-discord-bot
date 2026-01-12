import asyncio
import logging
from datetime import timedelta
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.bot.cache import CacheState
from src.bot.models import EconEvent
from src.bot.providers.base import EconProvider
from src.bot.utils.time import now_tz

log = logging.getLogger(__name__)

class EventScheduler:
    def __init__(
        self,
        *,
        tz_name: str,
        provider: EconProvider,
        cache: CacheState,
        cache_path: str,
        on_event_released,  # async callback(event: EconEvent)
        polling_cfg: dict,
        refresh_cfg: dict,
    ) -> None:
        self.tz_name = tz_name
        self.tz = ZoneInfo(tz_name)
        self.provider = provider
        self.cache = cache
        self.cache_path = cache_path
        self.on_event_released = on_event_released

        self.polling_cfg = polling_cfg
        self.refresh_cfg = refresh_cfg

        self.sched = AsyncIOScheduler(timezone=self.tz)

        # In-memory set of scheduled event_ids to avoid double scheduling
        self._scheduled: set[str] = set()

    def start(self) -> None:
        # weekly refresh (Sunday)
        self.sched.add_job(
            self.refresh_calendar,
            CronTrigger(
                day_of_week="sun",
                hour=self.refresh_cfg["weekly_sunday_hour"],
                minute=self.refresh_cfg["weekly_sunday_minute"],
            ),
            id="weekly_refresh",
            replace_existing=True,
        )

        # daily refresh
        self.sched.add_job(
            self.refresh_calendar,
            CronTrigger(
                hour=self.refresh_cfg["daily_hour"],
                minute=self.refresh_cfg["daily_minute"],
            ),
            id="daily_refresh",
            replace_existing=True,
        )

        # periodic refresh
        self.sched.add_job(
            self.refresh_calendar,
            "interval",
            minutes=self.refresh_cfg["periodic_minutes"],
            id="periodic_refresh",
            replace_existing=True,
        )

        # initial refresh on boot
        self.sched.add_job(self.refresh_calendar, "date", run_date=now_tz(self.tz_name) + timedelta(seconds=2))

        self.sched.start()
        log.info("Scheduler started")

    async def refresh_calendar(self) -> None:
        try:
            start = now_tz(self.tz_name) - timedelta(hours=1)
            end = now_tz(self.tz_name) + timedelta(days=8)

            events = self.provider.fetch_calendar(start, end)

            # schedule each event polling job
            for e in events:
                if e.event_id in self.cache.sent_event_ids:
                    continue
                if e.event_id in self._scheduled:
                    continue
                self._scheduled.add(e.event_id)

                self.sched.add_job(
                    lambda ev=e: asyncio.create_task(self.poll_event(ev)),
                    "date",
                    run_date=e.release_dt,
                    id=f"poll_{e.event_id}",
                    replace_existing=True,
                )

            self.cache.last_calendar_fetch_iso = now_tz(self.tz_name).isoformat()
            self.cache.save(self.cache_path)
            log.info("Calendar refreshed. Scheduled=%d sent=%d", len(self._scheduled), len(self.cache.sent_event_ids))
        except Exception as ex:
            log.exception("refresh_calendar failed: %s", ex)

    async def poll_event(self, event: EconEvent) -> None:
        """
        Poll provider for updated actual/forecast/previous around release time with backoff.
        """
        try:
            if event.event_id in self.cache.sent_event_ids:
                return

            cfg = self.polling_cfg
            elapsed = 0

            while elapsed < cfg["window_seconds"]:
                updated = self.provider.fetch_event_update(event)
                if updated.actual not in (None, "", "-", "N/A"):
                    await self.on_event_released(updated)
                    self.cache.sent_event_ids.add(updated.event_id)
                    self.cache.save(self.cache_path)
                    return

                # sleep based on phase
                if elapsed < cfg["phase1_seconds"]:
                    interval = cfg["phase1_interval"]
                elif elapsed < cfg["phase2_seconds"]:
                    interval = cfg["phase2_interval"]
                else:
                    interval = cfg["phase3_interval"]

                await asyncio.sleep(interval)
                elapsed += interval

            # window expired
            await self.on_event_released(
                EconEvent(
                    event_id=event.event_id,
                    title=event.title,
                    currency=event.currency,
                    release_dt=event.release_dt,
                    impact=event.impact,
                    forecast=event.forecast,
                    previous=event.previous,
                    actual="(not found / delayed)",
                )
            )
            self.cache.sent_event_ids.add(event.event_id)
            self.cache.save(self.cache_path)

        except Exception as ex:
            log.exception("poll_event failed for %s: %s", event.title, ex)
