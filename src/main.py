import asyncio
import logging
from datetime import timedelta

import discord
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from discord.ext import commands

from src.config import load_settings
from src.health.server import app as health_app  # noqa: F401
from src.logging_config import setup_logging
from src.services.calendar_service import CalendarService
from src.services.release_watcher import ReleaseWatcher
from src.utils.cache import load_events, save_events
from src.utils.http import HttpClient
from src.utils.text import format_week_summary, format_release_line
from src.utils.timeutil import now_et, week_bounds_et
from src.utils.state import load_state, save_state, cleanup_weekly_state, BotState

from src.providers.bls import BLSProvider
from src.providers.bea import BEAProvider
from src.providers.census import CensusProvider
from src.providers.dol import DOLProvider
from src.providers.fed import FedProvider
from src.providers.holidays import HolidaysProvider
from src.providers.private_stub import PrivateStubProvider

import uvicorn

log = logging.getLogger("main")

async def start_health_server(host: str, port: int) -> None:
    config = uvicorn.Config("src.health.server:app", host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()

def make_bot() -> commands.Bot:
    intents = discord.Intents.default()
    intents.message_content = True
    return commands.Bot(command_prefix="!", intents=intents)

def _events_signature(events):
    sig = []
    for e in events:
        r = e.release
        sig.append(
            (
                e.event_id,
                e.status,
                e.scheduled_time_et.isoformat(),
                e.provider,
                e.group_key,
                r.actual,
                r.previous,
                r.forecast,
                r.unit,
                r.source_url,
            )
        )
    return tuple(sig)

def _state_signature(state: BotState) -> tuple:
    return (
        state.active_start_et.isoformat(),
        tuple(sorted(state.posted_release_groups)),
        tuple(sorted(state.posted_missing_groups)),
    )


async def main() -> None:
    setup_logging()
    s = load_settings()

    http = HttpClient()

    providers = [
        HolidaysProvider(http=http, tz_name=s.timezone),
        BLSProvider(http=http, tz_name=s.timezone, api_key=s.bls_api_key),
        DOLProvider(http=http, tz_name=s.timezone),
        BEAProvider(http=http, tz_name=s.timezone),
        CensusProvider(http=http, tz_name=s.timezone),
        FedProvider(http=http, tz_name=s.timezone),
        PrivateStubProvider(),
    ]
    providers_by_name = {p.name: p for p in providers}

    calendar_service = CalendarService(providers=providers, post_only_configured_sources=s.post_only_configured_sources)
    watcher = ReleaseWatcher(
        providers_by_name=providers_by_name,
        burst_poll_seconds=s.burst_poll_seconds,
        burst_window_seconds=s.burst_window_seconds,
        backoff_start_seconds=s.backoff_start_seconds,
        backoff_max_seconds=s.backoff_max_seconds,
    )

    cache_path = s.cache_dir / "calendar.json"
    state_path = s.cache_dir / "state.json"

    events = load_events(cache_path)
    last_events_sig = _events_signature(events)

    state = load_state(state_path, tz_name=s.timezone)
    last_state_sig = _state_signature(state)

    events_lock = asyncio.Lock()

    log.info("Loaded %d cached events", len(events))
    log.info("State active_start_et=%s", state.active_start_et.isoformat())

    bot = make_bot()

    def maybe_save_events() -> None:
        nonlocal last_events_sig, events
        sig = _events_signature(events)
        if sig != last_events_sig:
            save_events(cache_path, events)
            last_events_sig = sig

    def maybe_save_state() -> None:
        nonlocal last_state_sig, state
        sig = _state_signature(state)
        if sig != last_state_sig:
            save_state(state_path, state)
            last_state_sig = sig

    async def rebuild_calendar() -> None:
        nonlocal events, state
        now = now_et(s.timezone)
        week_start, _week_end = week_bounds_et(now)

        start = max(state.active_start_et, week_start)
        end = start + timedelta(days=14)

        fresh = await calendar_service.build(start, end)

        # Merge: keep existing release data/status where IDs match
        by_id = {e.event_id: e for e in events}
        merged = []
        for e in fresh:
            if e.event_id in by_id:
                old = by_id[e.event_id]
                e.status = old.status
                e.release = old.release
            merged.append(e)

        events = merged

    async def clean_calendar(reason: str) -> None:
        nonlocal events, state
        now = now_et(s.timezone)
        week_start, _ = week_bounds_et(now)
        next_week_start = week_start + timedelta(days=7)

        state.active_start_et = next_week_start
        cleanup_weekly_state(state)

        events = [e for e in events if e.scheduled_time_et >= next_week_start]

        maybe_save_state()
        maybe_save_events()

        log.info("Calendar cleaned (%s). active_start_et=%s", reason, state.active_start_et.isoformat())

    async def scheduled_job() -> None:
        """
        Every 30 minutes: rebuild calendar (respecting active_start_et) and save if changed.
        Live release polling is handled by the live monitor loop.
        """
        try:
            async with events_lock:
                await rebuild_calendar()
                maybe_save_events()
                maybe_save_state()
        except Exception as ex:
            log.exception("Scheduled job error: %s", ex)

    async def weekly_clean_job() -> None:
        try:
            async with events_lock:
                await clean_calendar(reason="weekly_job")
        except Exception as ex:
            log.exception("Weekly clean job error: %s", ex)

    async def post_group_release(report_channel: discord.abc.Messageable, group_key: str, group_events: list) -> None:
        """
        Post one message for the group.
        Format: multiple lines, one per event.
        """
        lines = []
        for e in sorted(group_events, key=lambda x: x.name):
            lines.append(format_release_line(e))
        msg = "\n\n".join(lines)
        if len(msg) > 1800:
            msg = msg[:1800] + "\n…"
        await report_channel.send(msg)

    async def post_group_missing(report_channel: discord.abc.Messageable, group_key: str, group_events: list) -> None:
        names = ", ".join(sorted({e.name for e in group_events}))
        scheduled = min(e.scheduled_time_et for e in group_events).strftime("%a %m/%d %I:%M %p ET")
        await report_channel.send(
            f"⚠️ No data found within 1 minute for: **{names}** (scheduled {scheduled}). "
            f"I'll keep checking with increasing intervals until the data is available."
        )

    async def live_monitor_loop() -> None:
        """
        This is the missing piece you described.

        It runs continuously, but:
          - it only polls when events are due (burst/backoff planner)
          - it only saves to disk on changes
          - it only posts messages once per group (deduped in state.json)
        """
        await bot.wait_until_ready()

        report_ch = bot.get_channel(s.report_channel_id)
        if report_ch is None:
            log.error("Could not find REPORT_CHANNEL_ID=%s", s.report_channel_id)
            return

        while not bot.is_closed():
            try:
                now = now_et(s.timezone)

                async with events_lock:
                    if not events:
                        # nothing to do; wait a bit
                        pass
                    else:
                        updated = await watcher.check_due_live_once(events, now_et=now)

                        # Detect changes
                        before_sig = _events_signature(events)
                        after_sig = _events_signature(updated)
                        events[:] = updated  # mutate list in-place

                        # Posting logic (per group)
                        groups = watcher.groups(events)

                        # 1) Missing-after-1-minute: if now > scheduled+60 and group not released and not yet posted
                        for gkey, gevs in groups.items():
                            if gkey in state.posted_missing_groups:
                                continue
                            if gkey in state.posted_release_groups:
                                continue

                            # Only consider groups where at least one event is active
                            active = [e for e in gevs if e.status not in ("released", "disabled")]
                            if not active:
                                continue

                            scheduled = min(e.scheduled_time_et for e in gevs)
                            if now >= scheduled + timedelta(seconds=s.burst_window_seconds):
                                state.posted_missing_groups.add(gkey)
                                await post_group_missing(report_ch, gkey, gevs)

                        # 2) Released: if all (non-disabled) events are released OR at least one has actual and none are still scheduled
                        for gkey, gevs in groups.items():
                            if gkey in state.posted_release_groups:
                                continue

                            non_disabled = [e for e in gevs if e.status != "disabled"]
                            if not non_disabled:
                                continue

                            # Consider released if every non-disabled is released OR at least one has actual and none remain "scheduled"
                            all_released = all(e.status == "released" for e in non_disabled)
                            any_actual = any(e.release.actual is not None for e in non_disabled)
                            none_scheduled = all(e.status != "scheduled" for e in non_disabled)

                            if all_released or (any_actual and none_scheduled):
                                state.posted_release_groups.add(gkey)
                                await post_group_release(report_ch, gkey, gevs)

                        # Save only if something changed
                        if after_sig != before_sig:
                            maybe_save_events()
                        maybe_save_state()

            except Exception as ex:
                log.exception("Live monitor loop error: %s", ex)

            await asyncio.sleep(s.watcher_tick_seconds)

    @bot.command(name="calendar")
    async def calendar_cmd(ctx: commands.Context) -> None:
        nonlocal events
        if ctx.channel.id != s.command_channel_id:
            return

        # User command: rebuild calendar immediately (does not interfere with live loop due to lock)
        async with events_lock:
            await rebuild_calendar()
            maybe_save_events()
            maybe_save_state()

            now = now_et(s.timezone)
            week_start, week_end = week_bounds_et(now)

            effective_start = max(week_start, state.active_start_et)
            effective_end = effective_start + timedelta(days=7)

            past = [e for e in events if effective_start <= e.scheduled_time_et < now]
            upcoming = [e for e in events if now <= e.scheduled_time_et < effective_end]

        msg1 = format_week_summary(past, "This week (already happened)")
        msg2 = format_week_summary(upcoming, "This week (upcoming)")
        out = msg1 + "\n\n" + msg2
        if len(out) > 1800:
            out = out[:1800] + "\n…"
        await ctx.send(out)

    @bot.command(name="clean")
    async def clean_cmd(ctx: commands.Context) -> None:
        if ctx.channel.id != s.command_channel_id:
            return
        async with events_lock:
            await clean_calendar(reason="manual_command")
            start_str = state.active_start_et.strftime("%a %m/%d %I:%M %p ET")
        await ctx.send(f"Cleaned calendar. Next active week starts at: {start_str}")

    @bot.event
    async def on_ready():
        log.info("Logged in as %s", bot.user)

    scheduler = AsyncIOScheduler(timezone=s.timezone)
    scheduler.add_job(scheduled_job, "interval", minutes=s.calendar_refresh_minutes)
    scheduler.add_job(weekly_clean_job, "cron", day_of_week="mon", hour=0, minute=5)
    scheduler.start()

    # Startup: build calendar once
    await scheduled_job()

    await asyncio.gather(
        start_health_server(s.health_host, s.health_port),
        bot.start(s.discord_token),
        live_monitor_loop(),
    )

if __name__ == "__main__":
    asyncio.run(main())
