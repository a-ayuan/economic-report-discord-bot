import asyncio
import logging

import uvicorn
from dotenv import load_dotenv

from .config.settings import load_settings
from .services.keepalive_api import make_app
from .utils.logging import setup_logging
from .utils.http import get_session
from .utils.time import now_et, start_of_week_et, end_of_week_et

from .services.calendar_service import load_config_events, refresh_week_calendar
from .storage.cache import save_events, load_events
from .services.scheduler import EventScheduler
from .bot.discord_bot import EconDiscordBot

log = logging.getLogger("main")

async def calendar_refresh_loop(settings, session, bot, scheduler):
    cfg_events = load_config_events("src/config/events.yaml")

    while True:
        try:
            now = now_et()
            ws = start_of_week_et(now)
            we = end_of_week_et(now)
            events = refresh_week_calendar(session, cfg_events, ws, we)
            save_events(settings.cache_dir, events)
            bot.set_week_events(events)
            scheduler.schedule_events(events)
            log.info("Calendar refreshed and scheduled (loop)")
        except Exception as e:
            log.exception("Calendar refresh failed (loop): %s", e)

        await asyncio.sleep(settings.cal_refresh_minutes * 60)

async def run_discord_bot(settings, bot: EconDiscordBot):
    await bot.start(settings.discord_token)

def run_http_server():
    app = make_app()
    uvicorn.run(app, host="0.0.0.0", port=10000, log_level="info")

async def main_async():
    load_dotenv()
    setup_logging()
    settings = load_settings()

    session = get_session(settings.user_agent)

    bot = EconDiscordBot(
        command_channel_id=settings.command_channel_id,
        announce_channel_id=settings.announce_channel_id,
    )

    retry_cfg = dict(
        phase1_seconds=settings.retry_phase1_seconds,
        phase1_interval=settings.retry_phase1_interval,
        phase2_seconds=settings.retry_phase2_seconds,
        phase2_interval=settings.retry_phase2_interval,
        phase3_seconds=settings.retry_phase3_seconds,
        phase3_interval=settings.retry_phase3_interval,
    )

    scheduler = EventScheduler(session=session, cache_dir=settings.cache_dir, retry_cfg=retry_cfg)
    scheduler.on_event_released = bot.post_release
    scheduler.start()

    # Load cached events on boot (so !calendar works fast even before first loop refresh)
    cached = load_events(settings.cache_dir)
    if cached:
        bot.set_week_events(cached)
        scheduler.schedule_events(cached)

    # Inject a "refresh hook" so !calendar can force a live scrape and update state
    cfg_events = load_config_events("src/config/events.yaml")

    async def refresh_hook() -> list:
        # refresh_week_calendar is sync (requests/bs4). Run it in a thread executor.
        loop = asyncio.get_running_loop()

        def _do_refresh():
            now = now_et()
            ws = start_of_week_et(now)
            we = end_of_week_et(now)
            events = refresh_week_calendar(session, cfg_events, ws, we)
            save_events(settings.cache_dir, events)
            scheduler.schedule_events(events)
            return events

        return await loop.run_in_executor(None, _do_refresh)

    bot.set_refresh_hook(refresh_hook)

    # Start background calendar refresh task (still useful for periodic updates)
    asyncio.create_task(calendar_refresh_loop(settings, session, bot, scheduler))

    # Run discord bot (blocks until stopped)
    await run_discord_bot(settings, bot)

def main():
    # Run HTTP server in separate thread so Render sees an active web service.
    import threading
    t = threading.Thread(target=run_http_server, daemon=True)
    t.start()

    asyncio.run(main_async())

if __name__ == "__main__":
    main()
