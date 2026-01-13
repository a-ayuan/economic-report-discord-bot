import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

def _get_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _get_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    return int(v)

@dataclass(frozen=True)
class Settings:
    discord_token: str
    report_channel_id: int
    command_channel_id: int

    cache_dir: Path

    calendar_refresh_minutes: int

    watcher_tick_seconds: int
    burst_poll_seconds: int
    burst_window_seconds: int
    backoff_start_seconds: int
    backoff_max_seconds: int

    timezone: str

    bls_api_key: str | None

    post_only_configured_sources: bool

    health_host: str
    health_port: int

def load_settings() -> Settings:
    load_dotenv()
    token = os.getenv("DISCORD_TOKEN", "").strip()
    if not token:
        raise RuntimeError("DISCORD_TOKEN is required")

    report_ch = os.getenv("REPORT_CHANNEL_ID", "").strip()
    cmd_ch = os.getenv("COMMAND_CHANNEL_ID", "").strip()
    if not report_ch or not cmd_ch:
        raise RuntimeError("REPORT_CHANNEL_ID and COMMAND_CHANNEL_ID are required")

    cache_dir = Path(os.getenv("CACHE_DIR", "./data")).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    bls_key = os.getenv("BLS_API_KEY", "").strip() or None

    return Settings(
        discord_token=token,
        report_channel_id=int(report_ch),
        command_channel_id=int(cmd_ch),
        cache_dir=cache_dir,
        calendar_refresh_minutes=_get_int("CALENDAR_REFRESH_MINUTES", 30),
        watcher_tick_seconds=_get_int("WATCHER_TICK_SECONDS", 1),
        burst_poll_seconds=_get_int("BURST_POLL_SECONDS", 5),
        burst_window_seconds=_get_int("BURST_WINDOW_SECONDS", 60),
        backoff_start_seconds=_get_int("BACKOFF_START_SECONDS", 60),
        backoff_max_seconds=_get_int("BACKOFF_MAX_SECONDS", 900),
        timezone=os.getenv("TIMEZONE", "America/New_York"),
        bls_api_key=bls_key,
        post_only_configured_sources=_get_bool("POST_ONLY_CONFIGURED_SOURCES", True),
        health_host=os.getenv("HEALTH_HOST", "0.0.0.0"),
        health_port=_get_int("HEALTH_PORT", 8080),
    )
