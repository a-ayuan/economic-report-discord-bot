import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    discord_token: str
    announce_channel_id: int
    command_channel_id: int

    user_agent: str
    cache_dir: str
    tz: str

    cal_refresh_minutes: int

    retry_phase1_seconds: int
    retry_phase1_interval: int
    retry_phase2_seconds: int
    retry_phase2_interval: int
    retry_phase3_seconds: int
    retry_phase3_interval: int

def load_settings() -> Settings:
    def req(name: str) -> str:
        v = os.getenv(name, "").strip()
        if not v:
            raise RuntimeError(f"Missing required env var: {name}")
        return v

    return Settings(
        discord_token=req("DISCORD_TOKEN"),
        announce_channel_id=int(req("ANNOUNCE_CHANNEL_ID")),
        command_channel_id=int(req("COMMAND_CHANNEL_ID")),

        user_agent=os.getenv("USER_AGENT", "EconomicDiscordBot/1.0 (contact: you@example.com)"),
        cache_dir=os.getenv("CACHE_DIR", "/tmp/econbot-cache"),
        tz=os.getenv("TZ", "America/New_York"),

        cal_refresh_minutes=int(os.getenv("CAL_REFRESH_MINUTES", "360")),

        retry_phase1_seconds=int(os.getenv("RETRY_PHASE1_SECONDS", "30")),
        retry_phase1_interval=int(os.getenv("RETRY_PHASE1_INTERVAL", "5")),
        retry_phase2_seconds=int(os.getenv("RETRY_PHASE2_SECONDS", "120")),
        retry_phase2_interval=int(os.getenv("RETRY_PHASE2_INTERVAL", "15")),
        retry_phase3_seconds=int(os.getenv("RETRY_PHASE3_SECONDS", "600")),
        retry_phase3_interval=int(os.getenv("RETRY_PHASE3_INTERVAL", "60")),
    )
