import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

@dataclass(frozen=True)
class AppSettings:
    discord_bot_token: str
    discord_channel_id: int
    command_channel_id: Optional[int]

    tz: str
    port: int

    cache_path: str
    config_path: str
    user_agent: str

    raw_config: dict[str, Any]

    @staticmethod
    def load() -> "AppSettings":
        load_dotenv()

        config_path = os.getenv("CONFIG_PATH", "./config.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f) or {}

        token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
        if not token:
            raise RuntimeError("DISCORD_BOT_TOKEN is required")

        channel_id = os.getenv("DISCORD_CHANNEL_ID", "").strip()
        if not channel_id.isdigit():
            raise RuntimeError("DISCORD_CHANNEL_ID must be an integer")

        cmd_channel_id_raw = os.getenv("COMMAND_CHANNEL_ID", "").strip()
        command_channel_id: Optional[int] = None
        if cmd_channel_id_raw:
            if not cmd_channel_id_raw.isdigit():
                raise RuntimeError("COMMAND_CHANNEL_ID must be an integer if provided")
            command_channel_id = int(cmd_channel_id_raw)

        tz = os.getenv("TZ", "America/New_York")
        port = int(os.getenv("PORT", "10000"))

        cache_path = os.getenv("CACHE_PATH", "/tmp/econbot_cache.json")
        user_agent = os.getenv("USER_AGENT", "EconDiscordBot/1.0")

        # Ensure parent directory exists if using a non-/tmp path
        p = Path(cache_path).expanduser()
        if p.parent and str(p.parent) not in ("/tmp", ""):
            p.parent.mkdir(parents=True, exist_ok=True)

        return AppSettings(
            discord_bot_token=token,
            discord_channel_id=int(channel_id),
            command_channel_id=command_channel_id,
            tz=tz,
            port=port,
            cache_path=cache_path,
            config_path=config_path,
            user_agent=user_agent,
            raw_config=raw_config,
        )