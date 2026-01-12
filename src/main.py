import asyncio

import uvicorn

from src.bot.config import AppSettings
from src.bot.discord_bot import EconDiscordBot
from src.bot.log import setup_logging
from src.bot.web.health import create_app

async def run_bot(settings: AppSettings) -> None:
    bot = EconDiscordBot(settings=settings)
    await bot.start(settings.discord_bot_token)

async def run_web(settings: AppSettings) -> None:
    app = create_app()
    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=settings.port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    await server.serve()

async def main() -> None:
    setup_logging()
    settings = AppSettings.load()

    await asyncio.gather(
        run_web(settings),
        run_bot(settings),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
