import requests
import logging
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)

def get_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s

@dataclass
class HttpClient:
    timeout_s: int = 20

    async def get_text(self, url: str, *, headers: dict[str, str] | None = None) -> str:
        timeout = aiohttp.ClientTimeout(total=self.timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as resp:
                resp.raise_for_status()
                return await resp.text()
