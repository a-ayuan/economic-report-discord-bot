import logging
import re
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx

log = logging.getLogger("http")

@dataclass(frozen=True)
class HttpPolicy:
    user_agent: str = "economic-discord-bot/1.0 (+contact: you@example.com)"
    timeout_seconds: float = 20.0

class HttpClient:
    """
    Minimal client with:
    - explicit User-Agent
    - conservative timeouts
    - no aggressive crawling
    """

    def __init__(self, policy: HttpPolicy | None = None):
        self.policy = policy or HttpPolicy()
        self._client = httpx.AsyncClient(
            headers={"User-Agent": self.policy.user_agent},
            timeout=self.policy.timeout_seconds,
            follow_redirects=True,
        )

    async def get_text(self, url: str) -> str:
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.text

    async def get_bytes(self, url: str) -> bytes:
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.content

    async def get_json(self, url: str) -> dict:
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    async def post_json(self, url: str, payload: dict) -> dict:
        resp = await self._client.post(url, json=payload)
        resp.raise_for_status()
        return resp.json()

    async def aclose(self) -> None:
        await self._client.aclose()

def host_of(url: str) -> str:
    return urlparse(url).netloc.lower()

def safe_event_id(prefix: str, name: str, stamp: str) -> str:
    base = f"{prefix}:{name}:{stamp}".lower()
    return re.sub(r"[^a-z0-9:_-]+", "-", base)
