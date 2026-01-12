from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

@dataclass
class CacheState:
    sent_event_ids: set[str] = field(default_factory=set)
    last_calendar_fetch_iso: str | None = None
    # provider can store extra metadata if needed
    provider_state: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def load(path: str) -> "CacheState":
        if not os.path.exists(path):
            return CacheState()

        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f) or {}
            return CacheState(
                sent_event_ids=set(raw.get("sent_event_ids", [])),
                last_calendar_fetch_iso=raw.get("last_calendar_fetch_iso"),
                provider_state=raw.get("provider_state", {}) or {},
            )
        except Exception:
            # If cache is corrupted, start fresh (better than crashing)
            return CacheState()

    def save(self, path: str) -> None:
        tmp = f"{path}.tmp"
        raw = {
            "sent_event_ids": sorted(self.sent_event_ids),
            "last_calendar_fetch_iso": self.last_calendar_fetch_iso,
            "provider_state": self.provider_state,
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2, sort_keys=True)
        os.replace(tmp, path)
