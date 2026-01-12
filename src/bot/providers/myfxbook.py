from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from bs4 import BeautifulSoup

from src.bot.utils.http import HttpClient
from src.bot.utils.time import NY_TZ, UTC_TZ, week_bounds_ny


DAY_HDR_RE = re.compile(r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{4})$")
DT_RE = re.compile(r"^([A-Za-z]{3})\s+(\d{1,2}),\s+(\d{2}):(\d{2})$")
# e.g. "59 min", "12h 59min", "1 day"
TIMELEFT_RE = re.compile(r"(\d+\s*day|\d+\s*h|\d+\s*min)", re.IGNORECASE)


@dataclass(frozen=True)
class CalendarEvent:
    dt_ny: datetime
    currency: str
    title: str
    impact: str  # High/Medium/Low/No Impact
    previous: str | None
    forecast: str | None  # "Consensus" on Myfxbook
    actual: str | None


class MyFxBookProvider:
    """
    Scrapes Myfxbook's United States economic calendar HTML.

    Why this works:
      - The event rows are present in the server-rendered HTML (no JS required). :contentReference[oaicite:2]{index=2}
    """

    BASE_URL = "https://www.myfxbook.com/forex-economic-calendar/united-states"

    def __init__(self, http: HttpClient):
        self.http = http

    async def fetch_week(
        self,
        *,
        impacts: Iterable[str] = ("High",),
        currency: str = "USD",
        hardcoded_include_keywords: list[str] | None = None,
        hardcoded_exclude_keywords: list[str] | None = None,
    ) -> list[CalendarEvent]:
        week_start, week_end = week_bounds_ny(datetime.now(tz=NY_TZ))

        html = await self.http.get_text(
            self.BASE_URL,
            headers={
                # simple anti-bot friendliness
                "User-Agent": "Mozilla/5.0 (compatible; econ-discord-bot/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )

        events = self._parse(html)

        # Filter: this week (NY), USD only, impact only
        impacts_set = {x.strip().lower() for x in impacts}
        out: list[CalendarEvent] = []
        for ev in events:
            if not (week_start <= ev.dt_ny < week_end):
                continue
            if ev.currency.upper() != currency.upper():
                continue
            if ev.impact.strip().lower() not in impacts_set:
                continue

            title_l = ev.title.lower()

            if hardcoded_include_keywords:
                if not any(k.lower() in title_l for k in hardcoded_include_keywords):
                    continue
            if hardcoded_exclude_keywords:
                if any(k.lower() in title_l for k in hardcoded_exclude_keywords):
                    continue

            out.append(ev)

        out.sort(key=lambda e: e.dt_ny)
        return out

    def _parse(self, html: str) -> list[CalendarEvent]:
        soup = BeautifulSoup(html, "html.parser")

        # The simplest robust method here is to use the page's visible text stream
        # and parse the repeated structure:
        #
        #   Monday, Jan 12, 2026
        #   Jan 12, 16:30
        #   USD  3-Year Note Auction
        #   Low
        #   3.614%  3.609%          (prev + actual)  OR
        #   2.7%  2.7%  2.8%        (prev + consensus + actual)
        #
        text_lines = [ln.strip() for ln in soup.get_text("\n").splitlines() if ln.strip()]

        # Find the first day header to anchor parsing
        first_day_idx = None
        for i, ln in enumerate(text_lines):
            if DAY_HDR_RE.match(ln):
                first_day_idx = i
                break
        if first_day_idx is None:
            return []

        lines = text_lines[first_day_idx:]

        events: list[CalendarEvent] = []
        cur_day_year = None
        cur_day_month = None
        cur_day_day = None

        i = 0
        while i < len(lines):
            m_day = DAY_HDR_RE.match(lines[i])
            if m_day:
                # e.g. Monday, Jan 12, 2026
                _dow, mon_abbr, day_s, year_s = m_day.groups()
                cur_day_year = int(year_s)
                cur_day_month = mon_abbr
                cur_day_day = int(day_s)
                i += 1
                continue

            m_dt = DT_RE.match(lines[i])
            if m_dt and cur_day_year is not None:
                mon_abbr, day_s, hh_s, mm_s = m_dt.groups()
                day_int = int(day_s)
                hh = int(hh_s)
                mm = int(mm_s)

                # Myfxbook times on the public page commonly appear in UTC-like schedule.
                # We interpret them as UTC then convert to NY.
                # (Example: CPI 13:30 often corresponds to 08:30 NY.) :contentReference[oaicite:3]{index=3}
                dt_utc = _build_dt_utc(cur_day_year, mon_abbr, day_int, hh, mm)
                dt_ny = dt_utc.astimezone(NY_TZ)

                i += 1

                # Optional "time left" token can appear right after dt
                if i < len(lines) and TIMELEFT_RE.search(lines[i]) and not lines[i].startswith("USD"):
                    i += 1

                if i >= len(lines):
                    break

                # Expect: "USD  Event Name"
                currency_and_title = lines[i]
                i += 1

                # Try to split: currency is first 3 letters token
                cur = currency_and_title[:3].strip()
                title = currency_and_title[3:].strip()
                if not cur.isalpha():
                    # fallback: if the line begins with USD in the middle
                    parts = currency_and_title.split(None, 1)
                    if len(parts) == 2:
                        cur, title = parts[0], parts[1]
                    else:
                        cur, title = "USD", currency_and_title.strip()

                # Impact line
                impact = None
                if i < len(lines):
                    impact = lines[i].strip()
                    i += 1
                else:
                    impact = "Unknown"

                # Collect up to 3 value tokens until next dt/day header
                vals: list[str] = []
                while i < len(lines):
                    if DAY_HDR_RE.match(lines[i]) or DT_RE.match(lines[i]):
                        break
                    # Ignore UI words that sometimes appear around the grid
                    if lines[i] in {"All", "None", "Date", "Event", "Impact", "Previous", "Consensus", "Actual", "Time left"}:
                        i += 1
                        continue
                    vals.append(lines[i])
                    i += 1

                # vals can be: ["3.614%  3.609%"] as a single line or multiple lines
                tokens: list[str] = []
                for v in vals:
                    tokens.extend([t for t in re.split(r"\s{2,}|\t+", v.strip()) if t.strip()])
                tokens = [t.strip() for t in tokens if t.strip()]

                previous, forecast, actual = _map_values(tokens)

                events.append(
                    CalendarEvent(
                        dt_ny=dt_ny,
                        currency=cur.upper(),
                        title=title,
                        impact=impact,
                        previous=previous,
                        forecast=forecast,
                        actual=actual,
                    )
                )
                continue

            i += 1

        return events

def _build_dt_utc(year: int, mon_abbr: str, day: int, hh: int, mm: int) -> datetime:
    # Parse month abbrev
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }
    month = month_map.get(mon_abbr, 1)
    return datetime(year, month, day, hh, mm, tzinfo=UTC_TZ)

def _map_values(tokens: list[str]) -> tuple[str | None, str | None, str | None]:
    """
    Myfxbook columns are Previous / Consensus / Actual.

    But many events only show 1 or 2 values (e.g. auctions often show prev+actual).
    Heuristic:
      - 3 tokens: prev, consensus, actual
      - 2 tokens: prev, None, actual
      - 1 token: prev, None, None
      - 0: None, None, None
    """
    if len(tokens) >= 3:
        return tokens[0], tokens[1], tokens[2]
    if len(tokens) == 2:
        return tokens[0], None, tokens[1]
    if len(tokens) == 1:
        return tokens[0], None, None
    return None, None, None
