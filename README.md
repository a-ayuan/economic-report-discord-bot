# Economic Report Discord Bot (USD / US)

This bot builds an economic calendar and posts releases in real-time:
- Posts: `EVENT_NAME — Forecast: X | Previous: Y | Actual: Z`
- Command: `!calendar` prints current week's past + upcoming events

## What is “official by default”
Government/official sources implemented:
- BLS (Employment Situation, CPI, PPI, JOLTS): schedules from BLS schedule pages; values via BLS Public Data API when possible
- BEA: release schedule scraping (GDP / PCE schedule) + release detection hooks
- Census: retail sales series via Census time series API docs endpoints
- DOL: weekly UI claims via official DOL PDF release
- Fed: FOMC meeting calendar scraping (statement/minutes/press conf as events)
- FRB Services: banking holidays

Private events in your list (ADP, ISM PMIs, S&P Flash PMI, UoM sentiment/expectations) are included as event types
but are disabled unless you add an approved provider.

## Setup
1) Create a Discord bot application and invite it with message permissions.
2) Copy `.env.example` to `.env` and fill values.
3) Install and run:
```bash
pip install -r requirements.txt
python -m src.main
