"""NSE indices close adapter — the ONLY place the header strings live.

Header verified live in Task 9; adjust here if reality differs."""
from __future__ import annotations

from datetime import date

_BASE = "https://nsearchives.nseindia.com/content/indices"

# Raw indices-close columns as published in the daily ind_close_all CSV.
INDICES_RAW_COLUMNS: list[str] = [
    "Index Name",
    "Index Date",
    "Open Index Value",
    "High Index Value",
    "Low Index Value",
    "Closing Index Value",
    "Points Change",
    "Volume",
    "Turnover (Rs. Cr.)",
]


def build_indices_url(d: date) -> str:
    stamp = d.strftime("%d%m%Y")
    return f"{_BASE}/ind_close_all_{stamp}.csv"
