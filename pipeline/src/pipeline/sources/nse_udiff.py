"""NSE UDiFF CM bhavcopy adapter — the ONLY place the URL/filename pattern lives.

NSE changed this pattern in the 2024 UDiFF migration and again in Oct 2025
(four-digit year). If it changes again, this is the one file to edit."""
from __future__ import annotations

from datetime import date

_BASE = "https://nsearchives.nseindia.com/content/cm"

# Raw UDiFF columns we depend on downstream (subset of the full file).
UDIFF_COLUMNS: list[str] = [
    "TradDt",       # trade date
    "FinInstrmTp",  # 'STK' for cash equity
    "ISIN",
    "TckrSymb",     # symbol
    "SctySrs",      # series (EQ/BE/...)
    "SsnId",        # session (F1/F2 final; I1/I2 pre-open/interim)
    "OpnPric",
    "HghPric",
    "LwPric",
    "ClsPric",
    "PrvsClsgPric",
    "TtlTradgVol",
    "TtlTrfVal",
    "TtlNbOfTxsExctd",
]


def build_udiff_url(d: date) -> str:
    stamp = d.strftime("%Y%m%d")
    return f"{_BASE}/BhavCopy_NSE_CM_0_0_0_{stamp}_F_0000.csv.zip"
