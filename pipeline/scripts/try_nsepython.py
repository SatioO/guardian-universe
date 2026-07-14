#!/usr/bin/env python3
"""Quick probe of the nsepython library from THIS machine.

    pip install nsepython
    python scripts/try_nsepython.py            # RELIANCE
    python scripts/try_nsepython.py TATASTEEL

Docs: https://unofficed.com/nse-python/documentation/#installation

`nse_eq(symbol)` hits NSE's /api/quote-equity and returns its full JSON, which
includes the 4-tier `industryInfo` (macro / sector / industry / basicIndustry) —
exactly what the harvest needs.

Context: nsepython's default 'local' mode is the same requests + cookie-priming
approach we already tested (it returned Akamai "Access Denied" 403 here), and
curl_cffi impersonating Chrome's TLS fingerprint ALSO 403'd. So this is expected
to fail on this machine too — but it's a direct, fast test of the real library
in case its priming/session handling differs from our reimplementation. If it
DOES return industryInfo, tell me and we adopt nsepython instead of Playwright.
"""
from __future__ import annotations

import json
import sys


def main() -> int:
    symbol = (sys.argv[1] if len(sys.argv) > 1 else "RELIANCE").upper()
    try:
        from nsepython import nse_eq
    except ImportError:
        print("Install first:  pip install nsepython")
        return 2

    print(f"nse_eq({symbol!r}) ...")
    try:
        data = nse_eq(symbol)
    except Exception as e:  # noqa: BLE001 - report whatever nsepython raises
        print(f"  ✗ raised {type(e).__name__}: {e}")
        print("  (an empty/blocked response — NSE's Akamai denied the request)")
        return 1

    if not isinstance(data, dict) or not data:
        print("  ✗ returned empty/None — NSE blocked the request (Akamai 403).")
        return 1

    info = data.get("industryInfo") or {}
    if not info:
        print("  ✗ response has no industryInfo (partial/blocked response):")
        print("   ", json.dumps(data, indent=2)[:400])
        return 1

    print("  ✅ nsepython works on this machine — industryInfo:")
    print("     macro         :", info.get("macro"))
    print("     sector        :", info.get("sector"))
    print("     industry      :", info.get("industry"))
    print("     basicIndustry :", info.get("basicIndustry"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
