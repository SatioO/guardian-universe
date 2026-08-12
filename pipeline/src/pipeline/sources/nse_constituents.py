"""Index-constituents source: NSE's published per-index member CSVs.

Pure parse/normalize — no network, no I/O — so it is unit-testable in isolation.
The fetch, the atomic write and the fail-closed policy live in
`builders.build_index_constituents`, exactly as `nse_sector` splits from
`builders.build_sector_industry`.

Every index publishes its basket as its own CSV whose filename follows no
derivable rule (Defence keeps "india", Consumption drops it, Infrastructure
abbreviates, MidSmall Financial Services carries NSE's own typo "financail").
That mapping is therefore *recorded*, not computed — see
`seeds/index_constituents_catalog.csv` and its README.

Source CSV header, which is also the integrity gate:

    Company Name,Industry,Symbol,Series,ISIN Code
"""

from __future__ import annotations

import csv
import io

import pandas as pd

# Primary host. The desktop client has used this one for years; `csv_path` in
# the catalog is relative to it, and is usually a bare filename under
# /IndexConstituent/ — but not always (Nifty EV & New Age Automotive lives
# under /Index_Statistics/), which is why the catalog stores a PATH.
PRIMARY_BASE_URL = "https://niftyindices.com"
PRIMARY_DIR = "/IndexConstituent"

# CI-safe mirror. GitHub Actions cannot reach `www.nseindia.com/api/*` (Akamai
# blocks datacenter IPs), so this archives host is the proven one elsewhere in
# the pipeline. It flattens directories — everything is served from one folder
# by basename — and it does NOT carry every file: 23 of 134 are absent
# (Chemicals, Housing, Capital Goods, Consumer Services, ...). It is a
# fallback, never the sole source, or those indices vanish silently.
MIRROR_BASE_URL = "https://nsearchives.nseindia.com/content/indices"

# The exact first lines NSE publishes constituent CSVs with. A WHITELIST, not
# a fuzzy match: this is the guard that stops an HTML error page served with
# HTTP 200 from being read as "this index has no members" — an empty basket is
# indistinguishable from a real one downstream. Two spellings, because NSE
# ships two: most files say "Company Name"; a few (Nifty Housing) say
# "Company". Same five columns, same order, either way.
EXPECTED_HEADERS: tuple[str, ...] = (
    "Company Name,Industry,Symbol,Series,ISIN Code",
    "Company,Industry,Symbol,Series,ISIN Code",
)
# The canonical form, kept for messages and downstream docs.
EXPECTED_HEADER = EXPECTED_HEADERS[0]

# A published basket is never this small; anything shorter is a truncated
# response or an error document that happened to parse.
MIN_MEMBERS = 5

# Business columns, in emitted order. The builder appends the as-of `date`.
CONSTITUENT_COLUMNS: list[str] = [
    "index_key",       # "IDX:NIFTYBANK" — joins to indices_*.parquet.instrument_key
    "index_name",      # "Nifty Bank" — NSE's own display name, verbatim
    "family",          # NSE's own category: sectoral | thematic | strategy | broad.
                       # Travels with the rows — it cannot be re-derived from
                       # the name: NSE files Nifty Energy as thematic.
    "rotation_list",   # bool: on the Rotation tool's curated list. CURATION IS
                       # DATA, not app code — changing the tool's list is a
                       # seed edit + republish, never an app release.
    "display_label",   # what the Rotation tool calls it — the owner's name
                       # ("Nifty EV"), not NSE's mouthful ("Nifty EV & New Age
                       # Automotive"). Falls back to index_name when unset.
    "instrument_key",  # ISIN — THE join key, to ohlc_* and instruments_all
    "symbol",          # display/diagnostics only, never the join
    "isin",            # same value as instrument_key; mirrors the redundancy
                       # already in ohlc_*/instruments_all so a reader never
                       # has to know the key IS the ISIN
    "company",
    "industry",
    "series",
    "source_file",     # provenance — which irregular filename produced this row
]


def primary_url(csv_path: str) -> str:
    """Absolute URL on the primary host for a catalog `csv_path`.

    A bare filename is taken as living in the usual constituent directory; a
    value starting with "/" is already a full path from the site root.
    """
    path = csv_path.strip()
    if path.startswith("/"):
        return f"{PRIMARY_BASE_URL}{path}"
    return f"{PRIMARY_BASE_URL}{PRIMARY_DIR}/{path}"


def mirror_url(csv_path: str) -> str:
    """Absolute URL on the archives mirror, which serves everything flat."""
    return f"{MIRROR_BASE_URL}/{csv_path.strip().rsplit('/', 1)[-1]}"


class MalformedConstituentCsv(ValueError):
    """The payload is not a published constituent list."""


def parse_constituents_csv(
    payload: bytes,
    *,
    index_key: str,
    index_name: str,
    family: str,
    rotation_list: bool = False,
    display_label: str = "",
    source_file: str,
) -> pd.DataFrame:
    """Parse one index's member CSV into the normalized constituent frame.

    Raises `MalformedConstituentCsv` when the payload is not a real basket —
    caught by the builder, which then keeps the prior rows for that index
    rather than publishing an empty one.

    Robustness mirrors `nse_sector.parse_sector_csv`: the stdlib csv reader so
    a quoted comma inside a company name never mis-aligns columns, a tolerated
    UTF-8 BOM and CRLF, short/incomplete rows skipped, and a dedupe on ISIN
    keeping the first occurrence.
    """
    text = payload.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))

    try:
        header = next(reader)
    except StopIteration as e:
        raise MalformedConstituentCsv(f"{source_file}: empty payload") from e

    if all([h.strip() for h in header] != want.split(",") for want in EXPECTED_HEADERS):
        # Deliberately quote what we got: when NSE changes the format, the
        # message is the whole diagnosis.
        raise MalformedConstituentCsv(
            f"{source_file}: unexpected header {','.join(h.strip() for h in header)!r}"
        )

    # `str | bool` because `rotation_list` is a bool (see CONSTITUENT_COLUMNS):
    # every other column is a string, so a plain `dict[str, str]` type-errors on
    # the one curated-list flag.
    rows: list[dict[str, str | bool]] = []
    seen: set[str] = set()
    for rec in reader:
        if len(rec) < 5:
            continue
        company, industry, symbol, series, isin = (v.strip() for v in rec[:5])
        # A 12-character ISIN is the one field we cannot do without — it is the
        # join key. No prefix filter here: `IN9`-prefixed DVR equities are real
        # and universe policy belongs to the consumer, not the producer.
        if len(isin) != 12 or not symbol:
            continue
        isin = isin.upper()
        if isin in seen:
            continue
        seen.add(isin)
        rows.append(
            {
                "index_key": index_key,
                "index_name": index_name,
                "family": family,
                "rotation_list": rotation_list,
                "display_label": display_label.strip() or index_name,
                "instrument_key": isin,
                "symbol": symbol.upper(),
                "isin": isin,
                "company": company,
                "industry": industry,
                "series": series.upper(),
                "source_file": source_file,
            }
        )

    if len(rows) < MIN_MEMBERS:
        raise MalformedConstituentCsv(
            f"{source_file}: only {len(rows)} usable members (floor {MIN_MEMBERS})"
        )

    return pd.DataFrame(rows, columns=CONSTITUENT_COLUMNS)


def normalize_index_name(name: str) -> str:
    """Collapse a display name to its comparison form.

    The same collapse `normalize_indices.py` applies when it builds
    `instrument_key` — uppercase with spaces removed — so "Nifty Oil & Gas"
    matches whether a title spells it with or without spaces around the "&".
    Punctuation is preserved: 39 live index keys depend on `&`, `-`, `/`, `:`,
    `%` and parentheses.
    """
    return name.strip().upper().replace(" ", "")
