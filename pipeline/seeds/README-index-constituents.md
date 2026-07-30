# `index_constituents_catalog.csv` — how it was made, and how to extend it

NSE publishes each index's member list as its own CSV, and **the filenames
follow no derivable rule**. Three real examples from this catalog:

| Index | Filename |
|---|---|
| Nifty India Defence | `ind_niftyindiadefence_list.csv` |
| Nifty India Consumption | `ind_niftyconsumptionlist.csv` — drops "india" |
| Nifty Infrastructure | `ind_niftyinfralist.csv` — abbreviates |
| Nifty MidSmall Financial Services | `ind_niftymidsmallfinancailservice_list.csv` — NSE's own typo |

So the mapping cannot be computed; it has to be *recorded*. That is what this
file is. It replaces the 15 hardcoded pairs that used to live in the desktop
client (`src-tauri/src/constituents/catalog.rs`), where every new index meant a
Rust edit plus an app release.

## How it was harvested

Not by brute-forcing filename variants — by reading them off the source. Every
index page on niftyindices.com embeds a direct link to its own constituent CSV,
so the harvest walked all 192 index pages from the site nav and took the
filename from each page.

That method self-validates: it reproduced all 15 filenames the client had
hardcoded, byte for byte, with matching member counts (bank 14, energy 40,
healthcare 20, consumer durables 13).

Harvested 2026-07-30: **134 of the 162 index keys** present in
`indices_*.parquet` resolved and parse-verified. The 28 without a list split
into 22 that legitimately have no equity basket (G-Sec, BHARAT Bond, India VIX,
futures, PR/TR leverage and inverse, USD, arbitrage, dividend points) and 6 real
equity indices NSE simply does not publish lists for (Nifty100 ESG, Nifty100
Enhanced ESG, Nifty100 ESG Sector Leaders, Nifty500 Shariah, Nifty50 Shariah,
Nifty500 Ahimsa is published — see the file).

## Columns

| column | meaning |
|---|---|
| `index_name` | NSE's own display name. **The semantic key** — see the warning below. |
| `index_key` | `IDX:...`. Advisory/diagnostic only. |
| `csv_path` | Path under `https://niftyindices.com/`. Usually a bare filename appended to `/IndexConstituent/`, but **Nifty EV & New Age Automotive is a full path** (`/Index_Statistics/...`). |
| `mirror_file` | The basename, which is how `nsearchives.nseindia.com/content/indices/` serves it — that host flattens directories. |
| `on_mirror` | Whether the CI-safe mirror actually serves this file. **23 of 134 are `no`.** |
| `members_at_harvest` | Member count observed on 2026-07-30, keyed to ISINs starting `INE`. Seeds the per-index shrink guard; it is a reference point, not a contract — NSE genuinely reshuffles (Nifty Energy went 10 → 40). |

## Two things that will bite you

**1. Do not freeze `index_key` from this file.** Resolve it at build time by
looking the `index_name` up in `indices_*.parquet`. Index names drift, and when
they drift the key drifts with them — `IDX:NIFTYINDIAINTERNET&E-COMMERCE`
became `IDX:NIFTYINDIAINTERNET` on 2025-05-05. A key frozen here goes stale and
the client's drill silently empties, which is the worst possible failure: it
looks like an index with no members rather than an error.

**2. The mirror is not a complete substitute for the primary.** GitHub Actions
cannot reach `www.nseindia.com/api/*` (Akamai blocks datacenter IPs), which is
why the archives mirror is the CI-proven host elsewhere in this pipeline. But
the mirror is missing 23 of these files, including several indices worth having
— Chemicals, Housing, Capital Goods, Consumer Services, Transportation &
Logistics. A mirror-only fetcher would drop them **silently**, so the builder
must try the primary first and fall back, and must report per-index failures
rather than emitting an empty basket.

Whether `niftyindices.com` itself is reachable from GitHub Actions is **not yet
verified** — it is a different host from `www.nseindia.com` and the desktop
client uses it as its primary today, but that is a desktop IP. If CI turns out
to be blocked there, the fallback still yields 111 of 134 and the rest need a
locally dispatched run.

## Adding an index

Append a row. If NSE's page does not link a CSV, the index has no published
basket and does not belong here — do not invent a filename.
