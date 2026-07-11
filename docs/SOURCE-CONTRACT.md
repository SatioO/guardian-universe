# SOURCE-CONTRACT.md — P5 Fundamentals Phase-0 spike

**Date probed:** 2026-07-11 (IST evening; a Saturday — BSE feed still returned same-day filings)
**Probe origin:** residential IP, macOS, plain `curl` (~22 requests total, single requests, ≥1.5–2 s spacing; no 429/temporary blocks observed)
**Status:** evidence-gathering only. No production code. **Datacenter/CI reachability of `api.bseindia.com` / `www.bseindia.com` is NOT yet proven** — a follow-up one-off GitHub Actions run must re-execute the probes in §8 from `ubuntu-latest`. `nsearchives.nseindia.com` is already CI-proven (the price pipeline fetches it daily from Actions).

References: traderview `docs/superpowers/specs/2026-07-11-scanner-p4-p5-sector-fundamentals-design.md` (§0 P5 sourcing, §5 Phase 0) and `2026-06-27-fundamentals-ingest-architecture.md` (risk register R1 source reachability, R3 bulk discovery).

---

## 1. Source A — BSE bulk financial-results discovery (the R1+R3 crux)

### 1.1 `Corp_FinanceResult_ng/w` — the dedicated financial-results endpoint (WORKS, BULK)

This is the endpoint behind `www.bseindia.com/corporates/Comp_Resultsnew` (the page 301-redirects from the old `.aspx` URL; it is an Angular SPA and the endpoint was recovered from its `main-*.js` bundle, not guessed).

```
GET https://api.bseindia.com/BseIndiaAPI/api/Corp_FinanceResult_ng/w?SCRIP_CD=&FlagDur=1&HFQ=&ISUBGROUP_CODE=
Headers: Referer: https://www.bseindia.com/
         User-Agent: <real browser UA>
→ HTTP 200, application/json; charset=utf-8 (38,927 bytes on probe day)
```

- **Bulk:** yes — with `SCRIP_CD` empty, returns **all companies** whose results were broadcast in the window. The SPA's own code path allows empty scrip only when `FlagDur == 1`, but the API accepted empty scrip with `FlagDur=1` directly; other FlagDur values with empty scrip are untested (one of the §8 follow-up probes).
- **Window param `FlagDur`** (values from `Corp_GetFINANCE_DRDOWN_ng/w?flag=1`):
  `1`=Today, `2`=Last 1 Week, `3`=Last 15 Days, `4`=Last 1 month, `5`=Last 3 month, `6`=Last 1 year, `7`=Beyond last 1 year. **Rolling windows, not arbitrary from/to dates** — fine for a daily/weekly CI diff; use Source A2 for arbitrary date ranges/backfill.
- **`HFQ`** (from `?flag=3`): `1`=Half Yearly, `2`=Yearly, `3`=Quarterly, empty=all.
- **`ISUBGROUP_CODE`**: industry filter (from `?flag=2`), empty=all.
- **No pagination observed** — `FlagDur=1` returned the full day in one response (67 rows / 35 companies). Larger windows presumably return larger single responses; size behaviour at `FlagDur=6` is untested.

**Response shape** (real rows, trimmed):

```json
{"Table":[
 {"Scrip_cd":517506,"scrip_name":"TTK Prestige Ltd","quarter_code":"MQ2025-2026",
  "audited":"Audited","Qtr":370.00,"Fld_CreateDate":"2026-07-11T21:18:07.233",
  "DT_TM":"Jul 11 2026  9:18PM","Industry_name":"Consumer Durables",
  "company_name":"TTK Prestige Ltd","Fld_NatureOfReport":"Standalone",
  "XMLName":"IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_517506_1172026211629_IFIndAs.html",
  "Consol_XMLName":"IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_517506_1172026211820_IFIndAs.html",
  "URL":"https://www.bseindia.com/stock-share-price/ttk-prestige-ltd/ttkprestig/517506/",
  "Resultpageurl":null},
 {"Scrip_cd":517506, "...":"...", "quarter_code":"MC2025-2026","Qtr":373.00}
]}
```

Fields: `Scrip_cd`, `scrip_name`, `company_name`, `quarter_code`, `audited` (Audited/Un-audited), `Qtr` (internal numeric quarter id), `Fld_CreateDate`/`DT_TM` (broadcast timestamp, IST), `Industry_name`, `Fld_NatureOfReport` (Standalone/Consolidated), `XMLName`, `Consol_XMLName`, `URL`, `Resultpageurl`.

- **Document links:** the SPA renders `XMLName`/`Consol_XMLName` as `https://www.bseindia.com/XBRLFILES/{XMLName}` (recovered from the bundle template: `href = "/XBRLFILES/" + XMLName`). Confirmed: `HEAD https://www.bseindia.com/XBRLFILES/IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_517506_1172026211629_IFIndAs.html` → **200, text/html**. These are **HTML renderings of the SEBI Integrated Filing (IndAS)** — *not* raw XBRL instances. See §6 (the gap).
- **`quarter_code` semantics (INFERRED, unconfirmed):** first letter looks like quarter-end month (`J`=Jun, `S`=Sep, `M`=Mar), second letter `Q`=quarter / `H`=half-year / `C`=cumulative(annual?). Distinct values seen in one day: `JQ2026-2027, MQ2025-2026, MC2025-2026, MH2025-2026, MH2024-2025, MC2024-2025, SQ2016-2017, SH2016-2017, MQ2016-2017` (old-period rows are companies filing revisions/late). **Do not build on this until confirmed** (§7).
- **Original vs Revision:** no explicit flag in this feed. Multiple rows per `(Scrip_cd, quarter_code)` over time and old-period rows appearing on a current day are the observable signal; the announcements feed (A2) carries "Revised …" in the headline text.

### 1.2 `AnnSubCategoryGetData/w` — announcements feed, arbitrary date window (WORKS, BULK, paginated)

```
GET https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=1&strCat=Result&strPrevDate=20260701&strScrip=&strSearch=P&strToDate=20260710&strType=C&subcategory=Financial+Results
Headers: Referer: https://www.bseindia.com/  +  browser User-Agent
→ HTTP 200, application/json (55,331 bytes)
```

- **Bulk + arbitrary date window** (`strPrevDate`/`strToDate` = `YYYYMMDD`), empty `strScrip` = all companies. This is the **backfill/date-range discovery channel** complementing A1's rolling windows.
- **Pagination:** 50 rows/page via `pageno`; total in `Table1[0].ROWCNT` (probe window had 71 → page 2 returned the remaining 21; note `RN` restarts at 1 on each page — use `ROWCNT`, not `RN`, for iteration).

**Response shape** (real row, trimmed):

```json
{"Table":[
 {"NEWSID":"9632db29-e59b-40da-b33f-b41e39966723","SCRIP_CD":535648,
  "NEWSSUB":"Just Dial Ltd - 535648 - Unaudited Financial Results For The First Quarter Ended June 30, 2026",
  "NEWS_DT":"2026-07-10T22:50:56.42","News_submission_dt":"2026-07-10T22:50:56",
  "DissemDT":"2026-07-10T22:50:56.42","CATEGORYNAME":"Result","SUBCATNAME":"Financial Results",
  "ATTACHMENTNAME":"93a664e1-0844-4e74-b24b-90171209dee8.pdf","Fld_Attachsize":5782633,
  "SLONGNAME":"Just Dial Ltd","XML_NAME":"ANN_535648_9632DB29-...","QUARTER_ID":null, "...":"..."}],
 "Table1":[{"ROWCNT":71}]}
```

- **Attachments are PDFs**, hosted at `https://www.bseindia.com/xml-data/corpfiling/AttachLive/{ATTACHMENTNAME}` — confirmed `HEAD … .pdf` → **200, application/pdf**. (Historic attachments likely under `AttachHis/` — untested.)
- **Period:** free text in `NEWSSUB`/`HEADLINE` only (`QUARTER_ID` was `null` on all sampled rows). **Revisions:** headline text prefix "Revised …" (observed on 3 of 71 rows); no structured flag.

### 1.3 What did NOT work (honest failures)

| Probe | Result |
|---|---|
| `AnnGetData/w?pageno=1&strCat=Result&strPrevDate=20260707&strScrip=&strSearch=P&strToDate=20260710&strType=C` | HTTP 200 but body `"No Record Found!"` — the plain `AnnGetData` shape without `subcategory` returns nothing for this category. Use `AnnSubCategoryGetData` (§1.2). |
| `GetCorXbrlDetails_ng/w?scripcode=&categoryid=22&fromdate=…&todate=…` | **HTTP 302 → `api.bseindia.com/error_Bse.html`** (server-side param validation reject). |
| `GetCorXbrlDetails_ng/w?strScrip=&strCategory=22&strPrevDate=…&strToDate=…` | Same 302 → error page. |

`GetCorXbrlDetails_ng` matters because `GetCorXbrlDropdown_ng/w` (no params, **works**, 200) lists XBRL categories including **`{"xbrl_id":22,"xbrl_description":"Financial Results"}`** — i.e. BSE has a Corporate-XBRL listing API that plausibly links **raw XBRL** filings. Its parameter shape lives in a lazy-loaded SPA route chunk not present in `main.js`; two param-shape guesses were rejected. **Resolution needs a browser network trace of BSE's Corporate XBRL page** — top open question (§7.1), because it may close the gap in §6.

**Headers note:** all successful BSE API calls sent both `Referer: https://www.bseindia.com/` and a real-browser `User-Agent`. The minimal required set was **not** isolated (kept request count low). Treat both as required until the §8 Actions run tests dropping them.

---

## 2. Source B — BSE universe seed: `ListofScripData/w` (WORKS, BULK)

```
GET https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active
Headers: Referer + browser User-Agent
→ HTTP 200, application/json — 1,726,807 bytes, single response (no pagination)
```

- **4,919 active equity scrips**, of which **4,656 have an `INE…` ISIN** (the rest: blank/`INF…` fund-style ISINs — filter on `INE` prefix for the equity universe).

**Response shape** (real row):

```json
[{"SCRIP_CD":"500002","Scrip_Name":"ABB India Ltd","Status":"Active","GROUP":"A",
  "FACE_VALUE":"2.00","ISIN_NUMBER":"INE117A01022","INDUSTRY":null,"scrip_id":"ABB",
  "Segment":"Equity","Issuer_Name":"ABB India Limited","Mktcap":"144808.65",
  "NSURL":"https://www.bseindia.com/stock-share-price/abb-india-ltd/abb/500002/"}]
```

- Gives the full **`SCRIP_CD ↔ ISIN_NUMBER ↔ scrip_id` (symbol)** map in one call.
- **Bonus finding:** `Mktcap` (₹ crore) is included per scrip — a free full-universe **market-cap anchor** that can replace/feed the weekly `mcap-refresh` probe in the ingest design (R3's dominant cost), one request instead of ~1,000.

---

## 3. Source C — NSE archive XBRL instance fetch (`nsearchives`) (WORKS, with UA caveat)

- **Default `curl` (no UA tweak) FAILS:** `curl: (92) HTTP/2 stream … INTERNAL_ERROR` — Akamai resets the stream. This is a client-fingerprint gate, not an IP block.
- **With a real-browser `User-Agent` + `--http1.1`: works, no cookies, no Referer.**

```
GET https://nsearchives.nseindia.com/corporate/xbrl/INTEGRATED_FILING_1383099_13022025112058_WEB.xml
    -A "<browser UA>" --http1.1
→ HTTP 200, application/xml, 148,330 bytes
```

Body verified as a genuine SEBI `in-capmkt` instance: root `xbrli:xbrl` with `xmlns:in-capmkt="http://www.sebi.gov.in/xbrl/2024-12-31/in-capmkt"` — exactly what the app's proven Rust parser (`xbrl_integrated.rs`) consumes.

- The instance URL was taken from the app's real fixture data (traderview `src-tauri/src/fundamentals/fixtures/nse-integrated-list-reliance.json`); other real patterns seen there: `INDAS_<seq>_<seq>_<timestamp>[_WEB].xml`, `INTEGRATED_FILING_GOVERNANCE_<seq>_<timestamp>_WEB.xml`, and `archives/financial_results/financial_res_<SYMBOL>_<seq>.html`.
- **Failed probe (honest):** the spike brief's example URL `…/corporate/xbrl/INTEGRATED_FILING_INDAS_HIGH_WEB.xml` → **HTTP 404** — it is a hypothetical filename, not a real instance. Recorded so nobody re-probes it.
- **CI reachability:** already proven daily by the guardian-universe price pipeline (same host). The `_WEB.xml` fetch itself from Actions still deserves one confirmation GET in the §8 run (same host, near-zero risk).

## 4. Source D — nsearchives BULK filing index: **NOT FOUND**

The best-case outcome (discovery also off the CI-proven CDN, removing BSE entirely) did **not** materialize. All plausible-path probes 404'd (each `text/html` NSE 404 page, 3,540 bytes):

| Probed URL | Result |
|---|---|
| `https://nsearchives.nseindia.com/corporate/xbrl/` (directory listing) | 404 |
| `https://nsearchives.nseindia.com/content/corporate/INTEGRATED_FILING.csv` | 404 |
| `https://nsearchives.nseindia.com/content/equities/CF-Integrated-Filing-10072026.csv` | 404 |
| `https://nsearchives.nseindia.com/corporates/datafiles/CF-AN-equities-10-07-2026.csv` | 404 |

This was a guessed-path sample, not an exhaustive enumeration — but the only *known* index of integrated filings (with their `nsearchives` instance URLs) is `https://www.nseindia.com/api/integrated-filing-results?index=equities&symbol=<SYM>`, which is **per-symbol, cookie-primed, and Akamai-blocked from datacenter IPs** (ingest design R1, HIGH confidence). **Verdict: no CDN bulk index → discovery must run off BSE.**

## 5. Source E — NSE universe seed: `EQUITY_L.csv` (WORKS)

```
GET https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv   (browser UA, --http1.1)
→ HTTP 200, text/csv, 168,316 bytes, 2,384 data rows
```

```csv
SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE
20MICRONS,20 Microns Limited,EQ,06-OCT-2008,5,1,INE144J01027,5
```

`SYMBOL` + `ISIN NUMBER` confirmed present (note the leading spaces in some header names — trim on parse).

---

## 6. THE GAP — BSE discovery does not yield nsearchives instance URLs

The design's data flow is *discover on BSE → fetch XBRL bytes from nsearchives → parse with the app's Rust parser*. The spike confirms both endpoints individually but exposes a **missing link**: BSE discovery returns **BSE-hosted document names** (`XBRLFILES/...IFIndAs.html` — HTML renderings; `AttachLive/*.pdf`), while `nsearchives` instance filenames embed **NSE-internal sequence numbers** (`INTEGRATED_FILING_1383099_13022025112058_WEB.xml`) that are **not derivable** from anything in the BSE responses. The only known resolver of ISIN/symbol → nsearchives instance URL is the Akamai-blocked NSE API.

Candidate resolutions, in preference order:

1. **BSE raw XBRL** (§1.3): if `GetCorXbrlDetails_ng` (category 22 = Financial Results) links raw XBRL instances on a BSE host, the pipeline becomes BSE-discover → BSE-XBRL-bytes → same SEBI `in-capmkt` parser (taxonomy is identical on both exchanges, per the design §0). *Needs one browser network-trace session on BSE's Corporate XBRL page to capture the param shape.*
2. **Parse the BSE `IFIndAs.html` rendering** — same integrated-filing content, but HTML not XBRL: requires an HTML adapter, deviating from "reuse the proven parser as-is". Fallback only.
3. **Residential/self-hosted resolver step** for NSE API URL resolution only — violates the zero-babysitting constraint; last resort.

**This is the #1 item for the follow-up spike run.** Until resolved, "nsearchives as source-of-record" holds only for symbols whose instance URLs we can obtain — which today means not-from-CI.

---

## 7. CI reachability

| Source | Host | Status |
|---|---|---|
| NSE archive XBRL instances (`/corporate/xbrl/*.xml`) | `nsearchives.nseindia.com` | **CI-PROVEN** (price pipeline fetches this host daily from Actions); needs browser UA (+ HTTP/1.1 with curl) — no cookies |
| NSE `EQUITY_L.csv` | `nsearchives.nseindia.com` | **CI-PROVEN** (same host/path family) |
| BSE results feeds (`Corp_FinanceResult_ng`, `AnnSubCategoryGetData`) | `api.bseindia.com` | **NEEDS ACTIONS CONFIRMATION** — verified from residential IP only |
| BSE scrip master (`ListofScripData`) | `api.bseindia.com` | **NEEDS ACTIONS CONFIRMATION** |
| BSE documents (`/XBRLFILES/…`, `/xml-data/corpfiling/AttachLive/…`) | `www.bseindia.com` | **NEEDS ACTIONS CONFIRMATION** |
| NSE API (`www.nseindia.com/api/…`) | `www.nseindia.com` | **BLOCKED from CI** (R1, prior finding) — never in the CI dispatch chain |

## 8. Proposed follow-up: one-off GitHub Actions verification run

A throwaway `workflow_dispatch` job on `ubuntu-latest` that re-runs, with the same politeness (≥1.5 s spacing):

1. `Corp_FinanceResult_ng/w?SCRIP_CD=&FlagDur=2&HFQ=&ISUBGROUP_CODE=` → expect 200 JSON with ≥1 row.
2. Same, `FlagDur=4` with empty `SCRIP_CD` → confirms bulk works for wider windows.
3. `AnnSubCategoryGetData/w` (10-day window) → expect 200 + `ROWCNT`.
4. `ListofScripData/w` → expect 200, >4,000 rows.
5. One `www.bseindia.com/XBRLFILES/…IFIndAs.html` HEAD → 200.
6. One `nsearchives …_WEB.xml` GET → 200 `application/xml` (host already proven; instance-path confirmation).
7. Header-minimization matrix on probe 1 (drop Referer; drop UA) — establish the true minimal header set.
8. (After the network trace) `GetCorXbrlDetails_ng` with captured params → decide §6 option 1.

Record status + content-type + first bytes of each into the job log; paste results back into this file.

## 9. Discovery recommendation

**BSE-primary discovery stands** (no nsearchives bulk index exists — §4):

- **Daily incremental:** `Corp_FinanceResult_ng` `FlagDur=2` (last week, overlap-safe), diff `(Scrip_cd, quarter_code, Fld_NatureOfReport, Fld_CreateDate)` against state. One request per run. This directly delivers the ingest design's "bulk date-windowed discovery" (R3 fix).
- **Backfill / arbitrary windows:** `AnnSubCategoryGetData` with `strPrevDate`/`strToDate`, paginated by `ROWCNT`.
- **XBRL bytes:** `nsearchives` instance when its URL is resolvable; **until §6 is closed, the CI-only chain is BSE discovery + BSE documents** — treat the bytes-source decision as *open, gated on the §8 run + one browser trace*.

## 10. ISIN-union universe seed plan

- **BSE side:** `ListofScripData` → (`SCRIP_CD`, `scrip_id`, `ISIN_NUMBER`, `Mktcap`), filtered to `Status=Active`, `Segment=Equity`, ISIN prefix `INE` → **4,656 scrips**.
- **NSE side:** `EQUITY_L.csv` → (`SYMBOL`, `ISIN NUMBER`) → **2,384 symbols**.
- **Union keyed by ISIN** (`instrument_key`): BSE list is the superset (~2× NSE); NSE-only edge cases caught by the union as designed. Per-row provenance: `on_bse`, `on_nse`, `bse_scrip_cd`, `nse_symbol`, `bse_symbol` (`scrip_id`).
- Expect a small set of ISINs on NSE but not in the BSE active list (suspensions, new listings mid-sync) — count and disclose, never drop silently.
- Refresh weekly alongside the sector build; both endpoints are single-request bulk.

## 11. Rate-limit observations

- ~22 total requests over ~15 minutes, ≥1.5–2 s spacing: **zero 429s, zero blocks, no CAPTCHA/challenge pages** on either BSE or NSE hosts.
- Volume behaviour from a datacenter IP is **unknown** — the ingest design's `BSE_THROTTLE_MS ≥ 1500` + adaptive backoff stands unchallenged. Note the discovery design needs only **1–2 BSE API calls per run**, so BSE volume risk collapses to the document-fetch leg (only if BSE serves the XBRL bytes per §6).
- `nsearchives` gate is a **client fingerprint** (UA/HTTP2 quirk), not IP class or cookies, at least from residential. The price pipeline's daily success from Actions suggests the same holds from datacenter.

## 12. Proposed canary (start of every producer run)

Abort the run (red) if any fails; alert on 2 consecutive reds:

1. **BSE feed alive:** `Corp_FinanceResult_ng` `FlagDur=3` (15 days) → 200, valid JSON, `Table` is an array; **during filing season** (Jan/Apr/Jul/Oct 5th–31st) additionally require ≥ 1 row — the filing-season watchdog from the ingest design.
2. **Scrip master sane:** `ListofScripData` → 200 and row count within ±10 % of last run (schema-drift + truncation guard).
3. **XBRL bytes alive:** GET one pinned known-good instance (e.g. the §3 URL) → 200, `application/xml`, body starts `<?xml` and contains `in-capmkt`.
4. **EQUITY_L sane:** 200, header contains `SYMBOL` and `ISIN NUMBER`, ≥ 2,000 rows.

## 13. Open questions

1. **(Blocking for bytes-source)** `GetCorXbrlDetails_ng` param shape — needs one browser network trace of BSE's Corporate XBRL page; decides §6 (raw XBRL from BSE vs HTML adapter vs residential resolver).
2. **(Blocking for CI)** All `*.bseindia.com` endpoints from `ubuntu-latest` — §8 run.
3. `quarter_code` grammar confirmation (map codes → `period_end` + `basis` deterministically; current reading is inferred).
4. `Corp_FinanceResult_ng` behaviour with empty `SCRIP_CD` at `FlagDur≥2` (bulk beyond "today"), and response-size behaviour at `FlagDur=6`.
5. Minimal required header set for BSE APIs (Referer vs UA vs both).
6. Original-vs-Revision: is headline-text matching ("Revised…") reliable enough, or does another feed carry a structured flag?
7. `AttachHis/` path for historical announcement PDFs (backfill leg) — untested.
8. Legal/ToS sign-off (R6) before any public republish — owner action, gates publish not the spike.

---

## §9 RESOLUTION (2026-07-12) — the §6 missing link is CLOSED

The follow-up probe (JS-bundle mining + live curl, no browser) resolved the raw-XBRL gap:

### 9.1 `GetCorXbrlDetails_ng` — request shape captured from BSE's Angular bundle
```
GET https://api.bseindia.com/BseIndiaAPI/api/GetCorXbrlDetails_ng/w?Flag=<category>&scripcode=<scrip|empty>&fromdate=YYYYMMDD&todate=YYYYMMDD
Headers: User-Agent (browser), Referer: https://www.bseindia.com/   ·   HTTP/1.1
```
- Category ids from `GetCorXbrlDropdown_ng/w` — **22 = Financial Results** (no separate "Integrated Filing (Finance)" category; 44 is Governance only).
- Component enforces ≤1-week windows client-side; server accepted our 1-week probes.
- **Per-scrip works** (TCS `532540` → rows); **bulk (empty scripcode) returned empty** in both probes — treat as per-scrip-only.
- **Observed lag:** `xbrlurl` was empty for fresh (Jul-9) *and* April rows for TCS on this endpoint — do NOT depend on it for XBRL URL resolution.

### 9.2 THE KEY FINDING — raw XBRL lives on BSE via an `.html → .xml` swap
`Corp_FinanceResult_ng` rows carry `XMLName`/`Consol_XMLName` like
`IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_<scrip>_<ts>_IFIndAs.html`.
Fetching `https://www.bseindia.com/XBRLFILES/<name>` with the extension swapped to **`.xml`** returns the **raw XBRL instance** (verified 200, `text/xml`, 183 KB):
- Taxonomy: `xmlns:in-capmkt="http://www.sebi.gov.in/xbrl/2026-01-31/in-capmkt"` — the SEBI Integrated-Finance taxonomy (IFIndAs V2.1), **identical to what the consuming app's Rust parser already parses**.
- Real facts verified: `RevenueFromOperations`, `ProfitBeforeTax`, `ProfitLossForPeriod`, `BasicEarningsLossPerShare…`, `NatureOfReportStandaloneConsolidated`, with `unitRef`/`decimals`.
- **Context refs are the app parser's own selectors**: `OneD` (quarter 2026-01-01→2026-03-31), `FourD` (annual), plus instant contexts (balance-sheet inputs). 264 contexts in the sample.
- The `.html` itself also embeds `schemaRef` + `xbrli:context` (iXBRL-ish) — usable as a fallback, but unnecessary given the `.xml` twin.

### 9.3 Confirmed CI-viable end-to-end chain (no NSE API anywhere)
```
Corp_FinanceResult_ng (BULK discovery, FlagDur windows)      [api.bseindia.com]
  → XMLName/.Consol_XMLName  (.html → .xml swap)             [www.bseindia.com/XBRLFILES]
  → raw in-capmkt XBRL → existing Rust parser (unchanged)
```
nsearchives remains a per-symbol fallback for instance bytes; it is no longer on the critical path.

### 9.4 Still open
- **Datacenter reachability** of `api.bseindia.com` + `www.bseindia.com/XBRLFILES` from a GitHub-hosted runner (§8 throwaway workflow) — the only remaining Phase-0 gate.
- XBRL-attachment lag vs the PDF (fresh filings may briefly have no XMLName): incremental state must keep a symbol pending until its XMLName appears, not mark it done off the PDF row.
- `.xml`-twin existence should be verified across sectors (bank/NBFC/insurance variants of the IFIndAs path) during Phase 1's first 50-symbol run.
