//! # fundamentals-core
//!
//! Fetch-agnostic core of the fundamentals pipeline: SEBI `in-capmkt` XBRL
//! parsing (Integrated Filing, IFIndAs), context selection (`OneD` quarter /
//! `FourD` full-FY / `OneI` instant balance sheet), standalone-vs-consolidated
//! basis handling, per-sector fact extraction (general / bank / NBFC /
//! insurance) and balance-sheet valuation inputs.
//!
//! ## Provenance
//! Extracted from the traderview app `src-tauri/src/fundamentals/`
//! (`mod.rs` types + `xbrl_integrated.rs` parser) @ commit
//! `ed0eddb6d9541e383844382666f5a5c0d18cef5e` (traderview repo,
//! 2026-06-28). Surgery performed during extraction:
//! - Dropped the app-transport types (`FundamentalsDoc`, cache, decode,
//!   registry, providers) and all Tauri/reqwest-dependent code — this crate
//!   parses bytes it is handed; it never fetches.
//! - `fetch_integrated()` (NSE cookie-primed fetch) removed; the pure
//!   `select_integrated_filings()` list-selection and `parse_integrated_xbrl()`
//!   are kept verbatim (fixture tests included).
//! - Insurance fixtures (SBILIFE/ICICIGI, ~1.1 MB) were initially omitted;
//!   Phase 3 restored both fixtures and their tests (D6: per-sector element
//!   names are confirmed by fixture before rows ship unflagged).
//! - Phase-3 divergence from "vendored verbatim": the bank builder (a) reads
//!   NPA ratios as XBRL percentItemType FRACTIONS and scales x100 (verified
//!   on six live 2026 IFBanking instances; the app's raw-percent reading was
//!   never exercised — its fixture holds only 0-placeholders), (b) treats
//!   literal-0 NPA values as unfilled placeholders (→ None), (c) extracts
//!   `PercentageOfNpa` as `net_npa_pct`, and (d) drops post-scale values
//!   above 50% as implausible (see `build_bank`). The insurance builder maps
//!   core net_profit/pbt/tax to the TRUE shareholder P&L elements
//!   (life: `ProfitLossAfterTaxAndExtraordinaryItems`; general:
//!   `ProfitLossAfterTax`/`ProfitOrLossBeforeTax`/`ProvisionForTax`) instead
//!   of the surplus-transfer/operating-profit lines (see
//!   `build_insurance_general`). Both worth upstreaming to the app.
//! - NEW module [`instance`]: derives per-instance metadata (ISIN, symbol,
//!   period dates, basis, audited flag, sector fingerprint, context durations)
//!   from the XBRL itself — needed by the BSE flow where the discovery feed's
//!   `quarter_code` grammar is unconfirmed (SOURCE-CONTRACT.md §1.1).

pub mod instance;
pub mod types;
pub mod xbrl_integrated;

pub use types::*;
