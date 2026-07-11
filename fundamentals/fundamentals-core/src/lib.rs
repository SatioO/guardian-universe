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
//! - Insurance fixtures (SBILIFE/ICICIGI, ~1.1 MB) were not copied to keep the
//!   fixture set minimal; their tests are omitted. The insurance builder code
//!   itself is retained unchanged.
//! - NEW module [`instance`]: derives per-instance metadata (ISIN, symbol,
//!   period dates, basis, audited flag, sector fingerprint, context durations)
//!   from the XBRL itself — needed by the BSE flow where the discovery feed's
//!   `quarter_code` grammar is unconfirmed (SOURCE-CONTRACT.md §1.1).

pub mod instance;
pub mod types;
pub mod xbrl_integrated;

pub use types::*;
