//! Run orchestration: universe seed → floor partition → canary → registry
//! discovery → selection → throttled instance fetch → fundamentals-core parse
//! → Gate-1 (blocks + flags) → merge → deterministic parquet + state.
//!
//! Incrementality: a (filing, document) is fetched at most once — its
//! `FilingRef::dedup_key()` is recorded in state after processing. A re-run
//! whose discovery shows nothing new fetches nothing and leaves both outputs
//! byte-identical (the Phase-1 idempotency milestone). We diff on the
//! discovery tuple (scrip, quarter_code, basis, locator) rather than parsing
//! `quarter_code` into dates — its grammar is unconfirmed (SOURCE-CONTRACT
//! §1.1); real `(period_end, basis)` is read from each instance post-fetch
//! and recorded per symbol as `last_period_end`/`last_basis`.
//!
//! Phase-2 additions:
//! - **Full universe:** floor-based coverage (enter ≥ `min_mktcap_cr`, stay
//!   until `exit_mktcap_cr` for previously covered symbols) instead of top-N.
//!   `previously_covered` derives from the accumulated parquet itself, so
//!   hysteresis survives ephemeral runners even without state.
//! - **Canary (SOURCE-CONTRACT §12):** the scrip-master/EQUITY_L seeds must
//!   look sane, and during a filing-season window bulk discovery must return
//!   rows — a source outage is a hard red, never a silent green. A quiet
//!   no-filings day OUTSIDE filing season is a clean no-op success.
//! - **Self-healing merge:** re-processing a filing whose data is unchanged
//!   keeps the existing row (original `as_of`), so a lost state file costs
//!   one polite re-fetch cycle, never output churn.
//! - **Per-symbol failure isolation:** one bad symbol logs + skips; only
//!   ALL selected filings failing (fetch+parse) fails the run.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use fundamentals_core::instance::extract_instance_info;
use fundamentals_core::xbrl_integrated::{meta_from_iso_period_end, parse_integrated_xbrl};
use fundamentals_core::{SectorKind, StatementBasis};

use crate::bse::BseFilingSource;
use crate::gate::gate1;
use crate::http::PoliteClient;
use crate::output::{read_parquet, sort_rows, write_parquet};
use crate::rows::{build_row, derive_ttm_eps, FundRow};
use crate::source::{DiscoveryWindow, FilingRef, SourceRegistry};
use crate::state::ProducerState;
use crate::universe::{seed_universe, Universe};

/// Output filenames. `fundamentals_all.parquet` matches the Python pipeline's
/// `{file_prefix}_*.parquet` manifest glob (the `instruments_all` /
/// `sector_industry_all` convention); the state file carries its release-asset
/// name so the workflow can upload/download it verbatim.
pub const PARQUET_NAME: &str = "fundamentals_all.parquet";
pub const STATE_NAME: &str = "fundamentals_state.json";

pub struct RunConfig {
    /// Coverage entry floor, ₹ crore (design: 800).
    pub min_mktcap_cr: f64,
    /// Hysteresis exit floor, ₹ crore (design: 720). Must be ≤ entry floor.
    pub exit_mktcap_cr: f64,
    /// Optional cap on the covered set (mcap-descending) — smoke runs only.
    pub limit: Option<usize>,
    pub out_dir: PathBuf,
    pub window: DiscoveryWindow,
    pub throttle_ms: u64,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RunSummary {
    pub universe_union: usize,
    pub universe_nse_only: usize,
    /// Coverage disclosure (the run summary's headline numbers).
    pub covered: usize,
    pub below_floor: usize,
    pub unrankable_no_mcap: usize,
    pub hysteresis_retained: usize,
    pub discovered_refs: usize,
    pub refs_in_universe: usize,
    pub selected_filings: usize,
    pub already_processed: usize,
    pub pending_no_xml: usize,
    pub fetched: usize,
    pub fetch_errors: Vec<String>,
    pub parse_errors: Vec<String>,
    pub skipped_non_general: Vec<String>,
    pub gate_blocked: Vec<String>,
    pub rows_flagged: usize,
    pub rows_new_or_updated: usize,
    pub rows_total: usize,
    pub parquet_written: bool,
    pub http_requests: u64,
    #[serde(skip)]
    pub wall: std::time::Duration,
}

/// SOURCE-CONTRACT §12 filing-season window: Jan/Apr/Jul/Oct, 5th–31st.
/// During it, an empty bulk-discovery response is an outage, not a quiet day.
pub fn in_filing_season(iso_date: &str) -> bool {
    let month = iso_date.get(5..7).and_then(|m| m.parse::<u32>().ok());
    let day = iso_date.get(8..10).and_then(|d| d.parse::<u32>().ok());
    matches!((month, day), (Some(m), Some(d)) if matches!(m, 1 | 4 | 7 | 10) && (5..=31).contains(&d))
}

/// Build the default source registry. This is the ONE place concrete source
/// types appear (the registration site) — everywhere else resolves through
/// the registry. A future premium provider is registered here (before or
/// after BSE to set fallback order) and nothing else changes.
pub fn default_registry(client: Arc<PoliteClient>, universe: &Universe) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry.register(Box::new(BseFilingSource::new(
        client,
        universe.scrip_to_isin(),
    )));
    registry
}

pub fn run(cfg: &RunConfig) -> Result<RunSummary, String> {
    let started = Instant::now();
    let mut summary = RunSummary::default();

    if cfg.exit_mktcap_cr > cfg.min_mktcap_cr {
        return Err(format!(
            "exit floor ({}) must be ≤ entry floor ({})",
            cfg.exit_mktcap_cr, cfg.min_mktcap_cr
        ));
    }

    std::fs::create_dir_all(&cfg.out_dir)
        .map_err(|e| format!("create out dir {}: {e}", cfg.out_dir.display()))?;
    let parquet_path = cfg.out_dir.join(PARQUET_NAME);
    let state_path = cfg.out_dir.join(STATE_NAME);

    let mut state = ProducerState::load(&state_path)?;
    let client = Arc::new(PoliteClient::new(cfg.throttle_ms));

    // The accumulated parquet doubles as the coverage memory: a symbol with
    // published rows was covered before, which is exactly what hysteresis
    // needs — and it survives ephemeral runners via the release sync.
    let existing = read_parquet(&parquet_path)?;
    let previously_covered: HashSet<String> =
        existing.iter().map(|r| r.instrument_key.clone()).collect();

    // ── 1. Universe seed (2 bulk requests) + canary ───────────────────────────
    eprintln!("[1/5] seeding universe (BSE scrip master ∪ NSE EQUITY_L)…");
    let universe = seed_universe(&client)?;
    summary.universe_union = universe.entries.len();
    summary.universe_nse_only = universe.nse_only_count;

    // Canary (§12.2/§12.4): a truncated/empty seed means every downstream
    // count silently collapses — refuse to run rather than "succeed" small.
    if universe.bse_count < 3000 {
        return Err(format!(
            "canary: BSE scrip master returned only {} equity rows (expect ~4,600; floor 3,000) — aborting",
            universe.bse_count
        ));
    }
    if universe.nse_count < 1500 {
        return Err(format!(
            "canary: NSE EQUITY_L returned only {} EQ rows (expect ~2,400; floor 1,500) — aborting",
            universe.nse_count
        ));
    }

    let partition =
        universe.partition_by_floor(cfg.min_mktcap_cr, cfg.exit_mktcap_cr, &previously_covered);
    let mut covered = partition.covered;
    if let Some(limit) = cfg.limit {
        covered.truncate(limit);
    }
    summary.covered = covered.len();
    summary.below_floor = partition.below_floor;
    summary.unrankable_no_mcap = partition.unrankable;
    summary.hysteresis_retained = partition.hysteresis_retained;

    let covered_keys: HashSet<&str> = covered.iter().map(|e| e.instrument_key.as_str()).collect();
    let mcap_by_key: HashMap<&str, f64> = covered
        .iter()
        .filter_map(|e| e.mktcap_cr.map(|m| (e.instrument_key.as_str(), m)))
        .collect();
    let symbol_by_key: HashMap<&str, String> = covered
        .iter()
        .map(|e| {
            let sym = e
                .nse_symbol
                .clone()
                .or_else(|| e.bse_symbol.clone())
                .unwrap_or_else(|| e.instrument_key.clone());
            (e.instrument_key.as_str(), sym)
        })
        .collect();
    eprintln!(
        "      bse={} nse={} union={} (nse_only={}) | covered={} (floor {} cr, exit {} cr, hysteresis-retained {}) below-floor={} unrankable={}",
        universe.bse_count,
        universe.nse_count,
        summary.universe_union,
        summary.universe_nse_only,
        summary.covered,
        cfg.min_mktcap_cr,
        cfg.exit_mktcap_cr,
        summary.hysteresis_retained,
        summary.below_floor,
        summary.unrankable_no_mcap,
    );

    // ── 2. Discovery via the registry chain + filing-season watchdog ─────────
    let registry = default_registry(client.clone(), &universe);
    eprintln!(
        "[2/5] discovering filings via registry chain {:?} (window {:?})…",
        registry.ids(),
        cfg.window
    );
    let (served_by, refs) = registry.discover(cfg.window)?;
    summary.discovered_refs = refs.len();
    eprintln!("      {} refs discovered via '{served_by}'", refs.len());

    let today = today_iso();
    if refs.is_empty() && in_filing_season(&today) {
        // §12.1 watchdog: zero filings in a filing-season window is an outage
        // (or a schema drift that parses to nothing), never a quiet day.
        return Err(format!(
            "canary: bulk discovery returned 0 filings during filing season ({today}) — aborting"
        ));
    }

    // ── 3. Filter to the covered universe + select one document per filing ───
    let in_universe: Vec<FilingRef> = refs
        .into_iter()
        .filter(|r| {
            r.instrument_key
                .as_deref()
                .map(|k| covered_keys.contains(k))
                .unwrap_or(false)
        })
        .collect();
    summary.refs_in_universe = in_universe.len();

    // Group by (issuer, period tag); prefer Consolidated, newest broadcast.
    let mut by_filing: BTreeMap<String, Vec<FilingRef>> = BTreeMap::new();
    for r in in_universe {
        by_filing.entry(r.filing_key()).or_default().push(r);
    }

    let mut chosen: Vec<FilingRef> = Vec::new();
    for (filing_key, group) in &by_filing {
        let with_locator = |basis: StatementBasis| {
            group
                .iter()
                .filter(|r| r.instance_locator.is_some() && r.basis_hint == Some(basis))
                .max_by(|a, b| a.broadcast_at.cmp(&b.broadcast_at))
        };
        let pick = with_locator(StatementBasis::Consolidated)
            .or_else(|| with_locator(StatementBasis::Standalone))
            .or_else(|| group.iter().find(|r| r.instance_locator.is_some()));

        match pick {
            Some(r) => {
                // A locator arrived → clear any pending marker for this filing.
                if let Some(key) = r.instrument_key.as_deref() {
                    state.symbol_mut(key).pending_xml.remove(filing_key);
                }
                chosen.push(r.clone());
            }
            None => {
                // Broadcast without XBRL yet → PENDING, never done.
                if let Some(r) = group.first() {
                    if let Some(key) = r.instrument_key.as_deref() {
                        state.symbol_mut(key).pending_xml.insert(filing_key.clone());
                        summary.pending_no_xml += 1;
                        eprintln!(
                            "      pending (no XMLName yet): {} {} [{}]",
                            r.company_name, r.period_hint, filing_key
                        );
                    }
                }
            }
        }
    }
    summary.selected_filings = chosen.len();

    // Never fetch the same document twice in one run (an MQ and an MC row can
    // reference the same consolidated document).
    let mut seen_locators: HashSet<String> = HashSet::new();
    chosen.retain(|r| {
        r.instance_locator
            .as_ref()
            .map(|l| seen_locators.insert(l.clone()))
            .unwrap_or(false)
    });

    // Incremental: drop documents already processed in an earlier run.
    let mut to_fetch: Vec<FilingRef> = Vec::new();
    for r in chosen {
        let key = r.instrument_key.clone().unwrap_or_default();
        if state.is_processed(&key, &r.dedup_key()) {
            summary.already_processed += 1;
        } else {
            to_fetch.push(r);
        }
    }
    eprintln!(
        "[3/5] {} filings selected, {} already processed, {} to fetch, {} pending",
        summary.selected_filings, summary.already_processed, to_fetch.len(), summary.pending_no_xml
    );

    // ── 4. Fetch + parse + gate (per-symbol isolation: any single filing's
    // failure logs + skips; the loop always continues) ────────────────────────
    let to_fetch_count = to_fetch.len();
    let mut new_rows: Vec<FundRow> = Vec::new();
    for (i, r) in to_fetch.iter().enumerate() {
        let key = r.instrument_key.clone().unwrap_or_default();
        let source = registry
            .resolve(&r.source_id)
            .ok_or_else(|| format!("no registered source with id '{}'", r.source_id))?;
        eprintln!(
            "[4/5] ({}/{}) {} {} [{}]…",
            i + 1,
            to_fetch.len(),
            r.company_name,
            r.period_hint,
            r.source_id
        );

        let bytes = match source.fetch_instance(r) {
            Ok(b) => b,
            Err(e) => {
                // Transient by definition — NOT marked processed; retried next run.
                summary.fetch_errors.push(format!("{}: {e}", r.company_name));
                continue;
            }
        };
        summary.fetched += 1;

        let xml = String::from_utf8_lossy(&bytes).into_owned();
        let info = match extract_instance_info(&xml) {
            Ok(info) => info,
            Err(e) => {
                summary.parse_errors.push(format!("{}: {e}", r.company_name));
                state.symbol_mut(&key).processed.insert(r.dedup_key());
                continue;
            }
        };

        // Cross-check the instance's own ISIN against the universe key.
        if let Some(instance_isin) = info.isin.as_deref() {
            if instance_isin != key {
                summary.parse_errors.push(format!(
                    "{}: instance ISIN {} != universe key {} — skipped",
                    r.company_name, instance_isin, key
                ));
                state.symbol_mut(&key).processed.insert(r.dedup_key());
                continue;
            }
        }

        // Phase-1 scope: general sector only (design D6). Bank / NBFC /
        // insurance instances are flagged + skipped, never mis-parsed.
        let sector = info.sector_kind;
        if sector != Some(SectorKind::General) {
            summary.skipped_non_general.push(format!(
                "{} [{}]",
                r.company_name,
                sector.map(|s| s.as_str()).unwrap_or("unclassified")
            ));
            state.symbol_mut(&key).processed.insert(r.dedup_key());
            continue;
        }
        let sector = SectorKind::General;

        let Some(period_end) = info.quarter_end.clone().or_else(|| info.fy_end.clone()) else {
            summary
                .parse_errors
                .push(format!("{}: no OneD/FourD context dates", r.company_name));
            state.symbol_mut(&key).processed.insert(r.dedup_key());
            continue;
        };
        let basis = info.basis.or(r.basis_hint).unwrap_or_default();
        let meta = meta_from_iso_period_end(&period_end, info.is_audited, sector, basis);

        let (quarter, annual, val) = match parse_integrated_xbrl(&xml, &meta) {
            Ok(v) => v,
            Err(e) => {
                summary.parse_errors.push(format!("{}: {e}", r.company_name));
                state.symbol_mut(&key).processed.insert(r.dedup_key());
                continue;
            }
        };

        let symbol = info
            .symbol
            .clone()
            .or_else(|| symbol_by_key.get(key.as_str()).cloned())
            .unwrap_or_else(|| key.clone());
        let mcap = mcap_by_key.get(key.as_str()).copied();

        let mut candidate_rows: Vec<(FundRow, Option<i64>)> = Vec::new();
        if let Some(q) = &quarter {
            candidate_rows.push((
                build_row(&key, &symbol, sector, q, &val, &r.source_id, &today),
                info.quarter_duration_days(),
            ));
        }
        // The FourD context is YTD in non-Q4 filings; only a ~full-year
        // duration is a real annual. (YTD rows are a Phase-2 concern.)
        if let Some(a) = &annual {
            let fy_days = info.fy_duration_days();
            if matches!(fy_days, Some(d) if (350..=380).contains(&d)) {
                candidate_rows.push((
                    build_row(&key, &symbol, sector, a, &val, &r.source_id, &today),
                    fy_days,
                ));
            }
        }

        for (mut row, duration) in candidate_rows {
            let outcome = gate1(&row, duration, mcap);
            if outcome.blocks.is_empty() {
                if !outcome.flags.is_empty() {
                    row.dq_flags = outcome.flags.join(";");
                    summary.rows_flagged += 1;
                    eprintln!(
                        "      flagged (published): {} {} {}: {}",
                        symbol, row.period_end, row.fiscal_quarter, row.dq_flags
                    );
                }
                let sym_state = state.symbol_mut(&key);
                if sym_state.last_period_end.as_deref().unwrap_or("") < row.period_end.as_str() {
                    sym_state.last_period_end = Some(row.period_end.clone());
                    sym_state.last_basis = Some(row.basis.clone());
                }
                new_rows.push(row);
            } else {
                let reasons: Vec<String> = outcome.blocks.iter().map(|b| b.reason()).collect();
                summary.gate_blocked.push(format!(
                    "{} {} {}: {}",
                    symbol,
                    row.period_end,
                    row.fiscal_quarter,
                    reasons.join(",")
                ));
            }
        }

        state.symbol_mut(&key).processed.insert(r.dedup_key());
    }

    // ALL-failed tripwire: per-symbol isolation must never hide a systemic
    // outage. If every selected filing hard-failed (fetch or parse), the
    // source/parser is broken — red the run.
    let hard_failures = summary.fetch_errors.len() + summary.parse_errors.len();
    if to_fetch_count > 0 && hard_failures >= to_fetch_count {
        return Err(format!(
            "all {to_fetch_count} selected filings failed ({} fetch, {} parse) — systemic failure",
            summary.fetch_errors.len(),
            summary.parse_errors.len()
        ));
    }

    // ── 5. Merge + derive + write (only if changed) ──────────────────────────
    eprintln!("[5/5] merging {} new rows…", new_rows.len());
    let (mut all_rows, new_or_updated) = merge_rows(&existing, new_rows);
    summary.rows_new_or_updated = new_or_updated;
    derive_ttm_eps(&mut all_rows);
    sort_rows(&mut all_rows);
    summary.rows_total = all_rows.len();

    let mut sorted_existing = existing;
    sort_rows(&mut sorted_existing);
    if all_rows != sorted_existing {
        write_parquet(&parquet_path, &all_rows)?;
        summary.parquet_written = true;
    } else {
        eprintln!("      no row changes — parquet left untouched (idempotent)");
    }
    state.save(&state_path)?;

    summary.http_requests = client.request_count();
    summary.wall = started.elapsed();
    Ok(summary)
}

/// Merge new rows over the accumulated set. Existing rows are never dropped
/// (the publish shrink-guard's structural guarantee: fundamentals row counts
/// only grow; restatements replace in place). A new row whose DATA matches
/// the existing one (ignoring `as_of`/derived `ttm_eps`) keeps the existing
/// row untouched — re-processing after state loss is byte-idempotent.
/// Returns (merged rows, count actually new or updated).
fn merge_rows(existing: &[FundRow], new_rows: Vec<FundRow>) -> (Vec<FundRow>, usize) {
    let mut merged: BTreeMap<(String, String, String, String), FundRow> =
        existing.iter().map(|r| (r.key(), r.clone())).collect();
    let mut new_or_updated = 0usize;
    for row in new_rows {
        match merged.get(&row.key()) {
            Some(prev) if prev.same_data(&row) => {}
            _ => {
                new_or_updated += 1;
                merged.insert(row.key(), row);
            }
        }
    }
    (merged.into_values().collect(), new_or_updated)
}

fn today_iso() -> String {
    // Days since epoch → civil date (inverse of the core crate's days_from_civil).
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let z = secs.div_euclid(86_400) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn today_iso_is_plausible() {
        let t = today_iso();
        assert_eq!(t.len(), 10);
        assert!(t.starts_with("20"));
        // Round-trip through the core date math.
        assert_eq!(
            fundamentals_core::instance::duration_days_inclusive(&t, &t),
            Some(1)
        );
    }

    #[test]
    fn filing_season_window_matches_source_contract_s12() {
        // Jan/Apr/Jul/Oct, 5th–31st.
        assert!(in_filing_season("2026-07-12"));
        assert!(in_filing_season("2026-01-05"));
        assert!(in_filing_season("2026-10-31"));
        assert!(!in_filing_season("2026-07-04"), "before the 5th is grace");
        assert!(!in_filing_season("2026-06-15"), "off-season month");
        assert!(!in_filing_season("2026-12-25"));
        assert!(!in_filing_season("garbage"));
    }

    fn row(pe: &str, fq: &str, np: Option<f64>, as_of: &str) -> crate::rows::FundRow {
        crate::rows::FundRow {
            instrument_key: "INE000A01001".into(),
            symbol: "TEST".into(),
            period_end: pe.into(),
            fiscal_quarter: fq.into(),
            basis: "consolidated".into(),
            is_restated: false,
            sector_kind: "general".into(),
            revenue: Some(100.0),
            operating_profit: None,
            opm_pct: None,
            margin_kind: "opm".into(),
            other_income: None,
            interest: None,
            depreciation: None,
            pbt: None,
            tax: None,
            net_profit: np,
            eps: None,
            equity: None,
            total_debt: None,
            cash: None,
            shares_outstanding: None,
            face_value: None,
            ebitda_annual: None,
            capital_employed: None,
            ttm_eps: None,
            book_value_per_share: None,
            as_of: as_of.into(),
            source_channel: "test".into(),
            fields_resolved_pct: 1.0,
            dq_flags: String::new(),
            is_audited: true,
        }
    }

    #[test]
    fn merge_preserves_existing_row_when_data_unchanged() {
        // State loss → re-process produces the same data with a NEW as_of;
        // the merge must keep the original row so bytes never churn.
        let existing = vec![row("2026-03-31", "Q4", Some(10.0), "2026-05-01")];
        let refetched = vec![row("2026-03-31", "Q4", Some(10.0), "2026-07-12")];
        let (merged, n) = merge_rows(&existing, refetched);
        assert_eq!(n, 0, "unchanged data is not an update");
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].as_of, "2026-05-01", "original as_of preserved");
    }

    #[test]
    fn merge_replaces_on_restatement_and_appends_new_periods() {
        let existing = vec![row("2026-03-31", "Q4", Some(10.0), "2026-05-01")];
        let incoming = vec![
            row("2026-03-31", "Q4", Some(12.5), "2026-07-12"), // restated value
            row("2026-06-30", "Q1", Some(3.0), "2026-07-12"),  // new quarter
        ];
        let (merged, n) = merge_rows(&existing, incoming);
        assert_eq!(n, 2);
        assert_eq!(merged.len(), 2, "restatement replaces in place; rows never shrink");
        let q4 = merged.iter().find(|r| r.fiscal_quarter == "Q4").unwrap();
        assert_eq!(q4.net_profit, Some(12.5));
        assert_eq!(q4.as_of, "2026-07-12");
    }

    #[test]
    fn merge_never_drops_existing_rows() {
        // The shrink-guard's structural guarantee.
        let existing = vec![
            row("2025-12-31", "Q3", Some(1.0), "2026-02-01"),
            row("2026-03-31", "Q4", Some(2.0), "2026-05-01"),
        ];
        let (merged, _) = merge_rows(&existing, vec![]);
        assert_eq!(merged.len(), existing.len());
    }
}
