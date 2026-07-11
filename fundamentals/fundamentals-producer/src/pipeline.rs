//! Run orchestration: universe seed → registry discovery → selection →
//! throttled instance fetch → fundamentals-core parse → Gate-1 → merge →
//! deterministic parquet + state.
//!
//! Incrementality: a (filing, document) is fetched at most once — its
//! `FilingRef::dedup_key()` is recorded in state after processing. A re-run
//! whose discovery shows nothing new fetches nothing and leaves both outputs
//! byte-identical (the Phase-1 idempotency milestone). We diff on the
//! discovery tuple (scrip, quarter_code, basis, locator) rather than parsing
//! `quarter_code` into dates — its grammar is unconfirmed (SOURCE-CONTRACT
//! §1.1); real `(period_end, basis)` is read from each instance post-fetch
//! and recorded per symbol as `last_period_end`/`last_basis`.

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

pub struct RunConfig {
    pub top: usize,
    pub out_dir: PathBuf,
    pub window: DiscoveryWindow,
    pub throttle_ms: u64,
}

#[derive(Debug, Default)]
pub struct RunSummary {
    pub universe_union: usize,
    pub universe_nse_only: usize,
    pub top_n: usize,
    pub discovered_refs: usize,
    pub refs_in_top_n: usize,
    pub selected_filings: usize,
    pub already_processed: usize,
    pub pending_no_xml: usize,
    pub fetched: usize,
    pub fetch_errors: Vec<String>,
    pub parse_errors: Vec<String>,
    pub skipped_non_general: Vec<String>,
    pub gate_blocked: Vec<String>,
    pub rows_new_or_updated: usize,
    pub rows_total: usize,
    pub parquet_written: bool,
    pub http_requests: u64,
    pub wall: std::time::Duration,
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

    std::fs::create_dir_all(&cfg.out_dir)
        .map_err(|e| format!("create out dir {}: {e}", cfg.out_dir.display()))?;
    let parquet_path = cfg.out_dir.join("fundamentals.parquet");
    let state_path = cfg.out_dir.join("state.json");

    let mut state = ProducerState::load(&state_path)?;
    let client = Arc::new(PoliteClient::new(cfg.throttle_ms));

    // ── 1. Universe seed (2 bulk requests) ────────────────────────────────────
    eprintln!("[1/5] seeding universe (BSE scrip master ∪ NSE EQUITY_L)…");
    let universe = seed_universe(&client)?;
    summary.universe_union = universe.entries.len();
    summary.universe_nse_only = universe.nse_only_count;
    let top = universe.top_by_mktcap(cfg.top);
    summary.top_n = top.len();
    let top_keys: HashSet<&str> = top.iter().map(|e| e.instrument_key.as_str()).collect();
    let mcap_by_key: HashMap<&str, f64> = top
        .iter()
        .filter_map(|e| e.mktcap_cr.map(|m| (e.instrument_key.as_str(), m)))
        .collect();
    let symbol_by_key: HashMap<&str, String> = top
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
        "      bse={} nse={} union={} (nse_only={}), top-{} by Mktcap",
        universe.bse_count,
        universe.nse_count,
        summary.universe_union,
        summary.universe_nse_only,
        summary.top_n
    );

    // ── 2. Discovery via the registry chain ───────────────────────────────────
    let registry = default_registry(client.clone(), &universe);
    eprintln!(
        "[2/5] discovering filings via registry chain {:?} (window {:?})…",
        registry.ids(),
        cfg.window
    );
    let (served_by, refs) = registry.discover(cfg.window)?;
    summary.discovered_refs = refs.len();
    eprintln!("      {} refs discovered via '{served_by}'", refs.len());

    // ── 3. Filter to top-N + select one document per (issuer, period) ────────
    let in_top: Vec<FilingRef> = refs
        .into_iter()
        .filter(|r| {
            r.instrument_key
                .as_deref()
                .map(|k| top_keys.contains(k))
                .unwrap_or(false)
        })
        .collect();
    summary.refs_in_top_n = in_top.len();

    // Group by (issuer, period tag); prefer Consolidated, newest broadcast.
    let mut by_filing: BTreeMap<String, Vec<FilingRef>> = BTreeMap::new();
    for r in in_top {
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

    // ── 4. Fetch + parse + gate ───────────────────────────────────────────────
    let today = today_iso();
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

        for (row, duration) in candidate_rows {
            let blocks = gate1(&row, duration, mcap);
            if blocks.is_empty() {
                let sym_state = state.symbol_mut(&key);
                if sym_state.last_period_end.as_deref().unwrap_or("") < row.period_end.as_str() {
                    sym_state.last_period_end = Some(row.period_end.clone());
                    sym_state.last_basis = Some(row.basis.clone());
                }
                new_rows.push(row);
            } else {
                let reasons: Vec<String> = blocks.iter().map(|b| b.reason()).collect();
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

    // ── 5. Merge + derive + write (only if changed) ──────────────────────────
    eprintln!("[5/5] merging {} new rows…", new_rows.len());
    let existing = read_parquet(&parquet_path)?;
    let mut merged: BTreeMap<(String, String, String, String), FundRow> =
        existing.iter().map(|r| (r.key(), r.clone())).collect();
    summary.rows_new_or_updated = new_rows.len();
    for row in new_rows {
        merged.insert(row.key(), row);
    }
    let mut all_rows: Vec<FundRow> = merged.into_values().collect();
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
}
