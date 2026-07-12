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
use fundamentals_core::StatementBasis;

use crate::bse::BseFilingSource;
use crate::gate::gate1;
use crate::http::PoliteClient;
use crate::output::{read_parquet, sort_rows, write_parquet};
use crate::rows::{build_row, derive_ttm_eps, FundRow};
use crate::source::{DiscoveryWindow, FilingRef, SourceRegistry};
use crate::state::{Outcome, ProducerState};
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
    /// Phase 3: bank/NBFC/insurance are parsed; the only sector skip left is
    /// "no recognisable sector fingerprint" — still never guessed (D6).
    pub skipped_unclassified: Vec<String>,
    /// Rows published under the universe key despite an instance-ISIN
    /// mismatch (scrip-anchored; both ISINs recorded in the row's dq_flags).
    pub identity_flagged: Vec<String>,
    pub gate_blocked: Vec<String>,
    /// New rows per sector_kind this run (coverage disclosure).
    pub rows_by_sector: BTreeMap<String, usize>,
    /// Processed entries invalidated by a state-schema migration this run.
    pub state_migration_invalidated: usize,
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

/// Normalize an ISIN for comparison: uppercase + letter-O → digit-0. The
/// instance's ISIN fact is hand-typed in practice; O↔0 is the observed
/// confusion class (first full run: `INEOFHS…` vs `INE0FHS…`, `INEONT9…` vs
/// `INE0NT9…`). Both sides get the same mapping, so two ISINs compare equal
/// after normalization only when they differ exactly by O↔0 — the case we
/// want to accept. (Two distinct real securities differing only by O↔0 in
/// the same position do not occur; the check digit would differ too.)
fn normalize_isin(isin: &str) -> String {
    isin.trim().to_ascii_uppercase().replace('O', "0")
}

/// Decide what to do when the instance's own ISIN disagrees with the
/// universe key. Identity is ANCHORED by the BSE scrip code at discovery —
/// the instance ISIN is a cross-check, not the primary key.
///
/// Returns:
/// - `Ok(None)` — identities agree (raw or after O↔0 normalization): clean.
/// - `Ok(Some(flag))` — residual mismatch, but the instance's own ScripCode
///   fact matches the discovery scrip AND both ISINs share the 9-char issuer
///   prefix (the observed corporate-action pattern: same issuer, new
///   security serial + check digit — e.g. `INE745G01035` → `INE745G01043`).
///   Publish under the universe key, carrying both ISINs in `dq_flags`.
/// - `Err(reason)` — unanchorable (scrip mismatch/absent, or different
///   issuer): skip, never publish a row we cannot anchor (D6 honesty).
fn resolve_identity(
    instance_isin: Option<&str>,
    universe_key: &str,
    instance_scrip: Option<&str>,
    native_id: &str,
) -> Result<Option<String>, String> {
    let Some(instance_isin) = instance_isin else {
        return Ok(None); // no ISIN fact → nothing to cross-check (as before)
    };
    if instance_isin == universe_key {
        return Ok(None);
    }
    let norm_instance = normalize_isin(instance_isin);
    let norm_universe = normalize_isin(universe_key);
    if norm_instance == norm_universe {
        return Ok(None); // obvious O↔0 typo in the filing's ISIN fact
    }
    let scrip_anchored = instance_scrip.map(|s| s.trim() == native_id.trim()).unwrap_or(false);
    let same_issuer = norm_instance.len() >= 9
        && norm_universe.len() >= 9
        && norm_instance[..9] == norm_universe[..9];
    if scrip_anchored && same_issuer {
        return Ok(Some(format!(
            "identity_isin_mismatch(instance={instance_isin},universe={universe_key})"
        )));
    }
    Err(format!(
        "instance ISIN {instance_isin} != universe key {universe_key} \
         (scrip_anchored={scrip_anchored}, same_issuer={same_issuer}) — skipped"
    ))
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

    // Phase-3 state migration (v1 → v2): invalidate exactly the processed
    // entries of symbols with NO published rows — the D6 non-general skips
    // plus the identity/parse skips — so this run re-ingests that backlog
    // without re-fetching a single already-published general filing.
    summary.state_migration_invalidated = state.migrate(&previously_covered);
    if summary.state_migration_invalidated > 0 {
        eprintln!(
            "      state migrated to v{}: {} processed entries invalidated for re-ingest",
            crate::state::STATE_VERSION,
            summary.state_migration_invalidated
        );
    }

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
                state.symbol_mut(&key).record(&r.dedup_key(), Outcome::ParseError);
                continue;
            }
        };

        // Cross-check the instance's own ISIN against the universe key.
        // Identity is anchored by the BSE scrip code at discovery; the ISIN
        // fact inside the instance is issuer-typed free text in practice
        // (first-run findings: O↔0 typos, stale pre-corporate-action ISINs).
        let identity_flag = match resolve_identity(
            info.isin.as_deref(),
            &key,
            info.scrip_code.as_deref(),
            &r.native_id,
        ) {
            Ok(flag) => flag,
            Err(reason) => {
                summary.parse_errors.push(format!("{}: {reason}", r.company_name));
                state.symbol_mut(&key).record(&r.dedup_key(), Outcome::IdentityMismatch);
                continue;
            }
        };
        if let Some(flag) = &identity_flag {
            summary
                .identity_flagged
                .push(format!("{} {}", r.company_name, flag));
        }

        // Phase 3: bank / NBFC / insurance route through the app's vendored
        // per-sector builders. The ONLY remaining sector skip is "no
        // recognisable fingerprint" — never guessed (design D6).
        let Some(sector) = info.sector_kind else {
            summary
                .skipped_unclassified
                .push(r.company_name.clone());
            state
                .symbol_mut(&key)
                .record(&r.dedup_key(), Outcome::SkippedUnclassified);
            continue;
        };

        let Some(period_end) = info.quarter_end.clone().or_else(|| info.fy_end.clone()) else {
            summary
                .parse_errors
                .push(format!("{}: no OneD/FourD context dates", r.company_name));
            state.symbol_mut(&key).record(&r.dedup_key(), Outcome::ParseError);
            continue;
        };
        let basis = info.basis.or(r.basis_hint).unwrap_or_default();
        let meta = meta_from_iso_period_end(&period_end, info.is_audited, sector, basis);

        let (quarter, annual, val) = match parse_integrated_xbrl(&xml, &meta) {
            Ok(v) => v,
            Err(e) => {
                summary.parse_errors.push(format!("{}: {e}", r.company_name));
                state.symbol_mut(&key).record(&r.dedup_key(), Outcome::ParseError);
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

        let mut published_any = false;
        for (mut row, duration) in candidate_rows {
            let outcome = gate1(&row, duration, mcap);
            if outcome.blocks.is_empty() {
                // Row-level flags = Gate-1/Gate-3 flags + the identity flag
                // (both ISINs recorded in-band so every consumer sees them).
                let mut flags = outcome.flags;
                if let Some(f) = &identity_flag {
                    flags.push(f.clone());
                    flags.sort();
                    flags.dedup();
                }
                if !flags.is_empty() {
                    row.dq_flags = flags.join(";");
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
                *summary.rows_by_sector.entry(row.sector_kind.clone()).or_default() += 1;
                published_any = true;
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

        state.symbol_mut(&key).record(
            &r.dedup_key(),
            if published_any { Outcome::Published } else { Outcome::GateBlocked },
        );
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
            ..Default::default()
        }
    }

    // ── ISIN identity resolution (Phase 3, first-run findings) ──────────────

    #[test]
    fn identity_exact_match_is_clean() {
        assert_eq!(
            resolve_identity(Some("INE690A01028"), "INE690A01028", Some("517506"), "517506"),
            Ok(None)
        );
    }

    #[test]
    fn identity_no_instance_isin_is_clean() {
        assert_eq!(resolve_identity(None, "INE690A01028", None, "517506"), Ok(None));
    }

    #[test]
    fn identity_o_zero_typo_normalizes_clean() {
        // Real first-run cases: Deep Industries, Netweb Technologies.
        assert_eq!(
            resolve_identity(Some("INEOFHS01024"), "INE0FHS01024", Some("570005"), "570005"),
            Ok(None)
        );
        assert_eq!(
            resolve_identity(Some("INEONT901020"), "INE0NT901020", Some("543945"), "543945"),
            Ok(None)
        );
    }

    #[test]
    fn identity_scrip_anchored_same_issuer_publishes_flagged() {
        // Real first-run case: MCX — same issuer prefix INE745G01, new
        // security serial (corporate action), scrip anchor matches.
        let out = resolve_identity(Some("INE745G01035"), "INE745G01043", Some("534091"), "534091");
        let flag = out.expect("must publish").expect("must carry a flag");
        assert!(flag.starts_with("identity_isin_mismatch("), "flag = {flag}");
        assert!(flag.contains("INE745G01035") && flag.contains("INE745G01043"));
        assert!(!flag.contains(';'), "flag must stay a single ';'-joined token");
    }

    #[test]
    fn identity_mismatch_without_scrip_anchor_skips() {
        // Same issuer but the instance's own ScripCode disagrees (or is
        // absent) → cannot anchor → skip.
        assert!(resolve_identity(Some("INE745G01035"), "INE745G01043", Some("999999"), "534091")
            .is_err());
        assert!(resolve_identity(Some("INE745G01035"), "INE745G01043", None, "534091").is_err());
    }

    #[test]
    fn identity_different_issuer_skips_even_with_scrip_anchor() {
        // A misfiled document (another company's instance under this scrip
        // row) must never publish under this key.
        assert!(resolve_identity(Some("INE117A01022"), "INE745G01043", Some("534091"), "534091")
            .is_err());
    }

    #[test]
    fn normalize_isin_maps_o_to_zero_and_uppercases() {
        assert_eq!(normalize_isin(" ineOfhs01024 "), "INE0FHS01024");
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
