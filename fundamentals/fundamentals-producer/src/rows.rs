//! The §3.2 `fundamentals.parquet` row model (identity + income +
//! balance-sheet inputs + the Phase-3 sector union + derived TTM EPS / BVPS
//! + provenance), and row construction from `fundamentals-core` parse output.
//!
//! # Sector union (Phase 3)
//! Bank / NBFC / insurance rows carry sector-specific line items in a set of
//! NULLABLE columns; general rows leave them all NULL (and financial rows
//! leave the non-applicable ones NULL). Columns that duplicate a core column
//! for that sector (e.g. bank `other_income`, NBFC `finance_costs` == core
//! `interest`, insurance `shareholders_net_profit` == core `net_profit`) are
//! NOT repeated. Some union columns are structurally NULL today because the
//! in-capmkt taxonomy does not carry them (`nim_pct`, `gross_stage3_pct`,
//! `solvency_ratio`) — they exist so the client contract is stable when a
//! richer source appears, never to be guessed.
//!
//! # Derived growth + TTM (Phase 4)
//! `derive_ttm_eps` and `derive_growth` are PURE functions of the merged row
//! set, recomputed on every run (daily and backfill): they reset their
//! outputs first, so stale derived values can never survive, and they are
//! excluded from `same_data` so a recompute never looks like a restatement.
//! Growth guard semantics: NULL when the base period is missing or ≤ 0
//! (loss→profit = NULL, never +∞); profit→loss publishes as a decline below
//! -100% (base positive). YoY/QoQ row selection is day-span-based against
//! the SAME (instrument, basis) — gapped histories yield NULL, not a wrong
//! base.

use fundamentals_core::xbrl_integrated::ValuationRaw;
use fundamentals_core::{ResultPeriod, SectorKind, SectorLineItems};

/// One published row per (instrument_key, period_end, fiscal_quarter, basis).
/// `fiscal_quarter` distinguishes the Q4 quarter row from the FY annual row
/// that share a March `period_end`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FundRow {
    // ── Identity / period ────────────────────────────────────────────────────
    pub instrument_key: String,
    pub symbol: String,
    pub period_end: String,
    pub fiscal_quarter: String, // "Q1".."Q4" | "FY"
    pub basis: String,          // "standalone" | "consolidated"
    pub is_restated: bool,
    pub sector_kind: String,
    // ── Income (₹ crore; eps unscaled) ───────────────────────────────────────
    pub revenue: Option<f64>,
    pub operating_profit: Option<f64>,
    pub opm_pct: Option<f64>,
    pub margin_kind: String,
    pub other_income: Option<f64>,
    pub interest: Option<f64>,
    pub depreciation: Option<f64>,
    pub pbt: Option<f64>,
    pub tax: Option<f64>,
    pub net_profit: Option<f64>,
    pub eps: Option<f64>,
    // ── Sector union (Phase 3; ₹ crore, *_pct raw percent) ──────────────────
    // Bank + NBFC:
    pub total_income: Option<f64>,
    pub interest_earned: Option<f64>,
    pub net_interest_income: Option<f64>,
    pub nim_pct: Option<f64>, // not in the in-capmkt taxonomy — NULL today
    // Bank only:
    pub interest_expended: Option<f64>,
    pub operating_expenses: Option<f64>,
    pub pre_provision_operating_profit: Option<f64>,
    pub provisions_and_contingencies: Option<f64>,
    pub gross_npa_pct: Option<f64>,
    pub net_npa_pct: Option<f64>,
    // NBFC only:
    pub impairment_on_financial_instruments: Option<f64>,
    pub gross_stage3_pct: Option<f64>, // not in XBRL — NULL today
    // Insurance only:
    pub gross_premium_income: Option<f64>,
    pub net_premium_income: Option<f64>,
    pub investment_income: Option<f64>,
    pub net_commission: Option<f64>,
    pub benefits_paid: Option<f64>,
    pub combined_ratio_pct: Option<f64>,
    pub solvency_ratio: Option<f64>, // not in XBRL — NULL today
    // ── Balance-sheet inputs (₹ crore; shares raw count; face value ₹) ──────
    pub equity: Option<f64>,
    pub total_debt: Option<f64>,
    pub cash: Option<f64>,
    pub shares_outstanding: Option<f64>,
    pub face_value: Option<f64>,
    /// BSE full market cap (₹ crore) at row-creation time, paired with as_of;
    /// NULL when the universe seed had no BSE mktcap. Not a filing fact — a
    /// per-instrument market anchor the client uses to derive shares when XBRL
    /// share capital is absent.
    pub mktcap_cr: Option<f64>,
    pub ebitda_annual: Option<f64>,
    pub capital_employed: Option<f64>,
    // ── Derived ──────────────────────────────────────────────────────────────
    pub ttm_eps: Option<f64>,
    /// How `ttm_eps` was derived: "sum4q" (true 4-consecutive-quarter sum) |
    /// "fy_eps" (FY row's own EPS, or the matching FY row for a Q4/March row)
    /// | "" (no TTM derivable). Derived alongside `ttm_eps` every run.
    pub ttm_eps_method: String,
    pub book_value_per_share: Option<f64>,
    // ── Derived growth (Phase 4; recomputed from the full merged history on
    // every run — a pure function of the parquet, no extra state). NULL when
    // any input is missing or the base is ≤ 0 (the client is fail-closed):
    // a loss→profit swing is NULL, never +∞; a profit→loss swing IS a number
    // (a decline below -100%) because its base is positive. YoY compares the
    // same-basis row one year earlier; QoQ the immediately prior quarter.
    pub revenue_growth_yoy_pct: Option<f64>,
    pub revenue_growth_qoq_pct: Option<f64>,
    pub net_profit_growth_yoy_pct: Option<f64>,
    pub net_profit_growth_qoq_pct: Option<f64>,
    pub eps_growth_yoy_pct: Option<f64>,
    // ── Provenance / quality ─────────────────────────────────────────────────
    pub as_of: String,          // ISO date the row was first produced
    pub source_channel: String, // FilingSource id that served the instance
    pub fields_resolved_pct: f64,
    pub dq_flags: String,       // ';'-joined, empty = clean
    pub is_audited: bool,
}

impl FundRow {
    /// Merge/replace identity.
    pub fn key(&self) -> (String, String, String, String) {
        (
            self.instrument_key.clone(),
            self.period_end.clone(),
            self.fiscal_quarter.clone(),
            self.basis.clone(),
        )
    }

    /// Data equality ignoring `as_of` (provenance date) and every DERIVED
    /// column (`ttm_eps`/`ttm_eps_method` + the growth columns — all derived
    /// post-merge over the full row set; freshly built rows always carry
    /// their defaults). This is what makes state loss self-healing:
    /// re-fetching a filing whose numbers are unchanged keeps the EXISTING
    /// row (original `as_of` preserved) so the parquet stays byte-identical —
    /// no publish churn from a mere re-process.
    pub fn same_data(&self, other: &FundRow) -> bool {
        let norm = |r: &FundRow| {
            let mut c = r.clone();
            c.as_of = String::new();
            // Excluded like `as_of`: BSE market cap drifts DAILY with price, so
            // letting it participate in data-equality would mark every row
            // changed each run → full republish churn. Frozen with `as_of` at
            // row-creation time instead (the pair stays internally consistent).
            c.mktcap_cr = None;
            c.ttm_eps = None;
            c.ttm_eps_method = String::new();
            c.revenue_growth_yoy_pct = None;
            c.revenue_growth_qoq_pct = None;
            c.net_profit_growth_yoy_pct = None;
            c.net_profit_growth_qoq_pct = None;
            c.eps_growth_yoy_pct = None;
            c
        };
        norm(self) == norm(other)
    }
}

/// Share of the core income fields resolved (drives the Gate-1
/// `fields_resolved < 0.40` hard block), counted over the fields that are
/// MEANINGFUL for the sector. The vendored builders leave non-applicable
/// core fields None by design (banks have no OPM/depreciation in the core;
/// insurers report no pbt/tax in the shareholder view) — counting those as
/// "unresolved" would spuriously hard-block perfectly parsed financial rows.
pub fn fields_resolved_pct(p: &ResultPeriod, sector: SectorKind) -> f64 {
    let c = &p.core;
    let fields: &[Option<f64>] = match sector {
        SectorKind::General => &[
            c.revenue_equiv,
            c.operating_profit_equiv,
            c.margin_equiv_pct,
            c.other_income,
            c.interest,
            c.depreciation,
            c.pbt,
            c.tax,
            c.net_profit,
            c.eps,
        ],
        // Bank core: interest_earned (revenue), other income, pbt, tax, np, eps.
        SectorKind::Bank => &[c.revenue_equiv, c.other_income, c.pbt, c.tax, c.net_profit, c.eps],
        // NBFC core: revenue, other income, finance costs, pbt, tax, np, eps.
        SectorKind::Nbfc => &[
            c.revenue_equiv,
            c.other_income,
            c.interest,
            c.pbt,
            c.tax,
            c.net_profit,
            c.eps,
        ],
        // Insurance core: net premium (revenue), investment income, np, eps.
        SectorKind::Insurance => &[c.revenue_equiv, c.other_income, c.net_profit, c.eps],
    };
    let resolved = fields.iter().filter(|f| f.is_some()).count();
    resolved as f64 / fields.len() as f64
}

/// Build one row from a parsed period + the filing's balance-sheet extract.
///
/// Balance-sheet inputs (`OneI` instant = the filing's period end) are
/// attached to every row of the filing; `ebitda_annual` only to the FY row.
#[allow(clippy::too_many_arguments)]
pub fn build_row(
    instrument_key: &str,
    symbol: &str,
    sector_kind: SectorKind,
    period: &ResultPeriod,
    val: &ValuationRaw,
    source_channel: &str,
    as_of: &str,
) -> FundRow {
    let is_annual = period.fiscal_quarter == "FY";
    let equity = val.equity_cr;
    let total_debt = val.total_debt_cr;
    let capital_employed = equity.map(|e| e + total_debt.unwrap_or(0.0));
    let book_value_per_share = match (equity, val.shares_outstanding) {
        (Some(eq), Some(sh)) if sh > 0.0 => Some(eq * 1e7 / sh),
        _ => None,
    };

    // ── Sector union columns (Phase 3) ──────────────────────────────────────
    // Populated from the period's SectorLineItems variant; everything not
    // applicable to this sector stays None (published as NULL).
    let mut total_income = None;
    let mut interest_earned = None;
    let mut net_interest_income = None;
    let mut nim_pct = None;
    let mut interest_expended = None;
    let mut operating_expenses = None;
    let mut pre_provision_operating_profit = None;
    let mut provisions_and_contingencies = None;
    let mut gross_npa_pct = None;
    let mut net_npa_pct = None;
    let mut impairment_on_financial_instruments = None;
    let mut gross_stage3_pct = None;
    let mut gross_premium_income = None;
    let mut net_premium_income = None;
    let mut investment_income = None;
    let mut net_commission = None;
    let mut benefits_paid = None;
    let mut combined_ratio_pct = None;
    let mut solvency_ratio = None;
    match &period.sector {
        SectorLineItems::General(_) => {}
        SectorLineItems::Bank(b) => {
            total_income = b.total_income;
            interest_earned = b.interest_earned;
            interest_expended = b.interest_expended;
            net_interest_income = b.net_interest_income;
            nim_pct = b.nim_pct;
            operating_expenses = b.operating_expenses;
            pre_provision_operating_profit = b.pre_provision_operating_profit;
            provisions_and_contingencies = b.provisions_and_contingencies;
            gross_npa_pct = b.gross_npa_pct;
            net_npa_pct = b.net_npa_pct;
        }
        SectorLineItems::Nbfc(n) => {
            total_income = n.total_income;
            // NBFC "interest earned" is not published by the builder as a
            // dedicated line; NII already nets finance costs against it.
            net_interest_income = n.net_interest_income;
            nim_pct = n.nim_pct;
            pre_provision_operating_profit = n.pre_provision_operating_profit;
            impairment_on_financial_instruments = n.impairment_financial_instruments;
            gross_stage3_pct = n.gross_stage3_pct;
        }
        SectorLineItems::Insurance(i) => {
            gross_premium_income = i.gross_premium_income;
            net_premium_income = i.net_premium_income;
            investment_income = i.investment_income;
            net_commission = i.net_commission;
            benefits_paid = i.benefits_paid;
            combined_ratio_pct = i.combined_ratio_pct;
            solvency_ratio = i.solvency_ratio;
        }
    }

    FundRow {
        instrument_key: instrument_key.to_string(),
        symbol: symbol.to_string(),
        period_end: period.period_end.clone(),
        fiscal_quarter: period.fiscal_quarter.clone(),
        basis: period.basis.as_str().to_string(),
        is_restated: period.is_restated,
        sector_kind: sector_kind.as_str().to_string(),
        revenue: period.core.revenue_equiv,
        operating_profit: period.core.operating_profit_equiv,
        opm_pct: period.core.margin_equiv_pct,
        margin_kind: period.core.margin_kind.as_str().to_string(),
        other_income: period.core.other_income,
        interest: period.core.interest,
        depreciation: period.core.depreciation,
        pbt: period.core.pbt,
        tax: period.core.tax,
        net_profit: period.core.net_profit,
        eps: period.core.eps,
        total_income,
        interest_earned,
        net_interest_income,
        nim_pct,
        interest_expended,
        operating_expenses,
        pre_provision_operating_profit,
        provisions_and_contingencies,
        gross_npa_pct,
        net_npa_pct,
        impairment_on_financial_instruments,
        gross_stage3_pct,
        gross_premium_income,
        net_premium_income,
        investment_income,
        net_commission,
        benefits_paid,
        combined_ratio_pct,
        solvency_ratio,
        equity,
        total_debt,
        cash: val.cash_cr,
        shares_outstanding: val.shares_outstanding,
        face_value: val.face_value,
        mktcap_cr: None, // set by the pipeline after build (BSE per-instrument mktcap)
        ebitda_annual: if is_annual { val.ebitda_cr } else { None },
        capital_employed,
        ttm_eps: None, // filled by `derive_ttm_eps` over the full row set
        ttm_eps_method: String::new(),
        book_value_per_share,
        // Growth: filled by `derive_growth` over the full merged row set.
        revenue_growth_yoy_pct: None,
        revenue_growth_qoq_pct: None,
        net_profit_growth_yoy_pct: None,
        net_profit_growth_qoq_pct: None,
        eps_growth_yoy_pct: None,
        as_of: as_of.to_string(),
        source_channel: source_channel.to_string(),
        fields_resolved_pct: fields_resolved_pct(period, sector_kind),
        dq_flags: String::new(),
        is_audited: period.is_audited,
    }
}

/// Derive `ttm_eps` (+ `ttm_eps_method`) across the merged row set, per
/// (instrument_key, basis). Pure function of the row set — every value is
/// reset first, so stale derived values can never survive a recompute.
///
/// Precedence (Phase 4 — "true TTM first"):
/// - FY rows: TTM at fiscal-year end == the FY EPS itself (`fy_eps`).
/// - quarter rows: sum of this + previous 3 quarter EPS where those 4
///   quarters are truly consecutive (~1 year first→last quarter-end;
///   260–300 days) — `sum4q`. No fabrication across gaps.
/// - Q4 / March rows without 4 consecutive quarters: fall back to the
///   matching FY row's EPS when present (`fy_eps`, audited-adjusted).
pub fn derive_ttm_eps(rows: &mut [FundRow]) {
    use std::collections::HashMap;

    for r in rows.iter_mut() {
        r.ttm_eps = None;
        r.ttm_eps_method = String::new();
    }

    // Index FY rows: (instrument, basis, period_end) → eps.
    let fy_eps: HashMap<(String, String, String), Option<f64>> = rows
        .iter()
        .filter(|r| r.fiscal_quarter == "FY")
        .map(|r| {
            (
                (r.instrument_key.clone(), r.basis.clone(), r.period_end.clone()),
                r.eps,
            )
        })
        .collect();

    // Group quarter rows per (instrument, basis), sorted by period_end asc.
    let mut quarters: HashMap<(String, String), Vec<(String, Option<f64>)>> = HashMap::new();
    for r in rows.iter().filter(|r| r.fiscal_quarter != "FY") {
        quarters
            .entry((r.instrument_key.clone(), r.basis.clone()))
            .or_default()
            .push((r.period_end.clone(), r.eps));
    }
    for q in quarters.values_mut() {
        q.sort_by(|a, b| a.0.cmp(&b.0));
        q.dedup_by(|a, b| a.0 == b.0);
    }

    for r in rows.iter_mut() {
        if r.fiscal_quarter == "FY" {
            r.ttm_eps = r.eps;
            if r.ttm_eps.is_some() {
                r.ttm_eps_method = "fy_eps".into();
            }
            continue;
        }
        // True TTM first: sum of 4 consecutive quarters ending at this row.
        let series = quarters
            .get(&(r.instrument_key.clone(), r.basis.clone()))
            .map(Vec::as_slice)
            .unwrap_or_default();
        let pos = series.iter().position(|(pe, _)| *pe == r.period_end);
        if let Some(pos) = pos {
            if pos >= 3 {
                let window = &series[pos - 3..=pos];
                let span = fundamentals_core::instance::duration_days_inclusive(
                    &window[0].0,
                    &window[3].0,
                );
                let spans_a_year = matches!(span, Some(d) if (260..=300).contains(&d));
                if spans_a_year {
                    let eps_values: Vec<f64> = window.iter().filter_map(|(_, e)| *e).collect();
                    if eps_values.len() == 4 {
                        r.ttm_eps = Some(eps_values.iter().sum());
                        r.ttm_eps_method = "sum4q".into();
                        continue;
                    }
                }
            }
        }
        // Fallback: the FY row's EPS at the same period end (Q4/March rows).
        let fy_key = (r.instrument_key.clone(), r.basis.clone(), r.period_end.clone());
        if let Some(eps) = fy_eps.get(&fy_key).copied().flatten() {
            r.ttm_eps = Some(eps);
            r.ttm_eps_method = "fy_eps".into();
        }
    }
}

/// Growth guard (Phase 4): percentage change vs a prior-period base.
/// NULL when either side is missing or the base is ≤ 0 — a loss→profit
/// swing must be NULL (a percentage against a non-positive base is
/// meaningless), never +∞ or a sign-flipped number. A profit→loss swing
/// (positive base, negative current) IS published: a decline below -100%.
fn pct_growth(curr: Option<f64>, prev: Option<f64>) -> Option<f64> {
    let c = curr?;
    let p = prev?;
    if p <= 0.0 {
        return None;
    }
    Some((c - p) / p * 100.0)
}

/// One comparable point in a per-(instrument, basis) series.
#[derive(Clone)]
struct GrowthPoint {
    period_end: String,
    revenue: Option<f64>,
    net_profit: Option<f64>,
    eps: Option<f64>,
}

/// Find the series point whose period_end sits `lo..=hi` inclusive days
/// before `period_end` (the year-ago row for 360..=372; the prior quarter
/// for 85..=98). Series is sorted ascending; the LATEST match wins.
fn point_before<'a>(
    series: &'a [GrowthPoint],
    period_end: &str,
    lo: i64,
    hi: i64,
) -> Option<&'a GrowthPoint> {
    series
        .iter()
        .rev()
        .filter(|p| p.period_end.as_str() < period_end)
        .find(|p| {
            matches!(
                fundamentals_core::instance::duration_days_inclusive(&p.period_end, period_end),
                Some(d) if (lo..=hi).contains(&d)
            )
        })
}

/// Derive the Phase-4 growth columns across the merged row set. Pure
/// function of the row set (all values reset first; recomputed
/// deterministically on every run — no extra state).
///
/// Row selection, per (instrument_key, basis):
/// - **Quarter rows** compare against quarter rows only: YoY = the quarter
///   ending ~1 year earlier (360–372 inclusive days), QoQ = the immediately
///   prior quarter (85–98 inclusive days). Day-span matching (not calendar
///   arithmetic) means a gapped history yields NULL, never a wrong base.
/// - **FY rows** compare against the prior FY row (360–372 days); QoQ stays
///   NULL on FY rows by construction.
/// - `eps_growth_yoy_pct` only (QoQ EPS is noise for screening).
pub fn derive_growth(rows: &mut [FundRow]) {
    use std::collections::HashMap;

    for r in rows.iter_mut() {
        r.revenue_growth_yoy_pct = None;
        r.revenue_growth_qoq_pct = None;
        r.net_profit_growth_yoy_pct = None;
        r.net_profit_growth_qoq_pct = None;
        r.eps_growth_yoy_pct = None;
    }

    // (instrument, basis, is_fy) → ascending series of comparable points.
    let mut series: HashMap<(String, String, bool), Vec<GrowthPoint>> = HashMap::new();
    for r in rows.iter() {
        series
            .entry((r.instrument_key.clone(), r.basis.clone(), r.fiscal_quarter == "FY"))
            .or_default()
            .push(GrowthPoint {
                period_end: r.period_end.clone(),
                revenue: r.revenue,
                net_profit: r.net_profit,
                eps: r.eps,
            });
    }
    for s in series.values_mut() {
        s.sort_by(|a, b| a.period_end.cmp(&b.period_end));
        s.dedup_by(|a, b| a.period_end == b.period_end);
    }

    for r in rows.iter_mut() {
        let is_fy = r.fiscal_quarter == "FY";
        let key = (r.instrument_key.clone(), r.basis.clone(), is_fy);
        let Some(s) = series.get(&key) else { continue };

        let yoy = point_before(s, &r.period_end, 360, 372);
        r.revenue_growth_yoy_pct = pct_growth(r.revenue, yoy.and_then(|p| p.revenue));
        r.net_profit_growth_yoy_pct = pct_growth(r.net_profit, yoy.and_then(|p| p.net_profit));
        r.eps_growth_yoy_pct = pct_growth(r.eps, yoy.and_then(|p| p.eps));

        if !is_fy {
            let qoq = point_before(s, &r.period_end, 85, 98);
            r.revenue_growth_qoq_pct = pct_growth(r.revenue, qoq.and_then(|p| p.revenue));
            r.net_profit_growth_qoq_pct = pct_growth(r.net_profit, qoq.and_then(|p| p.net_profit));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quarter_row(pe: &str, fq: &str, eps: Option<f64>) -> FundRow {
        FundRow {
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
            net_profit: None,
            eps,
            equity: None,
            total_debt: None,
            cash: None,
            shares_outstanding: None,
            face_value: None,
            ebitda_annual: None,
            capital_employed: None,
            ttm_eps: None,
            book_value_per_share: None,
            as_of: "2026-07-12".into(),
            source_channel: "test".into(),
            fields_resolved_pct: 1.0,
            dq_flags: String::new(),
            is_audited: true,
            ..Default::default()
        }
    }

    #[test]
    fn fy_row_ttm_is_own_eps() {
        let mut rows = vec![quarter_row("2026-03-31", "FY", Some(13.54))];
        derive_ttm_eps(&mut rows);
        assert_eq!(rows[0].ttm_eps, Some(13.54));
        assert_eq!(rows[0].ttm_eps_method, "fy_eps");
    }

    #[test]
    fn q4_row_falls_back_to_matching_fy_eps_without_four_quarters() {
        let mut rows = vec![
            quarter_row("2026-03-31", "Q4", Some(3.71)),
            quarter_row("2026-03-31", "FY", Some(13.54)),
        ];
        derive_ttm_eps(&mut rows);
        assert_eq!(rows[0].ttm_eps, Some(13.54), "Q4 falls back to FY (audited) EPS");
        assert_eq!(rows[0].ttm_eps_method, "fy_eps");
    }

    #[test]
    fn q4_row_prefers_true_four_quarter_sum_over_fy_eps() {
        // Phase 4: with 4 consecutive quarters, the TRUE TTM sum wins even
        // when a matching FY row exists; the method column says which.
        let mut rows = vec![
            quarter_row("2025-06-30", "Q1", Some(1.0)),
            quarter_row("2025-09-30", "Q2", Some(2.0)),
            quarter_row("2025-12-31", "Q3", Some(3.0)),
            quarter_row("2026-03-31", "Q4", Some(4.0)),
            quarter_row("2026-03-31", "FY", Some(13.54)),
        ];
        derive_ttm_eps(&mut rows);
        let q4 = rows.iter().find(|r| r.fiscal_quarter == "Q4").unwrap();
        assert_eq!(q4.ttm_eps, Some(10.0), "sum of the 4 real quarters");
        assert_eq!(q4.ttm_eps_method, "sum4q");
        let fy = rows.iter().find(|r| r.fiscal_quarter == "FY").unwrap();
        assert_eq!(fy.ttm_eps, Some(13.54), "FY row keeps its own EPS");
        assert_eq!(fy.ttm_eps_method, "fy_eps");
    }

    #[test]
    fn derive_ttm_resets_stale_values_first() {
        // Rows read back from an older parquet carry derived values; the
        // recompute must be a pure function of today's row set.
        let mut lone = quarter_row("2026-06-30", "Q1", Some(5.0));
        lone.ttm_eps = Some(99.0);
        lone.ttm_eps_method = "sum4q".into();
        let mut rows = vec![lone];
        derive_ttm_eps(&mut rows);
        assert_eq!(rows[0].ttm_eps, None, "stale ttm_eps must not survive");
        assert_eq!(rows[0].ttm_eps_method, "");
    }

    #[test]
    fn four_consecutive_quarters_sum() {
        let mut rows = vec![
            quarter_row("2025-09-30", "Q2", Some(2.0)),
            quarter_row("2025-12-31", "Q3", Some(3.0)),
            quarter_row("2026-03-31", "Q4", Some(4.0)),
            quarter_row("2026-06-30", "Q1", Some(5.0)),
        ];
        derive_ttm_eps(&mut rows);
        let q1 = rows.iter().find(|r| r.period_end == "2026-06-30").unwrap();
        assert_eq!(q1.ttm_eps, Some(14.0));
        assert_eq!(q1.ttm_eps_method, "sum4q");
    }

    #[test]
    fn gapped_quarters_do_not_fabricate_ttm() {
        // Missing 2025-12-31: the 4 available quarters span > 1 year.
        let mut rows = vec![
            quarter_row("2025-06-30", "Q1", Some(1.0)),
            quarter_row("2025-09-30", "Q2", Some(2.0)),
            quarter_row("2026-03-31", "Q4", Some(4.0)),
            quarter_row("2026-06-30", "Q1", Some(5.0)),
        ];
        derive_ttm_eps(&mut rows);
        let q1 = rows.iter().find(|r| r.period_end == "2026-06-30").unwrap();
        assert_eq!(q1.ttm_eps, None, "gapped series must not produce a TTM");
    }

    #[test]
    fn lone_quarter_has_no_ttm() {
        let mut rows = vec![quarter_row("2026-06-30", "Q1", Some(5.0))];
        derive_ttm_eps(&mut rows);
        assert_eq!(rows[0].ttm_eps, None);
    }

    // ── Growth derivation (Phase 4) ──────────────────────────────────────────

    /// Quarter row with revenue / net profit / eps set explicitly.
    fn growth_row(pe: &str, fq: &str, rev: Option<f64>, np: Option<f64>, eps: Option<f64>) -> FundRow {
        let mut r = quarter_row(pe, fq, eps);
        r.revenue = rev;
        r.net_profit = np;
        r
    }

    #[test]
    fn growth_yoy_and_qoq_pick_the_right_rows() {
        let mut rows = vec![
            growth_row("2025-06-30", "Q1", Some(100.0), Some(10.0), Some(1.0)),
            growth_row("2025-09-30", "Q2", Some(110.0), Some(11.0), Some(1.1)),
            growth_row("2025-12-31", "Q3", Some(120.0), Some(12.0), Some(1.2)),
            growth_row("2026-03-31", "Q4", Some(130.0), Some(13.0), Some(1.3)),
            growth_row("2026-06-30", "Q1", Some(125.0), Some(15.0), Some(1.5)),
        ];
        derive_growth(&mut rows);
        let q1 = rows.iter().find(|r| r.period_end == "2026-06-30").unwrap();
        // YoY: vs 2025-06-30 (NOT the prior quarter).
        assert!((q1.revenue_growth_yoy_pct.unwrap() - 25.0).abs() < 1e-9);
        assert!((q1.net_profit_growth_yoy_pct.unwrap() - 50.0).abs() < 1e-9);
        assert!((q1.eps_growth_yoy_pct.unwrap() - 50.0).abs() < 1e-9);
        // QoQ: vs 2026-03-31 (NOT the year-ago quarter).
        assert!((q1.revenue_growth_qoq_pct.unwrap() - (-5.0 / 1.30)).abs() < 1e-6);
        assert!((q1.net_profit_growth_qoq_pct.unwrap() - (2.0 / 0.13)).abs() < 1e-6);
        // The earliest quarter has no prior rows at all → all NULL.
        let first = rows.iter().find(|r| r.period_end == "2025-06-30").unwrap();
        assert!(first.revenue_growth_yoy_pct.is_none());
        assert!(first.revenue_growth_qoq_pct.is_none());
    }

    #[test]
    fn growth_missing_year_ago_is_null_even_with_other_history() {
        // Year-ago quarter absent (gap) but older data exists: day-span
        // matching must yield NULL, never silently compare a wrong base.
        let mut rows = vec![
            growth_row("2025-03-31", "Q4", Some(90.0), Some(9.0), Some(0.9)),
            growth_row("2026-03-31", "Q4", Some(130.0), Some(13.0), Some(1.3)),
            growth_row("2026-06-30", "Q1", Some(125.0), Some(15.0), Some(1.5)),
        ];
        derive_growth(&mut rows);
        let q1 = rows.iter().find(|r| r.period_end == "2026-06-30").unwrap();
        assert!(q1.revenue_growth_yoy_pct.is_none(), "no 2025-06-30 row → NULL");
        assert!(q1.revenue_growth_qoq_pct.is_some(), "QoQ vs 2026-03-31 still works");
        let q4 = rows.iter().find(|r| r.period_end == "2026-03-31").unwrap();
        assert!(q4.revenue_growth_yoy_pct.is_some(), "2025-03-31 is a valid year-ago");
        assert!(q4.revenue_growth_qoq_pct.is_none(), "prior quarter missing → NULL");
    }

    #[test]
    fn growth_loss_to_profit_is_null_profit_to_loss_is_a_decline() {
        let mut rows = vec![
            growth_row("2025-06-30", "Q1", Some(100.0), Some(-5.0), Some(-0.5)),
            growth_row("2025-09-30", "Q2", Some(100.0), Some(10.0), Some(1.0)),
            growth_row("2025-12-31", "Q3", Some(100.0), Some(-2.0), None),
        ];
        derive_growth(&mut rows);
        // Loss → profit: base ≤ 0 → NULL, never +∞ or a sign-flipped number.
        let q2 = rows.iter().find(|r| r.fiscal_quarter == "Q2").unwrap();
        assert!(q2.net_profit_growth_qoq_pct.is_none(), "loss→profit must be NULL");
        // Profit → loss: positive base → a real decline below -100%.
        let q3 = rows.iter().find(|r| r.fiscal_quarter == "Q3").unwrap();
        assert!((q3.net_profit_growth_qoq_pct.unwrap() - (-120.0)).abs() < 1e-9);
        // Zero base guard: base == 0 is also NULL (division blow-up).
        assert_eq!(pct_growth(Some(5.0), Some(0.0)), None);
        assert_eq!(pct_growth(None, Some(5.0)), None, "missing current → NULL");
        assert_eq!(pct_growth(Some(5.0), None), None, "missing base → NULL");
    }

    #[test]
    fn growth_fy_rows_compare_fy_to_fy_and_never_qoq() {
        let mut rows = vec![
            growth_row("2025-03-31", "FY", Some(1000.0), Some(100.0), Some(10.0)),
            growth_row("2025-03-31", "Q4", Some(260.0), Some(26.0), Some(2.6)),
            growth_row("2026-03-31", "FY", Some(1200.0), Some(150.0), Some(15.0)),
            growth_row("2026-03-31", "Q4", Some(300.0), Some(30.0), Some(3.0)),
        ];
        derive_growth(&mut rows);
        let fy = rows
            .iter()
            .find(|r| r.fiscal_quarter == "FY" && r.period_end == "2026-03-31")
            .unwrap();
        assert!((fy.revenue_growth_yoy_pct.unwrap() - 20.0).abs() < 1e-9, "FY vs prior FY");
        assert!((fy.net_profit_growth_yoy_pct.unwrap() - 50.0).abs() < 1e-9);
        assert!((fy.eps_growth_yoy_pct.unwrap() - 50.0).abs() < 1e-9);
        assert!(fy.revenue_growth_qoq_pct.is_none(), "QoQ is meaningless on FY rows");
        // The Q4 row compares against the year-ago Q4 QUARTER row (not FY).
        let q4 = rows
            .iter()
            .find(|r| r.fiscal_quarter == "Q4" && r.period_end == "2026-03-31")
            .unwrap();
        assert!((q4.revenue_growth_yoy_pct.unwrap() - (40.0 / 2.6)).abs() < 1e-6);
    }

    #[test]
    fn growth_respects_basis_separation() {
        let mut rows = vec![
            growth_row("2025-06-30", "Q1", Some(100.0), Some(10.0), Some(1.0)),
            growth_row("2026-06-30", "Q1", Some(150.0), Some(15.0), Some(1.5)),
        ];
        rows[0].basis = "standalone".into(); // year-ago row is the OTHER basis
        derive_growth(&mut rows);
        let cur = rows.iter().find(|r| r.period_end == "2026-06-30").unwrap();
        assert!(
            cur.revenue_growth_yoy_pct.is_none(),
            "a standalone base must never serve a consolidated row"
        );
    }

    #[test]
    fn growth_is_reset_before_recompute() {
        let mut lone = growth_row("2026-06-30", "Q1", Some(100.0), Some(10.0), Some(1.0));
        lone.revenue_growth_yoy_pct = Some(42.0); // stale value from an old parquet
        let mut rows = vec![lone];
        derive_growth(&mut rows);
        assert!(rows[0].revenue_growth_yoy_pct.is_none(), "stale growth must not survive");
    }

    #[test]
    fn same_data_ignores_as_of_and_ttm_eps_only() {
        let a = quarter_row("2026-06-30", "Q1", Some(5.0));
        let mut b = a.clone();
        b.as_of = "2026-09-01".into();
        b.ttm_eps = Some(14.0);
        b.ttm_eps_method = "sum4q".into();
        b.revenue_growth_yoy_pct = Some(25.0);
        b.net_profit_growth_qoq_pct = Some(-3.0);
        b.eps_growth_yoy_pct = Some(11.1);
        b.mktcap_cr = Some(50000.0); // daily price drift must NOT count as a data change
        assert!(a.same_data(&b), "as_of/derived/mktcap differences are not data changes");
        let mut c = a.clone();
        c.net_profit = Some(999.0);
        assert!(!a.same_data(&c), "a real value change IS a data change");
        let mut d = a.clone();
        d.dq_flags = "negative_tax".into();
        assert!(!a.same_data(&d), "a dq_flags change IS a data change");
    }

    #[test]
    fn fields_resolved_counts_core_fields() {
        let mut p = ResultPeriod::default();
        assert_eq!(fields_resolved_pct(&p, SectorKind::General), 0.0);
        p.core.revenue_equiv = Some(1.0);
        p.core.net_profit = Some(1.0);
        p.core.eps = Some(1.0);
        p.core.pbt = Some(1.0);
        assert!((fields_resolved_pct(&p, SectorKind::General) - 0.4).abs() < 1e-9);
    }

    #[test]
    fn fields_resolved_is_sector_aware() {
        // A fully-parsed bank period leaves op/margin/interest/depreciation
        // None BY DESIGN — it must still count as fully resolved for banks.
        let mut p = ResultPeriod::default();
        p.core.revenue_equiv = Some(87182.5); // interest earned
        p.core.other_income = Some(12000.0);
        p.core.pbt = Some(27000.0);
        p.core.tax = Some(6600.0);
        p.core.net_profit = Some(20350.76);
        p.core.eps = Some(13.22);
        assert!((fields_resolved_pct(&p, SectorKind::Bank) - 1.0).abs() < 1e-9);
        // The same period under the general denominator would be 0.6 — the
        // exact spurious-block hazard the sector-aware denominator removes.
        assert!((fields_resolved_pct(&p, SectorKind::General) - 0.6).abs() < 1e-9);

        // A fully-parsed insurance period: 4 applicable fields.
        let mut ins = ResultPeriod::default();
        ins.core.revenue_equiv = Some(27683.79); // net premium
        ins.core.other_income = Some(3000.0);    // investment income
        ins.core.net_profit = Some(2363.62);
        ins.core.eps = Some(8.02);
        assert!((fields_resolved_pct(&ins, SectorKind::Insurance) - 1.0).abs() < 1e-9);
        // Missing eps alone must not fall below the 0.40 hard-block line.
        ins.core.eps = None;
        assert!(fields_resolved_pct(&ins, SectorKind::Insurance) >= 0.40);
    }

    #[test]
    fn build_row_populates_bank_union_and_leaves_rest_null() {
        use fundamentals_core::{BankLines, SectorLineItems};
        let mut p = ResultPeriod {
            period_end: "2026-03-31".into(),
            fiscal_quarter: "Q4".into(),
            ..Default::default()
        };
        p.core.revenue_equiv = Some(87182.5);
        p.sector = SectorLineItems::Bank(BankLines {
            total_income: Some(120000.0),
            interest_earned: Some(87182.5),
            other_income: Some(12000.0),
            interest_expended: Some(45220.44),
            net_interest_income: Some(41962.06),
            nim_pct: None,
            operating_expenses: Some(17000.0),
            pre_provision_operating_profit: Some(25000.0),
            provisions_and_contingencies: Some(3000.0),
            gross_npa_pct: Some(1.33),
            net_npa_pct: Some(0.43),
        });
        let row = build_row(
            "INE040A01034", "HDFCBANK", SectorKind::Bank, &p,
            &ValuationRaw::default(), "bse", "2026-07-12",
        );
        assert_eq!(row.sector_kind, "bank");
        assert_eq!(row.interest_earned, Some(87182.5));
        assert_eq!(row.interest_expended, Some(45220.44));
        assert_eq!(row.net_interest_income, Some(41962.06));
        assert_eq!(row.gross_npa_pct, Some(1.33));
        assert_eq!(row.net_npa_pct, Some(0.43));
        assert_eq!(row.total_income, Some(120000.0));
        assert_eq!(row.operating_expenses, Some(17000.0));
        assert_eq!(row.pre_provision_operating_profit, Some(25000.0));
        assert_eq!(row.provisions_and_contingencies, Some(3000.0));
        assert!(row.nim_pct.is_none(), "NIM is not in XBRL — must stay NULL");
        // Non-applicable sectors' columns stay NULL.
        assert!(row.gross_premium_income.is_none());
        assert!(row.impairment_on_financial_instruments.is_none());
    }

    #[test]
    fn build_row_populates_insurance_union() {
        use fundamentals_core::{InsuranceLines, SectorLineItems};
        let mut p = ResultPeriod {
            period_end: "2026-03-31".into(),
            fiscal_quarter: "Q4".into(),
            ..Default::default()
        };
        p.core.revenue_equiv = Some(27683.79);
        p.sector = SectorLineItems::Insurance(InsuranceLines {
            gross_premium_income: Some(27938.86),
            net_premium_income: Some(27683.79),
            investment_income: Some(3000.0),
            net_commission: Some(900.0),
            benefits_paid: Some(16254.62),
            combined_ratio_pct: None,
            solvency_ratio: None,
            shareholders_net_profit: Some(2363.62),
        });
        let row = build_row(
            "INE123W01016", "SBILIFE", SectorKind::Insurance, &p,
            &ValuationRaw::default(), "bse", "2026-07-12",
        );
        assert_eq!(row.sector_kind, "insurance");
        assert_eq!(row.gross_premium_income, Some(27938.86));
        assert_eq!(row.net_premium_income, Some(27683.79));
        assert_eq!(row.investment_income, Some(3000.0));
        assert_eq!(row.net_commission, Some(900.0));
        assert_eq!(row.benefits_paid, Some(16254.62));
        assert!(row.interest_earned.is_none());
        assert!(row.gross_npa_pct.is_none());
    }

    #[test]
    fn build_row_general_leaves_sector_union_null() {
        let mut p = ResultPeriod {
            period_end: "2026-03-31".into(),
            fiscal_quarter: "Q4".into(),
            ..Default::default()
        };
        p.core.revenue_equiv = Some(679.57);
        let row = build_row(
            "INE690A01028", "TTKPRESTIG", SectorKind::General, &p,
            &ValuationRaw::default(), "bse", "2026-07-12",
        );
        assert_eq!(row.sector_kind, "general");
        for v in [
            row.total_income, row.interest_earned, row.net_interest_income,
            row.nim_pct, row.interest_expended, row.operating_expenses,
            row.pre_provision_operating_profit, row.provisions_and_contingencies,
            row.gross_npa_pct, row.net_npa_pct,
            row.impairment_on_financial_instruments, row.gross_stage3_pct,
            row.gross_premium_income, row.net_premium_income,
            row.investment_income, row.net_commission, row.benefits_paid,
            row.combined_ratio_pct, row.solvency_ratio,
        ] {
            assert!(v.is_none(), "general rows must keep every union column NULL");
        }
    }
}
