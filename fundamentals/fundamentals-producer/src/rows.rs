//! The §3.2 `fundamentals.parquet` row model (Phase-1 subset: identity +
//! income + balance-sheet inputs + derived TTM EPS / BVPS + provenance),
//! and row construction from `fundamentals-core` parse output.

use fundamentals_core::xbrl_integrated::ValuationRaw;
use fundamentals_core::{ResultPeriod, SectorKind};

/// One published row per (instrument_key, period_end, fiscal_quarter, basis).
/// `fiscal_quarter` distinguishes the Q4 quarter row from the FY annual row
/// that share a March `period_end`.
#[derive(Debug, Clone, PartialEq)]
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
    // ── Balance-sheet inputs (₹ crore; shares raw count; face value ₹) ──────
    pub equity: Option<f64>,
    pub total_debt: Option<f64>,
    pub cash: Option<f64>,
    pub shares_outstanding: Option<f64>,
    pub face_value: Option<f64>,
    pub ebitda_annual: Option<f64>,
    pub capital_employed: Option<f64>,
    // ── Derived ──────────────────────────────────────────────────────────────
    pub ttm_eps: Option<f64>,
    pub book_value_per_share: Option<f64>,
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

    /// Data equality ignoring `as_of` (provenance date) and `ttm_eps`
    /// (derived post-merge over the full row set; freshly built rows always
    /// carry `None`). This is what makes state loss self-healing: re-fetching
    /// a filing whose numbers are unchanged keeps the EXISTING row (original
    /// `as_of` preserved) so the parquet stays byte-identical — no publish
    /// churn from a mere re-process.
    pub fn same_data(&self, other: &FundRow) -> bool {
        let norm = |r: &FundRow| {
            let mut c = r.clone();
            c.as_of = String::new();
            c.ttm_eps = None;
            c
        };
        norm(self) == norm(other)
    }
}

/// Share of the 10 core income fields resolved (drives the Gate-1
/// `fields_resolved < 0.40` hard block).
pub fn fields_resolved_pct(p: &ResultPeriod) -> f64 {
    let c = &p.core;
    let fields = [
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
    ];
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
        equity,
        total_debt,
        cash: val.cash_cr,
        shares_outstanding: val.shares_outstanding,
        face_value: val.face_value,
        ebitda_annual: if is_annual { val.ebitda_cr } else { None },
        capital_employed,
        ttm_eps: None, // filled by `derive_ttm_eps` over the full row set
        book_value_per_share,
        as_of: as_of.to_string(),
        source_channel: source_channel.to_string(),
        fields_resolved_pct: fields_resolved_pct(period),
        dq_flags: String::new(),
        is_audited: period.is_audited,
    }
}

/// Derive `ttm_eps` across the merged row set, per (instrument_key, basis):
/// - FY rows: TTM at fiscal-year end == the FY EPS itself.
/// - Q4 rows: use the matching FY row's EPS when present (audited-adjusted).
/// - other quarter rows: sum of this + previous 3 quarter EPS, but only if
///   those 4 quarters actually span ~1 year (270–290 days between the first
///   and last quarter-end) — no fabrication across gaps.
pub fn derive_ttm_eps(rows: &mut [FundRow]) {
    use std::collections::HashMap;

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
            continue;
        }
        // Prefer the FY row's EPS at the same period end (Q4 / March rows).
        let fy_key = (r.instrument_key.clone(), r.basis.clone(), r.period_end.clone());
        if let Some(eps) = fy_eps.get(&fy_key).copied().flatten() {
            r.ttm_eps = Some(eps);
            continue;
        }
        // Otherwise: sum of 4 consecutive quarters ending at this row.
        let series = quarters
            .get(&(r.instrument_key.clone(), r.basis.clone()))
            .cloned()
            .unwrap_or_default();
        let Some(pos) = series.iter().position(|(pe, _)| *pe == r.period_end) else {
            continue;
        };
        if pos < 3 {
            continue; // fewer than 4 quarters available
        }
        let window = &series[pos - 3..=pos];
        let span = fundamentals_core::instance::duration_days_inclusive(&window[0].0, &window[3].0);
        let spans_a_year = matches!(span, Some(d) if (260..=300).contains(&d));
        if !spans_a_year {
            continue; // quarters have gaps — refuse to fabricate a TTM
        }
        let eps_values: Vec<f64> = window.iter().filter_map(|(_, e)| *e).collect();
        if eps_values.len() == 4 {
            r.ttm_eps = Some(eps_values.iter().sum());
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
        }
    }

    #[test]
    fn fy_row_ttm_is_own_eps() {
        let mut rows = vec![quarter_row("2026-03-31", "FY", Some(13.54))];
        derive_ttm_eps(&mut rows);
        assert_eq!(rows[0].ttm_eps, Some(13.54));
    }

    #[test]
    fn q4_row_uses_matching_fy_eps() {
        let mut rows = vec![
            quarter_row("2026-03-31", "Q4", Some(3.71)),
            quarter_row("2026-03-31", "FY", Some(13.54)),
        ];
        derive_ttm_eps(&mut rows);
        assert_eq!(rows[0].ttm_eps, Some(13.54), "Q4 should use FY (audited) EPS");
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

    #[test]
    fn same_data_ignores_as_of_and_ttm_eps_only() {
        let a = quarter_row("2026-06-30", "Q1", Some(5.0));
        let mut b = a.clone();
        b.as_of = "2026-09-01".into();
        b.ttm_eps = Some(14.0);
        assert!(a.same_data(&b), "as_of/ttm_eps differences are not data changes");
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
        assert_eq!(fields_resolved_pct(&p), 0.0);
        p.core.revenue_equiv = Some(1.0);
        p.core.net_profit = Some(1.0);
        p.core.eps = Some(1.0);
        p.core.pbt = Some(1.0);
        assert!((fields_resolved_pct(&p) - 0.4).abs() < 1e-9);
    }
}
