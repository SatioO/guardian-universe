//! Gate-1 data-quality wall (design §2.4 "DQ gates are load-bearing").
//!
//! Two channels since Phase 2:
//! - **blocks** — hard failures: the row is dropped and counted, never
//!   published. A wrong number is worse than no number.
//! - **flags** — row-level `dq_flags` (';'-joined on the row): the value is
//!   plausible but deserves a caveat. Downgraded from Phase-1 blocks where
//!   real filings proved the strict rule wrong (e.g. `negative_tax`:
//!   NTPC/POWERGRID book genuine deferred-tax credits — those rows now
//!   publish flagged instead of being silently dropped).

use crate::rows::FundRow;

/// Why a row was hard-blocked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateBlock {
    /// fields_resolved < 0.40 — the parse recognised too little of the filing.
    LowFieldsResolved,
    /// Negative revenue — sign error / wrong element.
    NegativeRevenue,
    /// Annualized revenue > 10x market cap — almost certainly a scale error.
    ScaleRevenueTooLarge,
    /// Annualized revenue < mcap/1000 — almost certainly a scale error.
    ScaleRevenueTooSmall,
    /// Quarter (OneD) context duration outside 85–95 days.
    QuarterDurationOutOfRange(i64),
    /// FY (FourD) context duration far from a fiscal year (350–380 days).
    AnnualDurationOutOfRange(i64),
}

impl GateBlock {
    pub fn reason(&self) -> String {
        match self {
            GateBlock::LowFieldsResolved => "low_fields_resolved".into(),
            GateBlock::NegativeRevenue => "negative_revenue".into(),
            GateBlock::ScaleRevenueTooLarge => "scale_revenue_vs_mcap_too_large".into(),
            GateBlock::ScaleRevenueTooSmall => "scale_revenue_vs_mcap_too_small".into(),
            GateBlock::QuarterDurationOutOfRange(d) => format!("quarter_duration_{d}d"),
            GateBlock::AnnualDurationOutOfRange(d) => format!("annual_duration_{d}d"),
        }
    }
}

/// The Gate-1 verdict for one row: hard blocks + row-level DQ flags.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GateOutcome {
    pub blocks: Vec<GateBlock>,
    /// Sorted, deduplicated flag names for the row's `dq_flags` column.
    pub flags: Vec<String>,
}

impl GateOutcome {
    #[cfg(test)]
    pub fn is_clean_pass(&self) -> bool {
        self.blocks.is_empty() && self.flags.is_empty()
    }
}

/// Run Gate-1 over one row.
///
/// * `context_duration_days` — inclusive day count of the row's XBRL context
///   (`OneD` for quarter rows, `FourD` for FY rows), from the instance itself.
/// * `mktcap_cr` — the scrip master's market cap (₹ crore), when known.
pub fn gate1(
    row: &FundRow,
    context_duration_days: Option<i64>,
    mktcap_cr: Option<f64>,
) -> GateOutcome {
    let mut out = GateOutcome::default();

    if row.fields_resolved_pct < 0.40 {
        out.blocks.push(GateBlock::LowFieldsResolved);
    }

    if let Some(rev) = row.revenue {
        if rev < 0.0 {
            out.blocks.push(GateBlock::NegativeRevenue);
        }
    }

    // Phase-2 downgrade: negative tax is a FLAG, not a block. Genuine
    // deferred-tax credits exist (NTPC, POWERGRID) — publish, flagged.
    if let Some(tax) = row.tax {
        if tax < 0.0 {
            out.flags.push("negative_tax".into());
        }
    }

    // Cheap Gate-2-style cross-checks (flags, never blocks):
    // EPS vs net_profit / shares_outstanding coherence. np is ₹ crore,
    // shares a raw count → implied EPS = np * 1e7 / shares. >25% relative
    // divergence suggests a basis/weighted-average/face-value subtlety worth
    // caveating, not suppressing.
    if let (Some(eps), Some(np), Some(sh)) = (row.eps, row.net_profit, row.shares_outstanding) {
        if sh > 0.0 && eps.abs() > 0.01 {
            let implied = np * 1e7 / sh;
            let denom = eps.abs().max(implied.abs());
            if denom > 0.0 && (implied - eps).abs() / denom > 0.25 {
                out.flags.push("eps_vs_net_profit_mismatch".into());
            }
        }
    }
    // Tax larger than PBT (both positive) — legal but rare enough to caveat.
    if let (Some(tax), Some(pbt)) = (row.tax, row.pbt) {
        if pbt > 0.0 && tax > pbt {
            out.flags.push("tax_exceeds_pbt".into());
        }
    }

    // Scale cross-check: annualized revenue vs market cap (both ₹ crore).
    // A rupees-vs-crore slip is a factor of 1e7 — x10 / /1000 catches it with
    // huge margin while tolerating real high/low revenue-to-mcap businesses.
    if let (Some(rev), Some(mcap)) = (row.revenue, mktcap_cr) {
        if rev > 0.0 && mcap > 0.0 {
            let annualized = if row.fiscal_quarter == "FY" { rev } else { rev * 4.0 };
            if annualized > mcap * 10.0 {
                out.blocks.push(GateBlock::ScaleRevenueTooLarge);
            } else if annualized < mcap / 1000.0 {
                out.blocks.push(GateBlock::ScaleRevenueTooSmall);
            }
        }
    }

    if let Some(days) = context_duration_days {
        if row.fiscal_quarter == "FY" {
            if !(350..=380).contains(&days) {
                out.blocks.push(GateBlock::AnnualDurationOutOfRange(days));
            }
        } else if !(85..=95).contains(&days) {
            out.blocks.push(GateBlock::QuarterDurationOutOfRange(days));
        }
    }

    out.flags.sort();
    out.flags.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::FundRow;

    fn base_row() -> FundRow {
        FundRow {
            instrument_key: "INE000A01001".into(),
            symbol: "TEST".into(),
            period_end: "2026-03-31".into(),
            fiscal_quarter: "Q4".into(),
            basis: "consolidated".into(),
            is_restated: false,
            sector_kind: "general".into(),
            revenue: Some(1000.0),
            operating_profit: Some(150.0),
            opm_pct: Some(15.0),
            margin_kind: "opm".into(),
            other_income: Some(10.0),
            interest: Some(5.0),
            depreciation: Some(20.0),
            pbt: Some(140.0),
            tax: Some(35.0),
            net_profit: Some(105.0),
            eps: Some(10.5),
            equity: Some(5000.0),
            total_debt: Some(1000.0),
            cash: Some(500.0),
            shares_outstanding: Some(1e8),
            face_value: Some(10.0),
            ebitda_annual: None,
            capital_employed: Some(6000.0),
            ttm_eps: None,
            book_value_per_share: Some(500.0),
            as_of: "2026-07-12".into(),
            source_channel: "test".into(),
            fields_resolved_pct: 1.0,
            dq_flags: String::new(),
            is_audited: true,
        }
    }

    #[test]
    fn clean_row_passes_with_no_flags() {
        // 1000cr quarterly revenue, 40000cr mcap, 90-day quarter → clean.
        // (base_row eps 10.5 vs implied 105cr*1e7/1e8 = 10.5 → coherent.)
        let out = gate1(&base_row(), Some(90), Some(40_000.0));
        assert!(out.is_clean_pass(), "outcome = {out:?}");
    }

    #[test]
    fn low_fields_resolved_blocks() {
        let mut r = base_row();
        r.fields_resolved_pct = 0.39;
        let out = gate1(&r, Some(90), None);
        assert!(out.blocks.contains(&GateBlock::LowFieldsResolved));
    }

    #[test]
    fn boundary_040_passes() {
        let mut r = base_row();
        r.fields_resolved_pct = 0.40;
        assert!(!gate1(&r, Some(90), None).blocks.contains(&GateBlock::LowFieldsResolved));
    }

    #[test]
    fn negative_revenue_blocks() {
        let mut r = base_row();
        r.revenue = Some(-5.0);
        assert!(gate1(&r, Some(90), None).blocks.contains(&GateBlock::NegativeRevenue));
    }

    #[test]
    fn negative_tax_is_a_flag_not_a_block() {
        // The NTPC/POWERGRID case: deferred-tax credit → publish, flagged.
        let mut r = base_row();
        r.tax = Some(-1.0);
        let out = gate1(&r, Some(90), None);
        assert!(out.blocks.is_empty(), "negative tax must not block: {:?}", out.blocks);
        assert!(out.flags.iter().any(|f| f == "negative_tax"));
    }

    #[test]
    fn eps_vs_net_profit_mismatch_flags() {
        let mut r = base_row();
        r.eps = Some(50.0); // implied is 10.5 → wildly divergent
        let out = gate1(&r, Some(90), None);
        assert!(out.flags.iter().any(|f| f == "eps_vs_net_profit_mismatch"));
        assert!(out.blocks.is_empty());
    }

    #[test]
    fn tax_exceeds_pbt_flags() {
        let mut r = base_row();
        r.tax = Some(150.0); // > pbt 140
        let out = gate1(&r, Some(90), None);
        assert!(out.flags.iter().any(|f| f == "tax_exceeds_pbt"));
        assert!(out.blocks.is_empty());
    }

    #[test]
    fn revenue_scale_too_large_blocks() {
        let mut r = base_row();
        // 1e7 scale slip: quarterly revenue "1000 cr" becomes 1e10 cr.
        r.revenue = Some(1e10);
        let out = gate1(&r, Some(90), Some(40_000.0));
        assert!(out.blocks.contains(&GateBlock::ScaleRevenueTooLarge));
    }

    #[test]
    fn revenue_scale_too_small_blocks() {
        let mut r = base_row();
        // Inverse slip: revenue microscopic vs a 40,000cr mcap.
        r.revenue = Some(0.001);
        let out = gate1(&r, Some(90), Some(40_000.0));
        assert!(out.blocks.contains(&GateBlock::ScaleRevenueTooSmall));
    }

    #[test]
    fn scale_check_skipped_without_mcap() {
        let mut r = base_row();
        r.revenue = Some(1e10);
        // eps coherence breaks with this fake revenue? No — eps check uses
        // np/shares, untouched. No mcap → no scale verdict.
        assert!(gate1(&r, Some(90), None).blocks.is_empty(), "no mcap → no scale verdict");
    }

    #[test]
    fn quarter_duration_out_of_range_blocks() {
        let r = base_row();
        assert!(gate1(&r, Some(84), None)
            .blocks
            .iter()
            .any(|b| matches!(b, GateBlock::QuarterDurationOutOfRange(84))));
        assert!(gate1(&r, Some(96), None)
            .blocks
            .iter()
            .any(|b| matches!(b, GateBlock::QuarterDurationOutOfRange(96))));
        assert!(gate1(&r, Some(85), None).blocks.is_empty());
        assert!(gate1(&r, Some(95), None).blocks.is_empty());
    }

    #[test]
    fn annual_duration_out_of_range_blocks() {
        let mut r = base_row();
        r.fiscal_quarter = "FY".into();
        assert!(gate1(&r, Some(180), None)
            .blocks
            .iter()
            .any(|b| matches!(b, GateBlock::AnnualDurationOutOfRange(180))));
        assert!(gate1(&r, Some(365), None).blocks.is_empty());
    }

    #[test]
    fn missing_duration_does_not_block() {
        // Duration unknown → cannot judge; other gates still apply.
        assert!(gate1(&base_row(), None, None).blocks.is_empty());
    }
}
