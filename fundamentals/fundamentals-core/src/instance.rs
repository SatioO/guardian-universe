//! Instance-level metadata extraction for SEBI `in-capmkt` Integrated-Filing
//! XBRL. NEW in the extracted crate (not vendored from the app).
//!
//! The BSE discovery feed (`Corp_FinanceResult_ng`) exposes only an
//! unconfirmed `quarter_code` grammar (SOURCE-CONTRACT.md §1.1: "do not build
//! on this until confirmed"), so the producer derives period, basis, audit
//! status and identity **from the instance itself** — every in-capmkt filing
//! self-describes via `OneD`/`FourD`/`OneI` contexts and identity facts
//! (`ISIN`, `Symbol`, `ScripCode`, `NatureOfReportStandaloneConsolidated`,
//! `WhetherResultsAreAuditedOrUnaudited`).
//!
//! Sector is classified by **fact fingerprint** (the same signals the app's
//! per-sector builders key on), because BSE `/XBRLFILES/` paths do not carry
//! the NSE-style `INTEGRATED_FILING_<SECTOR>_` prefix.

use crate::types::{SectorKind, StatementBasis};

/// Everything the producer needs to know about one instance before building rows.
#[derive(Debug, Clone, Default)]
pub struct InstanceInfo {
    pub isin: Option<String>,
    pub symbol: Option<String>,
    pub scrip_code: Option<String>,
    pub company_name: Option<String>,
    /// `OneD` (current quarter) start/end ISO dates, if the context exists.
    pub quarter_start: Option<String>,
    pub quarter_end: Option<String>,
    /// `FourD` (full FY) start/end ISO dates, if the context exists.
    pub fy_start: Option<String>,
    pub fy_end: Option<String>,
    /// `OneI` instant date (balance-sheet snapshot), if present.
    pub instant: Option<String>,
    /// From `NatureOfReportStandaloneConsolidated` (OneD). None if absent.
    pub basis: Option<StatementBasis>,
    /// From `WhetherResultsAreAuditedOrUnaudited` (OneD).
    pub is_audited: bool,
    /// Fact-fingerprint sector classification. None = no recognisable sentinel.
    pub sector_kind: Option<SectorKind>,
}

impl InstanceInfo {
    /// Inclusive day count of the `OneD` context (e.g. 2026-01-01→2026-03-31 = 90).
    pub fn quarter_duration_days(&self) -> Option<i64> {
        duration_days_inclusive(self.quarter_start.as_deref()?, self.quarter_end.as_deref()?)
    }

    /// Inclusive day count of the `FourD` context (~365/366 for a full FY).
    pub fn fy_duration_days(&self) -> Option<i64> {
        duration_days_inclusive(self.fy_start.as_deref()?, self.fy_end.as_deref()?)
    }
}

/// Parse an in-capmkt instance and extract [`InstanceInfo`].
/// Returns `Err` only on structurally malformed XML.
pub fn extract_instance_info(xml: &str) -> Result<InstanceInfo, String> {
    let doc = roxmltree::Document::parse(xml)
        .map_err(|e| format!("instance XML parse error: {e}"))?;

    // ── Context periods ───────────────────────────────────────────────────────
    let context_dates = |id: &str| -> (Option<String>, Option<String>, Option<String>) {
        let ctx = doc.descendants().find(|n| {
            n.is_element() && n.tag_name().name() == "context" && n.attribute("id") == Some(id)
        });
        let Some(ctx) = ctx else { return (None, None, None) };
        let text_of = |name: &str| -> Option<String> {
            ctx.descendants()
                .find(|n| n.is_element() && n.tag_name().name() == name)
                .and_then(|n| n.text())
                .map(|t| t.trim().to_string())
        };
        (text_of("startDate"), text_of("endDate"), text_of("instant"))
    };

    let (q_start, q_end, _) = context_dates("OneD");
    let (fy_start, fy_end, _) = context_dates("FourD");
    let (_, _, instant) = context_dates("OneI");

    // ── String facts (first match on element local-name + contextRef) ────────
    let string_fact = |name: &str, ctx: &str| -> Option<String> {
        doc.descendants()
            .find(|n| {
                n.is_element()
                    && n.tag_name().name() == name
                    && n.attribute("contextRef") == Some(ctx)
            })
            .and_then(|n| n.text())
            .map(|t| t.trim().to_string())
            .filter(|s| !s.is_empty())
    };

    let has_fact = |name: &str| -> bool {
        doc.descendants().any(|n| {
            n.is_element()
                && n.tag_name().name() == name
                && matches!(n.attribute("contextRef"), Some("OneD") | Some("FourD"))
                && n.text().map(|t| !t.trim().is_empty()).unwrap_or(false)
        })
    };

    let basis = string_fact("NatureOfReportStandaloneConsolidated", "OneD")
        .or_else(|| string_fact("NatureOfReportStandaloneConsolidated", "FourD"))
        .and_then(|s| match s.as_str() {
            "Consolidated" => Some(StatementBasis::Consolidated),
            "Standalone" => Some(StatementBasis::Standalone),
            _ => None,
        });

    let is_audited = string_fact("WhetherResultsAreAuditedOrUnaudited", "OneD")
        .or_else(|| string_fact("WhetherResultsAreAuditedOrUnaudited", "FourD"))
        .map(|s| s == "Audited")
        .unwrap_or(false);

    // ── Sector fingerprint ────────────────────────────────────────────────────
    // Insurance: premium sentinels (life or general).
    // Bank:      InterestEarned + InterestExpended, no RevenueFromOperations.
    // NBFC:      RevenueFromOperations + NBFC-only signals (impairment / interest earned).
    // General:   RevenueFromOperations without financial-sector signals.
    let sector_kind = if has_fact("NetPremiumIncome") || has_fact("NetPremiumWritten") {
        Some(SectorKind::Insurance)
    } else if has_fact("InterestEarned") && has_fact("InterestExpended") && !has_fact("RevenueFromOperations") {
        Some(SectorKind::Bank)
    } else if has_fact("RevenueFromOperations")
        && (has_fact("ImpairmentOnFinancialInstruments") || has_fact("InterestEarned"))
    {
        Some(SectorKind::Nbfc)
    } else if has_fact("RevenueFromOperations") {
        Some(SectorKind::General)
    } else {
        None
    };

    Ok(InstanceInfo {
        isin: string_fact("ISIN", "OneD").or_else(|| string_fact("ISIN", "FourD")),
        symbol: string_fact("Symbol", "OneD").or_else(|| string_fact("Symbol", "FourD")),
        scrip_code: string_fact("ScripCode", "OneD").or_else(|| string_fact("ScripCode", "FourD")),
        company_name: string_fact("NameOfTheCompany", "OneD")
            .or_else(|| string_fact("NameOfTheCompany", "FourD")),
        quarter_start: q_start,
        quarter_end: q_end,
        fy_start,
        fy_end,
        instant,
        basis,
        is_audited,
        sector_kind,
    })
}

/// Inclusive day count between two ISO dates ("YYYY-MM-DD").
/// `2026-01-01 → 2026-03-31` = 90. Returns None on unparseable input.
pub fn duration_days_inclusive(start: &str, end: &str) -> Option<i64> {
    Some(days_from_civil(end)? - days_from_civil(start)? + 1)
}

/// Days since 1970-01-01 for an ISO date (Howard Hinnant's civil-days algorithm).
fn days_from_civil(iso: &str) -> Option<i64> {
    let mut it = iso.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let d: i64 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // Mar=0 .. Feb=11
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    Some(era * 146097 + doe - 719468)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BSE_TTK: &str = include_str!("../fixtures/bse-integrated-ttkprestige-517506.xml");
    const RIL: &str = include_str!("../fixtures/nse-integrated-reliance.xml");
    const HDFC: &str = include_str!("../fixtures/nse-integrated-hdfcbank.xml");
    const BAJ: &str = include_str!("../fixtures/nse-integrated-bajfinance.xml");

    #[test]
    fn ttk_identity_extracted() {
        let info = extract_instance_info(BSE_TTK).unwrap();
        assert_eq!(info.isin.as_deref(), Some("INE690A01028"));
        assert_eq!(info.symbol.as_deref(), Some("TTKPRESTIG"));
        assert_eq!(info.scrip_code.as_deref(), Some("517506"));
        assert_eq!(info.company_name.as_deref(), Some("TTK PRESTIGE LIMITED"));
    }

    #[test]
    fn ttk_contexts_extracted() {
        let info = extract_instance_info(BSE_TTK).unwrap();
        assert_eq!(info.quarter_start.as_deref(), Some("2026-01-01"));
        assert_eq!(info.quarter_end.as_deref(), Some("2026-03-31"));
        assert_eq!(info.fy_start.as_deref(), Some("2025-04-01"));
        assert_eq!(info.fy_end.as_deref(), Some("2026-03-31"));
        assert_eq!(info.instant.as_deref(), Some("2026-03-31"));
        assert_eq!(info.quarter_duration_days(), Some(90));
        assert_eq!(info.fy_duration_days(), Some(365));
    }

    #[test]
    fn ttk_basis_audited_sector() {
        let info = extract_instance_info(BSE_TTK).unwrap();
        assert_eq!(info.basis, Some(StatementBasis::Standalone));
        assert!(info.is_audited);
        assert_eq!(info.sector_kind, Some(SectorKind::General));
    }

    #[test]
    fn fingerprint_general_for_reliance() {
        let info = extract_instance_info(RIL).unwrap();
        assert_eq!(info.sector_kind, Some(SectorKind::General));
    }

    #[test]
    fn fingerprint_bank_for_hdfc() {
        let info = extract_instance_info(HDFC).unwrap();
        assert_eq!(info.sector_kind, Some(SectorKind::Bank));
    }

    #[test]
    fn fingerprint_nbfc_for_bajfinance() {
        let info = extract_instance_info(BAJ).unwrap();
        assert_eq!(info.sector_kind, Some(SectorKind::Nbfc));
    }

    #[test]
    fn fingerprint_insurance_for_sbilife_and_icicigi() {
        // Life (NetPremiumIncome) and general (NetPremiumWritten) insurers
        // must both fingerprint as Insurance — Phase 3 routes them to the
        // vendored insurance builder instead of skipping.
        let sbilife = include_str!("../fixtures/nse-integrated-sbilife.xml");
        let icicigi = include_str!("../fixtures/nse-integrated-icicigi.xml");
        assert_eq!(
            extract_instance_info(sbilife).unwrap().sector_kind,
            Some(SectorKind::Insurance)
        );
        assert_eq!(
            extract_instance_info(icicigi).unwrap().sector_kind,
            Some(SectorKind::Insurance)
        );
    }

    #[test]
    fn bse_instance_parses_with_proven_parser() {
        // End-to-end: the BSE `.xml` twin feeds the app parser unchanged.
        let info = extract_instance_info(BSE_TTK).unwrap();
        let meta = crate::xbrl_integrated::meta_from_iso_period_end(
            info.quarter_end.as_deref().unwrap(),
            info.is_audited,
            info.sector_kind.unwrap(),
            info.basis.unwrap(),
        );
        let (q, fy, val) = crate::xbrl_integrated::parse_integrated_xbrl(BSE_TTK, &meta).unwrap();
        let q = q.expect("OneD quarter must parse");
        let fy = fy.expect("FourD annual must parse");
        // RevenueFromOperations OneD = 6795700000 → 679.57 cr
        assert!((q.core.revenue_equiv.unwrap() - 679.57).abs() < 0.01);
        // ProfitLossForPeriod OneD = 507900000 → 50.79 cr
        assert!((q.core.net_profit.unwrap() - 50.79).abs() < 0.01);
        assert!((q.core.eps.unwrap() - 3.71).abs() < 0.001);
        assert_eq!(q.fiscal_quarter, "Q4");
        assert_eq!(q.period_end, "2026-03-31");
        // FourD revenue = 27726900000 → 2772.69 cr
        assert!((fy.core.revenue_equiv.unwrap() - 2772.69).abs() < 0.01);
        assert!((fy.core.eps.unwrap() - 13.54).abs() < 0.001);
        // Balance sheet: Equity 19932800000 → 1993.28 cr; shares = 137000000/1
        assert!((val.equity_cr.unwrap() - 1993.28).abs() < 0.01);
        assert!((val.shares_outstanding.unwrap() - 137_000_000.0).abs() < 1.0);
        // Zero borrowings → None
        assert!(val.total_debt_cr.is_none());
        assert!((val.cash_cr.unwrap() - 31.32).abs() < 0.01);
    }

    #[test]
    fn duration_math() {
        assert_eq!(duration_days_inclusive("2026-01-01", "2026-03-31"), Some(90));
        assert_eq!(duration_days_inclusive("2025-04-01", "2026-03-31"), Some(365));
        assert_eq!(duration_days_inclusive("2025-04-01", "2025-06-30"), Some(91));
        assert_eq!(duration_days_inclusive("bogus", "2026-03-31"), None);
    }

    #[test]
    fn malformed_instance_is_err() {
        assert!(extract_instance_info("<<<").is_err());
    }

    #[test]
    fn empty_instance_yields_defaults() {
        let info = extract_instance_info("<xbrl/>").unwrap();
        assert!(info.isin.is_none());
        assert!(info.sector_kind.is_none());
        assert!(info.quarter_duration_days().is_none());
    }
}
