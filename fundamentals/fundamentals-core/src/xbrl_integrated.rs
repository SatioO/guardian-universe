//! Pure Rust parser for Integrated Filing XBRL (SEBI in-capmkt taxonomy).
//!
//! Vendored VERBATIM (minus the NSE fetch path — see `lib.rs` provenance note)
//! from traderview `src-tauri/src/fundamentals/xbrl_integrated.rs`
//! @ ed0eddb6d9541e383844382666f5a5c0d18cef5e.
//!
//! Extracts BOTH the quarter (`contextRef="OneD"`) and the full FY
//! (`contextRef="FourD"`) from a single integrated filing XML for the
//! general / bank / nbfc / insurance sectors. The same taxonomy is served by
//! both exchanges (NSE `nsearchives` `_WEB.xml` and BSE `/XBRLFILES/*.xml`),
//! so this parser is exchange-agnostic.
//!
//! # Context naming
//! The in-capmkt taxonomy uses:
//! - `OneD`   — the current quarter (duration)
//! - `FourD`  — the full fiscal year (duration)
//! - `OneI`   — instant at period end (balance sheet)
//!
//! # Scale
//! Monetary values are in **rupees**; we divide by 1e7 to produce ₹ crore.
//! EPS and percentage values are already per-share / raw and are NOT scaled.

use crate::types::{
    BankLines, GeneralLines, InsuranceLines, MarginKind, NbfcLines, NormalizedCore,
    ResultPeriod, SectorKind, SectorLineItems, StatementBasis,
};

// ── Public types ──────────────────────────────────────────────────────────────

/// Caller-supplied metadata for a single integrated filing.
/// Period/audit/sector fields come from the filing list (or, in the BSE flow,
/// from [`crate::instance::extract_instance_info`]), NOT the XBRL fact scan.
pub struct IntegratedMeta {
    /// ISO date of the quarter-end, e.g. `"2026-03-31"`.
    pub period_end: String,
    /// Fiscal year label, e.g. `"FY26"`.
    pub fy_label: String,
    /// Quarter label, e.g. `"Q4"`.
    pub quarter_label: String,
    /// Whether the results are marked audited by the exchange.
    pub is_audited: bool,
    /// Sector (General / Bank / Nbfc / Insurance).
    pub sector_kind: SectorKind,
    /// Whether this filing is Consolidated or Standalone.
    pub basis: StatementBasis,
}

/// Raw valuation data extracted from a single (annual) integrated filing.
/// All monetary fields are in ₹ crore; shares is a raw count (not scaled).
/// All fields are Option — missing XML elements → None.
#[derive(Debug, Clone, Default)]
pub struct ValuationRaw {
    /// Face value per share in ₹ (unscaled, e.g. 10.0 for Rs 10 face value).
    pub face_value: Option<f64>,
    /// Total equity shares outstanding (raw count, NOT crore).
    /// Derived as EquityShareCapital(OneI) ÷ FaceValue(OneD).
    pub shares_outstanding: Option<f64>,
    /// Equity attributable to owners in ₹ crore (balance sheet, OneI context).
    pub equity_cr: Option<f64>,
    /// Total debt in ₹ crore (BorrowingsCurrent + BorrowingsNoncurrent for general;
    /// Borrowings for bank/nbfc). None if no borrowings in XBRL.
    pub total_debt_cr: Option<f64>,
    /// Cash & cash equivalents in ₹ crore (balance sheet, OneI context).
    pub cash_cr: Option<f64>,
    /// EBITDA in ₹ crore (PBT + FinanceCosts + Depreciation from FourD context).
    /// None for Bank / NBFC / Insurance.
    pub ebitda_cr: Option<f64>,
}

/// Result of `select_integrated_filings`: the chosen (meta, url) pairs for
/// quarterly and annual periods.  A March filing appears in **both** vecs —
/// its OneD is the Q4 quarter result and its FourD is the full-year result.
pub struct IntegratedSelection {
    /// Up to `n_quarters` most-recent distinct qe_Dates, newest-first.
    pub quarters: Vec<(IntegratedMeta, String)>,
    /// All 31-MAR-* qe_Dates (one per FY), newest-first, up to 5.
    pub annuals: Vec<(IntegratedMeta, String)>,
}

// ── Integrated filing-list selection ─────────────────────────────────────────

/// Parse the NSE `integrated-filing-results` JSON (`{"data":[…]}`), select
/// the right filings, and return an [`IntegratedSelection`].
///
/// **Keep** only records whose `xbrl` URL contains `INTEGRATED_FILING_` and
/// one of the known financial prefixes: `INDAS`, `BANKING`, `NBFC_INDAS`,
/// `LI`, `GI`.  `GOVERNANCE` filings and records with missing/invalid xbrl
/// are skipped.
///
/// **Dedup** by (`qe_Date`, `consolidated`) keeping the highest `seq_Id`
/// (latest revision). Then per `qe_Date`, prefer the `Consolidated` filing
/// over `Standalone` (insurers may be Standalone-only).
///
/// **quarters** = the `n_quarters` most-recent distinct qe_Dates.
/// **annuals**  = the `qe_Date == 31-MAR-*` filings (one per FY), ≤5.
///
/// Panic-safe: all JSON access via `.get()` / `.and_then()`.
pub fn select_integrated_filings(
    list_json: &str,
    n_quarters: usize,
) -> Result<IntegratedSelection, String> {
    let root = serde_json::from_str::<serde_json::Value>(list_json)
        .map_err(|e| format!("integrated filing list JSON parse: {e}"))?;

    let records = root
        .get("data")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "integrated filing list: expected {\"data\":[…]}".to_string())?;

    // ── 1. Filter to financial filings only ───────────────────────────────────
    // Known financial prefixes (after "INTEGRATED_FILING_").
    // GOVERNANCE is explicitly excluded here.
    struct Raw {
        qe_date_str: String, // "31-MAR-2026" as-is
        consolidated: Option<String>, // None | "Consolidated" | "Standalone"
        seq_id: i64,
        is_audited: bool,
        sector_kind: SectorKind,
        xbrl_url: String,
    }

    // ── 2. Parse + dedup by (qe_Date, consolidated) keeping max seq_Id ───────
    // Single pass over `records`: parse each financial filing into a `Raw`, then
    // keep only the highest-seq_Id record per (qe_Date, consolidated) key (latest
    // revision wins).
    use std::collections::HashMap;
    let mut dedup: HashMap<(String, String), Raw> = HashMap::new();
    for r in records.iter().filter_map(|r| {
        let xbrl = r.get("xbrl")?.as_str()?;
        // Must contain INTEGRATED_FILING_ with a known financial prefix.
        let after = xbrl
            .find("INTEGRATED_FILING_")
            .and_then(|i| xbrl.get(i + "INTEGRATED_FILING_".len()..))?;
        let sector_kind = integrated_sector_from_prefix(after)?; // None → skip
        let qe_date_str = r.get("qe_Date")?.as_str()?.to_string();
        let consolidated = r
            .get("consolidated")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let seq_id: i64 = r
            .get("seq_Id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let is_audited = r
            .get("audited")
            .and_then(|v| v.as_str())
            .map(|s| s == "Audited")
            .unwrap_or(false);
        Some(Raw {
            qe_date_str,
            consolidated,
            seq_id,
            is_audited,
            sector_kind,
            xbrl_url: xbrl.to_string(),
        })
    }) {
        let consol_key = r.consolidated.clone().unwrap_or_default();
        let key = (r.qe_date_str.clone(), consol_key);
        let seq = r.seq_id;
        let existing_seq = dedup.get(&key).map(|e| e.seq_id).unwrap_or(i64::MIN);
        if seq > existing_seq {
            dedup.insert(key, r);
        }
    }

    // ── 3. Collect unique qe_Dates sorted newest-first ────────────────────────
    // For each qe_Date, prefer Consolidated; fallback to Standalone (insurers).
    let mut by_date: HashMap<String, Vec<Raw>> = HashMap::new();
    for raw in dedup.into_values() {
        by_date.entry(raw.qe_date_str.clone()).or_default().push(raw);
    }

    // Sort qe_Dates newest-first using a comparable integer key.
    let mut sorted_dates: Vec<String> = by_date.keys().cloned().collect();
    sorted_dates.sort_by(|a, b| {
        qe_date_sort_key(b).cmp(&qe_date_sort_key(a)) // reversed = newest first
    });

    // For each date pick the best filing (Consolidated preferred). Basis is
    // recorded here so it can be threaded into each ResultPeriod.
    struct Chosen {
        qe_date_str: String,
        is_audited: bool,
        sector_kind: SectorKind,
        basis: StatementBasis,
        xbrl_url: String,
    }

    let chosen_by_date: Vec<Chosen> = sorted_dates
        .iter()
        .filter_map(|date| {
            let candidates = by_date.get(date)?;
            // Prefer Consolidated; fall back to Standalone if no Consolidated.
            let picked = candidates
                .iter()
                .find(|r| r.consolidated.as_deref() == Some("Consolidated"))
                .or_else(|| candidates.iter().find(|r| r.consolidated.as_deref() == Some("Standalone")))
                .or_else(|| candidates.first())?;
            let basis = if picked.consolidated.as_deref() == Some("Consolidated") {
                StatementBasis::Consolidated
            } else {
                StatementBasis::Standalone
            };
            Some(Chosen {
                qe_date_str: date.clone(),
                is_audited: picked.is_audited,
                sector_kind: picked.sector_kind,
                basis,
                xbrl_url: picked.xbrl_url.clone(),
            })
        })
        .collect();

    // ── 4. Build quarters and annuals ─────────────────────────────────────────
    let quarters: Vec<(IntegratedMeta, String)> = chosen_by_date
        .iter()
        .take(n_quarters)
        .filter_map(|c| {
            let meta = build_integrated_meta(&c.qe_date_str, c.is_audited, c.sector_kind, c.basis)?;
            Some((meta, c.xbrl_url.clone()))
        })
        .collect();

    // Annuals = 31-MAR-* dates only, newest-first, ≤5.
    let annuals: Vec<(IntegratedMeta, String)> = chosen_by_date
        .iter()
        .filter(|c| c.qe_date_str.starts_with("31-MAR-"))
        .take(5)
        .filter_map(|c| {
            let meta = build_integrated_meta(&c.qe_date_str, c.is_audited, c.sector_kind, c.basis)?;
            Some((meta, c.xbrl_url.clone()))
        })
        .collect();

    Ok(IntegratedSelection { quarters, annuals })
}

/// Detect sector from the INTEGRATED_FILING_ prefix segment.
/// Returns `None` for GOVERNANCE (skip) and unknown prefixes (skip).
/// Returns `Some(SectorKind)` for INDAS, BANKING, NBFC_INDAS, LI, GI.
fn integrated_sector_from_prefix(after_marker: &str) -> Option<SectorKind> {
    if after_marker.starts_with("NBFC_INDAS") {
        Some(SectorKind::Nbfc)
    } else if after_marker.starts_with("BANKING") {
        Some(SectorKind::Bank)
    } else if after_marker.starts_with("LI_") || after_marker.starts_with("GI_") {
        Some(SectorKind::Insurance)
    } else if after_marker.starts_with("INDAS") {
        Some(SectorKind::General)
    } else {
        // GOVERNANCE and any other unknown prefix → skip
        None
    }
}

/// Convert "31-MAR-2026" into a sortable u32 (yyyymmdd).
/// Returns 0 on any parse failure (safe: only affects ordering).
fn qe_date_sort_key(qe: &str) -> u32 {
    let parts: Vec<&str> = qe.split('-').collect();
    if parts.len() < 3 {
        return 0;
    }
    let dd: u32 = parts[0].parse().unwrap_or(0);
    let mm = month_abbr_to_num_integrated(parts[1]).unwrap_or(0);
    let yyyy: u32 = parts[2].parse().unwrap_or(0);
    yyyy * 10000 + mm * 100 + dd
}

/// Build `IntegratedMeta` from a qe_Date string ("31-MAR-2026").
/// Returns `None` on unparseable dates.
pub fn build_integrated_meta(
    qe_date_str: &str,
    is_audited: bool,
    sector_kind: SectorKind,
    basis: StatementBasis,
) -> Option<IntegratedMeta> {
    // period_end: "31-MAR-2026" → "2026-03-31"
    let parts: Vec<&str> = qe_date_str.split('-').collect();
    if parts.len() < 3 {
        return None;
    }
    let dd = parts[0];
    let mon = parts[1];
    let yyyy_str = parts[2];
    let yyyy: u32 = yyyy_str.parse().ok()?;
    let mm = month_abbr_to_num_integrated(mon)?;
    let period_end = format!("{yyyy}-{mm:02}-{dd:0>2}");

    Some(meta_from_iso_period_end(&period_end, is_audited, sector_kind, basis))
}

/// Build `IntegratedMeta` from an ISO `period_end` ("2026-03-31").
/// Used by the BSE flow where period_end is derived from the instance's own
/// `OneD` context rather than an exchange list row.
pub fn meta_from_iso_period_end(
    period_end: &str,
    is_audited: bool,
    sector_kind: SectorKind,
    basis: StatementBasis,
) -> IntegratedMeta {
    let mm: u32 = period_end
        .split('-')
        .nth(1)
        .and_then(|m| m.parse().ok())
        .unwrap_or(0);
    let yyyy: u32 = period_end
        .split('-')
        .next()
        .and_then(|y| y.parse().ok())
        .unwrap_or(0);

    // Indian FY: Apr–Mar spans two calendar years.
    // The FY label is determined by the END of the fiscal year:
    //   Jan/Feb/Mar → FY ends this year         e.g. MAR-2026 → FY26
    //   Apr–Dec     → FY ends next year          e.g. DEC-2025 → FY26
    let fy_year = if mm >= 4 { yyyy + 1 } else { yyyy };
    let fy_label = format!("FY{:02}", fy_year % 100);

    // Quarter label from month:
    //   Jun(6) → Q1,  Sep(9) → Q2,  Dec(12) → Q3,  Mar(3) → Q4
    let quarter_label = match mm {
        3  => "Q4",
        6  => "Q1",
        9  => "Q2",
        12 => "Q3",
        _  => "Q?",
    }
    .to_string();

    IntegratedMeta {
        period_end: period_end.to_string(),
        fy_label,
        quarter_label,
        is_audited,
        sector_kind,
        basis,
    }
}

fn month_abbr_to_num_integrated(mon: &str) -> Option<u32> {
    match mon {
        "JAN" | "Jan" => Some(1),
        "FEB" | "Feb" => Some(2),
        "MAR" | "Mar" => Some(3),
        "APR" | "Apr" => Some(4),
        "MAY" | "May" => Some(5),
        "JUN" | "Jun" => Some(6),
        "JUL" | "Jul" => Some(7),
        "AUG" | "Aug" => Some(8),
        "SEP" | "Sep" => Some(9),
        "OCT" | "Oct" => Some(10),
        "NOV" | "Nov" => Some(11),
        "DEC" | "Dec" => Some(12),
        _ => None,
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Parse ONE integrated filing → `(quarter from OneD, full-FY from FourD, valuation raw)`.
///
/// Either period side may be `None` if the relevant context yields no recognisable
/// revenue fact (which can happen for filings that omit certain contexts).
/// Returns `Err` only if the XML is structurally malformed.
pub fn parse_integrated_xbrl(
    xml: &str,
    meta: &IntegratedMeta,
) -> Result<(Option<ResultPeriod>, Option<ResultPeriod>, ValuationRaw), String> {
    let doc = roxmltree::Document::parse(xml)
        .map_err(|e| format!("Integrated XBRL parse error: {e}"))?;

    // Helper: find first element with the given local name and a specific
    // contextRef, then parse its text as f64.  Returns None on any miss.
    // Panic-safe: uses .find() / .and_then() with no unwrap/index.
    let fact = |name: &str, ctx: &str| -> Option<f64> {
        doc.descendants()
            .find(|n| {
                n.is_element()
                    && n.tag_name().name() == name
                    && n.attribute("contextRef") == Some(ctx)
            })
            .and_then(|n| n.text())
            .and_then(|t| t.trim().parse::<f64>().ok())
    };

    // Monetary scale: rupees → crore (÷ 1e7).
    let cr = |name: &str, ctx: &str| -> Option<f64> { fact(name, ctx).map(|v| v / 1e7) };

    // Build quarter (OneD) and FY (FourD) periods.
    let quarter = build_period(&fact, &cr, meta, "OneD", false)?;
    let annual  = build_period(&fact, &cr, meta, "FourD", true)?;

    if quarter.is_none() && annual.is_none() {
        return Err("Integrated XBRL: no usable facts in either OneD or FourD context".to_string());
    }

    // ── Extract valuation raw fields (balance sheet from OneI, EBITDA from FourD) ───
    let val_raw = extract_valuation_raw(&fact, &cr, meta.sector_kind);

    Ok((quarter, annual, val_raw))
}

/// Extract raw valuation data from the XBRL document using the `fact`/`cr` helpers.
///
/// Balance-sheet instant items use context `OneI` (current period end snapshot).
/// EBITDA is derived from the FourD (full-year) P&L context.
/// All fields are Option — missing elements → None. No unwrap, no panic.
fn extract_valuation_raw<F, C>(
    fact: &F,
    cr: &C,
    sector_kind: SectorKind,
) -> ValuationRaw
where
    F: Fn(&str, &str) -> Option<f64>,
    C: Fn(&str, &str) -> Option<f64>,
{
    // FaceValue: stored in OneD (duration) context in the in-capmkt taxonomy.
    // Try OneD first, fall back to FourD.
    let face_value = fact("FaceValueOfEquityShareCapital", "OneD")
        .or_else(|| fact("FaceValueOfEquityShareCapital", "FourD"));

    // EquityShareCapital: available in OneI (instant balance sheet).
    let equity_share_capital = fact("EquityShareCapital", "OneI");

    // Shares outstanding = EquityShareCapital(rupees) ÷ face_value.
    // Guard: face_value must be > 0; result is a raw count (NOT crore-scaled).
    let shares_outstanding = match (equity_share_capital, face_value) {
        (Some(esc), Some(fv)) if fv > 0.0 => Some(esc / fv),
        _ => None,
    };

    // Equity attributable to owners of parent (balance sheet, OneI).
    // Primary: EquityAttributableToOwnersOfParent; fallback: Equity.
    // Guard 0-value (can appear as an empty placeholder).
    let equity_attr = fact("EquityAttributableToOwnersOfParent", "OneI")
        .filter(|&v| v != 0.0)
        .or_else(|| fact("Equity", "OneI").filter(|&v| v != 0.0));
    let equity_cr = equity_attr.map(|v| v / 1e7);

    // Debt: General sector uses BorrowingsCurrent + BorrowingsNoncurrent.
    //       Bank / NBFC / Insurance use a single `Borrowings` element.
    let total_debt_cr = match sector_kind {
        SectorKind::General => {
            let curr    = cr("BorrowingsCurrent", "OneI");
            let noncurr = cr("BorrowingsNoncurrent", "OneI");
            match (curr, noncurr) {
                (Some(a), Some(b)) => Some(a + b),
                (Some(a), None)    => Some(a),
                (None,    Some(b)) => Some(b),
                (None,    None)    => None,
            }
        }
        _ => cr("Borrowings", "OneI"),
    };
    // Treat 0 debt as None (no meaningful debt on balance sheet).
    let total_debt_cr = total_debt_cr.filter(|&v| v != 0.0);

    // Cash: CashAndCashEquivalents (OneI) for General / NBFC.
    // Banks use CashAndBalancesWithReserveBankOfIndia — but we omit it
    // here since EV metrics are not computed for banks anyway.
    let cash_cr = cr("CashAndCashEquivalents", "OneI").filter(|&v| v != 0.0);

    // EBITDA: only meaningful for General sector (not Bank/NBFC/Insurance).
    let ebitda_cr = match sector_kind {
        SectorKind::General => {
            let pbt  = cr("ProfitBeforeTax", "FourD");
            let fc   = cr("FinanceCosts", "FourD");
            let dep  = cr("DepreciationDepletionAndAmortisationExpense", "FourD");
            match (pbt, fc, dep) {
                (Some(p), Some(f), Some(d)) => Some(p + f + d),
                _ => None,
            }
        }
        _ => None, // EV/EBITDA N/A for financials
    };

    ValuationRaw {
        face_value,
        shares_outstanding,
        equity_cr,
        total_debt_cr,
        cash_cr,
        ebitda_cr,
    }
}

// ── Internal builders ─────────────────────────────────────────────────────────

/// Build a single [`ResultPeriod`] for the given context (`OneD` or `FourD`).
/// Returns `None` if the context has no recognisable revenue fact.
fn build_period<F, C>(
    fact: &F,
    cr: &C,
    meta: &IntegratedMeta,
    ctx: &str,
    is_annual: bool,
) -> Result<Option<ResultPeriod>, String>
where
    F: Fn(&str, &str) -> Option<f64>,
    C: Fn(&str, &str) -> Option<f64>,
{
    // Sector-specific revenue sentinel to determine if this context is present.
    let has_facts = match meta.sector_kind {
        SectorKind::Bank      => cr("InterestEarned", ctx).is_some(),
        SectorKind::Insurance => {
            // Life: NetPremiumIncome; General: NetPremiumWritten
            cr("NetPremiumIncome", ctx).is_some() || cr("NetPremiumWritten", ctx).is_some()
        }
        _ => cr("RevenueFromOperations", ctx).is_some(),
    };

    if !has_facts {
        return Ok(None);
    }

    let period = match meta.sector_kind {
        SectorKind::Bank    => build_bank(fact, cr, meta, ctx, is_annual),
        SectorKind::Nbfc    => build_nbfc(fact, cr, meta, ctx, is_annual),
        SectorKind::Insurance => build_insurance_general(fact, cr, meta, ctx, is_annual),
        _                   => build_general(fact, cr, meta, ctx, is_annual),
    };

    Ok(Some(period))
}

/// Shared helper for assembling `period_end / period_label / fiscal_quarter`.
fn period_labels(meta: &IntegratedMeta, is_annual: bool) -> (String, String, String) {
    if is_annual {
        (
            meta.period_end.clone(),
            meta.fy_label.clone(),
            "FY".to_string(),
        )
    } else {
        (
            meta.period_end.clone(),
            format!("{} {}", meta.quarter_label, meta.fy_label),
            meta.quarter_label.clone(),
        )
    }
}

// ── General sector ────────────────────────────────────────────────────────────

fn build_general<F, C>(
    fact: &F,
    cr: &C,
    meta: &IntegratedMeta,
    ctx: &str,
    is_annual: bool,
) -> ResultPeriod
where
    F: Fn(&str, &str) -> Option<f64>,
    C: Fn(&str, &str) -> Option<f64>,
{
    // ── Core fields ──────────────────────────────────────────────────────────
    let revenue_equiv = cr("RevenueFromOperations", ctx);
    let other_income  = cr("OtherIncome", ctx);
    let interest      = cr("FinanceCosts", ctx);
    let depreciation  = cr("DepreciationDepletionAndAmortisationExpense", ctx);
    let pbt           = cr("ProfitBeforeTax", ctx);

    // TaxExpense primary; fallback to CurrentTax + DeferredTax.
    let tax = cr("TaxExpense", ctx).or_else(|| {
        let ct = cr("CurrentTax", ctx);
        let dt = cr("DeferredTax", ctx);
        match (ct, dt) {
            (Some(a), Some(b)) => Some(a + b),
            (Some(a), None)    => Some(a),
            (None,    Some(b)) => Some(b),
            (None,    None)    => None,
        }
    });

    let net_profit = cr("ProfitLossForPeriod", ctx);

    // EPS: not scaled.  Try combined name first, then continuing-only.
    let eps = fact("BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations", ctx)
        .or_else(|| fact("BasicEarningsLossPerShareFromContinuingOperations", ctx));

    // ── Derived: operating profit & margin ───────────────────────────────────
    // operating_profit_equiv = PBT + interest + depreciation − other_income
    let operating_profit_equiv = match (pbt, interest, depreciation, other_income) {
        (Some(p), Some(i), Some(d), Some(o)) => Some(p + i + d - o),
        _ => None,
    };

    let margin_equiv_pct = match (operating_profit_equiv, revenue_equiv) {
        (Some(op), Some(rev)) if rev != 0.0 => Some(op / rev * 100.0),
        _ => None,
    };

    // ── GeneralLines ─────────────────────────────────────────────────────────
    let sector = SectorLineItems::General(GeneralLines {
        revenue_from_operations: revenue_equiv,
        total_expenses:          cr("Expenses", ctx),
        cost_of_materials:       cr("CostOfMaterialsConsumed", ctx),
        employee_expense:        cr("EmployeeBenefitExpense", ctx),
        operating_profit:        operating_profit_equiv,
        opm_pct:                 margin_equiv_pct,
    });

    let core = NormalizedCore {
        revenue_equiv,
        operating_profit_equiv,
        margin_equiv_pct,
        margin_kind: MarginKind::Opm,
        other_income,
        interest,
        depreciation,
        pbt,
        tax,
        net_profit,
        eps,
    };

    let (period_end, period_label, fiscal_quarter) = period_labels(meta, is_annual);

    ResultPeriod {
        period_end,
        period_label,
        fiscal_quarter,
        basis:       meta.basis,
        is_audited:  meta.is_audited,
        is_restated: false,
        core,
        sector,
    }
}

// ── Bank sector ───────────────────────────────────────────────────────────────

fn build_bank<F, C>(
    fact: &F,
    cr: &C,
    meta: &IntegratedMeta,
    ctx: &str,
    is_annual: bool,
) -> ResultPeriod
where
    F: Fn(&str, &str) -> Option<f64>,
    C: Fn(&str, &str) -> Option<f64>,
{
    // ── Bank core fields ─────────────────────────────────────────────────────
    let interest_earned = cr("InterestEarned", ctx);
    let revenue_equiv   = interest_earned; // bank revenue proxy

    let pbt = cr("ProfitLossFromOrdinaryActivitiesBeforeTax", ctx);
    let tax = cr("TaxExpense", ctx);

    // Primary: ProfitLossForThePeriod; fallback: AfterTax variant.
    let net_profit = cr("ProfitLossForThePeriod", ctx)
        .or_else(|| cr("ProfitLossFromOrdinaryActivitiesAfterTax", ctx));

    // EPS for banks — not scaled.
    let eps = fact("BasicEarningsPerShareAfterExtraordinaryItems", ctx);

    let other_income_bank = cr("OtherIncome", ctx);

    // ── BankLines ────────────────────────────────────────────────────────────
    let interest_expended = cr("InterestExpended", ctx);
    let total_income      = cr("Income", ctx);

    // NII = InterestEarned − InterestExpended.
    let net_interest_income = match (interest_earned, interest_expended) {
        (Some(ie), Some(ix)) => Some(ie - ix),
        _ => None,
    };

    let operating_expenses             = cr("OperatingExpenses", ctx);
    let pre_provision_operating_profit = cr("OperatingProfitBeforeProvisionAndContingencies", ctx);
    let provisions_and_contingencies   = cr("ProvisionsOtherThanTaxAndContingencies", ctx);

    // PercentageOfGrossNpa is a raw percentage — NOT scaled by 1e7.
    let gross_npa_pct = fact("PercentageOfGrossNpa", ctx);

    let sector = SectorLineItems::Bank(BankLines {
        total_income,
        interest_earned,
        other_income: other_income_bank,
        interest_expended,
        net_interest_income,
        nim_pct:     None, // not in XBRL
        operating_expenses,
        pre_provision_operating_profit,
        provisions_and_contingencies,
        gross_npa_pct,
        net_npa_pct: None, // not in XBRL
    });

    let core = NormalizedCore {
        revenue_equiv,
        operating_profit_equiv: None, // not meaningful for banks
        margin_equiv_pct:       None,
        margin_kind:            MarginKind::Nim,
        other_income:           other_income_bank,
        interest:               None, // not meaningful in bank general core
        depreciation:           None,
        pbt,
        tax,
        net_profit,
        eps,
    };

    let (period_end, period_label, fiscal_quarter) = period_labels(meta, is_annual);

    ResultPeriod {
        period_end,
        period_label,
        fiscal_quarter,
        basis:       meta.basis,
        is_audited:  meta.is_audited,
        is_restated: false,
        core,
        sector,
    }
}

// ── NBFC sector ───────────────────────────────────────────────────────────────

fn build_nbfc<F, C>(
    fact: &F,
    cr: &C,
    meta: &IntegratedMeta,
    ctx: &str,
    is_annual: bool,
) -> ResultPeriod
where
    F: Fn(&str, &str) -> Option<f64>,
    C: Fn(&str, &str) -> Option<f64>,
{
    // ── NBFC core fields ─────────────────────────────────────────────────────
    let revenue_equiv = cr("RevenueFromOperations", ctx);
    let pbt           = cr("ProfitBeforeTax", ctx);
    let tax           = cr("TaxExpense", ctx);
    let net_profit    = cr("ProfitLossForPeriod", ctx);

    // EPS: not scaled. Try combined name first, fallback to continuing-only.
    let eps = fact("BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations", ctx)
        .or_else(|| fact("BasicEarningsLossPerShareFromContinuingOperations", ctx));

    let finance_costs = cr("FinanceCosts", ctx);
    let other_income  = cr("OtherIncome", ctx);

    // ── NbfcLines ─────────────────────────────────────────────────────────────
    let interest_earned = cr("InterestEarned", ctx);
    let total_income    = cr("Income", ctx);
    let impairment_financial_instruments = cr("ImpairmentOnFinancialInstruments", ctx);

    // NII = InterestEarned − FinanceCosts; None if either absent.
    let net_interest_income = match (interest_earned, finance_costs) {
        (Some(ie), Some(fc)) => Some(ie - fc),
        _ => None,
    };

    let sector = SectorLineItems::Nbfc(NbfcLines {
        revenue_from_operations: revenue_equiv,
        total_income,
        finance_costs,
        net_interest_income,
        nim_pct:                        None, // not in XBRL
        impairment_financial_instruments,
        pre_provision_operating_profit: None, // not in XBRL
        gross_stage3_pct:               None, // not in XBRL
    });

    let core = NormalizedCore {
        revenue_equiv,
        operating_profit_equiv: None, // not applicable for NBFCs
        margin_equiv_pct:       None,
        margin_kind:            MarginKind::Spread,
        other_income,
        interest:               finance_costs,
        depreciation:           None,
        pbt,
        tax,
        net_profit,
        eps,
    };

    let (period_end, period_label, fiscal_quarter) = period_labels(meta, is_annual);

    ResultPeriod {
        period_end,
        period_label,
        fiscal_quarter,
        basis:       meta.basis,
        is_audited:  meta.is_audited,
        is_restated: false,
        core,
        sector,
    }
}

// ── Insurance sector (life + general) ────────────────────────────────────────

/// Insurance sector builder: handles both life (SBILIFE-style) and general
/// (ICICIGI-style) insurance by trying life element names first, then falling
/// back to general insurance element names.
///
/// Presence sentinel: `NetPremiumIncome` (life) OR `NetPremiumWritten` (general).
fn build_insurance_general<F, C>(
    fact: &F,
    cr: &C,
    meta: &IntegratedMeta,
    ctx: &str,
    is_annual: bool,
) -> ResultPeriod
where
    F: Fn(&str, &str) -> Option<f64>,
    C: Fn(&str, &str) -> Option<f64>,
{
    // ── Detect life vs general by element presence ────────────────────────────
    // Life: GrossPremiumIncome / NetPremiumIncome
    // General: GrossPremiumsWritten / NetPremiumWritten
    let is_life = cr("NetPremiumIncome", ctx).is_some();

    // ── Premium lines ─────────────────────────────────────────────────────────
    let gross_premium_income = if is_life {
        cr("GrossPremiumIncome", ctx)
    } else {
        cr("GrossPremiumsWritten", ctx)
    };

    let net_premium_income = if is_life {
        cr("NetPremiumIncome", ctx)
    } else {
        cr("NetPremiumWritten", ctx)
    };

    // ── Shared elements (same name in both life and general) ──────────────────
    let investment_income = cr("IncomeFromInvestmentsNet", ctx);
    let net_commission    = cr("NetCommission", ctx);

    // ── Benefits / claims ─────────────────────────────────────────────────────
    let benefits_paid = if is_life {
        cr("BenefitsPaidNet", ctx)
    } else {
        cr("IncurredClaims", ctx)
    };

    // ── Profit ────────────────────────────────────────────────────────────────
    let shareholders_net_profit = if is_life {
        cr("TransferredToShareholdersAccount", ctx)
    } else {
        cr("OperatingProfitOrLoss", ctx)
    };

    // ── Combined ratio (general only): (IncurredClaims + OperatingExpenses) / NetPremiumWritten * 100
    let combined_ratio_pct = if !is_life {
        let claims  = cr("IncurredClaims", ctx);
        let opex    = cr("OperatingExpenses", ctx);
        let net_prem = cr("NetPremiumWritten", ctx);
        match (claims, opex, net_prem) {
            (Some(c), Some(o), Some(p)) if p != 0.0 => Some((c + o) / p * 100.0),
            _ => None,
        }
    } else {
        None
    };

    // ── EPS (unscaled, same element name for both) ────────────────────────────
    let eps = fact(
        "BasicAndDilutedEPSAfterExtraordinaryItemsNetOfTaxExpenseForThePeriodNotToBeAnnualized",
        ctx,
    );

    // ── NormalizedCore ────────────────────────────────────────────────────────
    let core = NormalizedCore {
        revenue_equiv:          net_premium_income,
        operating_profit_equiv: None,  // not meaningful for insurers
        margin_equiv_pct:       None,
        margin_kind:            MarginKind::UwMargin,
        other_income:           investment_income,
        interest:               None,
        depreciation:           None,
        pbt:                    None,
        tax:                    None,
        net_profit:             shareholders_net_profit,
        eps,
    };

    let sector = SectorLineItems::Insurance(InsuranceLines {
        gross_premium_income,
        net_premium_income,
        investment_income,
        net_commission,
        benefits_paid,
        combined_ratio_pct,
        solvency_ratio: None,
        shareholders_net_profit,
    });

    let (period_end, period_label, fiscal_quarter) = period_labels(meta, is_annual);

    ResultPeriod {
        period_end,
        period_label,
        fiscal_quarter,
        basis:       meta.basis,
        is_audited:  meta.is_audited,
        is_restated: false,
        core,
        sector,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// Vendored from the app verbatim, with two changes:
// - fixture paths are `../fixtures/…` (fixtures live at the crate root);
// - SBILIFE / ICICIGI insurance-fixture tests omitted (fixtures not copied —
//   ~1.1 MB; insurance rows are out of scope for the Phase-1 producer).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SectorLineItems;

    // Include fixture files relative to this source file.
    const RIL: &str  = include_str!("../fixtures/nse-integrated-reliance.xml");
    const HDFC: &str = include_str!("../fixtures/nse-integrated-hdfcbank.xml");
    const BAJ: &str  = include_str!("../fixtures/nse-integrated-bajfinance.xml");

    fn reliance_meta() -> IntegratedMeta {
        IntegratedMeta {
            period_end:    "2026-03-31".into(),
            fy_label:      "FY26".into(),
            quarter_label: "Q4".into(),
            is_audited:    true,
            sector_kind:   SectorKind::General,
            basis:         StatementBasis::Consolidated,
        }
    }

    fn hdfc_meta() -> IntegratedMeta {
        IntegratedMeta {
            period_end:    "2026-03-31".into(),
            fy_label:      "FY26".into(),
            quarter_label: "Q4".into(),
            is_audited:    true,
            sector_kind:   SectorKind::Bank,
            basis:         StatementBasis::Consolidated,
        }
    }

    fn baj_meta() -> IntegratedMeta {
        IntegratedMeta {
            period_end:    "2026-03-31".into(),
            fy_label:      "FY26".into(),
            quarter_label: "Q4".into(),
            is_audited:    true,
            sector_kind:   SectorKind::Nbfc,
            basis:         StatementBasis::Consolidated,
        }
    }

    // ── RELIANCE (General) ────────────────────────────────────────────────────

    #[test]
    fn reliance_quarter_revenue_equiv() {
        let (q, _, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let q = q.unwrap();
        assert!(
            (q.core.revenue_equiv.unwrap() - 298621.0).abs() < 1.0,
            "quarter revenue_equiv = {:?}", q.core.revenue_equiv
        );
    }

    #[test]
    fn reliance_quarter_net_profit() {
        let (q, _, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let q = q.unwrap();
        assert!(
            (q.core.net_profit.unwrap() - 20589.0).abs() < 1.0,
            "quarter net_profit = {:?}", q.core.net_profit
        );
    }

    #[test]
    fn reliance_annual_revenue_equiv() {
        let (_, fy, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let fy = fy.unwrap();
        assert!(
            (fy.core.revenue_equiv.unwrap() - 1075675.0).abs() < 1.0,
            "annual revenue_equiv = {:?}", fy.core.revenue_equiv
        );
    }

    #[test]
    fn reliance_annual_net_profit() {
        let (_, fy, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let fy = fy.unwrap();
        assert!(
            (fy.core.net_profit.unwrap() - 95754.0).abs() < 1.0,
            "annual net_profit = {:?}", fy.core.net_profit
        );
    }

    #[test]
    fn reliance_quarter_period_labels() {
        let (q, _, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let q = q.unwrap();
        assert_eq!(q.fiscal_quarter, "Q4");
        assert_eq!(q.period_label, "Q4 FY26");
        assert_eq!(q.period_end, "2026-03-31");
        assert!(q.is_audited);
        assert_eq!(q.basis, StatementBasis::Consolidated);
    }

    #[test]
    fn reliance_annual_period_labels() {
        let (_, fy, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let fy = fy.unwrap();
        assert_eq!(fy.fiscal_quarter, "FY");
        assert_eq!(fy.period_label, "FY26");
        assert_eq!(fy.period_end, "2026-03-31");
        assert_eq!(fy.basis, StatementBasis::Consolidated);
    }

    #[test]
    fn reliance_sector_is_general() {
        let (q, fy, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        assert!(matches!(q.unwrap().sector, SectorLineItems::General(_)));
        assert!(matches!(fy.unwrap().sector, SectorLineItems::General(_)));
    }

    #[test]
    fn reliance_margin_kind_is_opm() {
        let (q, _, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        assert_eq!(q.unwrap().core.margin_kind, MarginKind::Opm);
    }

    // ── HDFCBANK (Bank) ───────────────────────────────────────────────────────

    #[test]
    fn hdfc_quarter_interest_earned() {
        let (q, _, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        let q = q.unwrap();
        match &q.sector {
            SectorLineItems::Bank(b) => {
                assert!(
                    (b.interest_earned.unwrap() - 87182.5).abs() < 1.0,
                    "interest_earned = {:?}", b.interest_earned
                );
            }
            _ => panic!("expected Bank variant"),
        }
    }

    #[test]
    fn hdfc_quarter_interest_expended() {
        let (q, _, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        let q = q.unwrap();
        match &q.sector {
            SectorLineItems::Bank(b) => {
                assert!(
                    (b.interest_expended.unwrap() - 45220.44).abs() < 1.0,
                    "interest_expended = {:?}", b.interest_expended
                );
            }
            _ => panic!("expected Bank variant"),
        }
    }

    #[test]
    fn hdfc_quarter_net_interest_income() {
        let (q, _, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        let q = q.unwrap();
        match &q.sector {
            SectorLineItems::Bank(b) => {
                assert!(
                    (b.net_interest_income.unwrap() - 41962.06).abs() < 1.0,
                    "net_interest_income = {:?}", b.net_interest_income
                );
            }
            _ => panic!("expected Bank variant"),
        }
    }

    #[test]
    fn hdfc_quarter_net_profit() {
        let (q, _, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        let q = q.unwrap();
        assert!(
            (q.core.net_profit.unwrap() - 20350.76).abs() < 1.0,
            "net_profit = {:?}", q.core.net_profit
        );
    }

    #[test]
    fn hdfc_quarter_eps() {
        let (q, _, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        let q = q.unwrap();
        assert!(
            (q.core.eps.unwrap() - 13.22).abs() < 0.01,
            "eps = {:?}", q.core.eps
        );
    }

    #[test]
    fn hdfc_sector_is_bank() {
        let (q, fy, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        assert!(matches!(q.unwrap().sector, SectorLineItems::Bank(_)));
        assert!(matches!(fy.unwrap().sector, SectorLineItems::Bank(_)));
    }

    #[test]
    fn hdfc_margin_kind_is_nim() {
        let (q, _, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        assert_eq!(q.unwrap().core.margin_kind, MarginKind::Nim);
    }

    #[test]
    fn hdfc_gross_npa_pct_raw() {
        let (q, _, _) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        let q = q.unwrap();
        match &q.sector {
            SectorLineItems::Bank(b) => {
                if let Some(npa) = b.gross_npa_pct {
                    // Should be in percentage range, not rupee-scaled.
                    assert!(npa.abs() < 100.0, "gross_npa_pct looks scaled: {npa}");
                }
                // None is acceptable if the element has value 0 and doesn't parse
            }
            _ => panic!("expected Bank variant"),
        }
    }

    // ── BAJFINANCE (NBFC) ─────────────────────────────────────────────────────

    #[test]
    fn baj_quarter_revenue_from_operations() {
        let (q, _, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        let q = q.unwrap();
        match &q.sector {
            SectorLineItems::Nbfc(n) => {
                assert!(
                    (n.revenue_from_operations.unwrap() - 21605.79).abs() < 1.0,
                    "revenue_from_operations = {:?}", n.revenue_from_operations
                );
            }
            _ => panic!("expected Nbfc variant"),
        }
    }

    #[test]
    fn baj_quarter_finance_costs() {
        let (q, _, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        let q = q.unwrap();
        match &q.sector {
            SectorLineItems::Nbfc(n) => {
                assert!(
                    (n.finance_costs.unwrap() - 7398.28).abs() < 1.0,
                    "finance_costs = {:?}", n.finance_costs
                );
            }
            _ => panic!("expected Nbfc variant"),
        }
    }

    #[test]
    fn baj_quarter_impairment() {
        let (q, _, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        let q = q.unwrap();
        match &q.sector {
            SectorLineItems::Nbfc(n) => {
                assert!(
                    (n.impairment_financial_instruments.unwrap() - 2007.52).abs() < 1.0,
                    "impairment = {:?}", n.impairment_financial_instruments
                );
            }
            _ => panic!("expected Nbfc variant"),
        }
    }

    #[test]
    fn baj_quarter_net_profit() {
        let (q, _, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        let q = q.unwrap();
        assert!(
            (q.core.net_profit.unwrap() - 5553.3).abs() < 1.0,
            "net_profit = {:?}", q.core.net_profit
        );
    }

    #[test]
    fn baj_quarter_eps() {
        let (q, _, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        let q = q.unwrap();
        assert!(
            (q.core.eps.unwrap() - 8.79).abs() < 0.01,
            "eps = {:?}", q.core.eps
        );
    }

    #[test]
    fn baj_sector_is_nbfc() {
        let (q, fy, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        assert!(matches!(q.unwrap().sector, SectorLineItems::Nbfc(_)));
        assert!(matches!(fy.unwrap().sector, SectorLineItems::Nbfc(_)));
    }

    #[test]
    fn baj_margin_kind_is_spread() {
        let (q, _, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        assert_eq!(q.unwrap().core.margin_kind, MarginKind::Spread);
    }

    #[test]
    fn baj_operating_profit_is_none() {
        // NBFCs don't use OPM; operating_profit_equiv and margin_equiv_pct must be None.
        let (q, fy, _) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        let q = q.unwrap();
        let fy = fy.unwrap();
        assert!(q.core.operating_profit_equiv.is_none());
        assert!(q.core.margin_equiv_pct.is_none());
        assert!(fy.core.operating_profit_equiv.is_none());
        assert!(fy.core.margin_equiv_pct.is_none());
    }

    // ── Malformed XML ─────────────────────────────────────────────────────────

    #[test]
    fn malformed_xml_returns_err_no_panic() {
        // Truly malformed XML → must return Err, must not panic.
        let result = parse_integrated_xbrl("<<<", &reliance_meta());
        assert!(result.is_err(), "expected Err for malformed XML");
    }

    #[test]
    fn empty_xbrl_returns_err() {
        // Well-formed but no usable facts in either context → Err.
        let result = parse_integrated_xbrl("<xbrl/>", &reliance_meta());
        assert!(result.is_err(), "expected Err when no facts found");
    }

    // ── select_integrated_filings tests ──────────────────────────────────────

    const RELIANCE_LIST: &str =
        include_str!("../fixtures/nse-integrated-list-reliance.json");

    #[test]
    fn selection_skips_governance_urls() {
        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        for (_, url) in sel.quarters.iter().chain(sel.annuals.iter()) {
            assert!(
                !url.contains("GOVERNANCE"),
                "GOVERNANCE filing should be skipped, got: {url}"
            );
        }
    }

    #[test]
    fn selection_skips_null_xbrl() {
        // The 31-DEC-2024 record has an XBRL URL without INTEGRATED_FILING_ prefix
        // (no known financial sector prefix) and should be excluded.
        let sel = select_integrated_filings(RELIANCE_LIST, 10).unwrap();
        for (meta, _) in sel.quarters.iter().chain(sel.annuals.iter()) {
            assert_ne!(
                meta.period_end, "2024-12-31",
                "31-DEC-2024 record (no INTEGRATED_FILING_ prefix) should be skipped"
            );
        }
    }

    #[test]
    fn selection_quarters_newest_first() {
        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        assert!(!sel.quarters.is_empty(), "expected at least one quarter");
        let first = &sel.quarters[0];
        assert_eq!(first.0.period_end, "2026-03-31", "first quarter should be Q4FY26");
        assert_eq!(first.0.quarter_label, "Q4");
        assert_eq!(first.0.fy_label, "FY26");
        assert_eq!(first.0.sector_kind, SectorKind::General);
        assert!(first.0.is_audited, "31-MAR-2026 filing is Audited");
    }

    #[test]
    fn selection_quarter_labels_correct() {
        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        // Fixture has: MAR(Q4), DEC(Q3), SEP(Q2), JUN(Q1), MAR(Q4 prev year)
        let expected = [
            ("2026-03-31", "Q4", "FY26"),
            ("2025-12-31", "Q3", "FY26"),
            ("2025-09-30", "Q2", "FY26"),
            ("2025-06-30", "Q1", "FY26"),
            ("2025-03-31", "Q4", "FY25"),
        ];
        assert_eq!(sel.quarters.len(), expected.len(), "expected 5 quarters");
        for (i, (meta, _)) in sel.quarters.iter().enumerate() {
            let (exp_period, exp_q, exp_fy) = expected[i];
            assert_eq!(meta.period_end, exp_period, "period_end mismatch at index {i}");
            assert_eq!(meta.quarter_label, exp_q, "quarter_label mismatch at index {i}");
            assert_eq!(meta.fy_label, exp_fy, "fy_label mismatch at index {i}");
        }
    }

    #[test]
    fn selection_annuals_are_march_filings() {
        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        assert!(!sel.annuals.is_empty(), "expected at least one annual");
        for (meta, _) in &sel.annuals {
            assert!(
                meta.period_end.ends_with("-03-31"),
                "annual period_end must be March 31, got: {}", meta.period_end
            );
            assert_eq!(meta.quarter_label, "Q4", "annuals should have Q4 label");
        }
    }

    #[test]
    fn selection_annuals_fy_labels() {
        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        // Fixture has 31-MAR-2026 (FY26) and 31-MAR-2025 (FY25).
        assert!(sel.annuals.len() >= 2, "expected at least 2 annual filings");
        assert_eq!(sel.annuals[0].0.fy_label, "FY26");
        assert_eq!(sel.annuals[1].0.fy_label, "FY25");
    }

    #[test]
    fn selection_prefers_consolidated() {
        // The fixture has both a Consolidated and a Standalone INDAS filing for
        // 31-MAR-2026. We must pick the Consolidated one.
        const MAR2026_CONSOLIDATED: &str = "INTEGRATED_FILING_INDAS_1658776_";
        const MAR2026_STANDALONE: &str = "INTEGRATED_FILING_INDAS_1658775_";

        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        let q4 = sel
            .quarters
            .iter()
            .find(|(m, _)| m.period_end == "2026-03-31")
            .expect("Q4 FY26 quarter must be present");

        assert!(
            q4.1.contains(MAR2026_CONSOLIDATED),
            "Q4 FY26 must choose the Consolidated filing, got: {}", q4.1
        );
        assert!(
            !q4.1.contains(MAR2026_STANDALONE),
            "Q4 FY26 must NOT choose the Standalone filing, got: {}", q4.1
        );

        // Same filing also drives the FY26 annual entry.
        let fy26 = sel
            .annuals
            .iter()
            .find(|(m, _)| m.period_end == "2026-03-31")
            .expect("FY26 annual must be present");
        assert!(
            fy26.1.contains(MAR2026_CONSOLIDATED) && !fy26.1.contains(MAR2026_STANDALONE),
            "FY26 annual must use the Consolidated filing, got: {}", fy26.1
        );
    }

    #[test]
    fn selection_sector_kind_general_for_reliance() {
        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        for (meta, _) in sel.quarters.iter().chain(sel.annuals.iter()) {
            assert_eq!(
                meta.sector_kind,
                SectorKind::General,
                "RELIANCE uses INDAS_ prefix → General sector"
            );
        }
    }

    #[test]
    fn selection_at_most_n_quarters() {
        let sel = select_integrated_filings(RELIANCE_LIST, 3).unwrap();
        assert!(sel.quarters.len() <= 3, "got {} quarters", sel.quarters.len());
    }

    #[test]
    fn selection_annuals_at_most_5() {
        let sel = select_integrated_filings(RELIANCE_LIST, 10).unwrap();
        assert!(sel.annuals.len() <= 5, "annuals capped at 5, got {}", sel.annuals.len());
    }

    #[test]
    fn selection_malformed_json_is_err() {
        let r = select_integrated_filings("not json", 5);
        assert!(r.is_err(), "expected Err for malformed JSON");
    }

    #[test]
    fn selection_empty_data_is_ok_empty() {
        let r = select_integrated_filings(r#"{"data":[]}"#, 5).unwrap();
        assert!(r.quarters.is_empty());
        assert!(r.annuals.is_empty());
    }

    #[test]
    fn selection_missing_data_key_is_err() {
        let r = select_integrated_filings(r#"{"other":[]}"#, 5);
        assert!(r.is_err(), "expected Err when 'data' key missing");
    }

    #[test]
    fn selection_dedup_keeps_highest_seq_id() {
        // Synthetic: two records for the same (qe_Date, consolidated), different seq_Id.
        let json = r#"{"data":[
          {"qe_Date":"31-MAR-2026","consolidated":"Consolidated","seq_Id":"152826","audited":"Audited",
           "xbrl":"https://nsearchives.nseindia.com/corporate/xbrl/INTEGRATED_FILING_INDAS_HIGH_WEB.xml"},
          {"qe_Date":"31-MAR-2026","consolidated":"Consolidated","seq_Id":"150000","audited":"Audited",
           "xbrl":"https://nsearchives.nseindia.com/corporate/xbrl/INTEGRATED_FILING_INDAS_LOW_WEB.xml"}
        ]}"#;
        let sel = select_integrated_filings(json, 5).unwrap();
        assert_eq!(sel.quarters.len(), 1, "dedup must collapse both into one");
        assert!(
            sel.quarters[0].1.contains("HIGH"),
            "highest seq_Id URL must survive, got: {}", sel.quarters[0].1
        );
    }

    #[test]
    fn selection_sector_detection() {
        // Synthetic: one of each financial prefix.
        let json = r#"{"data":[
          {"qe_Date":"31-MAR-2026","consolidated":"Consolidated","seq_Id":"1","audited":"Audited",
           "xbrl":"https://x/INTEGRATED_FILING_INDAS_abc.xml"},
          {"qe_Date":"31-DEC-2025","consolidated":"Consolidated","seq_Id":"2","audited":"Audited",
           "xbrl":"https://x/INTEGRATED_FILING_NBFC_INDAS_abc.xml"},
          {"qe_Date":"30-SEP-2025","consolidated":"Consolidated","seq_Id":"3","audited":"Audited",
           "xbrl":"https://x/INTEGRATED_FILING_BANKING_abc.xml"},
          {"qe_Date":"30-JUN-2025","consolidated":"Standalone","seq_Id":"4","audited":"Audited",
           "xbrl":"https://x/INTEGRATED_FILING_LI_abc.xml"}
        ]}"#;
        let sel = select_integrated_filings(json, 5).unwrap();
        assert_eq!(sel.quarters.len(), 4);
        assert_eq!(sel.quarters[0].0.sector_kind, SectorKind::General,   "INDAS → General");
        assert_eq!(sel.quarters[1].0.sector_kind, SectorKind::Nbfc,      "NBFC_INDAS → Nbfc");
        assert_eq!(sel.quarters[2].0.sector_kind, SectorKind::Bank,      "BANKING → Bank");
        assert_eq!(sel.quarters[3].0.sector_kind, SectorKind::Insurance, "LI → Insurance");
    }

    // ── Basis threading tests ─────────────────────────────────────────────────

    /// RELIANCE fixture is Consolidated-preferred; basis must flow through to periods.
    #[test]
    fn reliance_basis_is_consolidated() {
        let (q, fy, _) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        assert_eq!(q.unwrap().basis, StatementBasis::Consolidated,
            "RELIANCE quarter basis should be Consolidated");
        assert_eq!(fy.unwrap().basis, StatementBasis::Consolidated,
            "RELIANCE annual basis should be Consolidated");
    }

    /// select_integrated_filings propagates Consolidated basis for RELIANCE.
    #[test]
    fn selection_basis_consolidated_for_reliance() {
        let sel = select_integrated_filings(RELIANCE_LIST, 5).unwrap();
        for (meta, _) in sel.quarters.iter().chain(sel.annuals.iter()) {
            assert_eq!(meta.basis, StatementBasis::Consolidated,
                "RELIANCE selection must be Consolidated, got Standalone for {}", meta.period_end);
        }
    }

    /// select_integrated_filings uses Standalone basis for Standalone-only filings.
    #[test]
    fn selection_basis_standalone_fallback() {
        let json = r#"{"data":[
          {"qe_Date":"31-MAR-2026","consolidated":"Standalone","seq_Id":"1","audited":"Audited",
           "xbrl":"https://x/INTEGRATED_FILING_LI_standalone.xml"}
        ]}"#;
        let sel = select_integrated_filings(json, 5).unwrap();
        assert_eq!(sel.quarters.len(), 1);
        assert_eq!(sel.quarters[0].0.basis, StatementBasis::Standalone,
            "Standalone-only filing must produce Standalone basis");
    }

    // ── Valuation extraction tests ────────────────────────────────────────────

    /// RELIANCE: shares_outstanding > 0, in a plausible range (6–14 billion).
    #[test]
    fn reliance_valuation_shares_outstanding() {
        let (_, _, val) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let shares = val.shares_outstanding.expect("RELIANCE shares_outstanding must be Some");
        assert!(shares > 0.0, "shares_outstanding must be > 0");
        // 13.53B shares (EquityShareCapital=135320000000 ÷ FaceValue=10)
        assert!(
            shares >= 6e9 && shares <= 14e9,
            "shares_outstanding={shares:.0} not in plausible 6–14B range"
        );
        // Mcap sanity at Rs1400/share (expect ~Rs17–21L crore)
        let mcap_cr = shares * 1400.0 / 1e7;
        assert!(
            mcap_cr >= 1_700_000.0 && mcap_cr <= 2_100_000.0,
            "mcap_cr={mcap_cr:.0} at Rs1400 outside Rs17–21L crore range"
        );
    }

    /// RELIANCE: face_value = 10.
    #[test]
    fn reliance_valuation_face_value() {
        let (_, _, val) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let fv = val.face_value.expect("RELIANCE face_value must be Some");
        assert!((fv - 10.0).abs() < 0.01, "RELIANCE face_value should be 10, got {fv}");
    }

    /// RELIANCE: book_value is equity_cr > 0.
    #[test]
    fn reliance_valuation_equity_cr() {
        let (_, _, val) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let eq_cr = val.equity_cr.expect("RELIANCE equity_cr must be Some");
        assert!(eq_cr > 0.0, "equity_cr must be > 0, got {eq_cr}");
        // EquityAttributableToOwnersOfParent = 9040300000000 → 904030 cr
        assert!(
            (eq_cr - 904030.0).abs() < 1.0,
            "equity_cr={eq_cr:.1} expected ~904030 cr"
        );
    }

    /// RELIANCE: total_debt > 0 (BorrowingsCurrent + BorrowingsNoncurrent).
    #[test]
    fn reliance_valuation_total_debt() {
        let (_, _, val) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let debt = val.total_debt_cr.expect("RELIANCE total_debt_cr must be Some");
        assert!(debt > 0.0, "total_debt_cr must be > 0");
        // 1036700000000 + 2707510000000 = 3744210000000 → 374421 cr
        assert!(
            (debt - 374421.0).abs() < 1.0,
            "total_debt_cr={debt:.1} expected ~374421 cr"
        );
    }

    /// RELIANCE: cash > 0 (CashAndCashEquivalents).
    #[test]
    fn reliance_valuation_cash() {
        let (_, _, val) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let cash = val.cash_cr.expect("RELIANCE cash_cr must be Some");
        assert!(cash > 0.0, "cash_cr must be > 0");
        // 1459770000000 → 145977 cr
        assert!(
            (cash - 145977.0).abs() < 1.0,
            "cash_cr={cash:.1} expected ~145977 cr"
        );
    }

    /// RELIANCE: ebitda > 0 (PBT + FinanceCosts + Depreciation from FourD).
    #[test]
    fn reliance_valuation_ebitda() {
        let (_, _, val) = parse_integrated_xbrl(RIL, &reliance_meta()).unwrap();
        let ebitda = val.ebitda_cr.expect("RELIANCE ebitda_cr must be Some");
        assert!(ebitda > 0.0, "ebitda_cr must be > 0");
        // (1231620000000 + 270610000000 + 576880000000) / 1e7 = 207911 cr
        assert!(
            (ebitda - 207911.0).abs() < 1.0,
            "ebitda_cr={ebitda:.1} expected ~207911 cr"
        );
    }

    /// HDFCBANK (Bank sector): ebitda must be None (not computed for financials).
    #[test]
    fn hdfcbank_valuation_ebitda_is_none() {
        let (_, _, val) = parse_integrated_xbrl(HDFC, &hdfc_meta()).unwrap();
        assert!(val.ebitda_cr.is_none(),
            "HDFCBANK ebitda_cr must be None for Bank sector");
    }

    /// BAJ (NBFC sector): ebitda must be None (not computed for financials).
    #[test]
    fn baj_valuation_ebitda_is_none() {
        let (_, _, val) = parse_integrated_xbrl(BAJ, &baj_meta()).unwrap();
        assert!(val.ebitda_cr.is_none(),
            "BAJFINANCE ebitda_cr must be None for NBFC sector");
    }
}
