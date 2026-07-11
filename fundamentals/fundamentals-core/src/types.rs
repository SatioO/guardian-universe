//! Canonical, source-agnostic fundamentals result types.
//!
//! Vendored from traderview `src-tauri/src/fundamentals/mod.rs`
//! @ ed0eddb6d9541e383844382666f5a5c0d18cef5e — the subset the parser
//! produces (period/core/sector-line types). App-transport types
//! (`FundamentalsDoc`, provenance envelope, cache) intentionally omitted.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SectorKind { General, Bank, Nbfc, Insurance }
impl Default for SectorKind { fn default() -> Self { SectorKind::General } }

impl SectorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SectorKind::General => "general",
            SectorKind::Bank => "bank",
            SectorKind::Nbfc => "nbfc",
            SectorKind::Insurance => "insurance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementBasis { Standalone, Consolidated }
impl Default for StatementBasis { fn default() -> Self { StatementBasis::Consolidated } }

impl StatementBasis {
    pub fn as_str(&self) -> &'static str {
        match self {
            StatementBasis::Standalone => "standalone",
            StatementBasis::Consolidated => "consolidated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarginKind { Opm, Nim, Spread, UwMargin, Unavailable }
impl Default for MarginKind { fn default() -> Self { MarginKind::Unavailable } }

impl MarginKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarginKind::Opm => "opm",
            MarginKind::Nim => "nim",
            MarginKind::Spread => "spread",
            MarginKind::UwMargin => "uw_margin",
            MarginKind::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResultPeriod {
    #[serde(default)] pub period_end: String,
    #[serde(default)] pub period_label: String,
    #[serde(default)] pub fiscal_quarter: String,  // "Q1".."Q4" | "H1" | "FY"
    #[serde(default)] pub basis: StatementBasis,
    #[serde(default)] pub is_audited: bool,
    #[serde(default)] pub is_restated: bool,
    #[serde(default)] pub core: NormalizedCore,
    #[serde(default)] pub sector: SectorLineItems,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NormalizedCore {
    #[serde(default)] pub revenue_equiv: Option<f64>,
    #[serde(default)] pub operating_profit_equiv: Option<f64>,
    #[serde(default)] pub margin_equiv_pct: Option<f64>,
    #[serde(default)] pub margin_kind: MarginKind,
    #[serde(default)] pub other_income: Option<f64>,
    #[serde(default)] pub interest: Option<f64>,
    #[serde(default)] pub depreciation: Option<f64>,
    #[serde(default)] pub pbt: Option<f64>,
    #[serde(default)] pub tax: Option<f64>,
    #[serde(default)] pub net_profit: Option<f64>,
    #[serde(default)] pub eps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum SectorLineItems {
    General(GeneralLines),
    Bank(BankLines),
    Nbfc(NbfcLines),
    Insurance(InsuranceLines),
}
impl Default for SectorLineItems {
    fn default() -> Self { SectorLineItems::General(GeneralLines::default()) }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GeneralLines {
    #[serde(default)] pub revenue_from_operations: Option<f64>,
    #[serde(default)] pub total_expenses: Option<f64>,
    #[serde(default)] pub cost_of_materials: Option<f64>,
    #[serde(default)] pub employee_expense: Option<f64>,
    #[serde(default)] pub operating_profit: Option<f64>,
    #[serde(default)] pub opm_pct: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BankLines {
    #[serde(default)] pub total_income: Option<f64>,
    #[serde(default)] pub interest_earned: Option<f64>,
    #[serde(default)] pub other_income: Option<f64>,
    #[serde(default)] pub interest_expended: Option<f64>,
    #[serde(default)] pub net_interest_income: Option<f64>,
    #[serde(default)] pub nim_pct: Option<f64>,
    #[serde(default)] pub operating_expenses: Option<f64>,
    #[serde(default)] pub pre_provision_operating_profit: Option<f64>,
    #[serde(default)] pub provisions_and_contingencies: Option<f64>,
    #[serde(default)] pub gross_npa_pct: Option<f64>,
    #[serde(default)] pub net_npa_pct: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NbfcLines {
    #[serde(default)] pub revenue_from_operations: Option<f64>,
    #[serde(default)] pub total_income: Option<f64>,
    #[serde(default)] pub finance_costs: Option<f64>,
    #[serde(default)] pub net_interest_income: Option<f64>,
    #[serde(default)] pub nim_pct: Option<f64>,
    #[serde(default)] pub impairment_financial_instruments: Option<f64>,
    #[serde(default)] pub pre_provision_operating_profit: Option<f64>,
    #[serde(default)] pub gross_stage3_pct: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InsuranceLines {
    #[serde(default)] pub gross_premium_income: Option<f64>,
    #[serde(default)] pub net_premium_income: Option<f64>,
    #[serde(default)] pub investment_income: Option<f64>,
    #[serde(default)] pub net_commission: Option<f64>,
    #[serde(default)] pub benefits_paid: Option<f64>,
    #[serde(default)] pub combined_ratio_pct: Option<f64>,
    #[serde(default)] pub solvency_ratio: Option<f64>,
    #[serde(default)] pub shareholders_net_profit: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sector_kind_serializes_snake_case() {
        assert_eq!(serde_json::to_string(&SectorKind::Nbfc).unwrap(), "\"nbfc\"");
        assert_eq!(
            serde_json::from_str::<SectorKind>("\"bank\"").unwrap(),
            SectorKind::Bank
        );
    }

    #[test]
    fn sector_line_items_is_tagged_union() {
        let v = SectorLineItems::General(GeneralLines::default());
        let j = serde_json::to_value(&v).unwrap();
        assert_eq!(j["kind"], "general");
        assert!(j["data"].is_object());
    }

    #[test]
    fn statement_basis_round_trips() {
        let j = serde_json::to_string(&StatementBasis::Standalone).unwrap();
        assert_eq!(j, "\"standalone\"");
        assert_eq!(
            serde_json::from_str::<StatementBasis>(&j).unwrap(),
            StatementBasis::Standalone
        );
    }
}
