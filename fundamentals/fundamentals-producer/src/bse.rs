//! `BseFilingSource` — the first registered [`FilingSource`] implementation.
//!
//! Implements the SOURCE-CONTRACT.md §9 chain:
//! - discovery: `Corp_FinanceResult_ng/w` (bulk, rolling `FlagDur` windows)
//! - instance bytes: `www.bseindia.com/XBRLFILES/<XMLName with .html → .xml>`
//!   (the §9.2 key finding: the `.xml` twin of the HTML rendering is the raw
//!   SEBI `in-capmkt` XBRL instance).
//!
//! All BSE specifics (endpoint shapes, header requirements, the `.html→.xml`
//! swap, scrip-code → ISIN resolution) live HERE, behind the trait.

use std::collections::HashMap;
use std::sync::Arc;

use fundamentals_core::StatementBasis;

use crate::http::PoliteClient;
use crate::source::{DiscoveryWindow, FilingRef, FilingSource};

const DISCOVERY_URL: &str =
    "https://api.bseindia.com/BseIndiaAPI/api/Corp_FinanceResult_ng/w";
const XBRLFILES_BASE: &str = "https://www.bseindia.com/XBRLFILES/";
const REFERER: &str = "https://www.bseindia.com/";

pub const BSE_SOURCE_ID: &str = "bse";

pub struct BseFilingSource {
    client: Arc<PoliteClient>,
    /// BSE scrip code → ISIN, injected from the universe seed so the source
    /// can resolve refs to the canonical instrument key itself.
    scrip_to_isin: HashMap<String, String>,
}

impl BseFilingSource {
    pub fn new(client: Arc<PoliteClient>, scrip_to_isin: HashMap<String, String>) -> Self {
        Self { client, scrip_to_isin }
    }

    fn flag_dur(window: DiscoveryWindow) -> u8 {
        match window {
            DiscoveryWindow::Today => 1,
            DiscoveryWindow::LastWeek => 2,
            DiscoveryWindow::Last15Days => 3,
            DiscoveryWindow::LastMonth => 4,
            DiscoveryWindow::Last3Months => 5,
            DiscoveryWindow::LastYear => 6,
        }
    }

    /// Parse the discovery feed body into refs. Pure; separated for tests.
    pub fn parse_discovery(&self, body: &str) -> Result<Vec<FilingRef>, String> {
        let root: serde_json::Value = serde_json::from_str(body)
            .map_err(|e| format!("Corp_FinanceResult_ng JSON parse: {e}"))?;
        let rows = root
            .get("Table")
            .and_then(|v| v.as_array())
            .ok_or_else(|| "Corp_FinanceResult_ng: expected {\"Table\":[…]}".to_string())?;

        let mut refs: Vec<FilingRef> = Vec::new();
        for row in rows {
            // Scrip_cd arrives as a JSON number.
            let scrip = match row.get("Scrip_cd") {
                Some(v) if v.is_number() => v.to_string(),
                Some(v) => v.as_str().unwrap_or_default().to_string(),
                None => continue,
            };
            if scrip.is_empty() {
                continue;
            }
            let s = |k: &str| -> String {
                row.get(k).and_then(|v| v.as_str()).unwrap_or_default().trim().to_string()
            };
            let quarter_code = s("quarter_code");
            if quarter_code.is_empty() {
                continue;
            }
            let company = {
                let c = s("company_name");
                if c.is_empty() { s("scrip_name") } else { c }
            };
            let nature = s("Fld_NatureOfReport");
            let row_basis = match nature.as_str() {
                "Consolidated" => Some(StatementBasis::Consolidated),
                "Standalone" => Some(StatementBasis::Standalone),
                _ => None,
            };
            let is_audited = s("audited") == "Audited";
            let broadcast_at = s("Fld_CreateDate");
            let instrument_key = self.scrip_to_isin.get(&scrip).cloned();

            let xml_name = s("XMLName");
            let consol_xml_name = s("Consol_XMLName");

            let mk = |basis: Option<StatementBasis>, locator: Option<String>| FilingRef {
                source_id: BSE_SOURCE_ID.to_string(),
                native_id: scrip.clone(),
                instrument_key: instrument_key.clone(),
                company_name: company.clone(),
                period_hint: quarter_code.clone(),
                basis_hint: basis,
                broadcast_at: broadcast_at.clone(),
                is_audited_hint: is_audited,
                instance_locator: locator,
            };

            // The row's own document (basis = Fld_NatureOfReport) …
            if !xml_name.is_empty() {
                refs.push(mk(row_basis, Some(xml_name.clone())));
            }
            // … plus the consolidated twin when present and distinct.
            if !consol_xml_name.is_empty() && consol_xml_name != xml_name {
                refs.push(mk(Some(StatementBasis::Consolidated), Some(consol_xml_name.clone())));
            }
            // Filing broadcast but XBRL attachment not yet available → PENDING
            // ref (locator = None). SOURCE-CONTRACT §9.4: never mark done off
            // the PDF row; keep pending until the XMLName appears.
            if xml_name.is_empty() && consol_xml_name.is_empty() {
                refs.push(mk(row_basis, None));
            }
        }
        Ok(refs)
    }

    /// `IF…IFIndAs.html` → `…IFIndAs.xml` (the §9.2 raw-XBRL twin).
    fn instance_url(locator: &str) -> String {
        let xml_name = if let Some(stripped) = locator.strip_suffix(".html") {
            format!("{stripped}.xml")
        } else {
            locator.to_string()
        };
        format!("{XBRLFILES_BASE}{xml_name}")
    }
}

impl FilingSource for BseFilingSource {
    fn source_id(&self) -> &'static str {
        BSE_SOURCE_ID
    }

    fn discover(&self, window: DiscoveryWindow) -> Result<Vec<FilingRef>, String> {
        let url = format!(
            "{DISCOVERY_URL}?SCRIP_CD=&FlagDur={}&HFQ=&ISUBGROUP_CODE=",
            Self::flag_dur(window)
        );
        let body = self.client.get_text(&url, Some(REFERER))?;
        self.parse_discovery(&body)
    }

    fn fetch_instance(&self, r: &FilingRef) -> Result<Vec<u8>, String> {
        let locator = r
            .instance_locator
            .as_deref()
            .ok_or_else(|| format!("ref {} has no instance locator (pending)", r.dedup_key()))?;
        let url = Self::instance_url(locator);
        let bytes = self.client.get_bytes(&url, Some(REFERER))?;
        // Cheap sanity: must look like an XBRL instance, not an error page.
        let head: String = String::from_utf8_lossy(&bytes[..bytes.len().min(2048)]).to_string();
        if !head.contains("xbrl") {
            return Err(format!("instance at {url} does not look like XBRL"));
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> BseFilingSource {
        let mut map = HashMap::new();
        map.insert("517506".to_string(), "INE690A01028".to_string());
        BseFilingSource::new(Arc::new(PoliteClient::new(0)), map)
    }

    const SAMPLE: &str = r#"{"Table":[
      {"Scrip_cd":517506,"scrip_name":"TTK Prestige Ltd","quarter_code":"MQ2025-2026",
       "audited":"Audited","Qtr":370.00,"Fld_CreateDate":"2026-07-11T21:18:07.233",
       "Industry_name":"Consumer Durables","company_name":"TTK Prestige Ltd",
       "Fld_NatureOfReport":"Standalone",
       "XMLName":"IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_517506_1172026211629_IFIndAs.html",
       "Consol_XMLName":"IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_517506_1172026211820_IFIndAs.html",
       "URL":"https://www.bseindia.com/stock-share-price/x/517506/","Resultpageurl":null},
      {"Scrip_cd":999999,"scrip_name":"Pending Co","quarter_code":"JQ2026-2027",
       "audited":"Un-audited","Fld_CreateDate":"2026-07-11T20:00:00",
       "company_name":"Pending Co","Fld_NatureOfReport":"Standalone",
       "XMLName":"","Consol_XMLName":""}
    ]}"#;

    #[test]
    fn discovery_rows_become_refs_with_isin_resolved() {
        let refs = source().parse_discovery(SAMPLE).unwrap();
        // TTK row yields two refs (standalone doc + consolidated twin).
        let ttk: Vec<_> = refs.iter().filter(|r| r.native_id == "517506").collect();
        assert_eq!(ttk.len(), 2);
        assert_eq!(ttk[0].instrument_key.as_deref(), Some("INE690A01028"));
        assert_eq!(ttk[0].basis_hint, Some(StatementBasis::Standalone));
        assert!(ttk[0].instance_locator.as_deref().unwrap().ends_with("211629_IFIndAs.html"));
        assert_eq!(ttk[1].basis_hint, Some(StatementBasis::Consolidated));
        assert!(ttk[1].instance_locator.as_deref().unwrap().ends_with("211820_IFIndAs.html"));
        assert!(ttk[0].is_audited_hint);
        assert_eq!(ttk[0].source_id, BSE_SOURCE_ID);
    }

    #[test]
    fn empty_xmlname_row_is_pending_not_dropped() {
        let refs = source().parse_discovery(SAMPLE).unwrap();
        let pending: Vec<_> = refs.iter().filter(|r| r.native_id == "999999").collect();
        assert_eq!(pending.len(), 1, "PDF-only filing must surface as a pending ref");
        assert!(pending[0].instance_locator.is_none());
        assert!(pending[0].instrument_key.is_none(), "unknown scrip → no ISIN");
    }

    #[test]
    fn instance_url_swaps_html_for_xml() {
        let url = BseFilingSource::instance_url(
            "IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_517506_1172026211629_IFIndAs.html",
        );
        assert_eq!(
            url,
            "https://www.bseindia.com/XBRLFILES/IFIndasDuplicateUploadDocument/Integrated_Finance_Ind_As_517506_1172026211629_IFIndAs.xml"
        );
    }

    #[test]
    fn malformed_discovery_is_err() {
        assert!(source().parse_discovery("nope").is_err());
        assert!(source().parse_discovery(r#"{"NotTable":[]}"#).is_err());
    }
}
