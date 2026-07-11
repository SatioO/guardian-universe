//! Universe seed: BSE `ListofScripData` ∪ NSE `EQUITY_L.csv`, keyed by ISIN
//! (SOURCE-CONTRACT.md §2/§5/§10). BSE gives the `SCRIP_CD ↔ ISIN ↔ symbol`
//! map plus a free full-universe `Mktcap` anchor (₹ crore); NSE catches
//! NSE-only edge cases. NSE-only entries have no mcap and are counted +
//! disclosed, never silently dropped.

use std::collections::HashMap;

use crate::http::PoliteClient;

const BSE_SCRIP_MASTER_URL: &str = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=Active";
const BSE_REFERER: &str = "https://www.bseindia.com/";
const NSE_EQUITY_L_URL: &str = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv";

#[derive(Debug, Clone, Default)]
pub struct UniverseEntry {
    /// ISIN — the universal join key.
    pub instrument_key: String,
    pub name: String,
    pub bse_scrip_cd: Option<String>,
    pub bse_symbol: Option<String>,
    pub nse_symbol: Option<String>,
    /// From the BSE scrip master, ₹ crore. None for NSE-only entries.
    pub mktcap_cr: Option<f64>,
    pub on_bse: bool,
    pub on_nse: bool,
}

pub struct Universe {
    pub entries: Vec<UniverseEntry>,
    pub bse_count: usize,
    pub nse_count: usize,
    pub nse_only_count: usize,
}

impl Universe {
    /// Top-N by the scrip master's `Mktcap`, descending. Entries without a
    /// market cap cannot be ranked and are excluded from top-N by definition.
    pub fn top_by_mktcap(&self, n: usize) -> Vec<&UniverseEntry> {
        let mut ranked: Vec<&UniverseEntry> =
            self.entries.iter().filter(|e| e.mktcap_cr.is_some()).collect();
        ranked.sort_by(|a, b| {
            b.mktcap_cr
                .partial_cmp(&a.mktcap_cr)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.instrument_key.cmp(&b.instrument_key))
        });
        ranked.truncate(n);
        ranked
    }

    /// BSE scrip code → ISIN (injected into `BseFilingSource` so scrip-code
    /// resolution stays inside the provider).
    pub fn scrip_to_isin(&self) -> HashMap<String, String> {
        self.entries
            .iter()
            .filter_map(|e| {
                e.bse_scrip_cd
                    .clone()
                    .map(|s| (s, e.instrument_key.clone()))
            })
            .collect()
    }
}

/// Build the ISIN-keyed union universe. Two bulk requests total.
pub fn seed_universe(client: &PoliteClient) -> Result<Universe, String> {
    let bse_body = client.get_text(BSE_SCRIP_MASTER_URL, Some(BSE_REFERER))?;
    let nse_body = client.get_text(NSE_EQUITY_L_URL, None)?;
    build_universe(&bse_body, &nse_body)
}

/// Pure builder, separated for tests.
pub fn build_universe(bse_json: &str, nse_csv: &str) -> Result<Universe, String> {
    let mut by_isin: HashMap<String, UniverseEntry> = HashMap::new();

    // ── BSE scrip master ──────────────────────────────────────────────────────
    let rows: serde_json::Value = serde_json::from_str(bse_json)
        .map_err(|e| format!("ListofScripData JSON parse: {e}"))?;
    let rows = rows
        .as_array()
        .ok_or_else(|| "ListofScripData: expected a JSON array".to_string())?;
    let mut bse_count = 0usize;
    for r in rows {
        let s = |k: &str| -> String {
            r.get(k).and_then(|v| v.as_str()).unwrap_or_default().trim().to_string()
        };
        let isin = s("ISIN_NUMBER");
        // Equity universe = INE-prefixed ISINs (blank / INF fund ISINs excluded).
        if !isin.starts_with("INE") {
            continue;
        }
        if s("Status") != "Active" || s("Segment") != "Equity" {
            continue;
        }
        bse_count += 1;
        let mktcap_cr = s("Mktcap").parse::<f64>().ok().filter(|v| *v > 0.0);
        let entry = by_isin.entry(isin.clone()).or_default();
        entry.instrument_key = isin;
        if entry.name.is_empty() {
            entry.name = {
                let n = s("Issuer_Name");
                if n.is_empty() { s("Scrip_Name") } else { n }
            };
        }
        entry.bse_scrip_cd = Some(s("SCRIP_CD"));
        entry.bse_symbol = Some(s("scrip_id")).filter(|v| !v.is_empty());
        entry.mktcap_cr = mktcap_cr;
        entry.on_bse = true;
    }

    // ── NSE EQUITY_L.csv ──────────────────────────────────────────────────────
    // Note: some header names carry leading spaces — trim on parse.
    let mut lines = nse_csv.lines();
    let header = lines
        .next()
        .ok_or_else(|| "EQUITY_L.csv: empty body".to_string())?;
    let cols: Vec<String> = header.split(',').map(|c| c.trim().to_uppercase()).collect();
    let idx = |name: &str| -> Result<usize, String> {
        cols.iter()
            .position(|c| c == name)
            .ok_or_else(|| format!("EQUITY_L.csv: missing column {name}"))
    };
    let sym_i = idx("SYMBOL")?;
    let isin_i = idx("ISIN NUMBER")?;
    let name_i = idx("NAME OF COMPANY").unwrap_or(sym_i);

    let mut nse_count = 0usize;
    for line in lines {
        let fields: Vec<&str> = line.split(',').map(|f| f.trim()).collect();
        if fields.len() <= isin_i.max(sym_i) {
            continue;
        }
        let isin = fields[isin_i].to_string();
        if !isin.starts_with("INE") {
            continue;
        }
        nse_count += 1;
        let entry = by_isin.entry(isin.clone()).or_default();
        entry.instrument_key = isin;
        if entry.name.is_empty() {
            entry.name = fields.get(name_i).unwrap_or(&"").to_string();
        }
        entry.nse_symbol = Some(fields[sym_i].to_string()).filter(|v| !v.is_empty());
        entry.on_nse = true;
    }

    let nse_only_count = by_isin.values().filter(|e| e.on_nse && !e.on_bse).count();

    let mut entries: Vec<UniverseEntry> = by_isin.into_values().collect();
    entries.sort_by(|a, b| a.instrument_key.cmp(&b.instrument_key));

    Ok(Universe { entries, bse_count, nse_count, nse_only_count })
}

#[cfg(test)]
mod tests {
    use super::*;

    const BSE: &str = r#"[
      {"SCRIP_CD":"500002","Scrip_Name":"ABB India Ltd","Status":"Active","GROUP":"A",
       "FACE_VALUE":"2.00","ISIN_NUMBER":"INE117A01022","INDUSTRY":null,"scrip_id":"ABB",
       "Segment":"Equity","Issuer_Name":"ABB India Limited","Mktcap":"144808.65"},
      {"SCRIP_CD":"517506","Scrip_Name":"TTK PRESTIGE","Status":"Active","GROUP":"A",
       "FACE_VALUE":"1.00","ISIN_NUMBER":"INE690A01028","scrip_id":"TTKPRESTIG",
       "Segment":"Equity","Issuer_Name":"TTK Prestige Ltd","Mktcap":"12000.00"},
      {"SCRIP_CD":"599999","Scrip_Name":"Fundy","Status":"Active","GROUP":"F",
       "ISIN_NUMBER":"INF204K012R1","scrip_id":"FUNDY","Segment":"Equity",
       "Issuer_Name":"Some Fund","Mktcap":"99999.99"},
      {"SCRIP_CD":"511111","Scrip_Name":"Gone Ltd","Status":"Suspended","GROUP":"Z",
       "ISIN_NUMBER":"INE999Z01010","scrip_id":"GONE","Segment":"Equity",
       "Issuer_Name":"Gone Ltd","Mktcap":"5.00"}
    ]"#;

    const NSE: &str = "SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE\nABB,ABB India Limited,EQ,06-OCT-2008,2,1,INE117A01022,2\nNSEONLY,NSE Only Co,EQ,01-JAN-2020,10,1,INE555Y01019,10\n";

    #[test]
    fn union_is_keyed_by_isin() {
        let u = build_universe(BSE, NSE).unwrap();
        // INE117A01022 merged (on both), INE690A01028 BSE-only, INE555Y01019 NSE-only.
        // INF… fund and Suspended rows excluded.
        assert_eq!(u.entries.len(), 3);
        let abb = u.entries.iter().find(|e| e.instrument_key == "INE117A01022").unwrap();
        assert!(abb.on_bse && abb.on_nse);
        assert_eq!(abb.bse_scrip_cd.as_deref(), Some("500002"));
        assert_eq!(abb.nse_symbol.as_deref(), Some("ABB"));
        assert_eq!(abb.mktcap_cr, Some(144808.65));
        assert_eq!(u.nse_only_count, 1);
    }

    #[test]
    fn inf_isin_and_suspended_are_excluded() {
        let u = build_universe(BSE, NSE).unwrap();
        assert!(u.entries.iter().all(|e| e.instrument_key.starts_with("INE")));
        assert!(!u.entries.iter().any(|e| e.instrument_key == "INE999Z01010"));
    }

    #[test]
    fn top_by_mktcap_ranks_descending_and_skips_unranked() {
        let u = build_universe(BSE, NSE).unwrap();
        let top = u.top_by_mktcap(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].instrument_key, "INE117A01022"); // 144808.65
        assert_eq!(top[1].instrument_key, "INE690A01028"); // 12000.00
        // NSE-only entry (no mcap) can never be in top-N.
        assert!(!top.iter().any(|e| e.instrument_key == "INE555Y01019"));
    }

    #[test]
    fn scrip_to_isin_map_covers_bse_entries() {
        let u = build_universe(BSE, NSE).unwrap();
        let map = u.scrip_to_isin();
        assert_eq!(map.get("517506").map(String::as_str), Some("INE690A01028"));
        assert_eq!(map.len(), 2);
    }
}
