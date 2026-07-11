//! Universe seed: BSE `ListofScripData` ∪ NSE `EQUITY_L.csv` (EQ series),
//! keyed by ISIN (SOURCE-CONTRACT.md §2/§5/§10). BSE gives the
//! `SCRIP_CD ↔ ISIN ↔ symbol` map plus a free full-universe `Mktcap` anchor
//! (₹ crore); NSE catches NSE-only edge cases. NSE-only entries have no mcap
//! and are counted + disclosed, never silently dropped.
//!
//! Phase 2: coverage is floor-based (`partition_by_floor`), not top-N — every
//! entry at/above the entry floor is covered, with hysteresis (a previously
//! covered symbol stays covered down to the exit floor, so mcap wobble around
//! the boundary never churns the universe).

use std::collections::{HashMap, HashSet};

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

/// Floor-based coverage partition (Phase 2). `covered` is sorted by mcap
/// descending (deterministic; also what `--limit` truncates for smoke runs).
pub struct UniversePartition<'a> {
    pub covered: Vec<&'a UniverseEntry>,
    /// Entries with a known mcap below the (hysteresis-adjusted) floor.
    pub below_floor: usize,
    /// Entries with no mcap at all (NSE-only — unrankable, disclosed).
    pub unrankable: usize,
    /// Covered only thanks to hysteresis (exit floor ≤ mcap < entry floor).
    pub hysteresis_retained: usize,
}

impl Universe {
    /// Partition the universe by market-cap floor with hysteresis:
    /// - enter coverage at `enter_cr` (design: ₹800 cr),
    /// - a symbol in `previously_covered` stays covered down to `exit_cr`
    ///   (design: ₹720 cr) so boundary wobble never churns the universe.
    ///
    /// Entries without a mcap (NSE-only) cannot be ranked against a floor and
    /// are counted as `unrankable`, never silently dropped.
    pub fn partition_by_floor(
        &self,
        enter_cr: f64,
        exit_cr: f64,
        previously_covered: &HashSet<String>,
    ) -> UniversePartition<'_> {
        let mut covered: Vec<&UniverseEntry> = Vec::new();
        let mut below_floor = 0usize;
        let mut unrankable = 0usize;
        let mut hysteresis_retained = 0usize;
        for e in &self.entries {
            match e.mktcap_cr {
                None => unrankable += 1,
                Some(m) if m >= enter_cr => covered.push(e),
                Some(m) if m >= exit_cr && previously_covered.contains(&e.instrument_key) => {
                    hysteresis_retained += 1;
                    covered.push(e);
                }
                Some(_) => below_floor += 1,
            }
        }
        covered.sort_by(|a, b| {
            b.mktcap_cr
                .partial_cmp(&a.mktcap_cr)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.instrument_key.cmp(&b.instrument_key))
        });
        UniversePartition { covered, below_floor, unrankable, hysteresis_retained }
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
    let series_i = idx("SERIES").ok();

    let mut nse_count = 0usize;
    for line in lines {
        let fields: Vec<&str> = line.split(',').map(|f| f.trim()).collect();
        if fields.len() <= isin_i.max(sym_i) {
            continue;
        }
        // Equity universe = EQ series only (ETFs / fund units / debt series
        // like GB/GS/BE never enter the fundamentals universe from NSE).
        if let Some(si) = series_i {
            if fields.get(si).copied().unwrap_or_default() != "EQ" {
                continue;
            }
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

    const NSE: &str = "SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE\nABB,ABB India Limited,EQ,06-OCT-2008,2,1,INE117A01022,2\nNSEONLY,NSE Only Co,EQ,01-JAN-2020,10,1,INE555Y01019,10\nBONDISH,Bond Series Co,GB,01-JAN-2020,10,1,INE777B01011,10\n";

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
    fn nse_non_eq_series_is_excluded() {
        let u = build_universe(BSE, NSE).unwrap();
        assert!(
            !u.entries.iter().any(|e| e.instrument_key == "INE777B01011"),
            "GB-series row must never enter the fundamentals universe"
        );
        assert_eq!(u.nse_count, 2, "only the EQ rows count as NSE entries");
    }

    #[test]
    fn floor_partition_covers_at_entry_floor_and_discloses_the_rest() {
        let u = build_universe(BSE, NSE).unwrap();
        let none = HashSet::new();
        // Entry floor 800: ABB (144808) and TTK (12000) covered; NSE-only
        // (no mcap) disclosed as unrankable, never silently dropped.
        let p = u.partition_by_floor(800.0, 720.0, &none);
        assert_eq!(p.covered.len(), 2);
        assert_eq!(p.covered[0].instrument_key, "INE117A01022"); // mcap desc
        assert_eq!(p.below_floor, 0);
        assert_eq!(p.unrankable, 1);
        assert_eq!(p.hysteresis_retained, 0);

        // Entry floor above TTK: TTK drops to below_floor.
        let p = u.partition_by_floor(20_000.0, 18_000.0, &none);
        assert_eq!(p.covered.len(), 1);
        assert_eq!(p.below_floor, 1);
    }

    #[test]
    fn hysteresis_retains_previously_covered_between_floors() {
        let u = build_universe(BSE, NSE).unwrap();
        // TTK mcap 12000 sits between exit (10000) and entry (13000) floors.
        let mut prev = HashSet::new();
        prev.insert("INE690A01028".to_string());
        let p = u.partition_by_floor(13_000.0, 10_000.0, &prev);
        assert!(p.covered.iter().any(|e| e.instrument_key == "INE690A01028"));
        assert_eq!(p.hysteresis_retained, 1);

        // Same floors, no prior coverage → TTK is out (enter only at ≥13000).
        let p = u.partition_by_floor(13_000.0, 10_000.0, &HashSet::new());
        assert!(!p.covered.iter().any(|e| e.instrument_key == "INE690A01028"));
        assert_eq!(p.below_floor, 1);

        // Below even the exit floor → hysteresis cannot retain it.
        let p = u.partition_by_floor(13_000.0, 12_500.0, &prev);
        assert!(!p.covered.iter().any(|e| e.instrument_key == "INE690A01028"));
    }

    #[test]
    fn scrip_to_isin_map_covers_bse_entries() {
        let u = build_universe(BSE, NSE).unwrap();
        let map = u.scrip_to_isin();
        assert_eq!(map.get("517506").map(String::as_str), Some("INE690A01028"));
        assert_eq!(map.len(), 2);
    }
}
