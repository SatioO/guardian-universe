//! Incremental producer state (`state.json`).
//!
//! Deterministic serialization (BTree collections, no timestamps) so a re-run
//! with no new filings writes byte-identical state — half of the Phase-1
//! idempotency milestone.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SymbolState {
    /// Newest successfully-published period end (ISO date).
    #[serde(default)]
    pub last_period_end: Option<String>,
    /// Basis of that newest period ("standalone" | "consolidated").
    #[serde(default)]
    pub last_basis: Option<String>,
    /// Dedup keys of (filing, document) pairs already fetched+processed —
    /// including non-general skips and Gate-1 blocks (with outcome recorded
    /// below), so they are not refetched every run.
    #[serde(default)]
    pub processed: BTreeSet<String>,
    /// Filing keys broadcast WITHOUT an XBRL locator yet (PDF-first lag).
    /// Stays pending until a locator appears — never marked done off the
    /// PDF row (SOURCE-CONTRACT §9.4).
    #[serde(default)]
    pub pending_xml: BTreeSet<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ProducerState {
    pub version: u32,
    /// instrument_key (ISIN) → per-symbol incremental state. Refs that could
    /// not be resolved to an ISIN key under their source-native id prefixed
    /// with the source id.
    #[serde(default)]
    pub symbols: BTreeMap<String, SymbolState>,
}

impl ProducerState {
    pub fn new() -> Self {
        Self { version: 1, symbols: BTreeMap::new() }
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("state read {}: {e}", path.display()))?;
        serde_json::from_str(&raw).map_err(|e| format!("state parse {}: {e}", path.display()))
    }

    /// Deterministic pretty JSON, atomic tmp+rename.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("state serialize: {e}"))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, json.as_bytes())
            .map_err(|e| format!("state write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| format!("state rename {}: {e}", path.display()))
    }

    pub fn symbol_mut(&mut self, key: &str) -> &mut SymbolState {
        self.symbols.entry(key.to_string()).or_default()
    }

    pub fn is_processed(&self, symbol_key: &str, dedup_key: &str) -> bool {
        self.symbols
            .get(symbol_key)
            .map(|s| s.processed.contains(dedup_key))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_is_byte_identical() {
        let mut st = ProducerState::new();
        {
            let s = st.symbol_mut("INE690A01028");
            s.last_period_end = Some("2026-03-31".into());
            s.last_basis = Some("standalone".into());
            s.processed.insert("517506|MQ2025-2026|standalone|a.html".into());
            s.pending_xml.insert("999999|JQ2026-2027".into());
        }
        let a = serde_json::to_string_pretty(&st).unwrap();
        let back: ProducerState = serde_json::from_str(&a).unwrap();
        let b = serde_json::to_string_pretty(&back).unwrap();
        assert_eq!(a, b, "state serialization must be deterministic");
        assert_eq!(st, back);
    }

    #[test]
    fn save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut st = ProducerState::new();
        st.symbol_mut("INE117A01022").processed.insert("k1".into());
        st.save(&path).unwrap();
        let bytes1 = std::fs::read(&path).unwrap();
        // Saving the same state again → byte-identical file.
        let loaded = ProducerState::load(&path).unwrap();
        loaded.save(&path).unwrap();
        let bytes2 = std::fs::read(&path).unwrap();
        assert_eq!(bytes1, bytes2);
        assert!(loaded.is_processed("INE117A01022", "k1"));
        assert!(!loaded.is_processed("INE117A01022", "k2"));
    }

    #[test]
    fn missing_state_file_is_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let st = ProducerState::load(&dir.path().join("nope.json")).unwrap();
        assert!(st.symbols.is_empty());
        assert_eq!(st.version, 1);
    }
}
