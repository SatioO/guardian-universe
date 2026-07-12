//! Incremental producer state (`state.json`).
//!
//! Deterministic serialization (BTree collections, no timestamps) so a re-run
//! with no new filings writes byte-identical state — half of the Phase-1
//! idempotency milestone.
//!
//! # Versioning (Phase 3)
//! `version` is a schema/semantics version for the state file itself.
//!
//! - **v1** (Phases 1–2): `processed` recorded WHICH (filing, document) pairs
//!   were handled but not WHY — a bank filing skipped under design D6 was
//!   indistinguishable from a published one.
//! - **v2** (Phase 3): every processed dedup key also records its
//!   [`Outcome`] in `outcomes`, so future migrations can invalidate exactly
//!   one outcome class (e.g. "re-ingest everything we skipped as
//!   `skipped_non_general`") without touching anything else — see
//!   [`ProducerState::invalidate_outcome`].
//!
//! **v1 → v2 migration** (`migrate`): v1 cannot be invalidated per outcome
//! (outcomes weren't recorded), so the migration uses the accumulated parquet
//! as ground truth: a symbol with ZERO published rows only ever produced
//! non-publishing outcomes (the D6 non-general skips, plus a handful of
//! parse errors / ISIN-identity skips) — its `processed` set is cleared so
//! Phase 3 re-fetches exactly that backlog. Symbols WITH published rows keep
//! their state untouched, so the ~2,700-row general universe is NOT
//! re-fetched. (A symbol with both published rows and a D6 skip cannot occur
//! in practice — sector classification is stable per issuer; if one ever
//! slipped through, the cost is one missed re-ingest until the next filing
//! revision, never wrong data.)

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Current state-file schema version.
pub const STATE_VERSION: u32 = 2;

/// Why a (filing, document) was marked processed. Recorded from v2 on so
/// future state bumps can invalidate a single outcome class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// At least one row from this document reached the parquet.
    Published,
    /// Sector fingerprint yielded no classification — never guessed (D6).
    SkippedUnclassified,
    /// Instance identity could not be anchored to the universe key.
    IdentityMismatch,
    /// Structural/parse failure on fetched bytes.
    ParseError,
    /// Every candidate row was hard-blocked by Gate-1.
    GateBlocked,
    /// Backfill (Phase 4): the instance document is permanently absent at the
    /// source (HTTP 404 on a historical locator that has had months to
    /// appear). Recorded so the shard does not re-request it forever;
    /// distinguishable from transient fetch errors, which are NEVER recorded.
    InstanceUnavailable,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SymbolState {
    /// Newest successfully-published period end (ISO date).
    #[serde(default)]
    pub last_period_end: Option<String>,
    /// Basis of that newest period ("standalone" | "consolidated").
    #[serde(default)]
    pub last_basis: Option<String>,
    /// Dedup keys of (filing, document) pairs already fetched+processed —
    /// including skips and Gate-1 blocks (with outcome recorded below), so
    /// they are not refetched every run.
    #[serde(default)]
    pub processed: BTreeSet<String>,
    /// dedup key → why it was processed (v2+; v1 entries have no outcome).
    #[serde(default)]
    pub outcomes: BTreeMap<String, Outcome>,
    /// Filing keys broadcast WITHOUT an XBRL locator yet (PDF-first lag).
    /// Stays pending until a locator appears — never marked done off the
    /// PDF row (SOURCE-CONTRACT §9.4).
    #[serde(default)]
    pub pending_xml: BTreeSet<String>,
    /// Phase 4 backfill bookkeeping. `Some(iso-date + from-date)` = this
    /// symbol's full history was discovered and every era filing processed
    /// (outcome recorded) with NO transient errors, under the recorded
    /// `--backfill-from` era start. A later backfill run with the SAME or
    /// NEWER `from` skips the symbol entirely (resume costs zero requests);
    /// a DEEPER `from` re-scans it. Skipped when None so pre-Phase-4 state
    /// files rewrite byte-identically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfilled: Option<BackfillMark>,
}

/// Proof-of-completion marker for one symbol's backfill.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackfillMark {
    /// ISO date the scan completed.
    pub scanned_at: String,
    /// The `--backfill-from` era floor the scan used (broadcast-date ISO).
    pub from: String,
    /// Filings older than `from` that were skipped WITHOUT fetching —
    /// the pre-era outcome, recorded compactly (per-symbol count, not one
    /// state key per decade-old filing) so state stays small.
    pub pre_era_skipped: u32,
}

impl SymbolState {
    /// Mark a dedup key processed with its outcome (v2 semantics).
    pub fn record(&mut self, dedup_key: &str, outcome: Outcome) {
        self.processed.insert(dedup_key.to_string());
        self.outcomes.insert(dedup_key.to_string(), outcome);
    }
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
        Self { version: STATE_VERSION, symbols: BTreeMap::new() }
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

    /// Migrate an older state file to [`STATE_VERSION`]. Idempotent: a
    /// current-version state is returned untouched.
    ///
    /// v1 → v2: clear `processed` for every symbol WITHOUT published parquet
    /// rows (`published_symbols` = instrument keys present in the accumulated
    /// parquet). That is exactly the Phase-2 non-publishing backlog — the D6
    /// non-general skips plus the ISIN-identity/parse skips — re-ingested on
    /// the next run at the cost of one polite fetch each, while the general
    /// universe's state (and therefore its fetch schedule) is untouched.
    /// `pending_xml` survives migration: pending is pending in any version.
    pub fn migrate(&mut self, published_symbols: &HashSet<String>) -> usize {
        if self.version >= STATE_VERSION {
            return 0;
        }
        let mut invalidated = 0usize;
        for (key, sym) in self.symbols.iter_mut() {
            if !published_symbols.contains(key) {
                invalidated += sym.processed.len();
                sym.processed.clear();
                sym.outcomes.clear();
            }
        }
        self.version = STATE_VERSION;
        invalidated
    }

    /// Targeted invalidation for FUTURE state bumps (v2+ semantics): drop
    /// every processed entry whose recorded outcome matches, so only that
    /// class is re-fetched. Entries without a recorded outcome are kept.
    #[allow(dead_code)]
    pub fn invalidate_outcome(&mut self, outcome: Outcome) -> usize {
        let mut invalidated = 0usize;
        for sym in self.symbols.values_mut() {
            let keys: Vec<String> = sym
                .outcomes
                .iter()
                .filter(|(_, o)| **o == outcome)
                .map(|(k, _)| k.clone())
                .collect();
            for k in keys {
                sym.processed.remove(&k);
                sym.outcomes.remove(&k);
                invalidated += 1;
            }
        }
        invalidated
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
            s.record("517506|MQ2025-2026|standalone|a.html", Outcome::Published);
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
        st.symbol_mut("INE117A01022").record("k1", Outcome::Published);
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
        assert_eq!(st.version, STATE_VERSION);
    }

    #[test]
    fn backfill_mark_round_trips_and_absence_stays_absent() {
        let mut st = ProducerState::new();
        st.symbol_mut("INE002A01018").backfilled = Some(BackfillMark {
            scanned_at: "2026-07-12".into(),
            from: "2023-04-01".into(),
            pre_era_skipped: 41,
        });
        st.symbol_mut("INE117A01022").record("k", Outcome::Published);
        let json = serde_json::to_string_pretty(&st).unwrap();
        // Symbols without a mark must not serialize the field at all — a
        // pre-Phase-4 state file rewrites byte-identically.
        assert_eq!(json.matches("backfilled").count(), 1);
        let back: ProducerState = serde_json::from_str(&json).unwrap();
        assert_eq!(back, st);
        assert!(back.symbols["INE117A01022"].backfilled.is_none());
    }

    #[test]
    fn v1_state_json_loads_with_empty_outcomes() {
        // A real Phase-2 state file has no `outcomes` field.
        let v1 = r#"{
          "version": 1,
          "symbols": {
            "INE040A01034": {
              "processed": ["500180|MQ2025-2026|consolidated|a.html"],
              "pending_xml": []
            }
          }
        }"#;
        let st: ProducerState = serde_json::from_str(v1).unwrap();
        assert_eq!(st.version, 1);
        assert!(st.symbols["INE040A01034"].outcomes.is_empty());
        assert!(st.is_processed("INE040A01034", "500180|MQ2025-2026|consolidated|a.html"));
    }

    #[test]
    fn migrate_v1_invalidates_only_unpublished_symbols() {
        let mut st = ProducerState { version: 1, symbols: BTreeMap::new() };
        // General symbol WITH published rows — must keep its state.
        st.symbol_mut("INE002A01018").processed.insert("gen|doc".into());
        st.symbol_mut("INE002A01018").last_period_end = Some("2026-03-31".into());
        // Bank symbol skipped under D6 — no rows → must be invalidated.
        st.symbol_mut("INE040A01034").processed.insert("bank|doc".into());
        // Pending marker must survive on both.
        st.symbol_mut("INE040A01034").pending_xml.insert("500180|JQ2026-2027".into());

        let published: HashSet<String> = ["INE002A01018".to_string()].into_iter().collect();
        let n = st.migrate(&published);

        assert_eq!(n, 1, "exactly the bank doc invalidated");
        assert_eq!(st.version, STATE_VERSION);
        assert!(st.is_processed("INE002A01018", "gen|doc"), "published symbol untouched");
        assert!(!st.is_processed("INE040A01034", "bank|doc"), "skipped symbol re-ingestable");
        assert!(
            st.symbols["INE040A01034"].pending_xml.contains("500180|JQ2026-2027"),
            "pending survives migration"
        );
        assert_eq!(
            st.symbols["INE002A01018"].last_period_end.as_deref(),
            Some("2026-03-31")
        );
    }

    #[test]
    fn migrate_is_idempotent_on_current_version() {
        let mut st = ProducerState::new();
        st.symbol_mut("INE040A01034").record("bank|doc", Outcome::Published);
        let before = st.clone();
        let n = st.migrate(&HashSet::new()); // empty published set
        assert_eq!(n, 0, "v2 state must never be invalidated by migrate()");
        assert_eq!(st, before);
    }

    #[test]
    fn invalidate_outcome_targets_one_class_only() {
        let mut st = ProducerState::new();
        st.symbol_mut("INE0A").record("a|1", Outcome::Published);
        st.symbol_mut("INE0A").record("a|2", Outcome::SkippedUnclassified);
        st.symbol_mut("INE0B").record("b|1", Outcome::SkippedUnclassified);
        st.symbol_mut("INE0C").record("c|1", Outcome::GateBlocked);

        let n = st.invalidate_outcome(Outcome::SkippedUnclassified);
        assert_eq!(n, 2);
        assert!(st.is_processed("INE0A", "a|1"), "published entry kept");
        assert!(!st.is_processed("INE0A", "a|2"));
        assert!(!st.is_processed("INE0B", "b|1"));
        assert!(st.is_processed("INE0C", "c|1"), "other classes kept");
    }

    #[test]
    fn outcome_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&Outcome::SkippedUnclassified).unwrap(),
            "\"skipped_unclassified\""
        );
        assert_eq!(
            serde_json::from_str::<Outcome>("\"identity_mismatch\"").unwrap(),
            Outcome::IdentityMismatch
        );
    }
}
