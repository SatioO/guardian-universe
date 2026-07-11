//! Pluggable filing-source abstraction (owner mandate).
//!
//! Discovery ("who filed, and where are the instance bytes") and instance
//! fetching sit behind [`FilingSource`] + [`SourceRegistry`], mirroring the
//! traderview broker / historical-provider doctrine:
//!
//! - **No source id is hardcoded at call sites.** The pipeline resolves the
//!   serving source from the registry ([`SourceRegistry::resolve`]) using the
//!   `source_id` recorded on each [`FilingRef`], and discovers via the
//!   registry's ordered fallback chain ([`SourceRegistry::chain`]).
//! - Adding a premium/commercial provider = implement [`FilingSource`] +
//!   register it. Zero edits at call sites.
//! - Source-native identity (e.g. a BSE scrip code) never leaks upward as a
//!   join key: each source resolves refs to the canonical `instrument_key`
//!   (ISIN) itself, from seed data injected at construction.
//! - Per-period provenance: every published row records `source_channel` =
//!   the `source_id` that served its instance bytes.

use fundamentals_core::StatementBasis;

/// Rolling discovery window (maps onto whatever the source supports; BSE maps
/// these onto its `FlagDur` values).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryWindow {
    Today,
    LastWeek,
    Last15Days,
    LastMonth,
    Last3Months,
    LastYear,
}

/// One discovered filing: who filed + an opaque instance locator.
#[derive(Debug, Clone)]
pub struct FilingRef {
    /// The registry id of the source that produced this ref.
    pub source_id: String,
    /// Source-native issuer id (opaque outside the source; e.g. BSE scrip code).
    pub native_id: String,
    /// Canonical join key (ISIN), resolved *inside* the source when possible.
    pub instrument_key: Option<String>,
    pub company_name: String,
    /// Source-native period tag (opaque; e.g. BSE `quarter_code`). Used only
    /// for dedup/state keys, never parsed into dates (grammar unconfirmed).
    pub period_hint: String,
    pub basis_hint: Option<StatementBasis>,
    /// Source-native broadcast timestamp (opaque, used in state keys).
    pub broadcast_at: String,
    /// Exchange-feed audit hint. The instance's own
    /// `WhetherResultsAreAuditedOrUnaudited` fact is authoritative; this hint
    /// exists for sources whose instances lack the fact.
    #[allow(dead_code)]
    pub is_audited_hint: bool,
    /// Opaque locator for the raw XBRL instance. `None` = the filing exists
    /// (e.g. a PDF was broadcast) but the XBRL attachment has not appeared yet
    /// — the pipeline must keep it PENDING, never mark it done.
    pub instance_locator: Option<String>,
}

impl FilingRef {
    /// Stable identity of this exact (filing, document) for incremental state.
    /// A revision (new locator / new broadcast time) produces a new key.
    pub fn dedup_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.native_id,
            self.period_hint,
            self.basis_hint.map(|b| b.as_str()).unwrap_or("?"),
            self.instance_locator.as_deref().unwrap_or("<no-instance>"),
        )
    }

    /// Filing identity ignoring the document (for pending tracking).
    pub fn filing_key(&self) -> String {
        format!("{}|{}", self.native_id, self.period_hint)
    }
}

/// A pluggable provider of filing discovery + raw XBRL instance bytes.
pub trait FilingSource {
    /// Stable registry id (e.g. `"bse"`). Never matched against string
    /// literals outside the source's own module and its registration site.
    fn source_id(&self) -> &'static str;

    /// Bulk discovery over a rolling window: who filed + instance locators.
    fn discover(&self, window: DiscoveryWindow) -> Result<Vec<FilingRef>, String>;

    /// Fetch the raw XBRL instance bytes for a ref this source discovered.
    fn fetch_instance(&self, r: &FilingRef) -> Result<Vec<u8>, String>;
}

/// Ordered registry of filing sources. Registration order = fallback order.
#[derive(Default)]
pub struct SourceRegistry {
    sources: Vec<Box<dyn FilingSource>>,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, source: Box<dyn FilingSource>) {
        self.sources.push(source);
    }

    /// Resolve a source by id (how the pipeline routes `FilingRef`s back to
    /// the source that owns them).
    pub fn resolve(&self, id: &str) -> Option<&dyn FilingSource> {
        self.sources
            .iter()
            .find(|s| s.source_id() == id)
            .map(|s| s.as_ref())
    }

    /// Ordered fallback chain for discovery.
    pub fn chain(&self) -> impl Iterator<Item = &dyn FilingSource> {
        self.sources.iter().map(|s| s.as_ref())
    }

    pub fn ids(&self) -> Vec<&'static str> {
        self.sources.iter().map(|s| s.source_id()).collect()
    }

    /// Discover via the fallback chain: first source that succeeds serves the
    /// run. Returns `(serving_source_id, refs)`.
    pub fn discover(&self, window: DiscoveryWindow) -> Result<(String, Vec<FilingRef>), String> {
        let mut errors: Vec<String> = Vec::new();
        for source in self.chain() {
            match source.discover(window) {
                Ok(refs) => return Ok((source.source_id().to_string(), refs)),
                Err(e) => errors.push(format!("{}: {e}", source.source_id())),
            }
        }
        Err(if errors.is_empty() {
            "no filing sources registered".to_string()
        } else {
            format!("all filing sources failed: {}", errors.join("; "))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeSource {
        id: &'static str,
        fail_discovery: bool,
    }

    impl FilingSource for FakeSource {
        fn source_id(&self) -> &'static str {
            self.id
        }
        fn discover(&self, _w: DiscoveryWindow) -> Result<Vec<FilingRef>, String> {
            if self.fail_discovery {
                return Err("boom".into());
            }
            Ok(vec![FilingRef {
                source_id: self.id.to_string(),
                native_id: "123".into(),
                instrument_key: Some("INE000A01001".into()),
                company_name: "Fake Co".into(),
                period_hint: "MQ2025-2026".into(),
                basis_hint: Some(StatementBasis::Consolidated),
                broadcast_at: "2026-07-11T21:18:07".into(),
                is_audited_hint: true,
                instance_locator: Some("doc.xml".into()),
            }])
        }
        fn fetch_instance(&self, _r: &FilingRef) -> Result<Vec<u8>, String> {
            Ok(b"<xbrl/>".to_vec())
        }
    }

    #[test]
    fn resolve_finds_registered_source_by_id() {
        let mut reg = SourceRegistry::new();
        reg.register(Box::new(FakeSource { id: "test-src", fail_discovery: false }));
        assert!(reg.resolve("test-src").is_some());
        assert!(reg.resolve("unknown").is_none());
        assert_eq!(reg.ids(), vec!["test-src"]);
    }

    #[test]
    fn discovery_falls_back_down_the_chain() {
        let mut reg = SourceRegistry::new();
        reg.register(Box::new(FakeSource { id: "primary", fail_discovery: true }));
        reg.register(Box::new(FakeSource { id: "fallback", fail_discovery: false }));
        let (served_by, refs) = reg.discover(DiscoveryWindow::LastWeek).unwrap();
        assert_eq!(served_by, "fallback");
        assert_eq!(refs.len(), 1);
        // Provenance: refs carry the id of the source that produced them.
        assert_eq!(refs[0].source_id, "fallback");
    }

    #[test]
    fn discovery_with_no_sources_is_err() {
        let reg = SourceRegistry::new();
        assert!(reg.discover(DiscoveryWindow::Today).is_err());
    }

    #[test]
    fn all_sources_failing_is_err_with_reasons() {
        let mut reg = SourceRegistry::new();
        reg.register(Box::new(FakeSource { id: "a", fail_discovery: true }));
        let err = reg.discover(DiscoveryWindow::Today).unwrap_err();
        assert!(err.contains("a: boom"), "err = {err}");
    }

    #[test]
    fn dedup_key_distinguishes_revisions_and_pending() {
        let mut r = FilingRef {
            source_id: "s".into(),
            native_id: "500325".into(),
            instrument_key: None,
            company_name: "X".into(),
            period_hint: "MQ2025-2026".into(),
            basis_hint: Some(StatementBasis::Standalone),
            broadcast_at: "t1".into(),
            is_audited_hint: false,
            instance_locator: None,
        };
        let pending_key = r.dedup_key();
        assert!(pending_key.ends_with("<no-instance>"));
        r.instance_locator = Some("A.xml".into());
        let k1 = r.dedup_key();
        r.instance_locator = Some("B.xml".into());
        let k2 = r.dedup_key();
        assert_ne!(k1, k2, "a revised document must produce a new dedup key");
        assert_eq!(r.filing_key(), "500325|MQ2025-2026");
    }
}
