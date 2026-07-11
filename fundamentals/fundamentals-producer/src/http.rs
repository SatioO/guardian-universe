//! Polite HTTP client: browser UA, HTTP/1.1 (ureq is HTTP/1.1-only, which is
//! exactly what the SOURCE-CONTRACT requires for BSE + nsearchives), a global
//! ≥`throttle_ms` gap between requests, and 3x exponential-backoff retry.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Real-browser UA — required by both BSE (api./www.bseindia.com) and
/// nsearchives (Akamai fingerprint gate). See SOURCE-CONTRACT.md §1/§3.
pub const BROWSER_UA: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

pub struct PoliteClient {
    agent: ureq::Agent,
    throttle: Duration,
    last_request: Mutex<Option<Instant>>,
    /// Total requests issued (for the run summary).
    requests: Mutex<u64>,
}

impl PoliteClient {
    pub fn new(throttle_ms: u64) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(60))
            .redirects(4)
            .build();
        Self {
            agent,
            throttle: Duration::from_millis(throttle_ms),
            last_request: Mutex::new(None),
            requests: Mutex::new(0),
        }
    }

    pub fn request_count(&self) -> u64 {
        *self.requests.lock().unwrap()
    }

    fn wait_turn(&self) {
        let mut last = self.last_request.lock().unwrap();
        if let Some(prev) = *last {
            let elapsed = prev.elapsed();
            if elapsed < self.throttle {
                std::thread::sleep(self.throttle - elapsed);
            }
        }
        *last = Some(Instant::now());
        *self.requests.lock().unwrap() += 1;
    }

    /// GET `url` with the browser UA and optional Referer; throttled; retried
    /// 3x with exponential backoff (1s / 3s / 9s) on transport errors, 429 and 5xx.
    pub fn get_bytes(&self, url: &str, referer: Option<&str>) -> Result<Vec<u8>, String> {
        let mut delay = Duration::from_secs(1);
        let mut last_err = String::new();
        for attempt in 0..=3 {
            if attempt > 0 {
                eprintln!("  retry {attempt}/3 after {}s: {url}", delay.as_secs());
                std::thread::sleep(delay);
                delay *= 3;
            }
            self.wait_turn();
            let mut req = self
                .agent
                .get(url)
                .set("User-Agent", BROWSER_UA)
                .set("Accept", "*/*");
            if let Some(r) = referer {
                req = req.set("Referer", r);
            }
            match req.call() {
                Ok(resp) => {
                    let mut buf = Vec::new();
                    use std::io::Read;
                    match resp.into_reader().read_to_end(&mut buf) {
                        Ok(_) => return Ok(buf),
                        Err(e) => last_err = format!("body read: {e}"),
                    }
                }
                Err(ureq::Error::Status(code, _)) if code == 429 || code >= 500 => {
                    last_err = format!("HTTP {code}");
                }
                Err(ureq::Error::Status(code, _)) => {
                    // Non-retryable client error.
                    return Err(format!("HTTP {code} for {url}"));
                }
                Err(e) => last_err = format!("transport: {e}"),
            }
        }
        Err(format!("GET {url} failed after retries: {last_err}"))
    }

    pub fn get_text(&self, url: &str, referer: Option<&str>) -> Result<String, String> {
        let bytes = self.get_bytes(url, referer)?;
        String::from_utf8(bytes).map_err(|e| format!("non-UTF8 body from {url}: {e}"))
    }
}
