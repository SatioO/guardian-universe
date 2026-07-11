//! Bulk fundamentals producer (P5 Phase 2).
//!
//! Pipeline: universe seed (BSE ListofScripData ∪ NSE EQUITY_L.csv EQ-series,
//! ISIN-keyed) → market-cap floor partition with hysteresis (enter ≥800 cr,
//! exit <720 cr) → canary → bulk discovery via the FilingSource registry →
//! raw in-capmkt XBRL fetch → fundamentals-core parse → §3.2 rows (general
//! sector only) → Gate-1 blocks + row-level dq_flags → deterministic
//! `fundamentals_all.parquet` + `fundamentals_state.json` + `run_summary.json`
//! (the machine-readable signal CI publishes on).
//!
//! Politeness: HTTP/1.1 + browser UA + BSE Referer, ≥1.5 s between requests,
//! 3x exponential-backoff retry (SOURCE-CONTRACT.md §11).

mod bse;
mod gate;
mod http;
mod output;
mod pipeline;
mod rows;
mod source;
mod state;
mod universe;

use clap::Parser;
use std::path::PathBuf;

use crate::pipeline::{run, RunConfig, RunSummary};
use crate::source::DiscoveryWindow;

#[derive(Parser, Debug)]
#[command(name = "fundamentals-producer", about = "Bulk BSE+NSE fundamentals producer (P5 Phase 2)")]
struct Args {
    /// Coverage entry floor: cover symbols with scrip-master Mktcap ≥ this (₹ crore).
    #[arg(long, default_value_t = 800.0)]
    min_mktcap_cr: f64,

    /// Hysteresis exit floor: previously covered symbols stay covered down to
    /// this (₹ crore). Must be ≤ --min-mktcap-cr.
    #[arg(long, default_value_t = 720.0)]
    exit_mktcap_cr: f64,

    /// Optional cap on the covered universe (mcap-descending) — smoke runs only.
    #[arg(long)]
    limit: Option<usize>,

    /// Output directory (fundamentals_all.parquet + fundamentals_state.json + run_summary.json).
    #[arg(long, default_value = "out")]
    out: PathBuf,

    /// Discovery window: today | week | 15d | month | 3m | 1y.
    /// Daily incremental uses `week` (overlap-safe); first full run: `3m`/`1y`.
    #[arg(long, default_value = "week")]
    window: String,

    /// Minimum milliseconds between HTTP requests (politeness floor 1500).
    #[arg(long, default_value_t = 1500)]
    throttle_ms: u64,
}

fn parse_window(s: &str) -> Result<DiscoveryWindow, String> {
    match s {
        "today" => Ok(DiscoveryWindow::Today),
        "week" => Ok(DiscoveryWindow::LastWeek),
        "15d" => Ok(DiscoveryWindow::Last15Days),
        "month" => Ok(DiscoveryWindow::LastMonth),
        "3m" => Ok(DiscoveryWindow::Last3Months),
        "1y" => Ok(DiscoveryWindow::LastYear),
        other => Err(format!("unknown window '{other}' (today|week|15d|month|3m|1y)")),
    }
}

/// Machine-readable run summary for CI (`run_summary.json` in the out dir):
/// the workflow's publish decision keys off `parquet_written` so a no-change
/// day never touches the release. Only written for successful runs — a failed
/// run exits non-zero and CI never reaches the publish gate.
fn write_run_summary(out_dir: &std::path::Path, s: &RunSummary) -> Result<(), String> {
    let path = out_dir.join("run_summary.json");
    let json = serde_json::to_string_pretty(s).map_err(|e| format!("summary serialize: {e}"))?;
    std::fs::write(&path, json.as_bytes()).map_err(|e| format!("write {}: {e}", path.display()))
}

fn main() {
    let args = Args::parse();
    let window = match parse_window(&args.window) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    // Politeness floor: never hammer, even if asked to.
    let throttle_ms = args.throttle_ms.max(1500);

    let cfg = RunConfig {
        min_mktcap_cr: args.min_mktcap_cr,
        exit_mktcap_cr: args.exit_mktcap_cr,
        limit: args.limit,
        out_dir: args.out,
        window,
        throttle_ms,
    };

    match run(&cfg) {
        Ok(s) => {
            println!("\n=== run summary ===");
            println!("universe union            : {} (nse-only: {})", s.universe_union, s.universe_nse_only);
            println!(
                "covered                   : {} (below floor: {}, unrankable/no-mcap: {}, hysteresis-retained: {})",
                s.covered, s.below_floor, s.unrankable_no_mcap, s.hysteresis_retained
            );
            println!("discovered refs           : {}", s.discovered_refs);
            println!("refs in covered universe  : {}", s.refs_in_universe);
            println!("selected filings          : {}", s.selected_filings);
            println!("already processed         : {}", s.already_processed);
            println!("pending (no XMLName yet)  : {}", s.pending_no_xml);
            println!("instances fetched         : {}", s.fetched);
            println!("fetch errors              : {}", s.fetch_errors.len());
            for e in &s.fetch_errors {
                println!("   - {e}");
            }
            println!("parse errors              : {}", s.parse_errors.len());
            for e in &s.parse_errors {
                println!("   - {e}");
            }
            println!("skipped non-general (D6)  : {}", s.skipped_non_general.len());
            for e in &s.skipped_non_general {
                println!("   - {e}");
            }
            println!("gate-1 blocked rows       : {}", s.gate_blocked.len());
            for e in &s.gate_blocked {
                println!("   - {e}");
            }
            println!("rows flagged (published)  : {}", s.rows_flagged);
            println!("rows new/updated          : {}", s.rows_new_or_updated);
            println!("rows total in parquet     : {}", s.rows_total);
            println!("parquet rewritten         : {}", s.parquet_written);
            println!("http requests             : {}", s.http_requests);
            println!("wall time                 : {:.1?}", s.wall);
            if let Err(e) = write_run_summary(&cfg.out_dir, &s) {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
