//! Bulk fundamentals producer (P5 Phase 1).
//!
//! Pipeline: universe seed (BSE ListofScripData ∪ NSE EQUITY_L.csv, ISIN-keyed)
//! → top-N by scrip-master Mktcap → bulk discovery via the FilingSource
//! registry → raw in-capmkt XBRL fetch → fundamentals-core parse → §3.2 rows
//! (general sector only) → Gate-1 DQ wall → deterministic
//! `fundamentals.parquet` + `state.json`.
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

use crate::pipeline::{run, RunConfig};
use crate::source::DiscoveryWindow;

#[derive(Parser, Debug)]
#[command(name = "fundamentals-producer", about = "Bulk BSE+NSE fundamentals producer (P5 Phase 1)")]
struct Args {
    /// Produce for the top-N universe entries by market cap.
    #[arg(long, default_value_t = 50)]
    top: usize,

    /// Output directory (fundamentals.parquet + state.json).
    #[arg(long, default_value = "out")]
    out: PathBuf,

    /// Discovery window: today | week | 15d | month | 3m | 1y.
    #[arg(long, default_value = "1y")]
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
        top: args.top,
        out_dir: args.out,
        window,
        throttle_ms,
    };

    match run(&cfg) {
        Ok(s) => {
            println!("\n=== run summary ===");
            println!("universe union            : {} (nse-only: {})", s.universe_union, s.universe_nse_only);
            println!("top-N                     : {}", s.top_n);
            println!("discovered refs           : {}", s.discovered_refs);
            println!("refs in top-N             : {}", s.refs_in_top_n);
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
            println!("rows new/updated          : {}", s.rows_new_or_updated);
            println!("rows total in parquet     : {}", s.rows_total);
            println!("parquet rewritten         : {}", s.parquet_written);
            println!("http requests             : {}", s.http_requests);
            println!("wall time                 : {:.1?}", s.wall);
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
