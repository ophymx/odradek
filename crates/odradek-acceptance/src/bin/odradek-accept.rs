//! CLI entry point for the acceptance suite.
//!
//! ```sh
//! # validate a server (broker or broker-compatible proxy)
//! odradek-accept --server localhost:9092
//!
//! # validate a client: listen, then point the client's bootstrap at us
//! odradek-accept --client-listen 127.0.0.1:19092
//!
//! # machine-readable output / baseline workflows
//! odradek-accept --server localhost:9092 --json
//! odradek-accept --server localhost:9092 --write-baseline conformance/kafka.json
//! odradek-accept --server localhost:9092 --baseline conformance/kafka.json
//! ```
//!
//! Exit code 0 on success (conformant, or matching the baseline when one is
//! given), 1 on failures/diffs, 2 on usage errors.

use std::process::ExitCode;

use odradek_acceptance::checks;
use odradek_acceptance::report::{Baseline, Report};
use tokio::net::TcpListener;

fn usage() -> ExitCode {
    eprintln!(
        "usage: odradek-accept (--server <host:port> | --client-listen <host:port>)\n\
         \x20                    [--json] [--baseline <file>] [--write-baseline <file>]"
    );
    ExitCode::from(2)
}

struct Args {
    server: Option<String>,
    client_listen: Option<String>,
    json: bool,
    baseline: Option<String>,
    write_baseline: Option<String>,
}

fn parse_args(args: &[String]) -> Option<Args> {
    let mut parsed = Args {
        server: None,
        client_listen: None,
        json: false,
        baseline: None,
        write_baseline: None,
    };
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--server" => parsed.server = Some(it.next()?.clone()),
            "--client-listen" => parsed.client_listen = Some(it.next()?.clone()),
            "--json" => parsed.json = true,
            "--baseline" => parsed.baseline = Some(it.next()?.clone()),
            "--write-baseline" => parsed.write_baseline = Some(it.next()?.clone()),
            _ => return None,
        }
    }
    // Exactly one subject.
    if parsed.server.is_some() == parsed.client_listen.is_some() {
        return None;
    }
    Some(parsed)
}

#[tokio::main]
async fn main() -> ExitCode {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let Some(args) = parse_args(&raw) else {
        return usage();
    };

    let report = if let Some(addr) = &args.server {
        checks::server::run(addr).await
    } else {
        let addr = args.client_listen.as_deref().unwrap();
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("cannot listen on {addr}: {e}");
                return ExitCode::FAILURE;
            }
        };
        eprintln!("waiting for a client connection on {addr} ...");
        checks::client::run(&listener, &checks::client::ObserveConfig::default()).await
    };

    if args.json {
        println!("{}", report.to_json());
    } else {
        println!("{report}");
    }

    if let Some(path) = &args.write_baseline {
        let json = serde_json::to_string_pretty(&report.to_baseline()).unwrap();
        if let Err(e) = std::fs::write(path, json + "\n") {
            eprintln!("cannot write baseline {path}: {e}");
            return ExitCode::FAILURE;
        }
        eprintln!("baseline written to {path}");
    }

    exit_status(&report, args.baseline.as_deref())
}

fn exit_status(report: &Report, baseline_path: Option<&str>) -> ExitCode {
    let Some(path) = baseline_path else {
        return if report.is_conformant() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    };
    let baseline: Baseline = match std::fs::read_to_string(path)
        .map_err(|e| e.to_string())
        .and_then(|s| serde_json::from_str(&s).map_err(|e| e.to_string()))
    {
        Ok(b) => b,
        Err(e) => {
            eprintln!("cannot read baseline {path}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let diffs = report.diff_against(&baseline);
    if diffs.is_empty() {
        eprintln!("run matches baseline {path}");
        ExitCode::SUCCESS
    } else {
        eprintln!("run diverges from baseline {path}:");
        for d in &diffs {
            eprintln!("  {d}");
        }
        ExitCode::FAILURE
    }
}
