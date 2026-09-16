//! CLI entry point for the acceptance suite.
//!
//! ```sh
//! odradek-accept --server localhost:9092
//! ```
//!
//! Exit code 0 when the subject is conformant, 1 on any failed check, 2 on
//! usage errors.

use std::process::ExitCode;

use odradek_acceptance::checks;

fn usage() -> ExitCode {
    eprintln!("usage: odradek-accept --server <host:port>");
    ExitCode::from(2)
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let addr = match args.as_slice() {
        [flag, addr] if flag == "--server" => addr.clone(),
        _ => return usage(),
    };

    let report = checks::server::run(&addr).await;
    println!("{report}");
    if report.is_conformant() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
