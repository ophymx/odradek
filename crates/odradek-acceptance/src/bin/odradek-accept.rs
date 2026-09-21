//! CLI entry point for the acceptance suite.
//!
//! ```sh
//! # what would run: every catalogued check with id, role, requirement
//! odradek-accept --list
//!
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
//! given), 1 on failures/diffs, 2 on usage errors. Checks that could not
//! run (`ERROR`, an infrastructure finding — connection refused, timeout,
//! harness setup failure) never satisfy a baseline and never pass a run,
//! but are reported distinctly from protocol violations.

use std::process::ExitCode;

use odradek_acceptance::checks;
use odradek_acceptance::report::{Baseline, Report};
use tokio::net::TcpListener;

fn usage() -> ExitCode {
    eprintln!(
        "usage: odradek-accept (--server <host:port> | --client-listen <host:port> | --list)\n\
         \x20 [--sasl-server <host:port>]  a second listener with SASL configured\n\
         \x20 [--authenticate <user>:<password>]  authenticate every\n\
         \x20                    connection with SCRAM-SHA-256, for a subject\n\
         \x20                    that will not answer otherwise\n\
         \x20 [--tls-ca <file>]  speak TLS to the subject, trusting exactly\n\
         \x20                    the certificates in this PEM file\n\
         \x20 [--tls-name <host>]  the name to verify the broker's\n\
         \x20                    certificate against (default: localhost)\n\
         \x20 [--cluster-control <cmd>]    how to stop and start a broker: run\n\
         \x20                    as `<cmd> <stop|start> <node-id> <host:port>`.\n\
         \x20                    Both names are given because not every\n\
         \x20                    implementation lets you choose node ids.\n\
         \x20                    Without it, the checks that need a broker to\n\
         \x20                    fail skip.\n\
         \x20                    [--json]\n\
         \x20 [--fault <name>]   stage one misbehavior for the client to cope\n\
         \x20                    with: leader-move, throttle,\n\
         \x20                    unknown-tagged-field, reject-sasl-token.\n\
         \x20                    Each arms the checks that need it; without\n\
         \x20                    one those checks skip.\n\
         \x20                    [--baseline <file>] [--write-baseline <file>]"
    );
    ExitCode::from(2)
}

struct Args {
    server: Option<String>,
    sasl_server: Option<String>,
    cluster_control: Option<String>,
    authenticate: Option<String>,
    tls_ca: Option<String>,
    tls_name: Option<String>,
    client_listen: Option<String>,
    list: bool,
    fault: Option<checks::client::HarnessFault>,
    json: bool,
    baseline: Option<String>,
    write_baseline: Option<String>,
}

fn parse_args(args: &[String]) -> Option<Args> {
    let mut parsed = Args {
        server: None,
        sasl_server: None,
        cluster_control: None,
        authenticate: None,
        tls_ca: None,
        tls_name: None,
        client_listen: None,
        list: false,
        fault: None,
        json: false,
        baseline: None,
        write_baseline: None,
    };
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--server" => parsed.server = Some(it.next()?.clone()),
            "--sasl-server" => parsed.sasl_server = Some(it.next()?.clone()),
            "--cluster-control" => parsed.cluster_control = Some(it.next()?.clone()),
            "--authenticate" => parsed.authenticate = Some(it.next()?.clone()),
            "--tls-ca" => parsed.tls_ca = Some(it.next()?.clone()),
            "--tls-name" => parsed.tls_name = Some(it.next()?.clone()),
            "--client-listen" => parsed.client_listen = Some(it.next()?.clone()),
            "--list" => parsed.list = true,
            "--fault" => {
                parsed.fault = match it.next()?.as_str() {
                    "leader-move" => Some(checks::client::HarnessFault::LeaderMove),
                    "throttle" => Some(checks::client::HarnessFault::Throttle),
                    "unknown-tagged-field" => {
                        Some(checks::client::HarnessFault::UnknownTaggedField)
                    }
                    "reject-sasl-token" => Some(checks::client::HarnessFault::RejectSaslToken),
                    _ => return None,
                }
            }
            "--json" => parsed.json = true,
            "--baseline" => parsed.baseline = Some(it.next()?.clone()),
            "--write-baseline" => parsed.write_baseline = Some(it.next()?.clone()),
            _ => return None,
        }
    }
    // --list stands alone; otherwise exactly one subject, and faults only
    // apply to the client harness.
    if parsed.list {
        let alone = parsed.server.is_none()
            && parsed.client_listen.is_none()
            && parsed.fault.is_none()
            && parsed.cluster_control.is_none()
            && parsed.authenticate.is_none()
            && parsed.tls_ca.is_none()
            && parsed.tls_name.is_none()
            && !parsed.json
            && parsed.baseline.is_none()
            && parsed.write_baseline.is_none();
        return alone.then_some(parsed);
    }
    if parsed.server.is_some() == parsed.client_listen.is_some() {
        return None;
    }
    if parsed.fault.is_some() && parsed.client_listen.is_none() {
        return None;
    }
    Some(parsed)
}

/// Print every catalogued check: id, subject role, requirement.
fn list_checks() {
    for check in checks::catalog() {
        println!("[{}] {}", check.role(), check.id);
        println!("    {}", check.requirement);
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let Some(args) = parse_args(&raw) else {
        return usage();
    };

    if args.list {
        list_checks();
        return ExitCode::SUCCESS;
    }

    let report = if let Some(addr) = &args.server {
        let mut config = checks::server::ProbeConfig::default();
        config.control = args.cluster_control.as_deref().map(
            |command| -> std::sync::Arc<dyn checks::server::ClusterControl> {
                std::sync::Arc::new(CommandControl {
                    command: command.to_owned(),
                })
            },
        );
        #[cfg(feature = "tls")]
        if let Some(path) = &args.tls_ca {
            let pem = match std::fs::read(path) {
                Ok(pem) => pem,
                Err(e) => {
                    eprintln!("--tls-ca {path}: {e}");
                    return ExitCode::from(2);
                }
            };
            // `localhost` by default: a broker's certificate names the
            // host an operator configured it for, and the suite reaches
            // it at whatever ephemeral port a container was published
            // on. Verifying against the dialled address would need a
            // certificate per run.
            let name = args.tls_name.as_deref().unwrap_or("localhost");
            match odradek_acceptance::raw::TlsTrust::from_ca_pem(&pem, name) {
                Ok(trust) => config.tls = Some(trust),
                Err(e) => {
                    eprintln!("--tls-ca {path}: {e}");
                    return ExitCode::from(2);
                }
            }
        }
        #[cfg(not(feature = "tls"))]
        if args.tls_ca.is_some() {
            eprintln!("--tls-ca needs this build's `tls` feature, which is off");
            return ExitCode::from(2);
        }
        if let Some(login) = &args.authenticate {
            // Split on the first colon: a password may contain one, a
            // username may not.
            let Some((user, password)) = login.split_once(':') else {
                eprintln!("--authenticate wants <user>:<password>");
                return ExitCode::from(2);
            };
            config.credentials = Some(checks::server::ScramLogin::scram_sha_256(user, password));
        }
        checks::server::run_with_sasl(addr, args.sasl_server.as_deref(), &config).await
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
        let mut config = checks::client::ObserveConfig::default();
        config.fault = args.fault;
        match checks::client::run(&listener, &config).await {
            Ok(report) => report,
            Err(e) => {
                eprintln!(
                    "client harness could not run: {e} (infrastructure, \
                     not evidence of nonconformance)"
                );
                return ExitCode::FAILURE;
            }
        }
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

/// Stops and starts brokers by running a command the caller supplied.
///
/// The suite knows nothing about how the subject is deployed and has no
/// business guessing: `docker stop`, `kubectl delete pod`, `systemctl
/// stop`, an ssh hop, a cloud API. So it is handed one command and
/// invokes it as `<command> stop <node-id>` / `<command> start
/// <node-id>`, and whoever wrote the command decides what those mean.
///
/// Split on whitespace so a command can carry fixed arguments
/// (`"./control.sh --cluster ci"`). Nothing here goes near a shell, so
/// quoting and redirection are not available and not needed; a command
/// that wants them can be a script, which is the expected shape anyway.
#[derive(Debug)]
struct CommandControl {
    command: String,
}

impl CommandControl {
    async fn run(&self, verb: &'static str, node_id: i32, addr: String) -> Result<(), String> {
        // `std::process` on a blocking thread rather than tokio's, which
        // would mean a `process` feature — and with it a signal-handling
        // dependency — on a published crate, for something that happens
        // a handful of times per run and is slow anyway.
        let command = self.command.clone();
        tokio::task::spawn_blocking(move || {
            let mut parts = command.split_whitespace();
            let program = parts.next().ok_or("--cluster-control is empty")?;
            let out = std::process::Command::new(program)
                .args(parts)
                .arg(verb)
                .arg(node_id.to_string())
                .arg(&addr)
                .output()
                .map_err(|e| format!("running {command:?}: {e}"))?;
            if out.status.success() {
                return Ok(());
            }
            Err(format!(
                "{command:?} {verb} {node_id} exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ))
        })
        .await
        .map_err(|e| format!("control task for {verb} {node_id} failed: {e}"))?
    }
}

impl checks::server::ClusterControl for CommandControl {
    fn stop(
        &self,
        broker: checks::server::Broker<'_>,
    ) -> checks::BoxFuture<'_, Result<(), String>> {
        Box::pin(self.run("stop", broker.node_id, broker.addr.to_owned()))
    }

    fn start(
        &self,
        broker: checks::server::Broker<'_>,
    ) -> checks::BoxFuture<'_, Result<(), String>> {
        Box::pin(self.run("start", broker.node_id, broker.addr.to_owned()))
    }
}

fn exit_status(report: &Report, baseline_path: Option<&str>) -> ExitCode {
    let Some(path) = baseline_path else {
        if report.errored() > 0 {
            eprintln!(
                "{} check(s) could not run (infrastructure) — the run proves \
                 neither conformance nor nonconformance",
                report.errored()
            );
            return ExitCode::FAILURE;
        }
        return if report.is_conformant() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    };
    let baseline = match std::fs::read_to_string(path)
        .map_err(|e| e.to_string())
        .and_then(|s| Baseline::from_json(&s))
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
