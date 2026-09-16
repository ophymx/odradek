//! `cargo xtask conformance` — run the acceptance suite against real
//! broker implementations in Docker and enforce the committed baselines.
//!
//! ```sh
//! cargo xtask conformance             # all subjects, enforce baselines
//! cargo xtask conformance --record    # (re)write baselines from this run
//! cargo xtask conformance redpanda    # only subjects whose name contains
//! ```
//!
//! Each subject runs in a throwaway container on an ephemeral host port,
//! with the broker's advertised listener pointed back at that port so a
//! future metadata-following check keeps working. Baselines live in
//! `conformance/<name>.json`; a run that diverges from its baseline fails,
//! which is what makes real brokers ground truth for the suite itself.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::workspace_root;

const READY_TIMEOUT: Duration = Duration::from_secs(90);

struct Subject {
    /// Baseline file stem, e.g. `apache-kafka-4.1.0`.
    name: &'static str,
    image: &'static str,
    /// Extra `docker run` arguments (env) and trailing command, both given
    /// the chosen host port via `{port}` substitution.
    run_args: &'static [&'static str],
}

/// The subject matrix. The container must expose its Kafka listener on
/// 9092; `{port}` in any argument is replaced with the ephemeral host port
/// the listener is published on.
const SUBJECTS: &[Subject] = &[
    Subject {
        name: "apache-kafka-4.1.0",
        image: "apache/kafka:4.1.0",
        run_args: &[
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
            "-e",
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port}",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        ],
    },
    Subject {
        name: "redpanda-25.2.1",
        image: "redpandadata/redpanda:v25.2.1",
        run_args: &[
            "--",
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--kafka-addr",
            "PLAINTEXT://0.0.0.0:9092",
            "--advertise-kafka-addr",
            "PLAINTEXT://127.0.0.1:{port}",
        ],
    },
];

pub fn conformance(args: &[String]) -> Result<()> {
    let record = args.iter().any(|a| a == "--record");
    let filters: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let selected: Vec<&Subject> = SUBJECTS
        .iter()
        .filter(|s| filters.is_empty() || filters.iter().any(|f| s.name.contains(f.as_str())))
        .collect();
    if selected.is_empty() {
        bail!(
            "no subject matches {filters:?}; available: {}",
            SUBJECTS
                .iter()
                .map(|s| s.name)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let root = workspace_root();
    build_accept(&root)?;
    let accept = root.join("target/debug/odradek-accept");
    let conf_dir = root.join("conformance");
    std::fs::create_dir_all(&conf_dir)?;

    let mut failures = Vec::new();
    for subject in selected {
        eprintln!("=== {} ({}) ===", subject.name, subject.image);
        if let Err(e) = run_subject(subject, &accept, &conf_dir, record) {
            eprintln!("{}: {e:#}", subject.name);
            failures.push(subject.name);
        }
    }
    if !failures.is_empty() {
        bail!("subjects failed: {}", failures.join(", "));
    }
    Ok(())
}

fn build_accept(root: &Path) -> Result<()> {
    let status = Command::new("cargo")
        .args(["build", "-p", "odradek-acceptance", "--bins"])
        .current_dir(root)
        .status()
        .context("running cargo build")?;
    if !status.success() {
        bail!("cargo build -p odradek-acceptance failed");
    }
    Ok(())
}

fn run_subject(subject: &Subject, accept: &Path, conf_dir: &Path, record: bool) -> Result<()> {
    let port = ephemeral_port()?;
    let container = Container::start(subject, port)?;
    let addr = format!("127.0.0.1:{port}");

    if let Err(e) = wait_ready(&addr) {
        container.dump_logs();
        return Err(e);
    }

    let baseline = conf_dir.join(format!("{}.json", subject.name));
    let mut cmd = Command::new(accept);
    cmd.args(["--server", &addr]);
    if record {
        cmd.args(["--write-baseline".as_ref(), baseline.as_os_str()]);
    } else {
        cmd.args(["--baseline".as_ref(), baseline.as_os_str()]);
    }
    let status = cmd.status().context("running odradek-accept")?;
    if !status.success() {
        // In record mode the accept run may exit non-zero because the
        // subject deviates; the point of recording is to capture exactly
        // that, so only enforcement failures are errors.
        if record {
            eprintln!(
                "note: {} deviates from full conformance (recorded)",
                subject.name
            );
        } else {
            container.dump_logs();
            bail!("acceptance run failed against {}", subject.name);
        }
    }
    Ok(())
}

/// Pick a free TCP port. Racy in principle; in practice docker publishes
/// the port fast enough that collisions with other suites are negligible.
fn ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

struct Container {
    name: String,
}

impl Container {
    fn start(subject: &Subject, port: u16) -> Result<Container> {
        let name = format!("odradek-accept-{}-{port}", subject.name);
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--rm".into(),
            "--name".into(),
            name.clone(),
            "-p".into(),
            format!("127.0.0.1:{port}:9092"),
        ];
        let mut trailing = false;
        for a in subject.run_args {
            if *a == "--" {
                trailing = true;
                args.push(subject.image.into());
                continue;
            }
            args.push(a.replace("{port}", &port.to_string()));
        }
        if !trailing {
            args.push(subject.image.into());
        }
        let out = Command::new("docker")
            .args(&args)
            .output()
            .context("running docker")?;
        if !out.status.success() {
            bail!(
                "docker run {} failed: {}",
                subject.image,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(Container { name })
    }

    fn dump_logs(&self) {
        eprintln!("--- docker logs {} (tail) ---", self.name);
        let _ = Command::new("docker")
            .args(["logs", "--tail", "40", &self.name])
            .status();
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// Block until the broker answers an ApiVersions v0 request on `addr`.
///
/// A TCP accept is not readiness — brokers open their listener well before
/// the request path works — so probe with a real (hand-rolled) exchange:
/// header v1 for api key 18 version 0 with a null client id and an empty
/// body, answered by anything frame-shaped.
fn wait_ready(addr: &str) -> Result<()> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last_err = String::new();
    while Instant::now() < deadline {
        match probe(addr) {
            Ok(()) => return Ok(()),
            Err(e) => last_err = e.to_string(),
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    bail!("{addr} not ready after {READY_TIMEOUT:?}: {last_err}");
}

fn probe(addr: &str) -> Result<()> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    #[rustfmt::skip]
    let request: [u8; 14] = [
        0, 0, 0, 10,      // frame length
        0, 18,            // api key: ApiVersions
        0, 0,             // api version: 0
        0, 0, 0, 7,       // correlation id
        0xff, 0xff,       // client id: null
    ];
    stream.write_all(&request)?;
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let len = i32::from_be_bytes(len);
    if !(4..=1 << 20).contains(&len) {
        bail!("implausible response frame length {len}");
    }
    let mut frame = vec![0u8; len as usize];
    stream.read_exact(&mut frame)?;
    if frame[..4] != [0, 0, 0, 7] {
        bail!("response does not echo the probe correlation id");
    }
    Ok(())
}
