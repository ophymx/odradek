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
//! future metadata-following check keeps working. Subjects share no
//! state, so they run concurrently — one thread each, output buffered and
//! printed per subject — and the wall clock is the slowest broker's
//! startup rather than the sum. Baselines live in
//! `conformance/<name>.json` as versioned envelopes (`format`/`suite`
//! plus the per-check statuses); a run that diverges from its baseline
//! fails, which is what makes real brokers ground truth for the suite
//! itself. Checks the suite could not run (infrastructure `error`
//! outcomes) never satisfy a baseline, but odradek-accept reports them
//! distinctly from protocol failures.

use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::workspace_root;

const READY_TIMEOUT: Duration = Duration::from_secs(90);

struct Subject {
    /// Baseline file stem, e.g. `apache-kafka-4.1.0`.
    name: &'static str,
    image: &'static str,
    /// Extra `docker run` arguments (env) and trailing command, both given
    /// the chosen host ports via `{port}` and `{sasl_port}` substitution.
    run_args: &'static [&'static str],
    /// Whether this subject serves a second, SASL-configured listener on
    /// 9094. Without one the `sasl/*` checks that need it skip.
    sasl_listener: bool,
}

/// The subject matrix. The container must expose its plaintext Kafka
/// listener on 9092 and a SASL one on 9094; `{port}` and `{sasl_port}`
/// in any argument are replaced with the ephemeral host ports each is
/// published on.
///
/// Two listeners because SASL cannot be asked about from a listener that
/// has none: such a listener answers ILLEGAL_SASL_STATE to every SASL
/// request, which is correct, so mechanism negotiation is unobservable
/// there. The plaintext listener keeps every other check credential-free.
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
            "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SASL://0.0.0.0:9094",
            "-e",
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port},SASL://127.0.0.1:{sasl_port}",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
            "-e",
            "KAFKA_SASL_ENABLED_MECHANISMS=PLAIN",
            // The credentials are never used: no check authenticates.
            // They exist so the listener has a mechanism to name when it
            // refuses one it does not have.
            "-e",
            "KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"conformance\" password=\"conformance\" user_conformance=\"conformance\";",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        ],
        sasl_listener: true,
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
            // No SASL listener here, deliberately. Redpanda's SASL is
            // switched on cluster-wide (`enable_sasl`), and once it is,
            // the listener configured `authentication_method: none`
            // starts refusing anonymous callers with
            // TOPIC_AUTHORIZATION_FAILED and GROUP_AUTHORIZATION_FAILED
            // — 13 of the other checks stop working. Kafka scopes SASL
            // to the listener and has no such coupling, which is why it
            // gets one. The `sasl/*` checks that need a SASL listener
            // skip here and say so, which is a truthful record of what
            // this configuration can be asked.
        ],
        sasl_listener: false,
    },
];

pub fn conformance(args: &[String]) -> Result<()> {
    let record = args.iter().any(|a| a == "--record");
    let filters: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    let selected: Vec<&'static Subject> = SUBJECTS
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

    // Subjects share nothing: ephemeral host ports, container names
    // stamped with the port, topic names stamped with pid and clock. So
    // run each on its own thread (everything below is blocking std) and
    // pay one startup instead of their sum. Each thread buffers its own
    // output; the reports are printed grouped, in matrix order, once its
    // subject is done.
    if selected.len() > 1 {
        eprintln!(
            "running {} subjects in parallel: {}",
            selected.len(),
            selected
                .iter()
                .map(|s| s.name)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let running: Vec<_> = selected
        .iter()
        .map(|&subject| {
            let accept = accept.clone();
            let conf_dir = conf_dir.clone();
            std::thread::spawn(move || {
                let mut log = String::new();
                let result = run_subject(subject, &accept, &conf_dir, record, &mut log);
                (log, result)
            })
        })
        .collect();

    let mut failures = Vec::new();
    for (subject, handle) in selected.iter().zip(running) {
        eprintln!("=== {} ({}) ===", subject.name, subject.image);
        match handle.join() {
            Ok((log, result)) => {
                eprint!("{log}");
                if let Err(e) = result {
                    eprintln!("{}: {e:#}", subject.name);
                    failures.push(subject.name);
                }
            }
            Err(_) => {
                eprintln!("{}: subject thread panicked", subject.name);
                failures.push(subject.name);
            }
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

/// Run one subject end to end, appending everything a human should see
/// to `log` rather than printing it — concurrent subjects would otherwise
/// interleave their reports line by line.
fn run_subject(
    subject: &Subject,
    accept: &Path,
    conf_dir: &Path,
    record: bool,
    log: &mut String,
) -> Result<()> {
    let port = ephemeral_port()?;
    let sasl_port = ephemeral_port()?;
    let container = Container::start(subject, port, sasl_port)?;
    let addr = format!("127.0.0.1:{port}");

    if let Err(e) = wait_ready(&addr) {
        container.dump_logs(log);
        return Err(e);
    }

    let baseline = conf_dir.join(format!("{}.json", subject.name));
    let mut cmd = Command::new(accept);
    cmd.args(["--server", &addr]);
    if subject.sasl_listener {
        cmd.args(["--sasl-server", &format!("127.0.0.1:{sasl_port}")]);
    }
    if record {
        cmd.args(["--write-baseline".as_ref(), baseline.as_os_str()]);
    } else {
        cmd.args(["--baseline".as_ref(), baseline.as_os_str()]);
    }
    let out = cmd.output().context("running odradek-accept")?;
    log.push_str(&String::from_utf8_lossy(&out.stdout));
    log.push_str(&String::from_utf8_lossy(&out.stderr));
    if !out.status.success() {
        // In record mode the accept run may exit non-zero because the
        // subject deviates; the point of recording is to capture exactly
        // that, so only enforcement failures are errors.
        if record {
            let _ = writeln!(
                log,
                "note: {} deviates from full conformance (recorded)",
                subject.name
            );
        } else {
            container.dump_logs(log);
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
    fn start(subject: &Subject, port: u16, sasl_port: u16) -> Result<Container> {
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
        if subject.sasl_listener {
            args.push("-p".into());
            args.push(format!("127.0.0.1:{sasl_port}:9094"));
        }
        let mut trailing = false;
        for a in subject.run_args {
            if *a == "--" {
                trailing = true;
                args.push(subject.image.into());
                continue;
            }
            args.push(
                a.replace("{port}", &port.to_string())
                    .replace("{sasl_port}", &sasl_port.to_string()),
            );
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

    fn dump_logs(&self, log: &mut String) {
        let _ = writeln!(log, "--- docker logs {} (tail) ---", self.name);
        match Command::new("docker")
            .args(["logs", "--tail", "40", &self.name])
            .output()
        {
            Ok(out) => {
                log.push_str(&String::from_utf8_lossy(&out.stdout));
                log.push_str(&String::from_utf8_lossy(&out.stderr));
            }
            Err(e) => {
                let _ = writeln!(log, "(docker logs failed: {e})");
            }
        }
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        // `docker rm -f` blocks for seconds on daemon-side teardown
        // (network namespace and published port release), and nothing
        // here needs to see the end of it: the container was started with
        // `--rm`, so the daemon reaps it either way, and the next run
        // picks a fresh ephemeral port rather than reusing this one. Fire
        // it and walk away — including on the error and panic paths this
        // guard exists for.
        let child = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        // Reap it off-thread so a long-lived xtask leaves no zombie; the
        // thread dies with the process if we exit first.
        if let Ok(mut child) = child {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
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
