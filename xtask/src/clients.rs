//! `cargo xtask client-matrix` — third-party clients through the
//! client-side harness.
//!
//! The mirror of `conformance.rs`, pointing the other way. There, real
//! brokers answer our checks; here, real clients are answered by our
//! harness and judged on what they say. The server matrix has six
//! subjects and sixty-two checks; this side had eleven checks and two
//! clients that had ever run against it, one of which we wrote and the
//! other of which we drove by hand, once.
//!
//! Each scenario is one harness configuration and one client
//! invocation, enforced against a committed baseline exactly as the
//! broker subjects are. The fault scenarios matter most: five of the
//! eleven checks only have anything to judge when a fault is armed, so
//! a matrix that ran the happy path alone would leave nearly half the
//! catalogue skipping and call it a pass.
//!
//! The client runs in Docker on the host network, because the harness
//! advertises itself as three brokers on three loopback ports and a
//! bridged container cannot reach them.

use std::io::{BufRead as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};

/// One client, and the scenarios it is put through.
struct ClientSubject {
    /// Baseline file stem, e.g. `kcat-1.7.1`.
    name: &'static str,
    image: &'static str,
    scenarios: &'static [Scenario],
}

/// One harness configuration and the client invocation that meets it.
struct Scenario {
    name: &'static str,
    /// `--fault` to arm, or `None` for the happy path.
    fault: Option<&'static str>,
    /// Client arguments; `{addr}` becomes the harness's address.
    args: &'static [&'static str],
    /// Fed to the client's stdin. A producer needs something to send.
    stdin: &'static str,
}

/// The lines a producer scenario sends. More than one, because a
/// throttle is only observable if there is a second request to hold
/// back, and a leader move is only observable if there is a retry.
const PRODUCE_LINES: &str = "one\ntwo\nthree\nfour\nfive\n";

const SUBJECTS: &[ClientSubject] = &[ClientSubject {
    name: "kcat-1.7.1",
    image: "edenhill/kcat:1.7.1",
    scenarios: &[
        Scenario {
            name: "produce",
            fault: None,
            args: &["-b", "{addr}", "-t", "odradek-routing", "-P"],
            stdin: PRODUCE_LINES,
        },
        // The other half of `routes-to-partition-leader`. A consumer
        // asks where the log starts before it fetches, so this scenario
        // is the reason the harness answers ListOffsets at all.
        Scenario {
            name: "consume",
            fault: None,
            args: &["-b", "{addr}", "-t", "odradek-routing", "-C", "-e", "-q"],
            stdin: "",
        },
        Scenario {
            name: "leader-move",
            fault: Some("leader-move"),
            args: &["-b", "{addr}", "-t", "odradek-routing", "-P"],
            stdin: PRODUCE_LINES,
        },
        // Pinned to one partition, one message per request. The check
        // can only judge a throttle the client had a chance to observe
        // — one followed by more traffic on the *same* connection —
        // and a producer that batches five messages into a single
        // request to a leader it then says goodbye to gives it nothing
        // to measure. That is not hypothetical: with default batching
        // this scenario skipped about one run in six, which is a flaky
        // baseline however green it looks the other five times.
        Scenario {
            name: "throttle",
            fault: Some("throttle"),
            args: &[
                "-b",
                "{addr}",
                "-t",
                "odradek-routing",
                "-P",
                "-p",
                "0",
                "-X",
                "batch.num.messages=1",
            ],
            stdin: PRODUCE_LINES,
        },
        Scenario {
            name: "unknown-tagged-field",
            fault: Some("unknown-tagged-field"),
            args: &["-b", "{addr}", "-t", "odradek-routing", "-P"],
            stdin: PRODUCE_LINES,
        },
        // OAUTHBEARER specifically: the refusal this fault injects is
        // RFC 7628's success-shaped failure challenge, which no other
        // mechanism has. Under PLAIN the harness refuses the handshake
        // and these two checks skip, which is correct and uninteresting
        // — so the matrix asks the question that has an answer.
        Scenario {
            name: "sasl-oauthbearer",
            fault: Some("reject-sasl-token"),
            args: &[
                "-b",
                "{addr}",
                "-t",
                "odradek-routing",
                "-P",
                "-X",
                "security.protocol=SASL_PLAINTEXT",
                "-X",
                "sasl.mechanism=OAUTHBEARER",
                "-X",
                "enable.sasl.oauthbearer.unsecure.jwt=true",
                "-X",
                "sasl.oauthbearer.config=principal=odradek",
            ],
            stdin: PRODUCE_LINES,
        },
    ],
}];

pub fn client_matrix(args: &[String]) -> Result<()> {
    let root = crate::workspace_root();
    let record = args.iter().any(|a| a == "--record");
    let filters: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();

    let accept = build_accept(&root)?;
    let mut log = std::io::stdout().lock();
    let mut failures = Vec::new();

    for subject in SUBJECTS {
        for scenario in subject.scenarios {
            let id = format!("{}-{}", subject.name, scenario.name);
            if !filters.is_empty() && !filters.iter().any(|f| id.contains(f.as_str())) {
                continue;
            }
            let _ = writeln!(log, "=== {id} ({}) ===", subject.image);
            let baseline = root.join(format!("conformance/client-{id}.json"));
            match run_scenario(&accept, subject, scenario, &baseline, record, &mut log) {
                Ok(true) => {}
                Ok(false) => failures.push(id),
                Err(e) => {
                    let _ = writeln!(log, "  infrastructure: {e:#}");
                    failures.push(id);
                }
            }
        }
    }

    if !failures.is_empty() {
        bail!(
            "{} scenario(s) diverged: {}",
            failures.len(),
            failures.join(", ")
        );
    }
    Ok(())
}

fn build_accept(root: &Path) -> Result<PathBuf> {
    let status = Command::new(env!("CARGO"))
        .current_dir(root)
        .args(["build", "-p", "odradek-acceptance", "--bins"])
        .status()
        .context("building odradek-accept")?;
    if !status.success() {
        bail!("building odradek-accept failed");
    }
    Ok(root.join("target/debug/odradek-accept"))
}

/// Run one scenario; `Ok(true)` when the report matched its baseline.
fn run_scenario(
    accept: &Path,
    subject: &ClientSubject,
    scenario: &Scenario,
    baseline: &Path,
    record: bool,
    log: &mut impl std::io::Write,
) -> Result<bool> {
    let port = ephemeral_port()?;
    let addr = format!("127.0.0.1:{port}");

    let mut cmd = Command::new(accept);
    cmd.args(["--client-listen", &addr]);
    if let Some(fault) = scenario.fault {
        cmd.args(["--fault", fault]);
    }
    if record {
        cmd.args(["--write-baseline".as_ref(), baseline.as_os_str()]);
    } else {
        cmd.args(["--baseline".as_ref(), baseline.as_os_str()]);
    }
    let mut harness = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning odradek-accept")?;

    // Wait for the harness to say it is listening, by reading it say
    // so. Probing the port with a connection would be simpler and
    // wrong: a session ends when its client hangs up, so the probe
    // would be the client, and it would leave immediately.
    let stderr = harness
        .stderr
        .take()
        .context("odradek-accept has no stderr")?;
    let mut stderr = std::io::BufReader::new(stderr);
    let mut line = String::new();
    loop {
        line.clear();
        if stderr
            .read_line(&mut line)
            .context("reading odradek-accept")?
            == 0
        {
            bail!("odradek-accept exited before it began listening");
        }
        if line.contains("waiting for a client connection") {
            break;
        }
    }

    let client_args: Vec<String> = scenario
        .args
        .iter()
        .map(|a| a.replace("{addr}", &addr))
        .collect();
    let mut docker = Command::new("docker");
    docker.args(["run", "--rm", "-i", "--network", "host", subject.image]);
    docker.args(&client_args);
    let mut child = docker
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning the client container")?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(scenario.stdin.as_bytes());
    }
    let client = child.wait_with_output().context("waiting for the client")?;

    let out = harness
        .wait_with_output()
        .context("waiting for odradek-accept")?;
    let report = String::from_utf8_lossy(&out.stdout);
    for line in report.lines() {
        let _ = writeln!(log, "{line}");
    }
    if !out.status.success() && !record {
        // The client's own output only matters when something went
        // wrong; printing it otherwise buries the report.
        let _ = writeln!(
            log,
            "--- {} stderr (tail) ---\n{}",
            subject.image,
            tail(&String::from_utf8_lossy(&client.stderr), 12)
        );
        return Ok(false);
    }
    Ok(true)
}

/// Keep the last `n` lines.
fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

fn ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}
