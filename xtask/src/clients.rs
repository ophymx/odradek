//! `cargo xtask client-matrix` — clients through the client-side
//! harness.
//!
//! The mirror of `conformance.rs`, pointing the other way. There, real
//! brokers answer our checks; here, real clients are answered by our
//! harness and judged on what they say.
//!
//! Each scenario is one harness configuration and one client
//! invocation, enforced against a committed baseline exactly as the
//! broker subjects are. The fault scenarios matter most: five of the
//! eleven checks only have anything to judge when a fault is armed, so
//! a matrix that ran the happy path alone would leave nearly half the
//! catalogue skipping and call it a pass.
//!
//! A containerised subject runs on the host network, because the
//! harness advertises itself as three brokers on three loopback ports
//! and a bridged container cannot reach them.
//!
//! `odradek-client` is a subject here too, and adding it was worth more
//! than the row it occupies. It is the one party in this workspace that
//! nothing had ever graded — the broker matrix runs the other direction
//! and its own tests answer it with fixtures it agrees with by
//! construction — and the first thing it did on arrival was reveal that
//! `client/honours-throttle-time` passed a client with the pause taken
//! out. See the check's own docs; the short version is that it treated
//! an absence of evidence as evidence, and so had been green for both
//! subjects without ever having been answered by either.

use std::io::{BufRead as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};

/// One client, and the scenarios it is put through.
struct ClientSubject {
    /// Baseline file stem, e.g. `kcat-1.7.1`.
    name: &'static str,
    runner: Runner,
    scenarios: &'static [Scenario],
}

/// How a subject is started.
enum Runner {
    /// A published container image, on the host network.
    Docker(&'static str),
    /// A cargo example from this workspace, built and run directly.
    ///
    /// This is how our own client gets in here, and it is worth being
    /// precise about why that is not the suite marking its own homework.
    /// The rule that `odradek-acceptance` never depends on
    /// `odradek-client` is about the *harness*: a judge that imported
    /// the thing it judges would agree with it by construction. A
    /// subject is the opposite arrangement — it arrives over a socket
    /// and is read exactly as kcat is, by a harness that does not know
    /// which one it is talking to.
    Example(&'static str),
}

impl Runner {
    /// What to call this in the log.
    fn describe(&self) -> String {
        match self {
            Runner::Docker(image) => (*image).to_owned(),
            Runner::Example(name) => format!("cargo example {name}"),
        }
    }
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

const SUBJECTS: &[ClientSubject] = &[
    ClientSubject {
        name: "kcat-1.7.1",
        runner: Runner::Docker("edenhill/kcat:1.7.1"),
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
    },
    // Our own client, as a subject rather than as the thing doing the
    // asking. Until this row existed it was the only participant in the
    // constellation that nothing ever graded: the broker matrix runs the
    // other direction entirely, and its own tests answer it with fixtures
    // it agrees with by construction.
    ClientSubject {
        name: "odradek-client",
        runner: Runner::Example("matrix_subject"),
        scenarios: &[
            Scenario {
                name: "produce",
                fault: None,
                args: &["{addr}", "produce"],
                stdin: "",
            },
            Scenario {
                name: "consume",
                fault: None,
                args: &["{addr}", "consume"],
                stdin: "",
            },
            Scenario {
                name: "leader-move",
                fault: Some("leader-move"),
                args: &["{addr}", "produce"],
                stdin: "",
            },
            // One record per request, pinned, for the same reason kcat is —
            // a throttle needs a later request on the same connection to
            // hold back. This client is sequential where librdkafka
            // pipelines, which is why the check can settle it and cannot
            // settle kcat: a producer that has already written its whole
            // backlog to the socket has nothing left to delay.
            Scenario {
                name: "throttle",
                fault: Some("throttle"),
                args: &[
                    "{addr}",
                    "produce",
                    "--partition",
                    "0",
                    "--record-per-request",
                ],
                stdin: "",
            },
            Scenario {
                name: "unknown-tagged-field",
                fault: Some("unknown-tagged-field"),
                args: &["{addr}", "produce"],
                stdin: "",
            },
            // Expected to exit non-zero: the token is refused and the
            // client says so. What is being graded is how it takes the
            // refusal, not whether it got in.
            Scenario {
                name: "sasl-oauthbearer",
                fault: Some("reject-sasl-token"),
                args: &["{addr}", "produce", "--oauthbearer"],
                stdin: "",
            },
        ],
    },
];

pub fn client_matrix(args: &[String]) -> Result<()> {
    let root = crate::workspace_root();
    let record = args.iter().any(|a| a == "--record");
    let filters: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();

    let accept = build_accept(&root)?;
    for subject in SUBJECTS {
        if let Runner::Example(name) = subject.runner {
            build_example(&root, name)?;
        }
    }
    let mut log = std::io::stdout().lock();
    let mut failures = Vec::new();

    for subject in SUBJECTS {
        for scenario in subject.scenarios {
            let id = format!("{}-{}", subject.name, scenario.name);
            if !filters.is_empty() && !filters.iter().any(|f| id.contains(f.as_str())) {
                continue;
            }
            let _ = writeln!(log, "=== {id} ({}) ===", subject.runner.describe());
            let baseline = root.join(format!("conformance/client-{id}.json"));
            match run_scenario(
                &root, &accept, subject, scenario, &baseline, record, &mut log,
            ) {
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

fn build_example(root: &Path, name: &str) -> Result<PathBuf> {
    let status = Command::new(env!("CARGO"))
        .current_dir(root)
        .args(["build", "-p", "odradek-client", "--example", name])
        .status()
        .with_context(|| format!("building the {name} example"))?;
    if !status.success() {
        bail!("building the {name} example failed");
    }
    Ok(root.join(format!("target/debug/examples/{name}")))
}

/// Run one scenario; `Ok(true)` when the report matched its baseline.
fn run_scenario(
    root: &Path,
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
    let mut client = match subject.runner {
        Runner::Docker(image) => {
            let mut docker = Command::new("docker");
            docker.args(["run", "--rm", "-i", "--network", "host", image]);
            docker
        }
        Runner::Example(name) => Command::new(root.join(format!("target/debug/examples/{name}"))),
    };
    client.args(&client_args);
    let mut child = client
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
            subject.runner.describe(),
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
