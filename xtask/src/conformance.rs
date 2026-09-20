//! `cargo xtask conformance` — run the acceptance suite against real
//! broker implementations in Docker and enforce the committed baselines.
//!
//! ```sh
//! cargo xtask conformance             # all subjects, enforce baselines
//! cargo xtask conformance --record    # (re)write baselines from this run
//! cargo xtask conformance redpanda    # only subjects whose name contains
//! cargo xtask conformance --no-proxy  # skip the second, proxied pass
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
//!
//! Every subject is then run a second time through
//! [the proxy example][proxy], enforced against the *same* baseline. See
//! [`proxy_pass`] for why that comparison is the interesting one.
//!
//! [proxy]: ../../../crates/odradek-protocol/examples/proxy.rs

use std::fmt::Write as _;
use std::io::{BufRead as _, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
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
    /// Commands to run inside the container once it is ready, before the
    /// suite starts — for state that cannot be configured at boot.
    provision: &'static [&'static [&'static str]],
    /// How many broker containers this subject runs. More than one gets
    /// a docker network, a shared cluster id, and `{id}`/`{quorum}`
    /// substitution; the suite is pointed at the first node and finds
    /// the rest through Metadata, as a client would.
    nodes: u8,
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
            "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,SASL://0.0.0.0:9094,INTERNAL://0.0.0.0:9099",
            "-e",
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port},SASL://127.0.0.1:{sasl_port},INTERNAL://localhost:9099",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT,INTERNAL:PLAINTEXT",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
            "-e",
            "KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-256",
            // SCRAM keeps its credentials in the metadata log rather
            // than in this config, but the broker still refuses to start
            // without a login module named for the mechanism.
            "-e",
            "KAFKA_LISTENER_NAME_SASL_SCRAM-SHA-256_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required;",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            // Same reason as the offsets topic above, for the log the
            // transaction coordinator keeps its state in: the default
            // asks for three replicas, a one-node cluster cannot give
            // them, and the coordinator then answers NOT_COORDINATOR
            // forever for a partition that was never created.
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        ],
        sasl_listener: true,
        nodes: 1,
        // SCRAM credentials live in the metadata log, so they are added
        // after the broker is up rather than configured into it.
        provision: &[&[
            "/opt/kafka/bin/kafka-configs.sh",
            // The INTERNAL listener, not the published one: the
            // published listener advertises a host address that is not
            // reachable from inside the container, so a tool run here
            // would be told to connect somewhere it cannot.
            "--bootstrap-server",
            "localhost:9099",
            "--alter",
            "--add-config",
            "SCRAM-SHA-256=[password=conformance]",
            "--entity-type",
            "users",
            "--entity-name",
            "conformance",
        ]],
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
        nodes: 1,
        provision: &[],
    },
    Subject {
        // Three brokers, because a cluster of one cannot be asked the
        // questions that matter most about routing. On a single node
        // every partition's leader and every group's and transaction's
        // coordinator is the broker you are already talking to, so a
        // suite that never consults Metadata passes anyway. Pointed at
        // this subject, the suite failed 31 of 51 checks and failed
        // different ones each run until it learned to route.
        //
        // Deliberately no SASL listener: what this subject is here to
        // exercise is routing, and the `sasl/*` checks are covered by
        // the single-node Kafka above.
        name: "apache-kafka-4.1.0-cluster",
        image: "apache/kafka:4.1.0",
        run_args: &[
            "-e",
            // Every node must format its storage with the same cluster
            // id or they will refuse to form a quorum together.
            "CLUSTER_ID=5L6g3nShT-eMCtK--X86sw",
            "-e",
            "KAFKA_NODE_ID={id}",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9099",
            // Two addresses for two audiences. Clients are on the host
            // and reach this node through its published port; the other
            // brokers are on the docker network and reach it by
            // container name. A node that advertised only the published
            // address would tell its peers to connect somewhere that,
            // from inside the network, is themselves.
            "-e",
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:{port},INTERNAL://{node}:9099",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS={quorum}",
            // Replicated for real, which is the point: with RF=3 the
            // leader of a partition is one of three brokers rather than
            // the only one there is.
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2",
            "-e",
            "KAFKA_DEFAULT_REPLICATION_FACTOR=3",
            // Three JVMs on a two-core CI runner: the default heap is
            // more than any of them needs for a suite's worth of data.
            "-e",
            "KAFKA_HEAP_OPTS=-Xmx512m -Xms256m",
        ],
        sasl_listener: false,
        nodes: 3,
        provision: &[],
    },
];

pub fn conformance(args: &[String]) -> Result<()> {
    let record = args.iter().any(|a| a == "--record");
    let proxied = !args.iter().any(|a| a == "--no-proxy");
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
    let proxy = proxied.then(|| root.join("target/debug/examples/proxy"));
    if proxied {
        build_proxy(&root)?;
    }
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
            let proxy = proxy.clone();
            let conf_dir = conf_dir.clone();
            std::thread::spawn(move || {
                let mut log = String::new();
                let result = run_subject(
                    subject,
                    &accept,
                    proxy.as_deref(),
                    &conf_dir,
                    record,
                    &mut log,
                );
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

fn build_proxy(root: &Path) -> Result<()> {
    let status = Command::new("cargo")
        .args(["build", "-p", "odradek-protocol", "--example", "proxy"])
        .current_dir(root)
        .status()
        .context("running cargo build")?;
    if !status.success() {
        bail!("cargo build --example proxy failed");
    }
    Ok(())
}

/// Run one subject end to end, appending everything a human should see
/// to `log` rather than printing it — concurrent subjects would otherwise
/// interleave their reports line by line.
fn run_subject(
    subject: &Subject,
    accept: &Path,
    proxy: Option<&Path>,
    conf_dir: &Path,
    record: bool,
    log: &mut String,
) -> Result<()> {
    let sasl_port = ephemeral_port()?;
    let cluster = Cluster::start(subject, sasl_port)?;
    // The suite is pointed at one node and finds the rest through
    // Metadata, which is the only way a client could find them either.
    let addr = cluster.bootstrap().to_owned();
    let sasl_addr = subject
        .sasl_listener
        .then(|| format!("127.0.0.1:{sasl_port}"));

    // Every node, not just the bootstrap: a partition led by a broker
    // that is not up yet is a check failing on the harness's impatience.
    for node in cluster.addrs() {
        if let Err(e) = wait_ready(node) {
            cluster.dump_logs(log);
            return Err(e);
        }
    }

    for command in subject.provision {
        cluster
            .exec(command)
            .with_context(|| format!("provisioning {} with {:?}", subject.name, command))?;
    }

    let baseline = conf_dir.join(format!("{}.json", subject.name));
    let direct = run_accept(accept, &addr, sasl_addr.as_deref(), &baseline, record, log)?;
    if !direct {
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
            cluster.dump_logs(log);
            bail!("acceptance run failed against {}", subject.name);
        }
    }

    // The proxy has one upstream and rewrites every broker in Metadata
    // to its own address, so in front of a cluster it would route all
    // three brokers' traffic to whichever one it was pointed at and
    // misdeliver most of it. That is not a flaw in the proxy — it is the
    // one-upstream design saying what it is for — but it does mean the
    // proxied pass belongs to the single-node subjects. Proxying a
    // cluster properly needs a listener per broker, which is a different
    // example than the one this is a witness for.
    match (proxy, subject.nodes) {
        (Some(proxy), 1) => proxy_pass(
            subject,
            accept,
            proxy,
            &addr,
            sasl_addr.as_deref(),
            &baseline,
            log,
        )?,
        (Some(_), _) => {
            let _ = writeln!(
                log,
                "note: no proxied pass for {} — the proxy example fronts a single broker",
                subject.name
            );
        }
        (None, _) => {}
    }
    Ok(())
}

/// The same suite again, with the proxy example between it and the
/// broker, enforced against the *same* baseline.
///
/// The protocol crate justifies a good deal of its design — unknown
/// tagged fields round-tripping raw, record batches re-encoding
/// byte-identically — with "a proxy needs this". `examples/proxy.rs` is
/// the witness for that claim, and this is what keeps the witness
/// honest: not a baseline of its own, but the broker's. A proxy that
/// dropped a tagged field, re-encoded a batch differently, or mislaid a
/// correlation id would make some check answer differently than the
/// broker it is standing in front of, and the diff names which one.
///
/// Recording is deliberately not offered here. A proxied baseline could
/// absorb exactly the regressions this is meant to catch; the assertion
/// is equality with the direct run, so there is nothing else it could
/// legitimately be.
fn proxy_pass(
    subject: &Subject,
    accept: &Path,
    proxy: &Path,
    addr: &str,
    sasl_addr: Option<&str>,
    baseline: &Path,
    log: &mut String,
) -> Result<()> {
    let front = Proxy::start(proxy, addr)?;
    // A second instance for the SASL listener: the proxy forwards SASL
    // frames without parsing them, but it only has one upstream, and
    // the two listeners are different upstreams.
    let sasl_front = sasl_addr.map(|a| Proxy::start(proxy, a)).transpose()?;

    if let Err(e) = wait_ready(front.addr()) {
        front.dump_logs(log);
        return Err(e).context("proxy never became reachable");
    }

    let _ = writeln!(
        log,
        "--- {} through the proxy ({} -> {addr}) ---",
        subject.name,
        front.addr()
    );
    let matched = run_accept(
        accept,
        front.addr(),
        sasl_front.as_ref().map(Proxy::addr),
        baseline,
        false,
        log,
    )?;
    if !matched {
        front.dump_logs(log);
        if let Some(sasl_front) = &sasl_front {
            sasl_front.dump_logs(log);
        }
        bail!(
            "{} answers differently through the proxy than directly — the \
             proxy is not transparent, or the suite is nondeterministic",
            subject.name
        );
    }
    Ok(())
}

/// One `odradek-accept --server` run. `Ok(false)` means it ran and the
/// subject did not satisfy the baseline; `Err` means it could not run.
fn run_accept(
    accept: &Path,
    addr: &str,
    sasl_addr: Option<&str>,
    baseline: &Path,
    record: bool,
    log: &mut String,
) -> Result<bool> {
    let mut cmd = Command::new(accept);
    cmd.args(["--server", addr]);
    if let Some(sasl_addr) = sasl_addr {
        cmd.args(["--sasl-server", sasl_addr]);
    }
    if record {
        cmd.args(["--write-baseline".as_ref(), baseline.as_os_str()]);
    } else {
        cmd.args(["--baseline".as_ref(), baseline.as_os_str()]);
    }
    let out = cmd.output().context("running odradek-accept")?;
    log.push_str(&String::from_utf8_lossy(&out.stdout));
    log.push_str(&String::from_utf8_lossy(&out.stderr));
    Ok(out.status.success())
}

/// Pick a free TCP port. Racy in principle; in practice docker publishes
/// the port fast enough that collisions with other suites are negligible.
fn ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// A running `examples/proxy` in front of one upstream listener.
struct Proxy {
    addr: String,
    child: std::process::Child,
    /// Everything the proxy has said, drained by a thread so a chatty
    /// proxy cannot block on a pipe nobody is reading.
    stderr: Arc<Mutex<String>>,
}

impl Proxy {
    fn start(binary: &Path, upstream: &str) -> Result<Proxy> {
        let addr = format!("127.0.0.1:{}", ephemeral_port()?);
        // --advertise defaults to --listen, which is what we want: the
        // proxy is reachable at the address it binds, so metadata
        // pointing there sends followers back through it.
        let mut child = Command::new(binary)
            .args(["--listen", &addr, "--upstream", upstream])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawning {}", binary.display()))?;
        let stderr = Arc::new(Mutex::new(String::new()));
        if let Some(pipe) = child.stderr.take() {
            let sink = Arc::clone(&stderr);
            std::thread::spawn(move || {
                for line in std::io::BufReader::new(pipe).lines().map_while(Result::ok) {
                    let _ = writeln!(sink.lock().unwrap(), "{line}");
                }
            });
        }
        Ok(Proxy {
            addr,
            child,
            stderr,
        })
    }

    fn addr(&self) -> &str {
        &self.addr
    }

    fn dump_logs(&self, log: &mut String) {
        let _ = writeln!(log, "--- proxy {} stderr ---", self.addr);
        log.push_str(&self.stderr.lock().unwrap());
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One broker container, and the host address its client listener is
/// published on.
struct Container {
    name: String,
    addr: String,
}

/// Every broker container a subject runs, plus the docker network they
/// share when there is more than one.
///
/// One node is the common case and gets no network: it talks to nobody.
/// Several need container-name DNS to find each other, because a broker
/// tells its peers where it is and the published host port is not an
/// address any of them can use.
struct Cluster {
    nodes: Vec<Container>,
    network: Option<Network>,
}

impl Cluster {
    fn start(subject: &Subject, sasl_port: u16) -> Result<Cluster> {
        let count = usize::from(subject.nodes.max(1));
        let ports: Vec<u16> = (0..count)
            .map(|_| ephemeral_port())
            .collect::<Result<_>>()?;
        // The names have to be known before the first container starts:
        // each node's quorum string names all of them, including the
        // ones that do not exist yet.
        let names: Vec<String> = (1..=count)
            .map(|id| format!("odradek-accept-{}-{}-{id}", subject.name, ports[0]))
            .collect();
        let quorum: Vec<String> = names
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{}@{name}:9093", i + 1))
            .collect();
        let quorum = quorum.join(",");

        let network = if count > 1 {
            Some(Network::create(&format!(
                "odradek-accept-net-{}",
                ports[0]
            ))?)
        } else {
            None
        };

        // Started in one pass and waited for afterwards: a KRaft node
        // holds an election with peers that are not up yet, so starting
        // them one-at-a-time-and-wait would wait for a quorum that
        // cannot form until the last one is running.
        let mut cluster = Cluster {
            nodes: Vec::new(),
            network,
        };
        for (i, name) in names.iter().enumerate() {
            let node = Container::start(
                subject,
                NodeSpec {
                    name,
                    id: i + 1,
                    port: ports[i],
                    sasl_port,
                    quorum: &quorum,
                    network: cluster.network.as_ref().map(Network::name),
                },
            )?;
            cluster.nodes.push(node);
        }
        Ok(cluster)
    }

    /// The node the suite is pointed at. It finds the others itself.
    fn bootstrap(&self) -> &str {
        &self.nodes[0].addr
    }

    fn addrs(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|n| n.addr.as_str())
    }

    /// Run a provisioning command on the first node.
    ///
    /// Provisioning that silently failed would leave the SASL checks
    /// skipping or failing for a reason that looks like the subject's
    /// fault, so this reports the command's own output.
    fn exec(&self, command: &[&str]) -> Result<()> {
        let mut args: Vec<&str> = vec!["exec", &self.nodes[0].name];
        args.extend_from_slice(command);
        let out = Command::new("docker")
            .args(&args)
            .output()
            .context("running docker exec")?;
        if !out.status.success() {
            bail!(
                "{}\n{}",
                String::from_utf8_lossy(&out.stdout).trim(),
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(())
    }

    fn dump_logs(&self, log: &mut String) {
        for node in &self.nodes {
            let _ = writeln!(log, "--- docker logs {} (tail) ---", node.name);
            match Command::new("docker")
                .args(["logs", "--tail", "40", &node.name])
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
}

/// What one node needs to know that the others do not: which it is,
/// where it is published, and how to reach the quorum.
struct NodeSpec<'a> {
    name: &'a str,
    id: usize,
    port: u16,
    sasl_port: u16,
    quorum: &'a str,
    network: Option<&'a str>,
}

impl Container {
    fn start(subject: &Subject, spec: NodeSpec<'_>) -> Result<Container> {
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--rm".into(),
            "--name".into(),
            spec.name.into(),
            "-p".into(),
            format!("127.0.0.1:{}:9092", spec.port),
        ];
        if let Some(network) = spec.network {
            args.push("--network".into());
            args.push(network.into());
        }
        if subject.sasl_listener {
            args.push("-p".into());
            args.push(format!("127.0.0.1:{}:9094", spec.sasl_port));
        }
        let mut trailing = false;
        for a in subject.run_args {
            if *a == "--" {
                trailing = true;
                args.push(subject.image.into());
                continue;
            }
            args.push(
                a.replace("{port}", &spec.port.to_string())
                    .replace("{sasl_port}", &spec.sasl_port.to_string())
                    .replace("{id}", &spec.id.to_string())
                    .replace("{node}", spec.name)
                    .replace("{quorum}", spec.quorum),
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
        Ok(Container {
            name: spec.name.to_owned(),
            addr: format!("127.0.0.1:{}", spec.port),
        })
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

/// A docker network the brokers of one cluster share.
struct Network {
    name: String,
}

impl Network {
    fn create(name: &str) -> Result<Network> {
        let out = Command::new("docker")
            .args(["network", "create", name])
            .output()
            .context("running docker network create")?;
        if !out.status.success() {
            bail!(
                "docker network create {name} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(Network {
            name: name.to_owned(),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        // Dropped after the containers on it (declaration order in
        // `Cluster`), but their removal is fire-and-forget, so the
        // network may still be in use for a moment. Retrying briefly
        // beats leaking a network per run.
        for _ in 0..20 {
            let out = Command::new("docker")
                .args(["network", "rm", &self.name])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            if matches!(out, Ok(status) if status.success()) {
                return;
            }
            std::thread::sleep(Duration::from_millis(250));
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
