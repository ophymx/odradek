//! `cargo xtask conformance` — run the acceptance suite against real
//! broker implementations in Docker and enforce the committed baselines.
//!
//! ```sh
//! cargo xtask conformance             # all subjects, enforce baselines
//! cargo xtask conformance --record    # (re)write baselines from this run
//! cargo xtask conformance redpanda    # only subjects whose name contains
//! cargo xtask conformance --no-proxy  # skip the second, proxied pass
//! cargo xtask conformance --no-client # skip the client pass
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
//! Last comes the **client pass** ([`client_pass`]): `odradek-client`'s
//! own examples, run unmodified against the same live broker. It rides
//! along here rather than in a job of its own because starting the
//! brokers is the expensive part and they are already up — and because
//! until it existed, the client had no automated contact with a real
//! broker anywhere. Every feature it shipped had been validated by
//! somebody running these examples by hand, which is a thing that holds
//! right up until it does not.
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

/// Docker label naming the xtask process that owns a container or
/// network.
const OWNER_LABEL: &str = "odradek-accept-owner";

/// Remove containers and networks left behind by an xtask that is no
/// longer running.
///
/// `Drop for Container` handles every ordinary ending, including the
/// error and panic paths. It cannot handle the process being killed
/// outright — and since the recovery checks need containers that
/// survive being stopped, `--rm` is not available to catch that either.
/// A killed run therefore leaves brokers running: on a developer's
/// machine, several of them, for hours, quietly competing with the next
/// run for the cores its timing assumptions depend on.
///
/// Keyed on the owning pid rather than on age or on the name, so a run
/// happening right now on the same machine is never disturbed: only a
/// container whose owner is gone is swept. A pid we cannot ask about is
/// left alone, which is the safe direction to be wrong in.
fn sweep_strays() {
    let label = format!("label={OWNER_LABEL}");
    for (kind, list) in [
        ("container", vec!["ps", "-aq"]),
        ("network", vec!["network", "ls", "-q"]),
    ] {
        let mut args = list;
        args.extend(["--filter", label.as_str()]);
        let Ok(out) = Command::new("docker").args(&args).output() else {
            return;
        };
        for id in String::from_utf8_lossy(&out.stdout).split_whitespace() {
            let Ok(inspect) = Command::new("docker")
                .args([
                    "inspect",
                    "-f",
                    &format!("{{{{index .Config.Labels \"{OWNER_LABEL}\"}}}}"),
                    id,
                ])
                .output()
            else {
                continue;
            };
            let owner = String::from_utf8_lossy(&inspect.stdout).trim().to_owned();
            // A label we cannot read, or a pid still alive: leave it.
            if owner.is_empty() || Path::new(&format!("/proc/{owner}")).exists() {
                continue;
            }
            let removed = match kind {
                "container" => Command::new("docker").args(["rm", "-f", "-v", id]).status(),
                _ => Command::new("docker").args(["network", "rm", id]).status(),
            };
            if matches!(removed, Ok(status) if status.success()) {
                eprintln!("removed stray {kind} {id} from pid {owner}");
            }
        }
    }
}

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
    /// Whether the subject's *own* listener demands authentication, so
    /// the suite must authenticate every connection rather than only
    /// the ones the `sasl/*` checks make.
    authenticated: bool,
    /// Commands to run inside the container once it is ready, before the
    /// suite starts — for state that cannot be configured at boot.
    provision: &'static [&'static [&'static str]],
    /// Whether the subject's Kafka listener speaks TLS, so the suite
    /// must too. A per-run CA and server certificate are generated and
    /// mounted at `/certs`; `{port}` is then the TLS port, which keeps
    /// every other piece of plumbing here unchanged.
    tls: bool,
    /// How many broker containers this subject runs. More than one gets
    /// a docker network, a shared cluster id, and `{id}`/`{quorum}`
    /// substitution; the suite is pointed at the first node and finds
    /// the rest through Metadata, as a client would.
    nodes: u8,
    /// [`CLIENT_SCENARIOS`] this subject is known not to serve, by name.
    ///
    /// Listed rather than skipped, because both directions are worth
    /// catching: a named scenario that starts succeeding is reported
    /// just as loudly as an unnamed one that starts failing. A broker
    /// growing support for something is news, and the alternative —
    /// quietly not running it — is how an entry outlives its reason.
    client_unsupported: &'static [&'static str],
}

/// One end-to-end exercise of `odradek-client` against a live broker.
///
/// These are the crate's own examples, run unmodified. That is the point
/// rather than a convenience: they are what the README points a reader
/// at, they are written in the public API, and they were the manual
/// validation this workspace leaned on for every feature it shipped.
/// What they were not is *automated* — the client had no contact with a
/// real broker in CI at all, so the whole adoption-gap list (SASL,
/// idempotence, admin, KIP-848, transactions, read_committed) rested on
/// somebody remembering to run them by hand.
struct ClientScenario {
    /// Names this in the log and in [`Subject::client_unsupported`].
    name: &'static str,
    /// The example to run, from `crates/odradek-client/examples`.
    example: &'static str,
    /// Arguments after the bootstrap address; `{ca}` becomes the
    /// subject's generated CA file.
    args: &'static [&'static str],
    /// The kind of listener this scenario needs.
    needs: Needs,
}

/// Which subjects a client scenario can be put to.
///
/// The split exists because the examples take a bootstrap address and
/// nothing else — a deliberate property, since an example carrying
/// connection flags is an example about connection flags. So the
/// subjects whose listeners demand TLS or SASL get `secure_smoke`,
/// which is the example that *is* about that, and the rest get
/// everything else.
enum Needs {
    /// A listener that answers without credentials or a handshake.
    Plain,
    /// A listener that speaks TLS.
    Tls,
    /// A listener that demands SASL.
    Authenticated,
}

impl Needs {
    fn met_by(&self, subject: &Subject) -> bool {
        match self {
            Needs::Plain => !subject.tls && !subject.authenticated,
            Needs::Tls => subject.tls,
            Needs::Authenticated => subject.authenticated,
        }
    }
}

/// How long one client scenario may take before it is killed.
///
/// Generous, because two of these wait out a real rebalance and one
/// waits for a transaction coordinator to be elected. It is a deadlock
/// backstop, not a performance assertion — a scenario that needs most of
/// it is one to look at, but not one to fail.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(120);

/// The client matrix's own rows: what `odradek-client` must be able to
/// do against every subject that can be asked.
const CLIENT_SCENARIOS: &[ClientScenario] = &[
    ClientScenario {
        name: "produce-consume",
        example: "produce_consume",
        args: &[],
        needs: Needs::Plain,
    },
    // One row per codec, because the *client* is what compresses: the
    // broker stores the batch as it arrives and hands it back, so a
    // codec this client frames wrongly is a codec no check on the
    // broker side can see. Snappy is xerial framing and lz4 is the
    // frame format, and both have been got wrong here before.
    ClientScenario {
        name: "produce-consume-gzip",
        example: "produce_consume",
        args: &["gzip"],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "produce-consume-lz4",
        example: "produce_consume",
        args: &["lz4"],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "produce-consume-snappy",
        example: "produce_consume",
        args: &["snappy"],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "produce-consume-zstd",
        example: "produce_consume",
        args: &["zstd"],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "admin",
        example: "admin",
        args: &[],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "group-join",
        example: "group_join",
        args: &[],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "group-consume",
        example: "group_consume",
        args: &[],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "kip848-join",
        example: "group848_join",
        args: &[],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "kip848-consume",
        example: "group848_consume",
        args: &[],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "transactions",
        example: "transactions",
        args: &[],
        needs: Needs::Plain,
    },
    ClientScenario {
        name: "tls",
        example: "secure_smoke",
        args: &["--ca", "{ca}"],
        needs: Needs::Tls,
    },
    // The credentials the subject was provisioned with, over a
    // plaintext listener — which SCRAM is fine on (it never transmits
    // the password) but which the client refuses for PLAIN. Asking for
    // the mechanism that is safe here means no scenario passes
    // `--allow-plaintext-credentials`, so a refusal that ought to
    // happen still would.
    ClientScenario {
        name: "sasl-scram",
        example: "secure_smoke",
        args: &[
            "--mechanism",
            "scram256",
            "--user",
            "conformance",
            "--pass",
            "conformance",
        ],
        needs: Needs::Authenticated,
    },
];

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
        authenticated: false,
        tls: false,
        nodes: 1,
        client_unsupported: &[],
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
    // The same broker again, behind TLS. Every other subject is
    // reached in plaintext, which is not how a broker is run anywhere
    // that matters -- and a suite that has never negotiated a
    // handshake cannot be pointed at a deployment that requires one.
    //
    // Its 9092 listener *is* the TLS listener, so `{port}` is the TLS
    // port and nothing else in this file changes. The certificate
    // names `localhost` rather than the ephemeral address the
    // container is published on, and the suite verifies against that
    // name: a certificate per run would otherwise be a certificate per
    // port.
    Subject {
        name: "apache-kafka-4.1.0-tls",
        image: "apache/kafka:4.1.0",
        run_args: &[
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            "KAFKA_LISTENERS=SSL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:9099",
            "-e",
            "KAFKA_ADVERTISED_LISTENERS=SSL://localhost:{port},INTERNAL://localhost:9099",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,SSL:SSL,INTERNAL:PLAINTEXT",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
            // PEM rather than JKS: Kafka has taken it since 2.7, and a
            // keystore built with keytool would need a JDK on whatever
            // runs this.
            "-e",
            "KAFKA_SSL_KEYSTORE_TYPE=PKCS12",
            // A filename and two credentials files rather than a path:
            // the entrypoint derives the location from these and
            // ignores KAFKA_SSL_KEYSTORE_LOCATION entirely.
            "-e",
            "KAFKA_SSL_KEYSTORE_FILENAME=server.keystore.p12",
            "-e",
            "KAFKA_SSL_KEYSTORE_CREDENTIALS=keystore_creds",
            "-e",
            "KAFKA_SSL_KEY_CREDENTIALS=key_creds",
            // No client certificate asked for: what is under test is
            // that the suite can speak TLS to a broker, not that it can
            // prove who it is.
            "-e",
            "KAFKA_SSL_CLIENT_AUTH=none",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        ],
        sasl_listener: false,
        authenticated: false,
        tls: true,
        nodes: 1,
        client_unsupported: &[],
        provision: &[],
    },
    // The same broker, four minor releases back, for the versions the
    // 4.x subjects never reach. Kafka 4.0 raised the *minimum* api
    // version it accepts on most apis (KIP-896), so against 4.1 the
    // suite can only ever negotiate the modern half of each range;
    // 3.7 still offers the old half, and offering it is what makes it
    // testable. Produce is the clearest case: 3.7 advertises v0 to v10
    // where 4.1 starts at v3, and v0-v2 carry a pre-KIP-98 message set
    // rather than a record batch — which this workspace does not model
    // and says so, a claim that had no broker to make it against until
    // now.
    //
    // 3.7 rather than 3.9 because the point is distance. The config is
    // byte-for-byte the 4.1 subject's, which is itself a small result:
    // nothing about the harness needed a version switch.
    Subject {
        name: "apache-kafka-3.7.0",
        image: "apache/kafka:3.7.0",
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
            "-e",
            "KAFKA_LISTENER_NAME_SASL_SCRAM-SHA-256_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required;",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        ],
        sasl_listener: true,
        authenticated: false,
        tls: false,
        nodes: 1,
        // ConsumerGroupHeartbeat is advertised and answered
        // UNSUPPORTED_VERSION: KIP-848 is early access here and off by
        // default. The client reports exactly that code, which is the
        // behaviour a caller needs to fall back on.
        client_unsupported: &["kip848-join", "kip848-consume"],
        provision: &[&[
            "/opt/kafka/bin/kafka-configs.sh",
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
        authenticated: false,
        tls: false,
        nodes: 1,
        // Redpanda 25.2 does not advertise ConsumerGroupHeartbeat at
        // all, so the client refuses before it sends: NoCommonVersion
        // rather than an error code. The honest failure of the two, and
        // worth recording as a distinct shape from Kafka 3.7's.
        client_unsupported: &["kip848-join", "kip848-consume"],
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
        authenticated: false,
        tls: false,
        nodes: 3,
        client_unsupported: &[],
        provision: &[],
    },
    Subject {
        // The same questions put to a second implementation. Cluster
        // semantics were the one part of the matrix tested against a
        // single implementation, which for a suite whose premise is
        // that Kafka is a protocol rather than a program is the wrong
        // place to have an n of 1.
        name: "redpanda-25.2.1-cluster",
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
            // Brokers find each other over RPC on the docker network,
            // by container name; clients reach them on the published
            // port. Two audiences, two addresses, as for Kafka.
            "--rpc-addr",
            "{node}:33145",
            "--advertise-rpc-addr",
            "{node}:33145",
            // Every node seeded from the first, including the first —
            // the documented shape for a cluster of fixed membership.
            // Redpanda assigns its own node ids from this, which is why
            // the cluster control is addressed by endpoint rather than
            // by id.
            "--seeds",
            "{node1}:33145",
            // dev-container mode keeps internal topics unreplicated,
            // which would make the coordinator-failure check a test of
            // this configuration rather than of Redpanda: the offsets
            // would be gone because nobody was keeping a copy.
            "--set",
            "redpanda.default_topic_replications=3",
            "--set",
            "redpanda.internal_topic_replication_factor=3",
        ],
        sasl_listener: false,
        authenticated: false,
        tls: false,
        nodes: 3,
        // Redpanda 25.2 does not advertise ConsumerGroupHeartbeat at
        // all, so the client refuses before it sends: NoCommonVersion
        // rather than an error code. The honest failure of the two, and
        // worth recording as a distinct shape from Kafka 3.7's.
        client_unsupported: &["kip848-join", "kip848-consume"],
        provision: &[],
    },
    Subject {
        // Redpanda with SASL on, which for Redpanda means on for the
        // whole cluster rather than for one listener. That is the
        // difference this subject exists to cover: Kafka scopes SASL to
        // a listener, so the suite can keep a plaintext one for the
        // other checks and point `--sasl-server` at a second; Redpanda
        // has no plaintext listener left to fall back to, and every
        // connection the suite makes has to authenticate.
        //
        // Which is how anybody actually runs a broker. Until this, the
        // suite could only validate one that lets anyone in.
        name: "redpanda-25.2.1-sasl",
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
            "SASL://0.0.0.0:9092",
            "--advertise-kafka-addr",
            "SASL://127.0.0.1:{port}",
            "--set",
            "redpanda.enable_sasl=true",
            // The listener has to say it authenticates, or SASL is on
            // cluster-wide and this listener still lets anyone in.
            "--set",
            "redpanda.kafka_api[0].authentication_method=sasl",
            // Without a superuser the credentials authenticate and then
            // are allowed to do nothing, and every check fails on
            // authorization rather than on anything it meant to ask.
            "--set",
            "redpanda.superusers=[conformance]",
        ],
        sasl_listener: false,
        authenticated: true,
        tls: false,
        nodes: 1,
        client_unsupported: &[],
        // The user is created through the admin api once the broker is
        // up: it lives in the cluster's own state, not in its config.
        provision: &[&[
            "curl",
            "-sS",
            "--fail",
            "-X",
            "POST",
            "http://localhost:9644/v1/security/users",
            "-H",
            "Content-Type: application/json",
            "-d",
            "{\"username\":\"conformance\",\"password\":\"conformance\",\"algorithm\":\"SCRAM-SHA-256\"}",
        ]],
    },
];

pub fn conformance(args: &[String]) -> Result<()> {
    // Before anything else, including argument validation: a machine
    // carrying brokers from a run that died is a machine this run's
    // timing assumptions are wrong about, and that is true however this
    // one was invoked.
    sweep_strays();

    let record = args.iter().any(|a| a == "--record");
    let proxied = !args.iter().any(|a| a == "--no-proxy");
    let client = !args.iter().any(|a| a == "--no-client");
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
    let examples = client.then(|| root.join("target/debug/examples"));
    if client {
        build_client_examples(&root)?;
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
            "running {} subjects, up to {} brokers at a time: {}",
            selected.len(),
            broker_cap(),
            selected
                .iter()
                .map(|s| s.name)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    // Subjects are independent and want to overlap, but they are not
    // free: a three-node subject is three brokers, and several of those
    // at once on a two-core CI runner is enough load to make a broker
    // take seconds over work it normally does in milliseconds. The
    // checks that wait on such work then fail for want of patience, and
    // report the subject rather than the machine. So the matrix
    // overlaps by *brokers* rather than by subjects.
    let budget = Arc::new(BrokerBudget::new(broker_cap()));
    let running: Vec<_> = selected
        .iter()
        .map(|&subject| {
            let accept = accept.clone();
            let proxy = proxy.clone();
            let examples = examples.clone();
            let conf_dir = conf_dir.clone();
            let budget = Arc::clone(&budget);
            std::thread::spawn(move || {
                let _permit = budget.acquire(usize::from(subject.nodes.max(1)));
                let mut log = String::new();
                let result = run_subject(
                    subject,
                    &accept,
                    proxy.as_deref(),
                    examples.as_deref(),
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

/// How many brokers the matrix may have running at once.
///
/// Half the cores, because a broker is a JVM or a Seastar runtime
/// rather than a thread and will take more than one core when it wants
/// them — with a floor of three so a three-node subject is never asked
/// to wait for a budget that could never satisfy it, and a ceiling
/// because beyond a point the constraint is memory instead.
///
/// The number matters more than it looks. Every wait in the suite is
/// for work a broker does asynchronously, and an oversubscribed machine
/// stretches that work until the waits expire — at which point the
/// report blames the subject for the harness's choice of how much to
/// run at once.
fn broker_cap() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(2)
        .div_euclid(2)
        .clamp(3, 6)
}

/// A count of brokers the matrix is allowed to be running.
///
/// Plain `Mutex`/`Condvar` rather than a semaphore crate: this is a
/// counter and a wait, and the xtask has no async runtime to borrow one
/// from.
struct BrokerBudget {
    cap: usize,
    state: Mutex<usize>,
    freed: std::sync::Condvar,
}

impl BrokerBudget {
    fn new(cap: usize) -> BrokerBudget {
        BrokerBudget {
            cap,
            state: Mutex::new(0),
            freed: std::sync::Condvar::new(),
        }
    }

    /// Wait until `want` brokers fit, then claim them until the
    /// returned guard is dropped.
    ///
    /// A subject wanting more than the whole budget is let through
    /// alone rather than deadlocked: the cap is a way to share a
    /// machine, not a rule about what may run on it.
    fn acquire(self: &Arc<Self>, want: usize) -> BrokerPermit {
        let want = want.min(self.cap);
        let mut running = self.state.lock().unwrap();
        while *running > 0 && *running + want > self.cap {
            running = self.freed.wait(running).unwrap();
        }
        *running += want;
        BrokerPermit {
            budget: Arc::clone(self),
            held: want,
        }
    }
}

struct BrokerPermit {
    budget: Arc<BrokerBudget>,
    held: usize,
}

impl Drop for BrokerPermit {
    fn drop(&mut self) {
        *self.budget.state.lock().unwrap() -= self.held;
        self.budget.freed.notify_all();
    }
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

/// Build the client examples the client pass runs.
///
/// All of them in one invocation: they share a crate, so building them
/// separately would be the same compile repeated.
fn build_client_examples(root: &Path) -> Result<()> {
    let status = Command::new(env!("CARGO"))
        .current_dir(root)
        .args(["build", "-p", "odradek-client", "--examples"])
        .status()
        .context("building the odradek-client examples")?;
    if !status.success() {
        bail!("building the odradek-client examples failed");
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
    examples: Option<&Path>,
    conf_dir: &Path,
    record: bool,
    log: &mut String,
) -> Result<()> {
    // Chosen in one batch with the node ports, for the reason
    // `ephemeral_ports` gives: the sasl listener is published alongside
    // them, so a node port that happens to equal it collides exactly as
    // two equal node ports would.
    let mut ports = ephemeral_ports(usize::from(subject.nodes.max(1)) + 1)?;
    let sasl_port = ports.pop().expect("one more port than nodes");
    // One CA and one server certificate per subject that needs them,
    // valid for `localhost` and 127.0.0.1 and for a day. Generated
    // rather than committed: a certificate in a repository is one that
    // expires on a date nobody remembers, in a run nobody changed.
    let certs = if subject.tls {
        Some(Certs::generate(subject.name)?)
    } else {
        None
    };
    let tls_ca = certs.as_ref().map(Certs::ca);
    let cluster = Cluster::start(subject, sasl_port, ports, certs.as_ref().map(Certs::dir))?;
    // The suite is pointed at one node and finds the rest through
    // Metadata, which is the only way a client could find them either.
    let addr = cluster.bootstrap().to_owned();
    // Where the `sasl/*` checks go to ask about mechanisms. Kafka gets
    // a second listener for it; a subject whose own listener demands
    // SASL *is* that listener, and pointing them at it is what turns
    // four skips into four answers.
    let sasl_addr = if subject.sasl_listener {
        Some(format!("127.0.0.1:{sasl_port}"))
    } else if subject.authenticated {
        Some(addr.clone())
    } else {
        None
    };

    // Every node, not just the bootstrap: a partition led by a broker
    // that is not up yet is a check failing on the harness's impatience.
    for node in cluster.addrs() {
        if let Err(e) = wait_ready(node, tls_ca.as_deref()) {
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
    // Only a cluster gets one. Stopping the only broker of a
    // single-node subject does not test failing over, it tests being
    // down — the checks would be about nothing, and they skip instead.
    let control = (subject.nodes > 1)
        .then(|| cluster.control_command())
        .transpose()?;
    // The credentials the suite authenticates with are the ones the
    // subject was provisioned with; a conformance run brings its own
    // account rather than borrowing somebody's.
    let authenticate = subject.authenticated.then_some("conformance:conformance");
    let tls_ca = certs.as_ref().map(Certs::ca);
    let target = Target {
        addr: &addr,
        sasl_addr: sasl_addr.as_deref(),
        control: control.as_deref(),
        authenticate,
        tls_ca: tls_ca.as_deref(),
    };
    let direct = run_accept(accept, &target, &baseline, record, log)?;
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
        // A TLS subject has no proxied pass: `examples/proxy.rs` is the
        // protocol crate plus a socket, and putting a TLS stack in it
        // would make it a different example than the one this is a
        // witness for.
        (Some(_), _) if subject.tls => {
            let _ = writeln!(
                log,
                "note: no proxied pass for {} — the proxy example speaks tcp, not tls",
                subject.name
            );
        }
        (Some(proxy), _) => proxy_pass(subject, accept, proxy, &target, &cluster, &baseline, log)?,
        (None, _) => {}
    }

    // Last, because it is the only pass that leaves state behind — it
    // creates topics and joins groups as a caller would — and the two
    // passes above are about what the broker does with a clean one.
    // Nested rather than a let chain: those need 1.88 and this
    // workspace compiles on 1.85.
    if let Some(examples) = examples {
        if let Err(e) = client_pass(subject, examples, &target, record, log) {
            cluster.dump_logs(log);
            return Err(e);
        }
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
    target: &Target<'_>,
    cluster: &Cluster,
    baseline: &Path,
    log: &mut String,
) -> Result<()> {
    let upstreams: Vec<String> = cluster.addrs().map(str::to_owned).collect();
    let front = Proxy::fronting(proxy, &upstreams)?;
    // A second instance for the SASL listener: the proxy forwards SASL
    // frames without parsing them, but it only has one upstream, and
    // the two listeners are different upstreams.
    let sasl_front = target
        .sasl_addr
        .map(|a| Proxy::start(proxy, a))
        .transpose()?;

    // Always plaintext: the proxy speaks tcp, and a tls subject never
    // gets a proxied pass.
    if let Err(e) = wait_ready(front.addr(), None) {
        front.dump_logs(log);
        return Err(e).context("proxy never became reachable");
    }

    let _ = writeln!(
        log,
        "--- {} through the proxy ({} -> {}) ---",
        subject.name,
        front.addr(),
        target.addr
    );
    // Named by the proxy's ports, because that is what the suite will
    // ask to have stopped.
    let control = (subject.nodes > 1)
        .then(|| cluster.control_command_via(front.addrs()))
        .transpose()?;
    let proxied = Target {
        addr: front.addr(),
        sasl_addr: sasl_front.as_ref().map(Proxy::addr),
        control: control.as_deref(),
        authenticate: target.authenticate,
        // The proxy example terminates TCP, not TLS; a subject behind
        // it is reached in plaintext or not at all.
        tls_ca: None,
    };
    let matched = run_accept(accept, &proxied, baseline, false, log)?;
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
/// Everything the accept binary needs in order to reach one subject.
struct Target<'a> {
    addr: &'a str,
    /// A second listener with SASL configured, for the `sasl/*` checks
    /// that need somewhere to ask about mechanisms.
    sasl_addr: Option<&'a str>,
    /// How to stop and start a broker, for the recovery checks.
    control: Option<&'a str>,
    /// Credentials, when the subject's own listener will not answer
    /// without them.
    authenticate: Option<&'a str>,
    /// The CA to trust, when the subject's listener speaks TLS.
    tls_ca: Option<&'a str>,
}

/// Put `odradek-client` through [`CLIENT_SCENARIOS`] against a live
/// subject.
///
/// Every applicable scenario runs, including the ones
/// [`Subject::client_unsupported`] says will fail, so the table is
/// checked in both directions rather than trusted.
fn client_pass(
    subject: &Subject,
    examples: &Path,
    target: &Target<'_>,
    record: bool,
    log: &mut String,
) -> Result<()> {
    let mut diverged = Vec::new();
    for scenario in CLIENT_SCENARIOS {
        if !scenario.needs.met_by(subject) {
            continue;
        }
        let expected_to_fail = subject.client_unsupported.contains(&scenario.name);
        let mut cmd = Command::new(examples.join(scenario.example));
        cmd.arg(target.addr);
        for arg in scenario.args {
            cmd.arg(match *arg {
                "{ca}" => target.tls_ca.unwrap_or_default().to_owned(),
                other => other.to_owned(),
            });
        }
        let outcome = run_bounded(cmd, CLIENT_TIMEOUT)
            .with_context(|| format!("running the {} example", scenario.example))?;
        let verdict = match (outcome.ok(), expected_to_fail) {
            (true, false) => "ok",
            (false, true) => "unsupported (as recorded)",
            (true, true) => {
                diverged.push(format!(
                    "{}: recorded as unsupported by {} but it worked — drop it from \
                     client_unsupported",
                    scenario.name, subject.name
                ));
                "UNEXPECTEDLY OK"
            }
            (false, false) => {
                diverged.push(format!("{}: {}", scenario.name, outcome.describe()));
                "FAILED"
            }
        };
        let _ = writeln!(log, "  client/{:<24} {verdict}", scenario.name);
        if !outcome.ok() {
            let _ = writeln!(log, "{}", indent(&tail(&outcome.output, 8)));
        }
    }
    if diverged.is_empty() {
        return Ok(());
    }
    if record {
        let _ = writeln!(
            log,
            "note: {} client scenario(s) diverge from the table (recorded run, not failing): {}",
            diverged.len(),
            diverged.join("; ")
        );
        return Ok(());
    }
    bail!(
        "{} client scenario(s) diverged against {}: {}",
        diverged.len(),
        subject.name,
        diverged.join("; ")
    );
}

/// What running one client scenario came to.
struct Outcome {
    status: Option<std::process::ExitStatus>,
    output: String,
}

impl Outcome {
    fn ok(&self) -> bool {
        self.status.is_some_and(|s| s.success())
    }

    fn describe(&self) -> String {
        match self.status {
            None => format!("did not finish within {}s", CLIENT_TIMEOUT.as_secs()),
            Some(status) => format!("exited {status}"),
        }
    }
}

/// Run `cmd` to completion, killing it if it outlasts `timeout`.
///
/// A scenario that hangs would otherwise hang the job, and the two group
/// examples wait on a coordinator that could in principle never answer.
/// Output is drained on its own threads because a child that fills a
/// pipe while nobody reads it deadlocks, which is the same hang wearing
/// a different hat.
fn run_bounded(mut cmd: Command, timeout: Duration) -> Result<Outcome> {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning the example")?;
    let mut stdout = child.stdout.take().context("example has no stdout")?;
    let mut stderr = child.stderr.take().context("example has no stderr")?;
    let out = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stdout.read_to_string(&mut s);
        s
    });
    let err = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = stderr.read_to_string(&mut s);
        s
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait().context("waiting for the example")? {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    let mut output = out.join().unwrap_or_default();
    output.push_str(&err.join().unwrap_or_default());
    Ok(Outcome { status, output })
}

/// Keep the last `n` lines.
fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

fn indent(text: &str) -> String {
    text.lines()
        .map(|l| format!("      {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn run_accept(
    accept: &Path,
    target: &Target<'_>,
    baseline: &Path,
    record: bool,
    log: &mut String,
) -> Result<bool> {
    let mut cmd = Command::new(accept);
    cmd.args(["--server", target.addr]);
    if let Some(sasl_addr) = target.sasl_addr {
        cmd.args(["--sasl-server", sasl_addr]);
    }
    if let Some(control) = target.control {
        cmd.args(["--cluster-control", control]);
    }
    if let Some(login) = target.authenticate {
        cmd.args(["--authenticate", login]);
    }
    if let Some(ca) = target.tls_ca {
        cmd.args(["--tls-ca", ca]);
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

/// `xtask cluster-node <name:port,...> <stop|start> <node-id> <host:port>`
/// — the command the recovery checks are given as `--cluster-control`.
///
/// This exists as an xtask subcommand rather than a shell script for one
/// reason: `start` must not return until the broker is *serving*, and
/// "serving" is an ApiVersions exchange, not an open port. A script
/// could poll a TCP connect, which brokers accept well before they
/// answer anything — and a check that resumed there would blame the
/// subject for the harness's impatience. [`wait_ready`] is already the
/// right probe, so the control reuses it.
///
/// The container list is passed in because the xtask that chose those
/// ports is a different process from this one, and a container keeps its
/// published port across a stop.
pub fn node_control(args: &[String]) -> Result<()> {
    let [names, verb, _node_id, addr] = args else {
        bail!("usage: xtask cluster-node <name:port,...> <stop|start> <node-id> <host:port>");
    };
    // Looked up by the address the broker advertised, not by node id.
    // The two coincide for Kafka, where the id is configured; they do
    // not for an implementation that assigns its own, and the address
    // is what the suite actually connected to either way.
    let port = addr.rsplit_once(':').map_or(addr.as_str(), |(_, p)| p);
    let name = names
        .split(',')
        .find_map(|entry| {
            let (name, mapped) = entry.rsplit_once(':')?;
            (mapped == port).then_some(name)
        })
        .ok_or_else(|| anyhow::anyhow!("no container for {addr} among {names}"))?
        .to_owned();
    match verb.as_str() {
        "stop" => {
            // A graceful stop, so the broker hands off cleanly and the
            // cluster elects around it promptly. The harsher question —
            // what a cluster does when a broker is killed outright and
            // has to be noticed missing — is a different check than the
            // ones written so far.
            docker(&["stop", "-t", "20", &name])?;
            Ok(())
        }
        "start" => {
            docker(&["start", &name])?;
            // Plaintext: `cluster-node` is only used by the recovery
            // checks, and no TLS subject is a cluster.
            wait_ready(addr, None).with_context(|| format!("{name} restarted but never served"))
        }
        other => bail!("unknown verb {other:?}; expected stop or start"),
    }
}

/// Run a docker command, returning its stdout.
fn docker(args: &[&str]) -> Result<String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .context("running docker")?;
    if !out.status.success() {
        bail!(
            "docker {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// A per-run CA and server certificate, on disk, removed when dropped.
///
/// Generated with `openssl` rather than committed. A certificate in a
/// repository has an expiry date nobody is watching, and the run that
/// discovers it is one that changed nothing — the failure arrives
/// detached from any cause, which is the worst kind to debug and the
/// exact shape of the bug this campaign spent an afternoon on.
struct Certs {
    dir: std::path::PathBuf,
}

/// Protects a key that exists for one run on one loopback port. In the
/// source because it has nothing to protect; see [`Certs::generate`].
const KEY_PASSPHRASE: &str = "conformance";

impl Certs {
    fn generate(subject: &str) -> Result<Certs> {
        let dir = std::env::temp_dir().join(format!(
            "odradek-certs-{}-{}",
            std::process::id(),
            subject.replace('.', "-")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).context("creating the certificate directory")?;
        let at = |name: &str| dir.join(name).to_string_lossy().into_owned();

        // The broker's certificate names `localhost` and 127.0.0.1,
        // because that is what the suite dials and what it verifies
        // against. Without the SAN a modern TLS stack refuses the
        // certificate outright -- CN alone has not been enough since
        // rustls existed.
        std::fs::write(
            dir.join("ext.cnf"),
            "subjectAltName=DNS:localhost,IP:127.0.0.1\n",
        )
        .context("writing the certificate extensions")?;

        openssl(&[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-days",
            "1",
            "-nodes",
            "-keyout",
            &at("ca.key"),
            "-out",
            &at("ca.crt"),
            "-subj",
            "/CN=odradek conformance ca",
        ])?;
        // Encrypted, because the apache/kafka entrypoint insists on a
        // key password: it reads one out of a credentials file and
        // exports it whether or not the key has one, and an empty
        // password against an unencrypted key is not the same as no
        // password. It protects nothing -- these certificates live for
        // one run on one loopback port -- which is why the passphrase
        // is in this file.
        openssl(&[
            "req",
            "-newkey",
            "rsa:2048",
            "-sha256",
            "-keyout",
            &at("server.key"),
            "-out",
            &at("server.csr"),
            "-subj",
            "/CN=localhost",
            "-passout",
            &format!("pass:{KEY_PASSPHRASE}"),
        ])?;
        openssl(&[
            "x509",
            "-req",
            "-sha256",
            "-days",
            "1",
            "-in",
            &at("server.csr"),
            "-CA",
            &at("ca.crt"),
            "-CAkey",
            &at("ca.key"),
            "-CAcreateserial",
            "-extfile",
            &at("ext.cnf"),
            "-out",
            &at("server.crt"),
        ])?;

        // PKCS#12 rather than PEM. Kafka reads both, but its PEM
        // keystore refuses to be given a *store* password -- "SSL key
        // store password cannot be specified with PEM format, only key
        // password may be specified" -- and this image's entrypoint
        // always sets one, out of a credentials file it requires. The
        // two cannot both be satisfied. PKCS#12 takes both passwords,
        // openssl builds it with no JDK anywhere in sight, and the
        // entrypoint's convention is met as written.
        openssl(&[
            "pkcs12",
            "-export",
            "-in",
            &at("server.crt"),
            "-inkey",
            &at("server.key"),
            "-passin",
            &format!("pass:{KEY_PASSPHRASE}"),
            "-out",
            &at("server.keystore.p12"),
            "-name",
            "broker",
            "-passout",
            &format!("pass:{KEY_PASSPHRASE}"),
        ])?;
        // The entrypoint reads the password out of a file rather than
        // an environment variable, and refuses to start without one.
        for name in ["keystore_creds", "key_creds"] {
            std::fs::write(dir.join(name), format!("{KEY_PASSPHRASE}\n"))
                .context("writing the credentials file")?;
        }
        // The broker reads these as a user the container picked, not as
        // whoever ran the xtask.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for name in [
                "ca.crt",
                "server.keystore.p12",
                "keystore_creds",
                "key_creds",
            ] {
                let _ = std::fs::set_permissions(
                    dir.join(name),
                    std::fs::Permissions::from_mode(0o644),
                );
            }
        }
        Ok(Certs { dir })
    }

    fn dir(&self) -> &str {
        // Created from `temp_dir()` and a pid, so it is utf-8 unless
        // TMPDIR is not, in which case docker would refuse it anyway.
        self.dir.to_str().unwrap_or_default()
    }

    /// What the suite trusts.
    fn ca(&self) -> String {
        self.dir.join("ca.crt").to_string_lossy().into_owned()
    }
}

impl Drop for Certs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn openssl(args: &[&str]) -> Result<()> {
    let out = Command::new("openssl")
        .args(args)
        .output()
        .context("running openssl (is it installed?)")?;
    if !out.status.success() {
        bail!(
            "openssl {} failed: {}",
            args.first().copied().unwrap_or_default(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
}

/// Pick `n` distinct free TCP ports.
///
/// Every listener is held open until the last port has been chosen, and
/// only then are they all released. Asking one at a time and releasing
/// each before asking for the next lets the kernel hand the same port
/// straight back — it is free again by then — and two containers told
/// to publish the same host port is a `docker run` failure that reads
/// as the subject's fault. That is what it read as: a three-node
/// cluster failing on its third node with "port is already allocated",
/// once, on a loaded runner.
///
/// Still racy against the rest of the machine, which nothing short of
/// letting docker choose can fix; not racy against itself any more.
fn ephemeral_ports(n: usize) -> Result<Vec<u16>> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()?;
    listeners
        .iter()
        .map(|listener| Ok(listener.local_addr()?.port()))
        .collect()
}

/// A running `examples/proxy` in front of one upstream listener.
struct Proxy {
    addr: String,
    /// Every listener, in upstream order, so a cluster's control can be
    /// rewritten to name the ports the suite actually connects to.
    addrs: Vec<String>,
    child: std::process::Child,
    /// Everything the proxy has said, drained by a thread so a chatty
    /// proxy cannot block on a pipe nobody is reading.
    stderr: Arc<Mutex<String>>,
}

impl Proxy {
    fn start(binary: &Path, upstream: &str) -> Result<Proxy> {
        Proxy::fronting(binary, std::slice::from_ref(&upstream.to_owned()))
    }

    /// A proxy with one listener per upstream broker.
    ///
    /// Every broker gets its own, and the first is what the suite is
    /// pointed at. One listener in front of a cluster would not be a
    /// proxy of it: metadata would name that one address for every
    /// broker, and the client would send each partition's writes to
    /// whichever broker happened to be behind it.
    fn fronting(binary: &Path, upstreams: &[String]) -> Result<Proxy> {
        let ports = ephemeral_ports(upstreams.len())?;
        let addrs: Vec<String> = ports
            .iter()
            .map(|port| format!("127.0.0.1:{port}"))
            .collect();
        let addr = addrs
            .first()
            .ok_or_else(|| anyhow::anyhow!("a proxy needs at least one upstream"))?
            .clone();
        // The listen address is also the advertised one: the proxy is
        // reachable where it binds, so metadata pointing there sends
        // clients back through it.
        let mut args = Vec::new();
        for (listen, upstream) in addrs.iter().zip(upstreams) {
            args.push("--map".to_owned());
            args.push(format!("{listen}={upstream}"));
        }
        let mut child = Command::new(binary)
            .args(&args)
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
            addrs,
            child,
            stderr,
        })
    }

    fn addr(&self) -> &str {
        &self.addr
    }

    fn addrs(&self) -> &[String] {
        &self.addrs
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
    fn start(
        subject: &Subject,
        sasl_port: u16,
        ports: Vec<u16>,
        certs: Option<&str>,
    ) -> Result<Cluster> {
        let count = usize::from(subject.nodes.max(1));
        debug_assert_eq!(ports.len(), count);
        // The names have to be known before the first container starts:
        // each node's quorum string names all of them, including the
        // ones that do not exist yet.
        // Dots stripped from the subject name: these become container
        // names, container names become hostnames, and a hostname with
        // a version number in it ("redpanda-25.2.1-cluster") is one
        // rpk's seed parser refuses as neither a host nor a host:port.
        let stem = subject.name.replace('.', "-");
        let names: Vec<String> = (1..=count)
            .map(|id| format!("odradek-accept-{stem}-{}-{id}", ports[0]))
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
                    first: &names[0],
                    id: i + 1,
                    port: ports[i],
                    sasl_port,
                    quorum: &quorum,
                    network: cluster.network.as_ref().map(Network::name),
                    certs,
                },
            )?;
            cluster.nodes.push(node);
        }
        Ok(cluster)
    }

    /// The `--cluster-control` command for this cluster: this very
    /// binary, told which containers it is about.
    ///
    /// Pointing at `current_exe` rather than at a written-out script
    /// keeps the readiness probe honest — see [`node_control`] — and
    /// leaves no temporary file to clean up.
    fn control_command(&self) -> Result<String> {
        let exe = std::env::current_exe().context("locating the xtask binary")?;
        let names: Vec<String> = self
            .nodes
            .iter()
            .map(|n| {
                let port = n.addr.rsplit_once(':').map_or("", |(_, p)| p);
                format!("{}:{port}", n.name)
            })
            .collect();
        Ok(format!(
            "{} cluster-node {}",
            exe.display(),
            names.join(",")
        ))
    }

    /// The same command, for a suite reaching these nodes through
    /// something else.
    ///
    /// `node_control` finds a container by the port the suite connected
    /// to, which is the whole point — the suite knows addresses, not
    /// container names. Through a proxy those are the proxy's ports, so
    /// stopping "the broker at 127.0.0.1:41000" has to mean the
    /// container behind that listener. Without this the two recovery
    /// checks would find no container, skip, and diverge from a
    /// baseline that records them passing.
    fn control_command_via(&self, fronts: &[String]) -> Result<String> {
        let exe = std::env::current_exe().context("locating the xtask binary")?;
        if fronts.len() != self.nodes.len() {
            bail!(
                "{} listeners in front of {} nodes",
                fronts.len(),
                self.nodes.len()
            );
        }
        let names: Vec<String> = self
            .nodes
            .iter()
            .zip(fronts)
            .map(|(node, front)| {
                let port = front.rsplit_once(':').map_or("", |(_, p)| p);
                format!("{}:{port}", node.name)
            })
            .collect();
        Ok(format!(
            "{} cluster-node {}",
            exe.display(),
            names.join(",")
        ))
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
    /// The first node's name, for implementations that bootstrap a
    /// cluster by pointing every node at one seed rather than by
    /// enumerating a quorum.
    first: &'a str,
    id: usize,
    port: u16,
    sasl_port: u16,
    quorum: &'a str,
    network: Option<&'a str>,
    /// Host directory holding this run's CA and server certificate,
    /// mounted at `/certs` when the subject speaks TLS.
    certs: Option<&'a str>,
}

impl Container {
    fn start(subject: &Subject, spec: NodeSpec<'_>) -> Result<Container> {
        // Deliberately not `--rm`. A container that removes itself on
        // stop cannot be started again, and the recovery checks stop a
        // broker precisely so they can watch it come back. Cleanup is
        // `Drop for Container`'s job either way — `--rm` was only ever
        // belt and braces, and the brace it cost was this.
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            spec.name.into(),
            // Stamped with the pid that owns it, so a later run can tell
            // a container this one is still using from one whose owner
            // died before `Drop` could run. See `sweep_strays`.
            "--label".into(),
            format!("{OWNER_LABEL}={}", std::process::id()),
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
        if let Some(certs) = spec.certs {
            args.push("-v".into());
            // Where the apache/kafka entrypoint looks: it builds the
            // keystore path out of `/etc/kafka/secrets` and a filename,
            // and overwrites any location it is given.
            args.push(format!("{certs}:/etc/kafka/secrets:ro"));
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
                    .replace("{node1}", spec.first)
                    .replace("{quorum}", spec.quorum),
            );
        }
        if !trailing {
            args.push(subject.image.into());
        }
        // The container's own logs are gone by the time a broker that
        // refused its arguments has exited, so being able to see the
        // arguments is the difference between a diagnosis and a guess.
        if std::env::var_os("ODRADEK_DEBUG_DOCKER").is_some() {
            eprintln!("docker {}", args.join(" "));
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
        // here needs to see the end of it: the next run picks a fresh
        // ephemeral port rather than reusing this one. Fire it and walk
        // away — including on the error and panic paths this guard
        // exists for.
        //
        // The ordinary path, and the only one `--rm` used to cover:
        // dropping it is what made the recovery checks possible, since
        // a container that removes itself on stop cannot be started
        // again. An xtask killed outright runs no destructor at all, so
        // `sweep_strays` collects what this misses on the next run.
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
        let owner = format!("{OWNER_LABEL}={}", std::process::id());
        let out = Command::new("docker")
            .args(["network", "create", "--label", &owner, name])
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
/// Wait until `addr` answers, over TLS when `ca` says the listener
/// speaks it.
///
/// The plaintext probe against a TLS listener does not merely fail, it
/// fails silently-ish: the broker is perfectly healthy, logs a handshake
/// failure every 250ms for ninety seconds, and the xtask reports the
/// subject as never having started.
fn wait_ready(addr: &str, ca: Option<&str>) -> Result<()> {
    let deadline = Instant::now() + READY_TIMEOUT;
    let mut last_err = String::new();
    while Instant::now() < deadline {
        let attempt = match ca {
            Some(ca) => probe_tls(addr, ca),
            None => probe(addr),
        };
        match attempt {
            Ok(()) => return Ok(()),
            Err(e) => last_err = e.to_string(),
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    bail!("{addr} not ready after {READY_TIMEOUT:?}: {last_err}");
}

/// Readiness for a TLS listener: a full handshake, verified against the
/// same CA the suite will use.
///
/// `openssl s_client` rather than a TLS stack in this crate. openssl is
/// already required here to make the certificates, and what is being
/// waited for is that the listener completes a handshake — which is
/// exactly what this asks and nothing more.
fn probe_tls(addr: &str, ca: &str) -> Result<()> {
    let out = Command::new("openssl")
        .args([
            "s_client",
            "-connect",
            addr,
            "-servername",
            "localhost",
            "-CAfile",
            ca,
            "-verify_return_error",
            "-brief",
        ])
        .stdin(Stdio::null())
        .output()
        .context("running openssl s_client")?;
    if !out.status.success() {
        bail!(
            "tls handshake refused: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(())
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
