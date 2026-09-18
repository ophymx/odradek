# Security policy

## Reporting a vulnerability

Please report security issues privately, not as a public issue.

- **GitHub:** [open a private advisory](https://github.com/ophymx/odradek/security/advisories/new)
  (Security → Advisories → Report a vulnerability). This is preferred:
  it gives us a private place to discuss and a coordinated disclosure
  path.
- **Email:** abic@ophymx.com, if you would rather not use GitHub.

Please include what the issue lets an attacker do, and enough detail to
reproduce it — a failing test, a captured exchange, or a description of
the wire traffic is ideal.

## What to expect

This is a small project without a security team, and saying so is more
useful than a response time we cannot keep. What we commit to:

- an acknowledgement that a human has read the report,
- an assessment of whether it is a vulnerability and in which crates,
- a fix, or a written explanation of why the behaviour is intended,
- credit in the advisory and the release notes, unless you prefer not.

If a report does not get a reply, assume it was missed rather than
ignored, and please chase it.

## Supported versions

Pre-1.0. Fixes land on the latest release of each affected crate; there
are no maintained release branches yet. If that changes, it changes
here.

## Scope

These crates parse input from the network and authenticate against it,
so the whole of that path is in scope:

- **`odradek-protocol`** decodes bytes from a peer. A panic, an
  unbounded allocation, or a decode that produces the wrong value from
  well-formed-looking input is a vulnerability, not a bug report.
- **`odradek-sasl`** handles credentials and authenticates both ends of
  an exchange. Anything that leaks key material, lets an exchange be
  replayed, weakens the key derivation, or lets either side skip proving
  itself is in scope.
- **`odradek-client`** puts credentials on connections. Sending a
  secret over a transport that cannot protect it — or failing to verify
  what it is talking to — is in scope.
- **`odradek-web-core`**, **`odradek-web-sse`**, **`odradek-web-ws`**
  serve Kafka data to browsers. Cross-origin access, serving topics
  outside the configured gate, and resource exhaustion by an anonymous
  request are all in scope.
- **`odradek-acceptance`** connects to untrusted servers and accepts
  connections from untrusted clients, so the same standard applies to
  its parsing.

### Known limits, deliberately

These are documented behaviours rather than vulnerabilities, but tell us
if you think the documentation is wrong about them:

- **SCRAM over a plaintext connection** protects the password from being
  read, not from being cracked: an observer collects the salt, iteration
  count, nonces and proof, which are the inputs to an offline attack.
  Use TLS.
- **`odradek-sasl` implements no channel binding.** Its client
  advertises that it does not support it, so a server requiring channel
  binding refuses rather than silently downgrading.
- **The topic gate in the web crates is process-level**, not per-user.
  Per-request authorization is the embedder's, in middleware over the
  router these crates hand back.
- **`odradek-acceptance`'s reference subject is deliberately wrong** on
  demand — that is what the fault injection is for. It is a test double
  and is not built to be exposed.
