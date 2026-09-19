# odradek-sasl

Sans-I/O SASL for the odradek crates: SCRAM-SHA-256/512 and PLAIN, both
the client and the server role.

This is not a general SASL framework, and the name says so. It is the
mechanisms the [odradek](https://github.com/ophymx/odradek) Kafka crates
need, in the shape they need them — reach for it because you are using
those, not because you want a SASL library.

It exists because three places in that workspace were deriving the same
keys: the client, a reference server, and the acceptance suite that
checks both. A security exchange implemented three times is implemented
zero times well.

```rust
use odradek_sasl::{Limits, Mechanism, ScramClient, ScramServer};

let mut client = ScramClient::new(
    Mechanism::ScramSha256, "user", "pencil", Limits::default(),
)?;
let first = client.client_first();
// ...carry `first` to the server, bring back its answer...
# Ok::<(), odradek_sasl::SaslError>(())
```

Sans-I/O, like the protocol crate: every type produces and consumes
strings and the caller carries them over whatever transport it has.
That is also what makes both roles testable against each other, and
against the RFC's worked examples, without a socket.

## What it holds to

- **RFC 7677 vectors, both directions.** The client's proof and the
  server's signature are checked against the published example byte for
  byte — evidence about the protocol rather than about our own
  arithmetic.
- **SASLprep (RFC 4013) on every credential.** Skip it and a password
  containing a non-break space derives a different key than the same
  password normalized, which surfaces as an authentication failure that
  looks exactly like a wrong password. Kafka's Java client normalizes;
  a Rust client that does not is the one that is wrong.
- **Nonces from a CSPRNG, in both roles.** A predictable *server* nonce
  lets a recorded exchange be replayed at a client, which is the one
  attack the client's own nonce cannot detect.
- **Constant-time comparison of both proofs.** The client's signature
  check and the server's proof check are each an attacker-supplied value
  compared against a secret-derived one.
- **Iteration counts bounded in both directions.** Below the floor is a
  KDF downgrade the client cannot refuse without failing to connect;
  above the ceiling is a peer-controlled CPU burn.
- **No credential in `Debug`, and none in an error.** Both are what end
  up in logs.

## What it deliberately is not

- **No channel binding.** The client advertises `n` — does not support
  it — so a server requiring it refuses rather than silently
  downgrading.
- **No credential store.** `ScramServer` serves one account, and holds
  a `ScramCredential` — derived keys, never a password. RFC 5802 is
  built so a server never needs one, and the type makes that the only
  option. Deciding which credential belongs to which account is the
  caller's job.
- **No token acquisition.** OAUTHBEARER carries a bearer token; getting
  one means talking to an authorization server over HTTP, which is an
  I/O and policy concern this crate is not.
- **No AWS credential resolution.** `AWS_MSK_IAM` (behind the
  `aws-msk-iam` feature) signs with credentials you pass in. No
  environment scanning, no profile parsing, no instance metadata: a
  crate that guesses where your keys live is one that will eventually
  pick the wrong ones.
- **No GSSAPI or DIGEST-MD5.** `Mechanism` is `#[non_exhaustive]`, so
  adding one later is not a breaking change.

## Verification

SCRAM is checked against RFC 7677's worked example in both directions,
and the client role is exercised against Apache Kafka on every CI run.
OAUTHBEARER is verified against a real broker: a valid token
authenticates, and a rejected one is reported as a rejection rather than
as success.

`AWS_MSK_IAM` is the exception and says so in its own docs. Its SigV4
core is anchored to AWS's published signing-key example, but the
MSK-specific assembly around it has **not** been run against a real MSK
cluster, because this project has none. Treat it as unproven against
the service.

## What SASL protects, and what it does not

SASL authenticates; it does not encrypt. **PLAIN** puts the password on
the wire. **SCRAM** never sends it, but a passive observer of a
plaintext connection still collects the username, salt, iteration count,
nonces and proof — exactly the inputs to an offline dictionary attack,
at the cost the iteration count sets. SCRAM over plaintext protects a
password from being *read*, not from being *cracked*. Use TLS.

Report security issues privately — see
[SECURITY.md](https://github.com/ophymx/odradek/blob/main/SECURITY.md).

Part of the [odradek](https://github.com/ophymx/odradek) constellation.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE)
or [MIT license](LICENSE-MIT) at your option.
