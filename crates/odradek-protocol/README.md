# odradek-protocol

Sans-I/O implementation of the Kafka wire protocol.

Kafka is increasingly a *protocol* with multiple independent server and
client implementations. This crate treats that protocol as the artifact:
primitive codecs (varints, compact strings, tagged fields), the API key
registry, header-version selection (including the ApiVersions
response-header quirk), and versioned message types generated from the
Apache Kafka schemas vendored in [`schemas/`](schemas/).

It does encoding and decoding only — no sockets, no async, no policy —
so a client, a server, a proxy, and a conformance suite can all share
the exact same codec.

## Guarantees

- **Unknown data round-trips.** Unrecognized tagged fields are preserved
  as raw bytes; known ones are materialized as typed `Option` fields.
  Tagged-field sections must be strictly ascending by tag, as the spec
  requires — a repeated tag would let one occurrence overwrite another
  and break the round-trip.
- **The proxy guarantee.** Record batches (v2, CRC-32C validated) carry
  compressed and unknown-codec payloads raw and re-encode
  byte-identically — verified against a golden segment from a real
  Kafka 4.1 broker.
- **Malformed input never panics.** Decoders return typed errors.
  Fuzzed every build: bit flips, truncations, splices and synthesized
  bytes against every generated type at every version it speaks, plus
  the record-set and header decoders. `src/fuzz.rs` holds that, the
  proxy guarantee above, and the round-trip below.
- **One pass through the codec settles.** A message is not guaranteed to
  re-encode to the bytes it arrived as, but re-encoding what was decoded
  always produces bytes that decode to the same value and re-encode
  unchanged. Three values have more than one spelling on the wire and
  the decoder accepts all of them, as the reference implementation does:
  a `BOOLEAN` is any non-zero byte and is written back as `0x01`, a
  nullable struct's marker is any negative byte and is written back as
  `0xff`, and an overlong varint decodes to the value it spells and is
  written back minimally. A proxy built on this crate normalizes those
  three, which for the last one means forwarding a message shorter than
  the one it received. Each has a test naming it.
- **Decoding is bounded.** A wire count is an instruction to allocate
  and the peer chooses it, so every decoder spends against a budget
  sized from its input: at most `clamp(16 × n, 64 KiB, 256 MiB)` bytes
  of decoded collections at any instant, growth transients included,
  plus the `String` contents it copies 1:1 off the wire. At the 64 MiB
  frame ceiling a hostile frame peaks at ~5× its bytes instead of the
  28× measured without it. `decode_with_limits` takes another policy;
  plain `decode` applies the default, so the bound is not opt-in.

```rust
use odradek_protocol::messages::metadata_request::MetadataRequest;

let mut buf = bytes::BytesMut::new();
MetadataRequest::default().encode(&mut buf, 12)?;
```

## Features

None by default — the crate depends only on `bytes` and `thiserror`,
which is what the proxy and embedded consumers want.

`hardware-crc` swaps the portable slicing-by-8 CRC-32C for the CPU's
CRC-32C instruction (via the [`crc32c`](https://crates.io/crates/crc32c)
crate, which detects support at runtime and falls back to software).
Measured here: CRC 1.2 GiB/s -> 2.6 GiB/s, and a whole 1 MiB fetch
response decodes 50% faster (697 MiB/s -> 1.02 GiB/s). Output is
bit-identical either way, enforced by a differential test that CI runs
in both configurations.

Part of the [odradek](https://github.com/ophymx/odradek) constellation:
a Kafka client, an acceptance suite, and web bridges all built on this
crate.

## License

Licensed under either of [Apache License, Version
2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

## The proxy guarantee, witnessed

Most of this crate's design is justified by "a proxy needs this" —
unknown tagged fields round-trip raw, record batches re-encode
byte-identically, decoding costs O(fields) rather than O(payload).
[`examples/proxy.rs`](examples/proxy.rs) is that argument's witness: a
pass-through Kafka proxy in about two hundred lines of this crate and
a socket, with no other odradek crate involved.

It forwards almost everything without parsing it. Two things force it
to look: response header versions are not carried in responses (so it
remembers what each correlation id asked for), and Metadata advertises
where to connect *next* (so those endpoints are decoded, rewritten to
point at the proxy, and re-encoded).

Run against Apache Kafka 4.1 with the Java console tools in front of
it, a topic is created, produced to and consumed back through the
proxy. More decisively, the full `odradek-acceptance` server suite —
47 checks over 23 APIs, including byte-level batch integrity and
compressed-batch passthrough — passes through the proxy with exactly
the results it gets against the broker directly.

The message schemas vendored in `schemas/` are copied from [Apache
Kafka](https://github.com/apache/kafka) and remain licensed to the
Apache Software Foundation under the Apache License, Version 2.0.
