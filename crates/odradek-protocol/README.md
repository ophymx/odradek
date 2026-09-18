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
- **The proxy guarantee.** Record batches (v2, CRC-32C validated) carry
  compressed and unknown-codec payloads raw and re-encode
  byte-identically — verified against a golden segment from a real
  Kafka 4.1 broker.
- **Malformed input never panics.** Decoders return typed errors.

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

The message schemas vendored in `schemas/` are copied from [Apache
Kafka](https://github.com/apache/kafka) and remain licensed to the
Apache Software Foundation under the Apache License, Version 2.0.
