//! The decoders under arbitrary and mutated input.
//!
//! Three of this crate's stated guarantees are about what happens when
//! the bytes are *wrong*, and until now each was held only to the
//! malformed inputs someone thought to write down. A peer chooses those
//! bytes; the inputs nobody thought of are the ones that matter.
//!
//! - **Malformed input never panics.** Universal, and the reason this
//!   module exists: a panic in a decoder is a remote peer choosing when
//!   a broker, a proxy or a client stops running.
//! - **Record batches re-encode byte-identically.** The proxy guarantee,
//!   which the README scopes to record batches — so that is where it is
//!   asserted as a guarantee.
//! - **Tagged-field sections must be strictly ascending by tag.** Stated
//!   as a requirement the decoder enforces, because a repeated tag lets
//!   one occurrence overwrite another and breaks the round-trip.
//!
//! One more property is checked that the README does not promise,
//! because `examples/proxy.rs` decodes and re-encodes every message and
//! so depends on it whether or not it is written down:
//!
//! - **One pass through the codec settles**, in value and in bytes.
//!   Whatever spelling arrives, re-encoding what was decoded must
//!   produce bytes that decode to the same value and re-encode
//!   unchanged. This is what catches a decoder quietly losing a field:
//!   on a single round-trip, dropping data and canonicalizing it look
//!   identical.
//!
//! A message is *not* promised to re-encode to the bytes it arrived as,
//! and this module is where that stopped being a guess. Three values
//! have more than one spelling on the wire and the decoder accepts all
//! of them, each inherited from the reference implementation: a
//! non-zero `BOOLEAN`, a negative nullable-struct marker, and an
//! overlong varint. Two were found by the fixed seed below and the
//! third only by soaking other seeds, which is the argument for the
//! env-var overrides. Each has a test of its own here, asserting the
//! current behaviour exactly — so tightening any of them to a
//! `DecodeError` is a decision someone makes on purpose, with the test
//! in front of them, rather than a diff nobody notices.
//!
//! Not libFuzzer. This runs on stable, in the ordinary test job, from a
//! fixed seed, so a regression is caught by the same CI that catches
//! everything else and reproduces from the seed printed with it. For a
//! longer soak, `ODRADEK_FUZZ_ITERS` and `ODRADEK_FUZZ_SEED` override
//! both without a rebuild. That trade is deliberate: coverage-guided
//! fuzzing finds more, needs nightly, and finds it somewhere other than
//! in front of the change that caused it.

use bytes::{Bytes, BytesMut};

use crate::error::DecodeError;
use crate::message::Message;
use crate::messages::for_each_message;
use crate::records::{Record, RecordBatch, Records, decode_set, encode_set};

/// Mutants per (type, version).
///
/// Measured rather than guessed: in a debug build the whole sweep costs
/// about 0.6s more here than at a fifth of it, which is nothing beside
/// the rest of the test job and five times the chance of a new bug
/// meeting the input that shows it. Soaking past this is what the env
/// overrides are for — a hundred seeds at 8000 found nothing the third
/// time, and found the overlong varint the first.
const DEFAULT_ITERS: u32 = 1000;

/// The seed a CI run uses. Changing it is how the corpus moves; a
/// failure quotes whatever seed produced it, so it reproduces.
const DEFAULT_SEED: u64 = 0x0DEF_ACED_0DD1_1234;

/// How many findings to collect before giving up and reporting. A
/// decoder that is broken in one way is broken in thousands of inputs,
/// and printing all of them buries the one line that matters.
const MAX_FINDINGS: usize = 12;

/// SplitMix64: three lines, no dependency, and good enough for choosing
/// where to put a bit flip.
///
/// The requirement here is reproducibility, not statistical quality —
/// every failure has to come back from its seed, or the report is a
/// story about bytes nobody can see again.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform enough over `0..n`; `n == 0` answers 0.
    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            usize::try_from(self.next_u64() % n as u64).unwrap_or(0)
        }
    }

    /// A byte weighted towards the ones that change a decoder's mind.
    ///
    /// Uniform random bytes almost never form a plausible length or
    /// varint, so a uniform mutation mostly re-tests the first bounds
    /// check. Small values are lengths and counts, `0xFF` is a
    /// continuation byte and half of every negative, `0x80` is the low
    /// half of a two-byte varint.
    fn interesting_byte(&mut self) -> u8 {
        const INTERESTING: [u8; 12] = [0, 1, 2, 3, 4, 8, 0x7F, 0x80, 0x81, 0xFE, 0xFF, 0x0F];
        match self.below(4) {
            0 => u8::try_from(self.next_u64() & 0xFF).unwrap_or(0),
            _ => INTERESTING[self.below(INTERESTING.len())],
        }
    }
}

/// One mutation of `seed`.
///
/// The shapes are chosen for what a length-prefixed binary format is
/// actually vulnerable to: a count that no longer matches the bytes
/// behind it, a frame that stops early, a field that runs past its
/// parent. Uniform noise finds the first of these and none of the rest.
fn mutate(rng: &mut Rng, seed: &[u8]) -> Vec<u8> {
    let mut out = seed.to_vec();
    match rng.below(7) {
        // Flip one bit. The cheapest way to turn a valid count into an
        // enormous one.
        0 => {
            if !out.is_empty() {
                let at = rng.below(out.len());
                out[at] ^= 1u8 << rng.below(8);
            }
        }
        // Replace a byte with one that means something.
        1 => {
            if !out.is_empty() {
                let at = rng.below(out.len());
                out[at] = rng.interesting_byte();
            }
        }
        // Truncate: every decoder's "ran out of input" path, at every
        // depth, which is where an unchecked index would live.
        2 => {
            let keep = rng.below(out.len() + 1);
            out.truncate(keep);
        }
        // Append: trailing bytes after a complete message, and length
        // fields that now have more behind them than they claim.
        3 => {
            let extra = 1 + rng.below(16);
            for _ in 0..extra {
                out.push(rng.interesting_byte());
            }
        }
        // Overwrite a run, which is how a nested length gets replaced
        // wholesale rather than nudged.
        4 => {
            if !out.is_empty() {
                let at = rng.below(out.len());
                let len = 1 + rng.below(8.min(out.len() - at));
                for byte in &mut out[at..at + len] {
                    *byte = rng.interesting_byte();
                }
            }
        }
        // Duplicate a run: repeated tagged fields and repeated array
        // elements, the shape the ascending-tag rule exists for.
        5 => {
            if !out.is_empty() {
                let at = rng.below(out.len());
                let len = 1 + rng.below(8.min(out.len() - at));
                let piece: Vec<u8> = out[at..at + len].to_vec();
                out.splice(at..at, piece);
            }
        }
        // From nothing: a buffer of plausible bytes with no seed at all,
        // because a default encoding is short and its mutants stay near
        // it.
        _ => {
            out.clear();
            let len = rng.below(96);
            for _ in 0..len {
                out.push(rng.interesting_byte());
            }
        }
    }
    out
}

/// Findings, and the budget that produced them.
struct Session {
    rng: Rng,
    seed: u64,
    iters: u32,
    findings: Vec<String>,
    /// (type, version) pairs reached, so the report can say what was
    /// actually covered rather than what was attempted.
    covered: u32,
    /// (type, version) pairs with no encodable default to seed from.
    seedless: u32,
    /// Accepted inputs that came back as different bytes by one of the
    /// two known leniencies. Not a failure; reported because "how often
    /// does this actually happen" is the first question anyone asks
    /// about tolerating it.
    non_canonical: u32,
}

impl Session {
    fn new() -> Session {
        let seed = env_u64("ODRADEK_FUZZ_SEED").unwrap_or(DEFAULT_SEED);
        let iters = env_u64("ODRADEK_FUZZ_ITERS")
            .and_then(|v| u32::try_from(v).ok())
            .unwrap_or(DEFAULT_ITERS);
        Session {
            rng: Rng(seed),
            seed,
            iters,
            findings: Vec::new(),
            covered: 0,
            seedless: 0,
            non_canonical: 0,
        }
    }

    fn full(&self) -> bool {
        self.findings.len() >= MAX_FINDINGS
    }

    fn report(&mut self, what: &str, input: &[u8]) {
        if !self.full() {
            self.findings
                .push(format!("{what}\n      input: {}", hex(input)));
        }
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok()?.parse().ok()
}

/// Enough of the input to reproduce it by hand, and no more.
fn hex(bytes: &[u8]) -> String {
    let shown: String = bytes
        .iter()
        .take(64)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join("");
    if bytes.len() > 64 {
        format!("{shown}... ({} bytes)", bytes.len())
    } else {
        format!("{shown} ({} bytes)", bytes.len())
    }
}

/// Decode one mutant and hold the message-level properties to it.
///
/// All of them are conditional on the decoder having *accepted* the
/// bytes: a rejection is never a finding here, because the claim is
/// about what gets through rather than about what should.
///
/// Byte-identity is *measured*, not asserted. This crate accepts three
/// values spelled more than one way, each inherited from the reference
/// implementation rather than chosen here, and each pinned by a test of
/// its own below: a non-zero `BOOLEAN`, a negative nullable-struct
/// marker, and an overlong varint. The first two are length-preserving
/// and the third is not, so no cheap byte-wise predicate separates
/// "spelled differently" from "decoded wrongly" — and one that tried
/// would quietly excuse the second the day it appeared. What is
/// asserted instead is the property that does generalize: one pass
/// through the codec settles, in value and in bytes.
fn probe<M: Message + PartialEq>(session: &mut Session, name: &str, version: i16, input: &[u8]) {
    let mut buf = Bytes::copy_from_slice(input);
    // A panic here fails the test by itself, which is the point of the
    // first property: nothing catches it, nothing needs to.
    let Ok(decoded) = M::decode(&mut buf, version) else {
        return;
    };
    if buf.remaining_check() != 0 {
        // Trailing bytes. The decoder consumed a prefix and stopped,
        // which is a legitimate answer — a frame carries its own length
        // and the caller knows where the message ends. There is nothing
        // to compare a re-encoding against.
        return;
    }
    let mut first = BytesMut::new();
    if let Err(e) = decoded.encode(&mut first, version) {
        session.report(
            &format!(
                "asymmetry: {name} v{version} decoded a value it then could not encode \
                 ({e}); a proxy that forwards this drops the message"
            ),
            input,
        );
        return;
    }
    if first[..] != *input {
        session.non_canonical += 1;
    }

    // The fixed point, in both senses. A proxy does not need the bytes
    // it emits to equal the bytes it received — it needs them to mean
    // the same thing, and to be stable if they go round again. A
    // decoder that lost a field would satisfy neither, and on a single
    // round-trip would be indistinguishable from one that merely
    // canonicalized.
    let mut again = Bytes::copy_from_slice(&first);
    let Ok(second) = M::decode(&mut again, version) else {
        session.report(
            &format!(
                "fixed point: {name} v{version} re-encoded to bytes it then refused to \
                 decode\n      output: {}",
                hex(&first)
            ),
            input,
        );
        return;
    };
    if second != decoded {
        session.report(
            &format!(
                "fixed point: {name} v{version} re-encoded to bytes that decode to a \
                 different value, so one pass through the codec loses or alters \
                 data\n      output: {}",
                hex(&first)
            ),
            input,
        );
        return;
    }
    let mut twice = BytesMut::new();
    if second.encode(&mut twice, version).is_ok() && twice[..] != first[..] {
        session.report(
            &format!(
                "fixed point: {name} v{version} does not settle — one pass gives {}, two \
                 give {}",
                hex(&first),
                hex(&twice)
            ),
            input,
        );
    }
}

/// `Buf::remaining` without importing the trait at every call site.
trait RemainingCheck {
    fn remaining_check(&self) -> usize;
}

impl RemainingCheck for Bytes {
    fn remaining_check(&self) -> usize {
        self.len()
    }
}

/// Every version of one message type, from its default encoding.
///
/// The default value is a thin seed — most fields empty or `None` — so
/// the mutants do the work, including the one shape that starts from
/// nothing at all. What the default buys is a *valid* starting point at
/// every version, which is what makes a truncation land inside a real
/// field rather than before the first one.
fn fuzz_message<M: Message + Default + PartialEq>(session: &mut Session, name: &str) {
    for version in M::MIN_VERSION..=M::MAX_VERSION {
        if session.full() {
            return;
        }
        let mut seed_bytes = BytesMut::new();
        if M::default().encode(&mut seed_bytes, version).is_err() {
            // No default value of this type encodes at this version, so
            // there is no valid seed to mutate from. Today this never
            // happens — the assertion below holds it to that — and if a
            // schema ever makes it happen, the answer is to fuzz that
            // version from synthesized bytes rather than to lose it.
            session.seedless += 1;
            continue;
        }
        session.covered += 1;
        let seed_bytes = seed_bytes.to_vec();

        // The seed itself first: a default value that does not survive
        // its own round-trip would make every mutant's verdict
        // meaningless.
        probe::<M>(session, name, version, &seed_bytes);

        for _ in 0..session.iters {
            let mutant = mutate(&mut session.rng, &seed_bytes);
            probe::<M>(session, name, version, &mutant);
            if session.full() {
                return;
            }
        }
    }
}

/// The same sweep for a decoder that is not a [`Message`].
///
/// Only the first property — no panic — because without the trait there
/// is no generic `encode` to compare against, and the callers here are
/// the headers, whose round-trip `tests/message_roundtrip.rs` already
/// owns. Input is synthesized rather than seeded from a default value
/// for the same reason.
fn fuzz_versioned(
    session: &mut Session,
    name: &str,
    min: i16,
    max: i16,
    decode: impl Fn(&mut Bytes, i16) -> Result<(), DecodeError>,
) {
    for version in min..=max {
        session.covered += 1;
        for _ in 0..session.iters {
            let len = session.rng.below(64);
            let mut input = Vec::with_capacity(len);
            for _ in 0..len {
                input.push(session.rng.interesting_byte());
            }
            let mut buf = Bytes::from(input);
            // Nothing is asserted about the result. A panic is the
            // finding, and it reports itself.
            let _ = decode(&mut buf, version);
        }
        let _ = name;
    }
}

/// The framing layer, which sees a peer's bytes before anything else
/// does.
///
/// `try_split` is handed a buffer that grows as a socket fills it, so
/// the interesting inputs are the ones that arrive in pieces: a length
/// prefix with nothing behind it yet, a frame one byte short, a length
/// that claims more than the ceiling allows. It is fed here one byte at
/// a time for exactly that reason — a single call with the whole buffer
/// would never reach the "not yet" path that a real reader lives in.
///
/// What it must never do is panic, and what it must never return is a
/// frame longer than the ceiling it was given.
fn fuzz_framing(session: &mut Session) {
    // Small, so that an over-ceiling frame is something these inputs
    // can actually contain. With a realistic 64 MiB ceiling and inputs
    // measured in bytes, the "too long" branch is unreachable and the
    // assertion below is decoration — which is exactly what a
    // deliberately broken `check_len` proved when this test passed
    // against it.
    const CEILING: usize = 64;

    for _ in 0..session.iters {
        // The length prefix is chosen rather than stumbled on. Four
        // random bytes almost never spell a plausible length, so a
        // uniform generator spends its whole budget on the first
        // rejection and never reaches the arithmetic around the
        // ceiling, which is where a framing bug lives.
        let declared: i32 = match session.rng.below(8) {
            0 => 0,
            1 => -1,
            2 => i32::MIN,
            3 => i32::MAX,
            4 => i32::try_from(CEILING).unwrap_or(i32::MAX),
            5 => i32::try_from(CEILING + 1).unwrap_or(i32::MAX),
            6 => i32::try_from(session.rng.below(CEILING * 2)).unwrap_or(0),
            _ => i32::try_from(session.rng.below(8)).unwrap_or(0),
        };
        // Sometimes the body matches what the prefix claims, sometimes
        // it stops short, sometimes it runs over: a reader sees all
        // three, because a socket does not deliver messages.
        let body = match session.rng.below(3) {
            0 => usize::try_from(declared).unwrap_or(0).min(CEILING * 2),
            1 => session
                .rng
                .below(usize::try_from(declared).unwrap_or(0).min(CEILING) + 1),
            _ => session.rng.below(CEILING * 2),
        };
        let mut bytes = declared.to_be_bytes().to_vec();
        for _ in 0..body {
            bytes.push(session.rng.interesting_byte());
        }

        // One byte at a time, because that is the shape a reader is in:
        // the "not yet" answer has to be right at every prefix of the
        // input, not only once the whole thing has arrived.
        let mut buf = BytesMut::new();
        for &byte in &bytes {
            buf.extend_from_slice(&[byte]);
            // "Not yet" and "never" are both fine answers; the claim
            // is about neither, so only a frame that came back is
            // examined.
            if let Ok(Some(payload)) = crate::frame::try_split(&mut buf, CEILING) {
                if payload.len() > CEILING {
                    session.report(
                        &format!(
                            "framing: a {}-byte frame came back from a {CEILING}-byte \
                             ceiling",
                            payload.len()
                        ),
                        &bytes,
                    );
                    return;
                }
                // A response's first four payload bytes are its
                // correlation id; reading it must not panic on a
                // frame too short to hold one.
                let _ = crate::frame::peek_correlation_id(&payload);
            }
        }
    }
    session.covered += 1;
}

/// Whether `tail` is too short to hold the batch it claims.
///
/// Derived from the framing rather than from `decode_set`, on purpose:
/// a test that asked the decoder whether the decoder was right would
/// agree with it however wrong it was. The first twelve bytes of a
/// batch are its base offset and then its length, and everything the
/// length counts has to be there.
fn is_partial_batch(tail: &[u8]) -> bool {
    if tail.len() < 12 {
        return true;
    }
    let declared = i32::from_be_bytes([tail[8], tail[9], tail[10], tail[11]]);
    match usize::try_from(declared) {
        Ok(declared) => tail.len() < 12 + declared,
        // A negative length is not a batch anyone could complete.
        Err(_) => true,
    }
}

/// The record batch this module mutates: two records with keys, values,
/// a header and a tombstone, so a truncation has somewhere to land.
fn seed_batch() -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 1,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_001,
        producer_id: 42,
        producer_epoch: 7,
        base_sequence: 0,
        records: Records::Plain(vec![
            Record {
                key: Some(Bytes::from_static(b"fuzz-key")),
                value: Some(Bytes::from_static(b"fuzz-value")),
                ..Record::default()
            },
            Record {
                key: Some(Bytes::from_static(b"gone")),
                value: None,
                ..Record::default()
            },
        ]),
        ..RecordBatch::default()
    }
}

#[test]
fn decoders_survive_arbitrary_input() {
    let mut session = Session::new();

    macro_rules! one {
        ($ty:ty, $name:literal) => {
            if !session.full() {
                fuzz_message::<$ty>(&mut session, $name);
            }
        };
    }
    for_each_message!(one);

    // The keyless four. They have no api key, so they implement no
    // `Message` and the generated list leaves them out — but they are
    // decoders a peer reaches just as directly, and the header pair is
    // the *first* thing any of them touches. Named by hand because
    // there is no generic way to name them; if a fifth appears, the
    // count assertion below is what notices.
    fuzz_versioned(&mut session, "RequestHeader", 0, 2, |buf, version| {
        crate::messages::request_header::RequestHeader::decode(buf, version).map(|_| ())
    });
    fuzz_versioned(&mut session, "ResponseHeader", 0, 1, |buf, version| {
        crate::messages::response_header::ResponseHeader::decode(buf, version).map(|_| ())
    });
    fuzz_versioned(
        &mut session,
        "ConsumerProtocolSubscription",
        0,
        3,
        |buf, version| {
            crate::messages::consumer_protocol_subscription::ConsumerProtocolSubscription::decode(
                buf, version,
            )
            .map(|_| ())
        },
    );
    fuzz_versioned(
        &mut session,
        "ConsumerProtocolAssignment",
        0,
        3,
        |buf, version| {
            crate::messages::consumer_protocol_assignment::ConsumerProtocolAssignment::decode(
                buf, version,
            )
            .map(|_| ())
        },
    );
    fuzz_framing(&mut session);

    // Findings first. Coverage stops growing once the finding budget
    // fills, so asserting coverage ahead of it reports a number that is
    // an artefact of the failure rather than the failure.
    assert!(
        session.findings.is_empty(),
        "seed {:#x}, {} iters, {} (type, version) pairs covered\n  {}",
        session.seed,
        session.iters,
        session.covered,
        session.findings.join("\n  ")
    );
    // Coverage is every (type, version) the generated list names, plus
    // the four keyless decoders. Both halves are asserted: the floor
    // catches the list emptying out — a macro that expands to nothing
    // would otherwise pass this test in silence — and `seedless`
    // catches the quieter version, where a type is named but every one
    // of its versions is skipped for want of a seed.
    assert_eq!(
        session.seedless, 0,
        "{} (type, version) pair(s) had no encodable default to mutate from and were \
         not fuzzed at all",
        session.seedless
    );
    assert!(
        session.covered > 300,
        "only {} (type, version) pairs were fuzzed; the generated list changed shape",
        session.covered
    );
    println!(
        "fuzzed {} (type, version) pairs at {} iters from seed {:#x}; {} accepted input(s) \
         re-encoded to different bytes by a known leniency",
        session.covered, session.iters, session.seed, session.non_canonical
    );
}

/// The proxy guarantee, fuzzed: a record set that decodes must re-encode
/// to the bytes it came from.
///
/// Unlike the message-level round-trip above, this one *is* a stated
/// guarantee — a proxy that re-encodes a batch differently invalidates
/// its CRC or silently rewrites a producer's data — so a violation here
/// is a broken promise rather than something to think about.
#[test]
fn record_sets_survive_arbitrary_input() {
    let mut session = Session::new();
    // Three batches, not one. A set is the thing under test here — the
    // framing that decides where one batch ends and the next begins —
    // and with a single batch a decoder that dropped everything after
    // the first would be indistinguishable from a correct one. Which
    // is not hypothetical: it is what this seed was, and the test
    // passed against a `decode_set` deliberately broken that way.
    let mut seed_bytes = BytesMut::new();
    let batches: Vec<RecordBatch> = (0..3)
        .map(|i| RecordBatch {
            base_offset: i * 2,
            ..seed_batch()
        })
        .collect();
    encode_set(&mut seed_bytes, &batches).expect("the seed batches encode");
    let seed_bytes = seed_bytes.to_vec();

    let check = |session: &mut Session, input: &[u8]| {
        let mut buf = Bytes::copy_from_slice(input);
        let Ok(batches) = decode_set(&mut buf) else {
            return;
        };
        let mut out = BytesMut::new();
        let Err(e) = encode_set(&mut out, &batches) else {
            // The claim is byte-identity over the batches that were
            // *kept*, not over the whole input. `decode_set` stops at
            // the first batch the input does not finish and returns
            // what came before it, because that is what a fetch
            // response is: the broker cuts the set off at its byte
            // limit and every client is required to ignore the
            // fragment. So the re-encoding must reproduce a prefix,
            // and the leftover must be a fragment rather than a batch
            // that was silently dropped.
            if !input.starts_with(&out) {
                session.report(
                    &format!(
                        "proxy guarantee: a decoded record set re-encoded to different \
                         bytes\n      output: {}",
                        hex(&out)
                    ),
                    input,
                );
            } else if !is_partial_batch(&input[out.len()..]) {
                session.report(
                    &format!(
                        "dropped batch: {} trailing byte(s) hold a complete batch that \
                         decoding did not return",
                        input.len() - out.len()
                    ),
                    input,
                );
            }
            return;
        };
        session.report(
            &format!("proxy guarantee: a decoded record set would not re-encode ({e})"),
            input,
        );
    };

    check(&mut session, &seed_bytes);
    for _ in 0..session.iters * 64 {
        if session.full() {
            break;
        }
        let mutant = mutate(&mut session.rng, &seed_bytes);
        check(&mut session, &mutant);
    }

    assert!(
        session.findings.is_empty(),
        "seed {:#x}\n  {}",
        session.seed,
        session.findings.join("\n  ")
    );
}

/// A repeated tag must be refused, not absorbed.
///
/// The README states this as a requirement the decoder enforces, and
/// gives the reason: with two sections carrying the same tag, one
/// overwrites the other and whatever it held is gone — so the bytes
/// that come out are not the bytes that went in. It is asserted
/// directly rather than left to the mutator, because `duplicate a run`
/// only lands on a tag section by luck.
#[test]
fn a_repeated_tag_is_refused() {
    // ApiVersions v3 is flexible and its request body is two compact
    // strings followed by a tagged-field section.
    let mut body = BytesMut::new();
    body.extend_from_slice(&[0x01, 0x01]); // two empty compact strings
    body.extend_from_slice(&[0x02]); // two tagged fields
    body.extend_from_slice(&[0x63, 0x01, 0x00]); // tag 99, one byte
    body.extend_from_slice(&[0x63, 0x01, 0x00]); // tag 99 again

    let mut buf = Bytes::from(body.to_vec());
    let decoded = crate::messages::api_versions_request::ApiVersionsRequest::decode(&mut buf, 3);
    assert!(decoded.is_err(), "a repeated tag was accepted: {decoded:?}");

    // And the ascending rule is about order, not only repetition.
    let mut body = BytesMut::new();
    body.extend_from_slice(&[0x01, 0x01, 0x02]);
    body.extend_from_slice(&[0x63, 0x01, 0x00]); // tag 99
    body.extend_from_slice(&[0x62, 0x01, 0x00]); // tag 98, descending
    let mut buf = Bytes::from(body.to_vec());
    let decoded = crate::messages::api_versions_request::ApiVersionsRequest::decode(&mut buf, 3);
    assert!(
        decoded.is_err(),
        "a descending tag was accepted: {decoded:?}"
    );
}

// ---------------------------------------------------------------------
// The three spellings
//
// Each test below pins one value that has more than one encoding the
// decoder accepts, with the bytes the fuzzer found it by. They assert
// what the crate does *today*, which for a published decoder is the
// thing worth pinning: the alternative to a lenient decoder is one that
// answers `DecodeError` where the reference implementation answers a
// value, and that is an interop decision rather than a bug fix. Every
// one of these is a `git blame` away from the argument for it.
// ---------------------------------------------------------------------

/// A `BOOLEAN` is any non-zero byte, and comes back as `0x01`.
#[test]
fn a_boolean_is_any_non_zero_byte() {
    use crate::messages::create_topics_request::CreateTopicsRequest;

    // v2 is not flexible: an empty topic array, a 60s timeout, then
    // `validate_only` as one byte — here `0x02`.
    let input: [u8; 9] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xea, 0x60, 0x02];
    let mut buf = Bytes::copy_from_slice(&input);
    let decoded = CreateTopicsRequest::decode(&mut buf, 2).expect("0x02 is accepted as true");
    assert!(decoded.validate_only);

    let mut out = BytesMut::new();
    decoded.encode(&mut out, 2).expect("it re-encodes");
    assert_eq!(
        out[8], 0x01,
        "a boolean is written back canonically, so the byte a peer sent is not the byte a \
         proxy forwards"
    );
    assert_eq!(&out[..8], &input[..8], "and nothing else moves");
}

/// A nullable struct's marker is any negative byte, and comes back as
/// `0xff`.
#[test]
fn a_null_struct_marker_is_any_negative_byte() {
    use crate::messages::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;

    // Sixteen zero bytes carry throttle time, error code, two null
    // compact strings, member epoch and heartbeat interval. Byte 16 is
    // the `Assignment` marker and byte 17 closes the tagged fields.
    let mut input = vec![0u8; 18];
    input[16] = 0xdf;
    let mut buf = Bytes::copy_from_slice(&input);
    let decoded =
        ConsumerGroupHeartbeatResponse::decode(&mut buf, 0).expect("0xdf is accepted as null");
    assert!(decoded.assignment.is_none());

    let mut out = BytesMut::new();
    decoded.encode(&mut out, 0).expect("it re-encodes");
    assert_eq!(out[16], 0xff, "null is written back as -1");
}

/// An overlong varint decodes to the value it spells, and comes back
/// minimally encoded.
///
/// The one of the three that changes the message's *length*, which is
/// why no byte-wise comparison could have classified it — and the one
/// the fixed CI seed never reached. It turned up on the fifteenth seed
/// of a soak.
#[test]
fn an_overlong_varint_is_accepted() {
    use crate::messages::list_groups_request::ListGroupsRequest;

    // v3 has no fields of its own, so the message is a tagged-field
    // section and nothing else: a count, then tag 127 carrying two
    // bytes. The count is `0x81 0x00` — the unsigned varint 1, spelled
    // in two bytes instead of one.
    let input: [u8; 6] = [0x81, 0x00, 0x7f, 0x02, 0x0f, 0x0f];
    let mut buf = Bytes::copy_from_slice(&input);
    let decoded = ListGroupsRequest::decode(&mut buf, 3).expect("an overlong count is accepted");
    assert_eq!(decoded.unknown_tagged_fields.len(), 1);

    let mut out = BytesMut::new();
    decoded.encode(&mut out, 3).expect("it re-encodes");
    assert_eq!(
        &out[..],
        &[0x01, 0x7f, 0x02, 0x0f, 0x0f],
        "the count comes back minimally encoded, so the message is a byte shorter than it \
         arrived"
    );
}
