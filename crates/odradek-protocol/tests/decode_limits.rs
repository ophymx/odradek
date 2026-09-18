//! The decode allocation bound, enforced.
//!
//! Four shapes measured by a security review turned a frame at the
//! `frame::DEFAULT_MAX_FRAME` ceiling into gigabytes of live structs —
//! 28x, 24x, 20x and 14.7x the input bytes — because a count-prefixed
//! array is an allocation instruction and the elements are far larger
//! than the bytes that ask for them. Each is rebuilt here at a size a
//! test can afford, decoded twice (once with the bound off as a control,
//! once with the default on), and the peak heap of each run measured.
//!
//! The assertion is the bound the crate documents: a decode's peak
//! allocation never exceeds `clamp(16 * n, 64 KiB, 256 MiB)`. The
//! unbounded control is what gives that teeth — a bound that merely
//! matched what the vector already did would look like a success.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bytes::{Bytes, BytesMut};
use odradek_protocol::budget::{Budget, Limits};
use odradek_protocol::error::DecodeError;
use odradek_protocol::messages::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use odradek_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use odradek_protocol::records::{
    Record, RecordBatch, RecordHeader, Records, decode_set_with_budget, encode_set,
};
use odradek_protocol::wire;

// ---------------------------------------------------------------------------
// Peak-heap measurement
// ---------------------------------------------------------------------------

// Per-thread, so the other tests in this binary running in parallel
// cannot contaminate a measurement. `const`-initialized, so touching the
// cells never itself allocates.
thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<usize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

struct Tracking;

fn record_alloc(size: usize) {
    if ARMED.try_with(Cell::get) != Ok(true) {
        return;
    }
    let live = LIVE.with(|l| {
        let next = l.get().saturating_add(size);
        l.set(next);
        next
    });
    PEAK.with(|p| {
        if live > p.get() {
            p.set(live);
        }
    });
}

fn record_free(size: usize) {
    if ARMED.try_with(Cell::get) != Ok(true) {
        return;
    }
    LIVE.with(|l| l.set(l.get().saturating_sub(size)));
}

unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_alloc(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record_free(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // A reallocation holds both blocks while it copies, and that
        // moment is precisely what the budget claims to bound: charge
        // the new block first, release the old one only after the move.
        record_alloc(new_size);
        let out = unsafe { System.realloc(ptr, layout, new_size) };
        record_free(layout.size());
        out
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

/// Run `f` and report the high-water mark of heap it held, in bytes.
fn peak_heap_of<T>(f: impl FnOnce() -> T) -> (T, usize) {
    LIVE.with(|l| l.set(0));
    PEAK.with(|p| p.set(0));
    ARMED.with(|a| a.set(true));
    let value = f();
    ARMED.with(|a| a.set(false));
    (value, PEAK.with(Cell::get))
}

// ---------------------------------------------------------------------------
// The four vectors
// ---------------------------------------------------------------------------

/// Wire bytes each vector is built to — small enough for a debug test
/// run. Amplification is a ratio, so the conclusions do not depend on
/// this number; only the absolute figures do.
const INPUT_BYTES: usize = 1 << 20;

/// One record whose header count is enormous and whose headers are all
/// empty: 2 wire bytes each (`key` length 0, `value` null), 56 bytes of
/// `RecordHeader` each. The review's 28x vector.
fn record_batch_with_empty_headers() -> Bytes {
    let batch = RecordBatch {
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: Records::Plain(vec![Record {
            headers: vec![RecordHeader::default(); INPUT_BYTES / 2],
            ..Record::default()
        }]),
        ..RecordBatch::default()
    };
    let mut buf = BytesMut::new();
    encode_set(&mut buf, std::slice::from_ref(&batch)).expect("encodes");
    buf.freeze()
}

/// A flexible v0 heartbeat subscribing to a great many topics, each
/// named "": 1 wire byte each, 24 bytes of `String` each. The 24x
/// vector.
fn heartbeat_with_empty_topic_names() -> Bytes {
    let mut req = ConsumerGroupHeartbeatRequest::default();
    req.subscribed_topic_names = Some(vec![String::new(); INPUT_BYTES]);
    let mut buf = BytesMut::new();
    req.encode(&mut buf, 0).expect("encodes");
    buf.freeze()
}

/// A tagged-field section of empty tags, strictly ascending — which is
/// now the only form that reaches the loop at all, and the reason this
/// vector no longer amplifies the way the review measured. Distinct tags
/// need ever-wider varints, so the wire cost per element rises with the
/// count; the 20x of a repeated tag is unreachable. See
/// [`the_cheapest_tagged_field_vector_is_now_a_wire_violation`].
fn ascending_empty_tagged_fields() -> Bytes {
    let mut buf = BytesMut::new();
    let count = INPUT_BYTES / 4;
    wire::put_unsigned_varint(&mut buf, count as u64);
    for tag in 0..count {
        wire::put_unsigned_varint(&mut buf, tag as u64);
        wire::put_unsigned_varint(&mut buf, 0);
    }
    buf.freeze()
}

/// A v4 fetch response of empty topic responses: 6 wire bytes each (an
/// empty classic string and an empty partition array), 88 bytes of
/// `FetchableTopicResponse` each. The 14.7x vector.
fn fetch_response_with_empty_topics() -> Bytes {
    let mut resp = FetchResponse::default();
    resp.responses = vec![FetchableTopicResponse::default(); INPUT_BYTES / 6];
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, 4).expect("encodes");
    buf.freeze()
}

type Decoder = fn(&Bytes, Limits) -> Result<(), DecodeError>;

fn decode_record_batch(wire: &Bytes, limits: Limits) -> Result<(), DecodeError> {
    RecordBatch::decode_with_limits(&mut wire.clone(), limits).map(drop)
}

fn decode_heartbeat(wire: &Bytes, limits: Limits) -> Result<(), DecodeError> {
    ConsumerGroupHeartbeatRequest::decode_with_limits(&mut wire.clone(), 0, limits).map(drop)
}

fn decode_tagged_fields(wire: &Bytes, limits: Limits) -> Result<(), DecodeError> {
    let mut budget = limits.budget(wire.len());
    wire::get_tagged_fields_with_budget(&mut wire.clone(), &mut budget).map(drop)
}

fn decode_fetch_response(wire: &Bytes, limits: Limits) -> Result<(), DecodeError> {
    FetchResponse::decode_with_limits(&mut wire.clone(), 4, limits).map(drop)
}

#[test]
fn hostile_counts_cannot_outspend_their_input() {
    // `over_budget` says whether this shape still wants more than the
    // default allows. Three of the four do and are refused; the
    // tagged-field one is now held down by the ordering rule instead,
    // and must come in under the bound on its own.
    let vectors: [(&str, Bytes, Decoder, bool); 4] = [
        (
            "RecordBatch, one record of empty headers",
            record_batch_with_empty_headers(),
            decode_record_batch,
            true,
        ),
        (
            "ConsumerGroupHeartbeatRequest v0, empty topic names",
            heartbeat_with_empty_topic_names(),
            decode_heartbeat,
            true,
        ),
        (
            "wire::get_tagged_fields, empty ascending tags",
            ascending_empty_tagged_fields(),
            decode_tagged_fields,
            false,
        ),
        (
            "FetchResponse v4, empty topic responses",
            fetch_response_with_empty_topics(),
            decode_fetch_response,
            true,
        ),
    ];

    for (name, wire_bytes, decode, over_budget) in vectors {
        let input = wire_bytes.len();
        let bound = Limits::default().alloc_bytes_for(input);

        // Control: what the shape reaches with the bound switched off.
        let (unbounded, unbounded_peak) = peak_heap_of(|| decode(&wire_bytes, Limits::UNLIMITED));
        // And under the bound every caller gets without asking.
        let (bounded, bounded_peak) = peak_heap_of(|| decode(&wire_bytes, Limits::default()));

        println!(
            "{name}\n    {input} wire bytes; unbounded peak {unbounded_peak} ({:.1}x), \
             bounded peak {bounded_peak} ({:.1}x), bound {bound} ({:.1}x)",
            unbounded_peak as f64 / input as f64,
            bounded_peak as f64 / input as f64,
            bound as f64 / input as f64,
        );

        assert!(
            unbounded.is_ok(),
            "{name}: the vector must be well-formed, or it proves nothing ({unbounded:?})"
        );
        assert!(
            bounded_peak <= bound,
            "{name}: peaked at {bounded_peak} bytes on {input} bytes of input, over the \
             documented bound of {bound}"
        );
        if over_budget {
            assert!(
                unbounded_peak > bound,
                "{name}: reached only {unbounded_peak} bytes unbounded, already inside the \
                 {bound}-byte bound — this vector no longer shows the amplification it was \
                 written for"
            );
            assert!(
                matches!(bounded, Err(DecodeError::AllocationLimit { .. })),
                "{name}: expected the budget to refuse this, got {bounded:?}"
            );
        } else {
            assert!(
                bounded.is_ok(),
                "{name}: expected this to fit within the default bound, got {bounded:?}"
            );
        }
    }
}

#[test]
fn the_cheapest_tagged_field_vector_is_now_a_wire_violation() {
    // The cheapest tagged-field amplification was a repeated tag: one
    // byte of tag varint however many times it appears, so the count
    // never has to pay for distinctness. Ascending order makes that a
    // wire violation, refused before the first allocation.
    let mut buf = BytesMut::new();
    wire::put_unsigned_varint(&mut buf, 1_000_000);
    for _ in 0..1_000_000u32 {
        wire::put_unsigned_varint(&mut buf, 0); // tag 0, again and again
        wire::put_unsigned_varint(&mut buf, 0); // with no payload
    }
    let wire_bytes = buf.freeze();

    let (result, peak) = peak_heap_of(|| {
        let mut budget = Limits::default().budget(wire_bytes.len());
        wire::get_tagged_fields_with_budget(&mut wire_bytes.clone(), &mut budget)
    });
    assert_eq!(
        result,
        Err(DecodeError::TaggedFieldOrder {
            previous: 0,
            tag: 0
        })
    );
    assert!(
        peak < 1024,
        "a rejected section still allocated {peak} bytes"
    );
}

#[test]
fn a_frame_at_the_protocol_ceiling_is_capped_absolutely() {
    // The regime that matters under attack: past 16 MiB of input the
    // factor stops growing the budget and the absolute cap takes over,
    // so the worst a 64 MiB frame can reach is ~5x rather than 28x.
    let limits = Limits::default();
    let frame = odradek_protocol::frame::DEFAULT_MAX_FRAME;
    assert_eq!(limits.alloc_bytes_for(frame), 4 * frame);
    assert!(limits.alloc_bytes_for(frame) < 16 * frame);
}

#[test]
fn the_bound_is_policy_not_a_wire_rule() {
    // The same bytes the default refuses decode under a wider factor:
    // what the budget enforces is a deployment choice, and a caller who
    // really does expect dense tiny elements can say so.
    let wire_bytes = fetch_response_with_empty_topics();
    assert!(matches!(
        FetchResponse::decode_with_limits(&mut wire_bytes.clone(), 4, Limits::default()),
        Err(DecodeError::AllocationLimit { .. })
    ));
    let roomy = Limits::default()
        .with_alloc_factor(128)
        .with_max_alloc_bytes(usize::MAX);
    let decoded = FetchResponse::decode_with_limits(&mut wire_bytes.clone(), 4, roomy)
        .expect("decodes with room to spare");
    assert_eq!(decoded.responses.len(), INPUT_BYTES / 6);
}

#[test]
fn an_absolute_ceiling_holds_whatever_the_input_says() {
    // What a server wants: a number per connection, not a multiple of
    // whatever the peer chose to send.
    let wire_bytes = heartbeat_with_empty_topic_names();
    let capped = Limits::default().with_max_alloc_bytes(64 << 10);
    let (result, peak) = peak_heap_of(|| {
        ConsumerGroupHeartbeatRequest::decode_with_limits(&mut wire_bytes.clone(), 0, capped)
    });
    assert!(matches!(result, Err(DecodeError::AllocationLimit { .. })));
    assert!(
        peak <= 64 << 10,
        "peaked at {peak} bytes against a 64 KiB ceiling"
    );
}

#[test]
fn well_formed_messages_still_decode_under_the_default() {
    // The bound must not cost real traffic. A consumer polling 64 topics
    // of 16 idle partitions is the worst-amplifying *ordinary* shape
    // measured (7.7x), and it has to keep working.
    let mut resp = FetchResponse::default();
    resp.responses = (0..64)
        .map(|t| {
            let mut topic = FetchableTopicResponse::default();
            topic.topic = format!("events-topic-{t}");
            topic.partitions = (0..16)
                .map(|p| {
                    let mut partition = PartitionData::default();
                    partition.partition_index = p;
                    partition.high_watermark = 1_000;
                    partition
                })
                .collect();
            topic
        })
        .collect();
    let mut buf = BytesMut::new();
    resp.encode(&mut buf, 4).expect("encodes");
    let wire_bytes = buf.freeze();

    let decoded =
        FetchResponse::decode(&mut wire_bytes.clone(), 4).expect("a real response decodes");
    assert_eq!(decoded.responses.len(), 64);
    assert_eq!(decoded.responses[0].partitions.len(), 16);
    assert_eq!(decoded, resp, "round-trip under the default bound");
}

#[test]
fn nesting_does_not_multiply_the_bound() {
    // A record set decoded against a budget already mostly spent has
    // the remainder, not a fresh allowance: nesting shares one pool.
    let set = record_batch_with_empty_headers();
    let mut budget = Budget::new(1 << 20);
    budget.charge((1 << 20) - 4096).expect("fits");
    assert!(matches!(
        decode_set_with_budget(&mut set.clone(), &mut budget),
        Err(DecodeError::AllocationLimit { .. })
    ));
    assert!(budget.used() <= budget.limit());
}
