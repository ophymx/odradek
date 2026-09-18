//! The zero-copy decode guarantee, enforced.
//!
//! Decoding a fetch response must be O(number of fields), not O(payload
//! bytes): the `records` field and every key/value inside it are
//! refcounted slices of the caller's buffer, never copies. That property
//! is not visible in any signature — it comes from `Bytes`'s override of
//! `Buf::copy_to_bytes` — so nothing but a test stops a refactor from
//! silently reintroducing a copy per record, or a consumer from decoding
//! through a `Buf` that lacks the override (measurably ~370x slower).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use bytes::{Bytes, BytesMut};
use odradek_protocol::messages::fetch_response::{
    FetchResponse, FetchableTopicResponse, PartitionData,
};
use odradek_protocol::records::{Record, RecordBatch, Records, decode_set, encode_set};

/// Counts allocations while armed, so a test can measure exactly the
/// region it cares about.
struct Counting;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ARMED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Run `f`, returning its value and how many allocations it made.
fn allocations_of<T>(f: impl FnOnce() -> T) -> (T, usize) {
    ALLOCS.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let value = f();
    ARMED.store(false, Ordering::Relaxed);
    (value, ALLOCS.load(Ordering::Relaxed))
}

/// A fetch response whose one partition carries `record_count` records
/// of `value_len` bytes each.
fn fetch_response(record_count: usize, value_len: usize) -> Bytes {
    let value = Bytes::from(vec![0xa5u8; value_len]);
    let records: Vec<Record> = (0..record_count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).expect("fits"),
            timestamp_delta: i64::try_from(i).expect("fits"),
            value: Some(value.clone()),
            ..Record::default()
        })
        .collect();
    let batch = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        attributes: 0,
        last_offset_delta: i32::try_from(record_count - 1).expect("fits"),
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: Records::Plain(records),
    };
    let mut set = BytesMut::new();
    encode_set(&mut set, std::slice::from_ref(&batch)).expect("encodes");

    let mut partition = PartitionData::default();
    partition.partition_index = 0;
    partition.records = Some(set.freeze());
    let mut topic = FetchableTopicResponse::default();
    topic.topic = "guard".into();
    topic.partitions = vec![partition];
    let mut response = FetchResponse::default();
    response.responses = vec![topic];
    let mut buf = BytesMut::new();
    response.encode(&mut buf, 12).expect("encodes");
    buf.freeze()
}

#[test]
fn message_decode_does_not_scale_with_payload_size() {
    // Same field count, wildly different payload: if decoding copied the
    // records, the larger response would allocate more.
    let small = fetch_response(8, 64);
    let large = fetch_response(8, 64 << 10);
    assert!(large.len() > small.len() * 100, "payloads differ enough");

    let (_, small_allocs) =
        allocations_of(|| FetchResponse::decode(&mut small.clone(), 12).expect("decodes"));
    let (_, large_allocs) =
        allocations_of(|| FetchResponse::decode(&mut large.clone(), 12).expect("decodes"));

    assert_eq!(
        small_allocs,
        large_allocs,
        "message decode allocated {large_allocs} for a {}-byte response vs {small_allocs} for \
         {} bytes — the records field is being copied instead of sliced",
        large.len(),
        small.len()
    );
}

#[test]
fn record_materialization_allocates_per_record_not_per_byte() {
    // Ten times the payload per record, same record count: materializing
    // must cost the same, because keys and values are slices.
    let thin = fetch_response(64, 32);
    let fat = fetch_response(64, 32 * 10);

    let materialize = |response: &Bytes| {
        let decoded = FetchResponse::decode(&mut response.clone(), 12).expect("decodes");
        let mut raw = decoded.responses[0].partitions[0]
            .records
            .clone()
            .expect("records");
        decode_set(&mut raw).expect("decodes")
    };

    let (thin_batches, thin_allocs) = allocations_of(|| materialize(&thin));
    let (fat_batches, fat_allocs) = allocations_of(|| materialize(&fat));
    assert_eq!(thin_batches.len(), 1);
    assert_eq!(fat_batches.len(), 1);

    assert_eq!(
        thin_allocs, fat_allocs,
        "materializing 64 records cost {fat_allocs} allocations at 320-byte values vs \
         {thin_allocs} at 32-byte values — record payloads are being copied"
    );
}

#[test]
fn decoded_payloads_alias_the_input_buffer() {
    // The strongest form of the guarantee: the bytes a caller gets back
    // point *into* the buffer it passed, with no copy anywhere.
    let response = fetch_response(4, 512);
    let base = response.as_ptr() as usize;
    let end = base + response.len();

    let decoded = FetchResponse::decode(&mut response.clone(), 12).expect("decodes");
    let mut raw = decoded.responses[0].partitions[0]
        .records
        .clone()
        .expect("records");
    let within = |bytes: &Bytes| {
        let addr = bytes.as_ptr() as usize;
        addr >= base && addr < end
    };
    assert!(
        within(&raw),
        "the records field was copied out of the response buffer"
    );

    let batches = decode_set(&mut raw).expect("decodes");
    let Records::Plain(records) = &batches[0].records else {
        panic!("expected plain records");
    };
    for record in records {
        let value = record.value.as_ref().expect("value");
        assert!(
            within(value),
            "a record value was copied out of the response buffer"
        );
    }
}
