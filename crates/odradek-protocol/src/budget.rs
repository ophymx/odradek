//! The decode allocation budget: what stops a small hostile frame from
//! becoming a large hostile allocation.
//!
//! Every count-prefixed array on the wire is an instruction to allocate.
//! The counts are attacker-controlled and the decoded elements are much
//! larger than the bytes that summon them — a record header is two bytes
//! on the wire and 56 in memory, a `RawTaggedField` two and 40, an empty
//! compact string one and 24. Left alone, a 64 MiB frame (the
//! [`frame::DEFAULT_MAX_FRAME`](crate::frame::DEFAULT_MAX_FRAME)
//! ceiling) decodes into gigabytes: 1.75 GiB live and 2.00 GiB peak for
//! a single record with 33.5 million empty headers, 28x the input,
//! reachable from either end of a connection.
//!
//! The fix is not `with_capacity(wire_count)` — that hands the attacker
//! the allocator directly, and is a far worse bug than the loop it
//! replaces. It is to spend against a budget derived from how many bytes
//! the peer actually sent.
//!
//! # The bound
//!
//! A decode charges the budget for the *peak* bytes its collections
//! occupy, growth transients included: when a `Vec` grows, the old block
//! is still mapped while the elements are copied into the new one, so
//! both are charged for that moment. Nothing is amortized away.
//!
//! With the default [`Limits`], decoding *n* input bytes therefore
//! allocates at most
//!
//! - `clamp(16 * n, 64 KiB, 256 MiB)` bytes of collections, at any
//!   instant, and
//! - *n* bytes of `String` contents, which are copied 1:1 out of the
//!   wire (every other payload — `bytes`, `records`, tagged-field data —
//!   is a refcounted slice of the caller's buffer and allocates
//!   nothing, see [`crate`] docs on zero-copy decoding).
//!
//! Two regimes, and the absolute one is what matters under attack. At
//! the 64 MiB [`frame::DEFAULT_MAX_FRAME`](crate::frame::DEFAULT_MAX_FRAME)
//! ceiling the 256 MiB cap binds, so **a hostile frame now peaks at
//! ~5x its bytes instead of 28x — 320 MiB rather than 2.00 GiB.**
//! Below 16 MiB of input the factor binds instead, and 16x of a small
//! frame is a small number. Lower `max_len` at
//! [`frame::try_split`](crate::frame::try_split) and both regimes
//! tighten with it.
//!
//! # Why 16, and not less
//!
//! This is the honest tension in the fix, so it is written down rather
//! than buried in a constant. The decoded form of a Kafka message is
//! genuinely much larger than its wire form, and the worst ratios belong
//! to *ordinary* traffic, not just to attacks. Measured here:
//!
//! | Message | Decoded peak / wire bytes |
//! |---|---|
//! | FetchResponse v12, 200 topics x 1 empty partition | 8.1x |
//! | FetchResponse v12, 8 topics x 100 empty partitions | 7.8x |
//! | FetchResponse v4, 64 topics x 16 empty partitions | 7.7x |
//! | Record set, 32k records with 8-byte values | 5.7x |
//! | MetadataResponse, 50 topics x 32 partitions | 3.5x |
//! | FetchResponse carrying 40-byte records | 0.1x |
//!
//! A `PartitionData` is 232 bytes for the ~28 wire bytes an empty
//! partition costs; a `Record` is 104 for a 7-byte minimum. So a bound
//! anywhere near the amplification an attacker gets would also reject a
//! consumer polling 200 idle partitions. Sixteen clears the worst
//! measured real shape by 2x and still cuts the hostile cases below the
//! cap; the 256 MiB ceiling is what actually bounds the damage at frame
//! scale, and is the knob a server should reach for first.
//!
//! Tune it per position, which is the point of it being a parameter:
//!
//! ```
//! use odradek_protocol::budget::Limits;
//! use odradek_protocol::messages::fetch_request::FetchRequest;
//!
//! // A server parsing requests it never asked for: hard ceiling,
//! // whatever length prefix the peer chose.
//! let strict = Limits::default().with_max_alloc_bytes(8 << 20);
//! let mut buf = bytes::Bytes::from_static(&[0, 0, 0, 0]);
//! let _ = FetchRequest::decode_with_limits(&mut buf, 0, strict);
//! ```

use crate::error::DecodeError;

/// Bytes of collection storage a decode may allocate per input byte,
/// when [`Limits`] is left at its default. See the module docs for why
/// this is 16 and not something braver.
pub const DEFAULT_ALLOC_FACTOR: usize = 16;

/// Floor under the derived budget, so that a small message whose nested
/// structs outweigh its wire form still decodes.
pub const DEFAULT_MIN_ALLOC_BYTES: usize = 64 << 10;

/// Ceiling on the derived budget, whatever the input size: four times
/// [`frame::DEFAULT_MAX_FRAME`](crate::frame::DEFAULT_MAX_FRAME), which
/// covers a full-size fetch response of real records (measured at 2.5x
/// its wire bytes) with room over, and is what caps a hostile frame.
pub const DEFAULT_MAX_ALLOC_BYTES: usize = 4 * crate::frame::DEFAULT_MAX_FRAME;

/// How much a decode may allocate, as a function of how much it was
/// given to decode.
///
/// The budget for an input of *n* bytes is
/// `clamp(n * alloc_factor, min_alloc_bytes, max_alloc_bytes)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    alloc_factor: usize,
    min_alloc_bytes: usize,
    max_alloc_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            alloc_factor: DEFAULT_ALLOC_FACTOR,
            min_alloc_bytes: DEFAULT_MIN_ALLOC_BYTES,
            max_alloc_bytes: DEFAULT_MAX_ALLOC_BYTES,
        }
    }
}

impl Limits {
    /// Limits that enforce nothing.
    ///
    /// For inputs that are trusted by construction — a buffer this
    /// process encoded, a fixture in a test. Never for a socket.
    pub const UNLIMITED: Limits = Limits {
        alloc_factor: usize::MAX,
        min_alloc_bytes: usize::MAX,
        max_alloc_bytes: usize::MAX,
    };

    /// Bytes of collection storage allowed per input byte.
    #[must_use]
    pub fn with_alloc_factor(mut self, factor: usize) -> Limits {
        self.alloc_factor = factor;
        self
    }

    /// Floor under the derived budget, for inputs too small to pay for
    /// their own decoded shape.
    #[must_use]
    pub fn with_min_alloc_bytes(mut self, bytes: usize) -> Limits {
        self.min_alloc_bytes = bytes;
        self
    }

    /// Absolute ceiling, whatever the input size — what a server that
    /// wants a hard per-connection number should set.
    #[must_use]
    pub fn with_max_alloc_bytes(mut self, bytes: usize) -> Limits {
        self.max_alloc_bytes = bytes;
        self
    }

    /// The byte budget these limits give an `input_len`-byte input.
    #[must_use]
    pub fn alloc_bytes_for(&self, input_len: usize) -> usize {
        input_len
            .saturating_mul(self.alloc_factor)
            .max(self.min_alloc_bytes)
            .min(self.max_alloc_bytes)
    }

    /// A fresh [`Budget`] for an `input_len`-byte input.
    #[must_use]
    pub fn budget(&self, input_len: usize) -> Budget {
        Budget::new(self.alloc_bytes_for(input_len))
    }
}

/// A decode's remaining allowance, spent as collections grow.
///
/// Created from [`Limits::budget`] and threaded through one decode, so
/// that a message's arrays, its nested structs' arrays, and its
/// tagged-field sections all draw on the same pool: nesting cannot
/// multiply the bound.
#[derive(Debug)]
pub struct Budget {
    limit: usize,
    used: usize,
}

impl Budget {
    /// A budget of `max_alloc_bytes` peak collection bytes.
    #[must_use]
    pub fn new(max_alloc_bytes: usize) -> Budget {
        Budget {
            limit: max_alloc_bytes,
            used: 0,
        }
    }

    /// The ceiling this budget enforces.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Collection bytes currently charged against it.
    #[must_use]
    pub fn used(&self) -> usize {
        self.used
    }

    /// Charge `bytes` of allocation, for a decoder building a collection
    /// this module does not know about.
    pub fn charge(&mut self, bytes: usize) -> Result<(), DecodeError> {
        let used = self.used.saturating_add(bytes);
        self.check(used)?;
        self.used = used;
        Ok(())
    }

    /// Release `bytes` charged earlier, for storage a decoder has since
    /// dropped.
    pub fn release(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
    }

    /// Push `value` onto `items`, charging the budget for any allocation
    /// the push needs *before* making it.
    ///
    /// Capacity is grown here rather than by `Vec::push` so that what is
    /// charged is exactly what is allocated: doubling, so pushes stay
    /// amortized O(1), but through `try_reserve_exact`, so a length the
    /// budget did not approve is never requested — and so that an
    /// allocator refusal is a [`DecodeError`] rather than an abort.
    ///
    /// Note what this is not: capacity never comes from a wire count.
    /// It comes from the elements that have actually arrived.
    ///
    /// This sits in the innermost loop of every array decode, so the
    /// common case — spare capacity, nothing to charge — is one
    /// comparison, and the growth path is outlined so it does not bloat
    /// the caller. (A zero-sized element needs no special case: its
    /// `Vec` reports `usize::MAX` capacity, so the test never fires.)
    #[inline]
    pub fn push<T>(&mut self, items: &mut Vec<T>, value: T) -> Result<(), DecodeError> {
        if items.len() == items.capacity() {
            self.grow_for(items)?;
        }
        items.push(value);
        Ok(())
    }

    /// Charge for, and take, the next capacity step of `items`.
    #[inline(never)]
    fn grow_for<T>(&mut self, items: &mut Vec<T>) -> Result<(), DecodeError> {
        let elem = size_of::<T>();
        if elem == 0 {
            return Ok(());
        }
        let old = items.capacity();
        let target = if old == 0 {
            first_capacity(elem)
        } else {
            old.saturating_mul(2)
        };
        let old_bytes = old.saturating_mul(elem);
        let new_bytes = target.saturating_mul(elem);
        self.grow(old_bytes, new_bytes)?;
        items.try_reserve_exact(target - items.len()).map_err(|_| {
            DecodeError::AllocationFailed {
                bytes: new_bytes,
                limit: self.limit,
            }
        })?;
        // `try_reserve_exact` is permitted to hand back more than asked
        // for; settle the charge against what it actually did.
        self.settle(new_bytes, items.capacity().saturating_mul(elem))
    }

    /// Require that an element decode consumed input.
    ///
    /// `before` and `after` are `Buf::remaining` either side of it. A
    /// count-driven loop whose body can consume nothing is an infinite
    /// loop with an allocation in it; no element of the vendored schemas
    /// is zero-width today, but that is a fact about those schemas and
    /// not about the code that walks them.
    #[inline]
    pub fn progress(&self, before: usize, after: usize) -> Result<(), DecodeError> {
        if after >= before {
            return Err(DecodeError::NoProgress);
        }
        Ok(())
    }

    /// Charge a reallocation from `old_bytes` to `new_bytes`.
    ///
    /// Both blocks are mapped while the elements move between them, so
    /// the moment of the copy — not the steady state after it — is what
    /// the limit is checked against.
    fn grow(&mut self, old_bytes: usize, new_bytes: usize) -> Result<(), DecodeError> {
        let peak = self.used.saturating_add(new_bytes);
        self.check(peak)?;
        self.used = peak.saturating_sub(old_bytes);
        Ok(())
    }

    fn settle(&mut self, charged: usize, actual: usize) -> Result<(), DecodeError> {
        if actual == charged {
            return Ok(());
        }
        let used = self.used.saturating_add(actual).saturating_sub(charged);
        self.check(used)?;
        self.used = used;
        Ok(())
    }

    fn check(&self, wanted: usize) -> Result<(), DecodeError> {
        if wanted > self.limit {
            return Err(DecodeError::AllocationLimit {
                limit: self.limit,
                wanted,
            });
        }
        Ok(())
    }
}

/// `Vec`'s own first non-zero capacity, mirrored so that the budgeted
/// growth path allocates no more eagerly than the path it replaces.
const fn first_capacity(elem: usize) -> usize {
    if elem == 1 {
        8
    } else if elem <= 1024 {
        4
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_scales_with_input_and_respects_floor_and_ceiling() {
        let limits = Limits::default();
        assert_eq!(limits.alloc_bytes_for(1 << 20), 16 << 20);
        // Floor wins for inputs too small to pay for a decoded struct.
        assert_eq!(limits.alloc_bytes_for(16), DEFAULT_MIN_ALLOC_BYTES);
        // Ceiling wins for a frame at the protocol's own size limit:
        // 64 MiB in, 256 MiB of structs out, not 1 GiB.
        assert_eq!(
            limits.alloc_bytes_for(crate::frame::DEFAULT_MAX_FRAME),
            DEFAULT_MAX_ALLOC_BYTES
        );
        let capped = limits.with_max_alloc_bytes(1 << 20);
        assert_eq!(capped.alloc_bytes_for(64 << 20), 1 << 20);
        // No overflow at absurd input lengths.
        assert_eq!(
            limits
                .with_max_alloc_bytes(usize::MAX)
                .alloc_bytes_for(usize::MAX),
            usize::MAX
        );
        assert_eq!(Limits::UNLIMITED.alloc_bytes_for(0), usize::MAX);
    }

    #[test]
    fn push_stops_at_the_limit_rather_than_allocating_past_it() {
        // Room for four 8-byte elements and the growth that reaches
        // them, but not for the next doubling.
        let mut budget = Budget::new(32);
        let mut items: Vec<u64> = Vec::new();
        for i in 0..4u64 {
            budget.push(&mut items, i).expect("within budget");
        }
        assert_eq!(items.len(), 4);
        assert_eq!(budget.used(), 32);
        assert!(matches!(
            budget.push(&mut items, 4),
            Err(DecodeError::AllocationLimit { limit: 32, .. })
        ));
        // The refusal left the vector untouched, not half-grown.
        assert_eq!(items.len(), 4);
        assert_eq!(items.capacity(), 4);
    }

    #[test]
    fn growth_transient_is_charged_not_amortized() {
        // Four u64 fit in 32 bytes, but growing 4 -> 8 needs 32 + 64
        // bytes mapped at once, which a 64-byte budget cannot cover even
        // though the 64-byte result would fit exactly.
        let mut budget = Budget::new(64);
        let mut items: Vec<u64> = Vec::new();
        for i in 0..4u64 {
            budget.push(&mut items, i).expect("within budget");
        }
        assert!(budget.push(&mut items, 4).is_err());
    }

    #[test]
    fn zero_sized_elements_never_charge() {
        let mut budget = Budget::new(0);
        let mut items: Vec<()> = Vec::new();
        for _ in 0..1000 {
            budget
                .push(&mut items, ())
                .expect("no allocation to charge");
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn progress_rejects_a_zero_width_element() {
        let budget = Budget::new(0);
        assert_eq!(budget.progress(10, 9), Ok(()));
        assert_eq!(budget.progress(10, 10), Err(DecodeError::NoProgress));
    }

    #[test]
    fn charge_and_release_are_symmetric() {
        let mut budget = Budget::new(100);
        budget.charge(60).expect("fits");
        assert!(budget.charge(60).is_err());
        budget.release(60);
        assert_eq!(budget.used(), 0);
        budget.release(1); // saturates rather than wrapping
        assert_eq!(budget.used(), 0);
    }
}
