//! The multi-partition resume cursor: `partition:offset` pairs,
//! comma-separated, sorted by partition — `0:17,1:4,2:9`.
//!
//! A topic-level stream interleaves partitions, so one offset cannot
//! name a position; the cursor carries the last offset *seen* in each
//! partition, and each stream resumes after it. Transports put it in
//! every event's id (SSE) or accept it as `from=` (WebSocket), so a
//! reconnecting client resumes loss-free — echoing back exactly the
//! pairs it was given, never a number it had to work out.

use std::collections::BTreeMap;
use std::fmt::Write;

/// The bytes one `partition:offset` pair can take: two decimal i64s, a
/// colon, and the separator.
const PAIR_BYTES: usize = 44;

/// Render a cursor (deterministic order: sorted by partition).
///
/// A topic stream re-renders this per event, so it writes the pairs
/// into one buffer rather than allocating a `String` per pair.
pub fn encode(cursor: &BTreeMap<i32, i64>) -> String {
    let mut out = String::with_capacity(cursor.len() * PAIR_BYTES);
    for (partition, offset) in cursor {
        if !out.is_empty() {
            out.push(',');
        }
        // Writing to a String cannot fail.
        let _ = write!(out, "{partition}:{offset}");
    }
    out
}

/// Parse a cursor produced by [`encode`].
pub fn parse(raw: &str) -> Result<BTreeMap<i32, i64>, String> {
    let mut cursor = BTreeMap::new();
    for pair in raw.split(',') {
        let (partition, offset) = pair
            .split_once(':')
            .ok_or_else(|| format!("cursor pair {pair:?} is not partition:offset"))?;
        let partition = partition
            .trim()
            .parse()
            .map_err(|_| format!("bad partition in cursor pair {pair:?}"))?;
        let offset = offset
            .trim()
            .parse()
            .map_err(|_| format!("bad offset in cursor pair {pair:?}"))?;
        cursor.insert(partition, offset);
    }
    Ok(cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_sorted() {
        let mut cursor = BTreeMap::new();
        cursor.insert(2, 9);
        cursor.insert(0, 17);
        let encoded = encode(&cursor);
        assert_eq!(encoded, "0:17,2:9");
        assert_eq!(parse(&encoded).unwrap(), cursor);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse("0:1,nope").is_err());
        assert!(parse("a:b").is_err());
    }
}
