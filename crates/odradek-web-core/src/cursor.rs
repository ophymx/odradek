//! The multi-partition resume cursor: `partition:next_offset` pairs,
//! comma-separated, sorted by partition — `0:17,1:4,2:9`.
//!
//! A topic-level stream interleaves partitions, so one offset cannot
//! name a position; the cursor carries the next offset owed per
//! partition. Transports put it in every event's id (SSE) or accept it
//! as `from=` (WebSocket), so a reconnecting client resumes loss-free.

use std::collections::BTreeMap;

/// Render a cursor (deterministic order: sorted by partition).
pub fn encode(cursor: &BTreeMap<i32, i64>) -> String {
    let mut out = String::new();
    for (partition, next_offset) in cursor {
        if !out.is_empty() {
            out.push(',');
        }
        out.push_str(&format!("{partition}:{next_offset}"));
    }
    out
}

/// Parse a cursor produced by [`encode`].
pub fn parse(raw: &str) -> Result<BTreeMap<i32, i64>, String> {
    let mut cursor = BTreeMap::new();
    for pair in raw.split(',') {
        let (partition, next_offset) = pair
            .split_once(':')
            .ok_or_else(|| format!("cursor pair {pair:?} is not partition:offset"))?;
        let partition = partition
            .trim()
            .parse()
            .map_err(|_| format!("bad partition in cursor pair {pair:?}"))?;
        let next_offset = next_offset
            .trim()
            .parse()
            .map_err(|_| format!("bad offset in cursor pair {pair:?}"))?;
        cursor.insert(partition, next_offset);
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
