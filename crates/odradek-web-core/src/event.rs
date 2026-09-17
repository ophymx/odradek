//! The web-shaped view of a Kafka record, and how subscribers select
//! where to start and what to see.

use bytes::Bytes;

/// One record, self-describing enough for a web client to resume from:
/// the offset doubles as a resume token (subscribe again at
/// `Position::Offset(offset + 1)` — the natural `Last-Event-ID`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Milliseconds since epoch.
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

/// Where a subscription starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// The oldest available record (full replay).
    Earliest,
    /// Only records produced from now on.
    Latest,
    /// A specific offset (inclusive) — the resume path.
    Offset(i64),
}

/// A per-subscriber selection over the partition's records. Empty
/// matches everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filter {
    /// Keep only records whose key starts with these bytes.
    pub key_prefix: Option<Bytes>,
    /// Keep only records carrying this header with exactly this value.
    pub header: Option<(String, Bytes)>,
}

impl Filter {
    pub fn matches(&self, event: &Event) -> bool {
        if let Some(prefix) = &self.key_prefix {
            match &event.key {
                Some(key) if key.starts_with(prefix) => {}
                _ => return false,
            }
        }
        if let Some((name, value)) = &self.header {
            let hit = event
                .headers
                .iter()
                .any(|(k, v)| k == name && v.as_ref() == Some(value));
            if !hit {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(key: Option<&'static [u8]>, headers: Vec<(String, Option<Bytes>)>) -> Event {
        Event {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            key: key.map(Bytes::from_static),
            value: None,
            headers,
        }
    }

    #[test]
    fn empty_filter_matches_everything() {
        assert!(Filter::default().matches(&event(None, Vec::new())));
    }

    #[test]
    fn key_prefix_filters() {
        let filter = Filter {
            key_prefix: Some(Bytes::from_static(b"user:")),
            ..Default::default()
        };
        assert!(filter.matches(&event(Some(b"user:42"), Vec::new())));
        assert!(!filter.matches(&event(Some(b"order:42"), Vec::new())));
        assert!(!filter.matches(&event(None, Vec::new())));
    }

    #[test]
    fn header_must_match_exactly() {
        let filter = Filter {
            header: Some(("tenant".into(), Bytes::from_static(b"acme"))),
            ..Default::default()
        };
        let hit = event(
            None,
            vec![("tenant".into(), Some(Bytes::from_static(b"acme")))],
        );
        let miss = event(
            None,
            vec![("tenant".into(), Some(Bytes::from_static(b"other")))],
        );
        assert!(filter.matches(&hit));
        assert!(!filter.matches(&miss));
        assert!(!filter.matches(&event(None, Vec::new())));
    }
}
