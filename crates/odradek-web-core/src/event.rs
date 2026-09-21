//! The web-shaped view of a Kafka record, and how subscribers select
//! where to start and what to see.

use std::ops::Deref;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

/// One record, self-describing enough to resume from: a transport mints
/// its resume token out of this offset (subscribe again at
/// `Position::After(offset)`) and hands the client something to echo
/// back. Minting is the server's side of that contract; clients are
/// told only "send this back", never how it is built — and because the
/// token is the offset itself rather than the one after it, minting is
/// a copy rather than a calculation.
///
/// # Construction
///
/// [`Event::at`] takes the four values a record cannot be without and
/// leaves the rest to assignment:
///
/// ```
/// # use odradek_web_core::Event;
/// let mut event = Event::at("orders", 0, 17, 1_700_000_000_000);
/// event.value = Some(b"{}".as_slice().into());
/// ```
///
/// There is a constructor rather than a struct literal because this
/// type is `#[non_exhaustive]`, and that is deliberate: `Event` is what
/// crosses the [`RecordSource`](crate::RecordSource) boundary in both
/// directions, so every implementor downstream builds one. A Kafka
/// record carries more than is exposed here — the leader epoch, the
/// timestamp type — and pulling any of it up later must not be a major
/// version for everyone who wrote an adapter.
///
/// `Default` would have been the cheaper way to allow construction and
/// is the wrong one: an event at offset 0 of the empty topic is not a
/// sensible default, it is a bug that compiles.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
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

impl Event {
    /// An event at a position, with no key, value, or headers; assign
    /// those after. See the [type docs](Event#construction).
    pub fn at(topic: impl Into<String>, partition: i32, offset: i64, timestamp: i64) -> Event {
        Event {
            topic: topic.into(),
            partition,
            offset,
            timestamp,
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }
}

/// One [`Event`] as every subscriber of a partition sees it: a shared
/// handle, so fanning an event out to N subscribers costs N refcount
/// bumps rather than N deep copies of its topic, key, value, and
/// headers.
///
/// It derefs to the [`Event`], so `event.offset` and friends read the
/// same as before. Its JSON rendering ([`SharedEvent::json`]) is
/// computed by whichever subscriber gets there first and reused by the
/// rest — the wire form of one event is built once, not once per
/// subscriber.
#[derive(Debug, Clone)]
pub struct SharedEvent(Arc<Shared>);

#[derive(Debug)]
struct Shared {
    event: Event,
    json: OnceLock<Bytes>,
}

impl SharedEvent {
    pub fn new(event: Event) -> SharedEvent {
        SharedEvent(Arc::new(Shared {
            event,
            json: OnceLock::new(),
        }))
    }

    /// The event itself.
    pub fn event(&self) -> &Event {
        &self.0.event
    }

    /// The event's JSON body (the shape [`event_json`](crate::json::event_json)
    /// describes), rendered on first use and shared from then on.
    pub fn json(&self) -> &str {
        // Rendered by `serde_json`, so this is UTF-8 by construction.
        std::str::from_utf8(self.json_bytes()).expect("rendered json is utf-8")
    }

    /// The same rendering as raw bytes, for transports that can send a
    /// shared buffer without copying it.
    pub fn json_bytes(&self) -> &Bytes {
        self.0
            .json
            .get_or_init(|| crate::json::event_json_bytes(&self.0.event))
    }
}

impl Deref for SharedEvent {
    type Target = Event;

    fn deref(&self) -> &Event {
        &self.0.event
    }
}

impl From<Event> for SharedEvent {
    fn from(event: Event) -> SharedEvent {
        SharedEvent::new(event)
    }
}

/// Two handles are equal when their events are: the cached rendering is
/// derived state, never identity.
impl PartialEq for SharedEvent {
    fn eq(&self, other: &SharedEvent) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0.event == other.0.event
    }
}

impl Eq for SharedEvent {}

/// Where a subscription starts.
///
/// [`After`](Position::After) is exclusive, like every position this
/// crate handles: it names the last event the subscriber received, and
/// the stream resumes with whatever follows it. A client therefore
/// echoes back the offset it last saw, unmodified — it never has to
/// know what the next one would be called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Position {
    /// The oldest available record (full replay).
    Earliest,
    /// Only records produced from now on.
    Latest,
    /// Everything after this offset — the resume path.
    After(i64),
}

/// Where a topic-level subscription starts, across all partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopicPosition {
    /// Every partition from its oldest record.
    Earliest,
    /// Every partition from now on.
    Latest,
    /// Resume: the last offset seen per partition, each stream
    /// continuing after it. A partition absent from the map replays
    /// from earliest — loss-free beats duplicate-free, so consumers
    /// should be idempotent.
    After(std::collections::BTreeMap<i32, i64>),
}

/// A per-subscriber selection over the partition's records. Empty
/// matches everything.
///
/// [`matches`](Filter::matches) runs on the pump loop, once per event
/// per subscriber, before the [`SharedEvent`] clone — so the cheap case
/// is an uninterested subscriber costing one comparison. Every variant
/// here is therefore O(1) in record size: prefixes and header values
/// are compared as bytes, never parsed. Anything that must *understand*
/// a value (JSON path, schema registry) is an unbounded parse on the
/// live path, and it would not even fail loudly — backpressure would
/// dutifully demote every subscriber into permanent catch-up, and the
/// bridge would look slow rather than broken.
///
/// # Construction
///
/// Build from [`Default`] and assign, rather than by struct literal:
///
/// ```
/// # use odradek_web_core::Filter;
/// let mut filter = Filter::default();
/// filter.key_prefix = Some(b"tenant-7/".as_slice().into());
/// ```
///
/// The type is `#[non_exhaustive]` to keep room for one specific future
/// field: a caller-supplied predicate. Filtering is the request this
/// crate expects to refuse most often — JSON paths, expression
/// languages, anything that must parse a value — and the answer is
/// meant to be an escape hatch rather than a feature: hand us your own
/// closure, own its cost. Adding that field to a struct downstream code
/// could build by literal would be a breaking change, so the room is
/// reserved before publication rather than after. The `Debug`,
/// `PartialEq`, and `Eq` derives here do not stand in the way of that —
/// a closure satisfies none of them, but replacing a derive with a
/// hand-written impl (comparing predicates by `Arc::ptr_eq`) is not a
/// breaking change the way struct-literal construction is.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
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

    /// The point of the handle: clones share one buffer, so the JSON is
    /// rendered for the first subscriber that asks and nobody else.
    #[test]
    fn clones_share_one_rendering() {
        let first = SharedEvent::new(event(Some(b"k"), Vec::new()));
        let second = first.clone();
        assert_eq!(first.json(), second.json());
        assert!(
            std::ptr::eq(first.json_bytes(), second.json_bytes()),
            "each clone rendered its own copy"
        );
        assert_eq!(first.offset, 0, "deref reaches the event's fields");
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
