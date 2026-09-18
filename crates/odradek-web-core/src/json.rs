//! The JSON view of an [`Event`], shared by every transport.
//!
//! Keys, values, and header values become UTF-8 strings when they are
//! valid UTF-8 (`key`, `value`), else base64 (`key_base64`,
//! `value_base64`) — web-friendly without lying about binary data.
//!
//! The wire path is [`event_json_bytes`]: it serializes an event
//! straight into a byte buffer, with no `serde_json::Value` in between.
//! Transports do not call it directly — [`crate::event::SharedEvent::json`] renders
//! once per *event* and every subscriber reuses that buffer.
//! [`event_json`] builds the same object as a `serde_json::Value`, for
//! callers that want to inspect or reshape it.

use base64::Engine as _;
use bytes::Bytes;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::event::Event;

/// One event as the JSON object a web client receives.
pub fn event_json(event: &Event) -> serde_json::Value {
    serde_json::to_value(EventJson(event)).expect("event json is representable")
}

/// One event as the JSON bytes a web client receives — the rendering
/// [`crate::event::SharedEvent::json`] caches.
pub fn event_json_bytes(event: &Event) -> Bytes {
    Bytes::from(serde_json::to_vec(&EventJson(event)).expect("event json is representable"))
}

/// An [`Event`] in its wire shape; the single definition of the JSON
/// contract, used for both the `Value` and the byte rendering.
struct EventJson<'a>(&'a Event);

impl Serialize for EventJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let event = self.0;
        let len = 4
            + usize::from(event.key.is_some())
            + usize::from(event.value.is_some())
            + usize::from(!event.headers.is_empty());
        let mut object = serializer.serialize_map(Some(len))?;
        object.serialize_entry("topic", &event.topic)?;
        object.serialize_entry("partition", &event.partition)?;
        object.serialize_entry("offset", &event.offset)?;
        object.serialize_entry("timestamp", &event.timestamp)?;
        if let Some(key) = &event.key {
            let encoded = Encoded::of(key);
            object.serialize_entry(KEY.name(&encoded), &encoded)?;
        }
        if let Some(value) = &event.value {
            let encoded = Encoded::of(value);
            object.serialize_entry(VALUE.name(&encoded), &encoded)?;
        }
        if !event.headers.is_empty() {
            object.serialize_entry("headers", &Headers(&event.headers))?;
        }
        object.end()
    }
}

struct Headers<'a>(&'a [(String, Option<Bytes>)]);

impl Serialize for Headers<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut list = serializer.serialize_seq(Some(self.0.len()))?;
        for (name, value) in self.0 {
            list.serialize_element(&Header { name, value })?;
        }
        list.end()
    }
}

struct Header<'a> {
    name: &'a str,
    value: &'a Option<Bytes>,
}

impl Serialize for Header<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut object = serializer.serialize_map(Some(1 + usize::from(self.value.is_some())))?;
        object.serialize_entry("name", self.name)?;
        if let Some(value) = self.value {
            let encoded = Encoded::of(value);
            object.serialize_entry(VALUE.name(&encoded), &encoded)?;
        }
        object.end()
    }
}

/// A byte string as JSON: the text itself when it is valid UTF-8,
/// base64 otherwise. Which one it is also decides the field name, so
/// the two travel together.
enum Encoded<'a> {
    Text(&'a str),
    Base64(String),
}

impl Encoded<'_> {
    fn of(bytes: &Bytes) -> Encoded<'_> {
        match std::str::from_utf8(bytes) {
            Ok(text) => Encoded::Text(text),
            Err(_) => Encoded::Base64(base64::engine::general_purpose::STANDARD.encode(bytes)),
        }
    }
}

impl Serialize for Encoded<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Encoded::Text(text) => serializer.serialize_str(text),
            Encoded::Base64(text) => serializer.serialize_str(text),
        }
    }
}

/// The pair of field names one byte string can land under — plain for
/// UTF-8, `_base64` for binary. Both are literals: naming a field costs
/// nothing.
struct Field {
    text: &'static str,
    base64: &'static str,
}

impl Field {
    fn name(&self, encoded: &Encoded<'_>) -> &'static str {
        match encoded {
            Encoded::Text(_) => self.text,
            Encoded::Base64(_) => self.base64,
        }
    }
}

const KEY: Field = Field {
    text: "key",
    base64: "key_base64",
};
const VALUE: Field = Field {
    text: "value",
    base64: "value_base64",
};

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Event {
        Event {
            topic: "t".into(),
            partition: 0,
            offset: 7,
            timestamp: 1,
            key: Some(Bytes::from_static(b"plain")),
            value: Some(Bytes::from_static(&[0xff, 0xfe])),
            headers: vec![("h".into(), Some(Bytes::from_static(b"v")))],
        }
    }

    #[test]
    fn utf8_stays_text_and_binary_goes_base64() {
        let json = event_json(&sample());
        assert_eq!(json["key"], "plain");
        assert_eq!(json["value_base64"], "//4=");
        assert_eq!(json["headers"][0]["name"], "h");
        assert_eq!(json["headers"][0]["value"], "v");
        assert_eq!(json["offset"], 7);
    }

    /// The byte rendering and the `Value` rendering are the same object:
    /// one contract, two ways out.
    #[test]
    fn bytes_and_value_renderings_agree() {
        let event = sample();
        let from_bytes: serde_json::Value =
            serde_json::from_slice(&event_json_bytes(&event)).unwrap();
        assert_eq!(from_bytes, event_json(&event));
    }

    /// A binary header value moves to `value_base64`, like a record's.
    #[test]
    fn binary_header_values_go_base64() {
        let mut event = sample();
        event.headers = vec![
            ("bin".into(), Some(Bytes::from_static(&[0xff]))),
            ("none".into(), None),
        ];
        let json = event_json(&event);
        assert_eq!(json["headers"][0]["value_base64"], "/w==");
        assert_eq!(json["headers"][1]["name"], "none");
        assert!(json["headers"][1].get("value").is_none());
    }
}
