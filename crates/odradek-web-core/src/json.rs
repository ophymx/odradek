//! The JSON view of an [`Event`], shared by every transport.
//!
//! Keys, values, and header values become UTF-8 strings when they are
//! valid UTF-8 (`key`, `value`), else base64 (`key_base64`,
//! `value_base64`) — web-friendly without lying about binary data.

use base64::Engine as _;
use bytes::Bytes;

use crate::event::Event;

/// One event as the JSON object a web client receives.
pub fn event_json(event: &Event) -> serde_json::Value {
    let mut body = serde_json::json!({
        "topic": event.topic,
        "partition": event.partition,
        "offset": event.offset,
        "timestamp": event.timestamp,
    });
    let object = body.as_object_mut().expect("literal object");
    if let Some(key) = &event.key {
        let (field, value) = utf8_or_base64("key", key);
        object.insert(field, value);
    }
    if let Some(value) = &event.value {
        let (field, json) = utf8_or_base64("value", value);
        object.insert(field, json);
    }
    if !event.headers.is_empty() {
        let headers: Vec<serde_json::Value> = event
            .headers
            .iter()
            .map(|(name, value)| {
                let mut h = serde_json::json!({ "name": name });
                if let Some(value) = value {
                    let (field, json) = utf8_or_base64("value", value);
                    h.as_object_mut()
                        .expect("literal object")
                        .insert(field, json);
                }
                h
            })
            .collect();
        object.insert("headers".into(), headers.into());
    }
    body
}

/// UTF-8 as a plain string under `name`, anything else as base64 under
/// `name_base64`.
fn utf8_or_base64(name: &str, bytes: &Bytes) -> (String, serde_json::Value) {
    match std::str::from_utf8(bytes) {
        Ok(s) => (name.to_owned(), s.into()),
        Err(_) => (
            format!("{name}_base64"),
            base64::engine::general_purpose::STANDARD
                .encode(bytes)
                .into(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_stays_text_and_binary_goes_base64() {
        let event = Event {
            topic: "t".into(),
            partition: 0,
            offset: 7,
            timestamp: 1,
            key: Some(Bytes::from_static(b"plain")),
            value: Some(Bytes::from_static(&[0xff, 0xfe])),
            headers: vec![("h".into(), Some(Bytes::from_static(b"v")))],
        };
        let json = event_json(&event);
        assert_eq!(json["key"], "plain");
        assert_eq!(json["value_base64"], "//4=");
        assert_eq!(json["headers"][0]["name"], "h");
        assert_eq!(json["headers"][0]["value"], "v");
        assert_eq!(json["offset"], 7);
    }
}
