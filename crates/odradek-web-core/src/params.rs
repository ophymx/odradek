//! The query-parameter grammar every web transport shares.
//!
//! `from=`, `key_prefix=`, and `header=<name>:<value>` are the public
//! contract of the constellation's HTTP-facing streams; both the SSE
//! and WebSocket transports deserialize into [`StreamParams`] so the
//! grammar cannot drift between them. Resume tokens mean "start here"
//! everywhere, and are interchangeable across transports.
//!
//! A resume token is **opaque to the client**: the server mints it, the
//! client echoes it back unchanged. Its contents are this crate's
//! business — today an offset for a partition stream and a
//! `partition:offset,...` cursor for a topic stream, both parsed
//! here. Publishing that shape would invite clients to do arithmetic on
//! it, and every such client would break the day a source's positions
//! stop being integers.

use bytes::Bytes;

use crate::cursor;
use crate::event::{Filter, Position, TopicPosition};

/// The common stream query parameters, ready for `axum::extract::Query`.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[non_exhaustive]
pub struct StreamParams {
    /// `earliest`, `latest`, an offset (partition streams), or a cursor
    /// (topic streams). Default: `latest`.
    pub from: Option<String>,
    /// Only records whose key starts with this UTF-8 prefix.
    pub key_prefix: Option<String>,
    /// `<name>:<value>` — only records carrying this header.
    pub header: Option<String>,
}

impl StreamParams {
    /// The partition-stream start position. A `resume` token (e.g. SSE's
    /// `Last-Event-ID`) wins over `from` and is honoured exactly as the
    /// server minted it — see the [module docs](self) on why its shape
    /// is not part of the client contract.
    pub fn position(&self, resume: Option<&str>) -> Result<Position, String> {
        if let Some(raw) = resume {
            let offset: i64 = raw
                .parse()
                .map_err(|_| "resume token must be an offset".to_owned())?;
            return Ok(Position::After(offset));
        }
        match self.from.as_deref() {
            None | Some("latest") => Ok(Position::Latest),
            Some("earliest") => Ok(Position::Earliest),
            Some(raw) => raw
                .parse()
                .map(Position::After)
                .map_err(|_| format!("from must be earliest, latest, or an offset (got {raw:?})")),
        }
    }

    /// The topic-stream start position. A `resume` cursor wins over
    /// `from`; `from` accepts `earliest`, `latest`, or a cursor.
    pub fn topic_position(&self, resume: Option<&str>) -> Result<TopicPosition, String> {
        if let Some(raw) = resume {
            return Ok(TopicPosition::After(cursor::parse(raw)?));
        }
        match self.from.as_deref() {
            None | Some("latest") => Ok(TopicPosition::Latest),
            Some("earliest") => Ok(TopicPosition::Earliest),
            Some(raw) => Ok(TopicPosition::After(cursor::parse(raw)?)),
        }
    }

    /// The per-subscriber filter the parameters describe.
    pub fn filter(&self) -> Result<Filter, String> {
        let header = match &self.header {
            None => None,
            Some(raw) => {
                let (name, value) = raw
                    .split_once(':')
                    .ok_or_else(|| "header filter must be <name>:<value>".to_owned())?;
                Some((name.to_owned(), Bytes::copy_from_slice(value.as_bytes())))
            }
        };
        Ok(Filter {
            key_prefix: self
                .key_prefix
                .as_ref()
                .map(|p| Bytes::copy_from_slice(p.as_bytes())),
            header,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(from: Option<&str>) -> StreamParams {
        StreamParams {
            from: from.map(str::to_owned),
            ..StreamParams::default()
        }
    }

    #[test]
    fn resume_token_wins_over_from() {
        let p = params(Some("earliest"));
        assert_eq!(p.position(Some("7")).unwrap(), Position::After(7));
        assert_eq!(p.position(None).unwrap(), Position::Earliest);
    }

    #[test]
    fn position_grammar() {
        assert_eq!(params(None).position(None).unwrap(), Position::Latest);
        assert_eq!(
            params(Some("latest")).position(None).unwrap(),
            Position::Latest
        );
        assert_eq!(
            params(Some("42")).position(None).unwrap(),
            Position::After(42)
        );
        assert!(params(Some("yesterday")).position(None).is_err());
        assert!(params(None).position(Some("not-a-number")).is_err());
    }

    #[test]
    fn topic_grammar_takes_cursors() {
        let position = params(Some("0:2,1:5")).topic_position(None).unwrap();
        let TopicPosition::After(cursor) = position else {
            panic!("expected a cursor");
        };
        assert_eq!(cursor[&0], 2);
        assert_eq!(cursor[&1], 5);
    }

    #[test]
    fn filter_grammar() {
        let mut p = StreamParams {
            key_prefix: Some("user:".into()),
            header: Some("kind:audit".into()),
            ..StreamParams::default()
        };
        let filter = p.filter().unwrap();
        assert_eq!(filter.key_prefix.as_deref(), Some(b"user:".as_slice()));
        assert_eq!(
            filter.header,
            Some(("kind".to_owned(), Bytes::from_static(b"audit")))
        );

        p.header = Some("malformed".into());
        assert!(p.filter().is_err());
    }
}
