//! Requests and replies on the daemon's local socket.

use serde::{Deserialize, Serialize};

use crate::frame::HandlerInfo;
use crate::handlers::Skipped;

/// The first line a client sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Request {
    Peer,
    Call { name: String, argv: Vec<String> },
    Ls,
    Reload,
}

/// The state of the link as `ls` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum LinkInfo {
    Up { version: u32 },
    Down { reason: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub ok: bool,
    pub code: u16,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsReply {
    pub ok: bool,
    pub link: LinkInfo,
    pub local: Vec<HandlerInfo>,
    pub remote: Vec<HandlerInfo>,
    pub skipped: Vec<Skipped>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReloadReply {
    pub ok: bool,
    pub handlers: usize,
    pub skipped: Vec<Skipped>,
}

/// Encodes a reply or request as one JSON line.
///
/// # Panics
///
/// Never in practice: every type here serializes to JSON.
#[must_use]
pub fn line<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).expect("local messages always serialize");
    bytes.push(b'\n');
    bytes
}

#[must_use]
pub fn ack_line() -> Vec<u8> {
    b"{\"ok\":true}\n".to_vec()
}

#[must_use]
pub fn refusal_line(code: u16, error: &str) -> Vec<u8> {
    line(&Refusal {
        ok: false,
        code,
        error: error.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_use_the_kind_tag() {
        let request = Request::Call {
            name: "echo".to_owned(),
            argv: vec!["a".to_owned()],
        };
        let text = String::from_utf8(line(&request)).expect("utf-8");
        assert_eq!(
            text,
            "{\"kind\":\"call\",\"name\":\"echo\",\"argv\":[\"a\"]}\n"
        );
        let back: Request = serde_json::from_str(&text).expect("parses");
        assert_eq!(back, request);
        let peer: Request = serde_json::from_str("{\"kind\":\"peer\"}").expect("parses");
        assert_eq!(peer, Request::Peer);
    }

    #[test]
    fn link_info_matches_the_documented_shape() {
        let up = serde_json::to_string(&LinkInfo::Up { version: 1 }).expect("serializes");
        assert_eq!(up, "{\"state\":\"up\",\"version\":1}");
        let down = serde_json::to_string(&LinkInfo::Down { reason: None }).expect("serializes");
        assert_eq!(down, "{\"state\":\"down\",\"reason\":null}");
    }

    #[test]
    fn replies_are_single_lines() {
        assert_eq!(ack_line(), b"{\"ok\":true}\n");
        let refusal = String::from_utf8(refusal_line(255, "link not up")).expect("utf-8");
        assert_eq!(
            refusal,
            "{\"ok\":false,\"code\":255,\"error\":\"link not up\"}\n"
        );
    }
}
