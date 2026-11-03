//! Newline-delimited JSON frames carried over a ferry link.

use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

pub const MAX_LINE: usize = 128 * 1024;
pub const MAX_CHUNK: usize = 64 * 1024;

const KNOWN_KINDS: [&str; 9] = [
    "hello", "handlers", "ping", "open", "chunk", "eof", "exit", "error", "cancel",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CallId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    Stdin,
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerInfo {
    pub name: String,
    pub desc: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Frame {
    Hello {
        version: u32,
        kinds: Vec<String>,
        handlers: Vec<HandlerInfo>,
    },
    Handlers {
        handlers: Vec<HandlerInfo>,
    },
    Ping,
    Open {
        id: CallId,
        name: String,
        argv: Vec<String>,
    },
    Chunk {
        id: CallId,
        stream: Stream,
        #[serde(with = "base64_data")]
        data: Vec<u8>,
    },
    Eof {
        id: CallId,
        stream: Stream,
    },
    Exit {
        id: CallId,
        code: i32,
    },
    Error {
        id: CallId,
        code: u16,
        message: String,
    },
    Cancel {
        id: CallId,
    },
}

mod base64_data {
    use super::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(data))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(deserializer)?;
        STANDARD.decode(text).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("line longer than {MAX_LINE} bytes")]
    TooLong,
    #[error("malformed frame: {0}")]
    Malformed(String),
    #[error("unknown frame kind {0}")]
    Unknown(String),
    #[error("read failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Encodes a frame as one JSON line ending in `\n`.
///
/// # Panics
///
/// Never in practice: every frame serializes to JSON.
#[must_use]
pub fn encode(frame: &Frame) -> Vec<u8> {
    let mut line = serde_json::to_vec(frame).expect("frames always serialize");
    line.push(b'\n');
    line
}

/// Reads the next frame, or `None` at a clean end of stream.
///
/// # Errors
///
/// `TooLong` when a line passes 128 KiB, `Malformed` for invalid JSON, a missing
/// `kind` or a bad field, `Unknown` for a well-formed frame of an unknown kind
/// (the caller skips it and reads on), `Io` when the read fails.
pub async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Frame>, FrameError> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            return Err(FrameError::Malformed("truncated line".to_owned()));
        }
        let mut newline = None;
        for (index, byte) in available.iter().enumerate() {
            if *byte == b'\n' {
                newline = Some(index);
                break;
            }
        }
        let take = newline.unwrap_or(available.len());
        if line.len() + take > MAX_LINE {
            return Err(FrameError::TooLong);
        }
        line.extend_from_slice(&available[..take]);
        let consumed = match newline {
            Some(_) => take + 1,
            None => take,
        };
        reader.consume(consumed);
        if newline.is_some() {
            break;
        }
    }
    decode(&line).map(Some)
}

fn decode(line: &[u8]) -> Result<Frame, FrameError> {
    let value: serde_json::Value =
        serde_json::from_slice(line).map_err(|err| FrameError::Malformed(err.to_string()))?;
    let Some(kind) = value.get("kind").and_then(serde_json::Value::as_str) else {
        return Err(FrameError::Malformed("missing kind".to_owned()));
    };
    let kind = kind.to_owned();
    let mut known = false;
    for candidate in KNOWN_KINDS {
        if candidate == kind {
            known = true;
        }
    }
    if !known {
        return Err(FrameError::Unknown(kind));
    }
    serde_json::from_value(value).map_err(|err| FrameError::Malformed(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    fn samples() -> Vec<Frame> {
        vec![
            Frame::Hello {
                version: 1,
                kinds: vec!["open".to_owned(), "chunk".to_owned()],
                handlers: vec![HandlerInfo {
                    name: "echo".to_owned(),
                    desc: "echoes".to_owned(),
                }],
            },
            Frame::Handlers { handlers: vec![] },
            Frame::Ping,
            Frame::Open {
                id: CallId(1),
                name: "echo".to_owned(),
                argv: vec!["a b".to_owned()],
            },
            Frame::Chunk {
                id: CallId(2),
                stream: Stream::Stdout,
                data: vec![0, 255, 10, 13],
            },
            Frame::Eof {
                id: CallId(2),
                stream: Stream::Stdin,
            },
            Frame::Exit {
                id: CallId(2),
                code: 130,
            },
            Frame::Error {
                id: CallId(3),
                code: 127,
                message: "no handler".to_owned(),
            },
            Frame::Cancel { id: CallId(4) },
        ]
    }

    async fn read_all(bytes: &[u8]) -> Vec<Result<Option<Frame>, FrameError>> {
        let mut reader = BufReader::new(bytes);
        let mut out = Vec::new();
        loop {
            let item = read_frame(&mut reader).await;
            let done = match &item {
                Ok(Some(_)) | Err(FrameError::Unknown(_)) => false,
                Ok(None) | Err(_) => true,
            };
            out.push(item);
            if done {
                return out;
            }
        }
    }

    #[tokio::test]
    async fn round_trips_every_variant() {
        let mut bytes = Vec::new();
        for frame in samples() {
            bytes.extend(encode(&frame));
        }
        let mut reader = BufReader::new(bytes.as_slice());
        for frame in samples() {
            let read = read_frame(&mut reader).await.expect("frame reads");
            assert_eq!(read, Some(frame));
        }
        let end = read_frame(&mut reader).await.expect("clean end");
        assert_eq!(end, None);
    }

    #[test]
    fn chunk_data_is_base64_text() {
        let line = encode(&Frame::Chunk {
            id: CallId(1),
            stream: Stream::Stderr,
            data: vec![0xff, 0xfe],
        });
        let text = String::from_utf8(line).expect("utf8");
        assert_eq!(
            text,
            "{\"kind\":\"chunk\",\"id\":1,\"stream\":\"stderr\",\"data\":\"//4=\"}\n"
        );
    }

    #[tokio::test]
    async fn full_chunk_fits_under_line_limit() {
        let frame = Frame::Chunk {
            id: CallId(u64::MAX),
            stream: Stream::Stdout,
            data: vec![0xab; MAX_CHUNK],
        };
        let bytes = encode(&frame);
        assert!(bytes.len() <= MAX_LINE);
        let mut reader = BufReader::new(bytes.as_slice());
        let read = read_frame(&mut reader).await.expect("frame reads");
        assert_eq!(read, Some(frame));
    }

    #[tokio::test]
    async fn line_over_limit_is_too_long() {
        let mut bytes = vec![b'x'; MAX_LINE + 1];
        bytes.push(b'\n');
        let mut results = read_all(&bytes).await;
        let Some(Err(FrameError::TooLong)) = results.pop() else {
            panic!("expected TooLong");
        };
    }

    #[tokio::test]
    async fn too_long_does_not_consume_the_rest() {
        let bytes = vec![b'x'; MAX_LINE * 4];
        let mut reader = BufReader::with_capacity(1024, bytes.as_slice());
        let Err(FrameError::TooLong) = read_frame(&mut reader).await else {
            panic!("expected TooLong");
        };
        assert!(reader.buffer().len() <= 1024);
    }

    #[tokio::test]
    async fn invalid_json_and_missing_kind_are_malformed() {
        for input in ["not json\n", "{\"id\":1}\n", "{\"kind\":5}\n", "\n"] {
            let mut results = read_all(input.as_bytes()).await;
            let Some(Err(FrameError::Malformed(_))) = results.pop() else {
                panic!("expected Malformed for {input:?}");
            };
        }
    }

    #[tokio::test]
    async fn bad_fields_of_a_known_kind_are_malformed() {
        let input = "{\"kind\":\"exit\",\"id\":1}\n";
        let mut results = read_all(input.as_bytes()).await;
        let Some(Err(FrameError::Malformed(_))) = results.pop() else {
            panic!("expected Malformed");
        };
    }

    #[tokio::test]
    async fn unknown_kind_is_skipped_and_next_frame_reads() {
        let mut bytes = b"{\"kind\":\"future\",\"x\":1}\n".to_vec();
        bytes.extend(encode(&Frame::Ping));
        let mut reader = BufReader::new(bytes.as_slice());
        let first = read_frame(&mut reader).await;
        let Err(FrameError::Unknown(kind)) = first else {
            panic!("expected Unknown");
        };
        assert_eq!(kind, "future");
        let second = read_frame(&mut reader).await.expect("next frame reads");
        assert_eq!(second, Some(Frame::Ping));
    }

    #[tokio::test]
    async fn truncated_last_line_is_malformed() {
        let mut results = read_all(b"{\"kind\":\"ping\"}").await;
        let Some(Err(FrameError::Malformed(_))) = results.pop() else {
            panic!("expected Malformed");
        };
    }
}
