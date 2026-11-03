//! The commands that talk to the local daemon: `call`, `ls` and `reload`.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use crate::frame::{
    CallId, Frame, FrameError, HandlerInfo, MAX_CHUNK, MAX_LINE, Stream, encode, read_frame,
};
use crate::handlers::Skipped;
use crate::local::{LinkInfo, LsReply, ReloadReply, Request, line};
use crate::paths::Paths;

const NO_DAEMON: &str = "no daemon on this host";
const FAILED_CODE: i32 = 255;

/// The socket path of the current user's daemon.
///
/// # Errors
/// Fails when `$HOME` is not set or the socket path is too long.
pub fn socket_path() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(Paths::new(&PathBuf::from(home))?.socket)
}

/// The first line a daemon answers with.
#[derive(Debug, Deserialize)]
struct Reply {
    ok: bool,
    #[serde(default)]
    code: Option<u16>,
    #[serde(default)]
    error: Option<String>,
}

/// The outcome of sending a request line.
#[derive(Debug, PartialEq, Eq)]
pub enum Answer {
    Accepted,
    Refused { code: u16, error: String },
}

/// Reads one reply line a byte at a time, so that no frame behind it is
/// swallowed by a buffer.
///
/// # Errors
/// Fails when the connection ends before a newline or the line passes 128 KiB.
pub async fn read_reply<R: AsyncReadExt + Unpin>(read: &mut R) -> Result<Answer> {
    let mut bytes = Vec::new();
    loop {
        let byte = read
            .read_u8()
            .await
            .context("the daemon closed the connection")?;
        if byte == b'\n' {
            break;
        }
        bytes.push(byte);
        if bytes.len() > MAX_LINE {
            bail!("the daemon's reply is too long");
        }
    }
    let Reply { ok, code, error } =
        serde_json::from_slice(&bytes).context("the daemon's reply is not valid")?;
    if ok {
        return Ok(Answer::Accepted);
    }
    Ok(Answer::Refused {
        code: code.unwrap_or(255),
        error: error.unwrap_or_else(|| "refused".to_owned()),
    })
}

async fn connect() -> Result<UnixStream> {
    let socket = socket_path()?;
    UnixStream::connect(&socket)
        .await
        .map_err(|_| anyhow!(NO_DAEMON))
}

async fn pump_stdin(mut write: OwnedWriteHalf) {
    let id = CallId(0);
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0u8; MAX_CHUNK];
    loop {
        let read = stdin.read(&mut buffer).await.unwrap_or(0);
        if read == 0 {
            let eof = Frame::Eof {
                id,
                stream: Stream::Stdin,
            };
            if write.write_all(&encode(&eof)).await.is_err() {
                return;
            }
            // Dropping the write half would read as a cancel to the daemon.
            std::future::pending::<()>().await;
        }
        let chunk = Frame::Chunk {
            id,
            stream: Stream::Stdin,
            data: buffer[..read].to_vec(),
        };
        if write.write_all(&encode(&chunk)).await.is_err() {
            return;
        }
    }
}

async fn emit<W: AsyncWriteExt + Unpin>(out: &mut W, data: &[u8]) -> bool {
    out.write_all(data).await.is_ok() && out.flush().await.is_ok()
}

async fn pump_output(read: OwnedReadHalf) -> i32 {
    let mut reader = BufReader::new(read);
    let mut stdout = tokio::io::stdout();
    let mut stderr = tokio::io::stderr();
    loop {
        let frame = match read_frame(&mut reader).await {
            Ok(Some(frame)) => frame,
            Err(FrameError::Unknown(_)) => continue,
            Ok(None) | Err(FrameError::TooLong | FrameError::Malformed(_) | FrameError::Io(_)) => {
                eprintln!("ferry: connection to the daemon lost");
                return FAILED_CODE;
            }
        };
        match frame {
            Frame::Chunk {
                id: _,
                stream,
                data,
            } => match stream {
                Stream::Stdout => {
                    if !emit(&mut stdout, &data).await {
                        return FAILED_CODE;
                    }
                }
                Stream::Stderr => {
                    if !emit(&mut stderr, &data).await {
                        return FAILED_CODE;
                    }
                }
                Stream::Stdin => {}
            },
            Frame::Exit { id: _, code } => return code,
            Frame::Error {
                id: _,
                code,
                message,
            } => {
                eprintln!("ferry: {message}");
                return i32::from(code);
            }
            Frame::Eof { .. }
            | Frame::Hello { .. }
            | Frame::Handlers { .. }
            | Frame::Ping
            | Frame::Open { .. }
            | Frame::Cancel { .. } => {}
        }
    }
}

/// Runs `ferry call` and returns the exit code the process should end with.
pub async fn call(name: String, argv: Vec<String>) -> i32 {
    let request = line(&Request::Call { name, argv });
    if request.len() > MAX_LINE {
        eprintln!("ferry: arguments too large");
        return FAILED_CODE;
    }
    let mut stream = match connect().await {
        Ok(stream) => stream,
        Err(err) => {
            eprintln!("ferry: {err:#}");
            return FAILED_CODE;
        }
    };
    if stream.write_all(&request).await.is_err() {
        eprintln!("ferry: {NO_DAEMON}");
        return FAILED_CODE;
    }
    match read_reply(&mut stream).await {
        Ok(Answer::Accepted) => {}
        Ok(Answer::Refused { code, error }) => {
            eprintln!("ferry: {error}");
            return i32::from(code);
        }
        Err(err) => {
            eprintln!("ferry: {err:#}");
            return FAILED_CODE;
        }
    }
    let (read, write) = stream.into_split();
    tokio::spawn(pump_stdin(write));
    pump_output(read).await
}

async fn ask(request: &Request) -> Result<Vec<u8>> {
    let mut stream = connect().await?;
    stream
        .write_all(&line(request))
        .await
        .context("cannot send the request")?;
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .await
        .context("cannot read the reply")?;
    let refusal: Reply =
        serde_json::from_slice(&bytes).context("the daemon's reply is not valid")?;
    if !refusal.ok {
        bail!(refusal.error.unwrap_or_else(|| "refused".to_owned()));
    }
    Ok(bytes)
}

/// Renders an `ls` reply as the text table of `ferry ls`.
#[must_use]
pub fn render_ls(reply: &LsReply) -> String {
    let LsReply {
        ok: _,
        link,
        local,
        remote,
        skipped,
    } = reply;
    let mut out = match link {
        LinkInfo::Up { version } => format!("link: up (peer version {version})\n"),
        LinkInfo::Down {
            reason: Some(reason),
        } => format!("link: down ({reason})\n"),
        LinkInfo::Down { reason: None } => "link: down\n".to_owned(),
    };
    out.push_str("\nthis host:\n");
    for HandlerInfo { name, desc } in local {
        let row = format!("  {name:<24} {desc}\n");
        out.push_str(&row);
    }
    out.push_str("\nother side:\n");
    for HandlerInfo { name, desc } in remote {
        let row = format!("  {name:<24} {desc}\n");
        out.push_str(&row);
    }
    if !skipped.is_empty() {
        out.push_str("\nskipped:\n");
        for Skipped { file, reason } in skipped {
            let row = format!("  {file}: {reason}\n");
            out.push_str(&row);
        }
    }
    out
}

/// Runs `ferry ls` and `ferry ls --json`.
///
/// # Errors
/// Fails when no daemon answers or the reply is malformed.
pub async fn ls(json: bool) -> Result<()> {
    let bytes = ask(&Request::Ls).await?;
    if json {
        let mut value: serde_json::Value =
            serde_json::from_slice(&bytes).context("the daemon's reply is not valid")?;
        if let Some(object) = value.as_object_mut() {
            object.remove("ok");
        }
        println!("{value}");
        return Ok(());
    }
    let reply: LsReply =
        serde_json::from_slice(&bytes).context("the daemon's reply is not valid")?;
    print!("{}", render_ls(&reply));
    Ok(())
}

/// Runs `ferry reload`.
///
/// # Errors
/// Fails when no daemon answers or the reply is malformed.
pub async fn reload() -> Result<()> {
    let bytes = ask(&Request::Reload).await?;
    let ReloadReply {
        ok: _,
        handlers,
        skipped,
    } = serde_json::from_slice(&bytes).context("the daemon's reply is not valid")?;
    println!("{handlers} handlers");
    for Skipped { file, reason } in skipped {
        println!("skipped {file}: {reason}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(name: &str, desc: &str) -> HandlerInfo {
        HandlerInfo {
            name: name.to_owned(),
            desc: desc.to_owned(),
        }
    }

    #[test]
    fn ls_text_lists_link_handlers_and_skipped_files() {
        let reply = LsReply {
            ok: true,
            link: LinkInfo::Up { version: 1 },
            local: vec![info("echo", "says it back")],
            remote: vec![info("service-proxy", "")],
            skipped: vec![Skipped {
                file: "bad.sh".to_owned(),
                reason: "not executable".to_owned(),
            }],
        };
        let text = render_ls(&reply);
        assert!(text.starts_with("link: up (peer version 1)\n"), "{text}");
        assert!(text.contains("this host:\n  echo"), "{text}");
        assert!(text.contains("says it back"), "{text}");
        assert!(text.contains("other side:\n  service-proxy"), "{text}");
        assert!(
            text.contains("skipped:\n  bad.sh: not executable\n"),
            "{text}"
        );
    }

    #[test]
    fn ls_text_shows_why_the_link_is_down() {
        let reply = LsReply {
            ok: true,
            link: LinkInfo::Down {
                reason: Some("ssh exited".to_owned()),
            },
            local: Vec::new(),
            remote: Vec::new(),
            skipped: Vec::new(),
        };
        let text = render_ls(&reply);
        assert!(text.starts_with("link: down (ssh exited)\n"), "{text}");
        assert!(!text.contains("skipped"), "{text}");
    }

    #[tokio::test]
    async fn a_reply_is_read_without_swallowing_what_follows() {
        let mut data: &[u8] = b"{\"ok\":true}\nrest";
        let answer = read_reply(&mut data).await.expect("reply");
        assert_eq!(answer, Answer::Accepted);
        assert_eq!(data, b"rest");
    }

    #[tokio::test]
    async fn a_refusal_carries_its_code_and_text() {
        let mut data: &[u8] = b"{\"ok\":false,\"code\":255,\"error\":\"link not up\"}\n";
        let answer = read_reply(&mut data).await.expect("reply");
        assert_eq!(
            answer,
            Answer::Refused {
                code: 255,
                error: "link not up".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn a_connection_that_ends_before_the_newline_is_an_error() {
        let mut data: &[u8] = b"{\"ok\":tr";
        assert!(read_reply(&mut data).await.is_err());
    }
}
