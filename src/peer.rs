//! The bridge sshd runs on the far host: stdio on one side, the daemon socket
//! on the other.

use std::fs::OpenOptions;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::{Instant, sleep};

use crate::client::{Answer, read_reply, socket_path};
use crate::frame::MAX_CHUNK;
use crate::local::{Request, line};
use crate::paths::Paths;

const START_WAIT: Duration = Duration::from_secs(5);
const RETRY: Duration = Duration::from_millis(50);

fn start_daemon(paths: &Paths) -> Result<()> {
    paths.create_state_dir()?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log)
        .with_context(|| format!("cannot open {}", paths.log.display()))?;
    let err = log.try_clone().context("cannot duplicate the log handle")?;
    let exe = std::env::current_exe().context("cannot find the ferry binary")?;
    std::process::Command::new(exe)
        .arg("daemon")
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(err)
        .spawn()
        .context("cannot start the daemon")?;
    Ok(())
}

async fn connect_or_start() -> Result<UnixStream> {
    let socket = socket_path()?;
    if let Ok(stream) = UnixStream::connect(&socket).await {
        return Ok(stream);
    }
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    start_daemon(&Paths::new(std::path::Path::new(&home))?)?;
    let deadline = Instant::now() + START_WAIT;
    loop {
        sleep(RETRY).await;
        if let Ok(stream) = UnixStream::connect(&socket).await {
            return Ok(stream);
        }
        if Instant::now() >= deadline {
            bail!("the daemon did not come up");
        }
    }
}

async fn copy_stdin(mut write: tokio::net::unix::OwnedWriteHalf) {
    let mut stdin = tokio::io::stdin();
    let mut buffer = vec![0u8; MAX_CHUNK];
    loop {
        let Ok(read) = stdin.read(&mut buffer).await else {
            return;
        };
        if read == 0 || write.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}

async fn copy_socket(mut read: tokio::net::unix::OwnedReadHalf) {
    let mut stdout = tokio::io::stdout();
    let mut buffer = vec![0u8; MAX_CHUNK];
    loop {
        let Ok(count) = read.read(&mut buffer).await else {
            return;
        };
        if count == 0 {
            return;
        }
        if stdout.write_all(&buffer[..count]).await.is_err() || stdout.flush().await.is_err() {
            return;
        }
    }
}

/// Runs `ferry peer` and returns the exit code the process should end with.
pub async fn run() -> i32 {
    let mut stream = match connect_or_start().await {
        Ok(stream) => stream,
        Err(err) => {
            eprintln!("ferry: {err:#}");
            return 255;
        }
    };
    if stream.write_all(&line(&Request::Peer)).await.is_err() {
        eprintln!("ferry: the daemon closed the connection");
        return 255;
    }
    match read_reply(&mut stream).await {
        Ok(Answer::Accepted) => {}
        Ok(Answer::Refused { code, error }) => {
            eprintln!("ferry: {error}");
            return i32::from(code);
        }
        Err(err) => {
            eprintln!("ferry: {err:#}");
            return 255;
        }
    }
    let (read, write) = stream.into_split();
    tokio::select! {
        () = copy_stdin(write) => {}
        () = copy_socket(read) => {}
    }
    0
}
