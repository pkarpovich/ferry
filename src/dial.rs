//! The dialer: keeps one ssh stream to the far host up.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStderr, Command};
use tokio::time::{sleep, timeout};

use crate::daemon::Inner;
use crate::session::Role;

const BACKOFF: [u64; 5] = [1, 2, 4, 8, 16];
const BACKOFF_CAP: Duration = Duration::from_secs(30);
const EXIT_WAIT: Duration = Duration::from_secs(2);
const STDERR_WAIT: Duration = Duration::from_secs(1);

/// The wait after the `attempt`-th failure in a row, counting from 0.
#[must_use]
pub fn backoff(attempt: u32) -> Duration {
    let Ok(index) = usize::try_from(attempt) else {
        return BACKOFF_CAP;
    };
    match BACKOFF.get(index) {
        Some(seconds) => Duration::from_secs(*seconds),
        None => BACKOFF_CAP,
    }
}

pub(crate) async fn run(inner: Arc<Inner>) {
    let mut closing = inner.closing();
    let mut attempt: u32 = 0;
    loop {
        if *closing.borrow() {
            return;
        }
        let established = connect_once(&inner).await;
        if established {
            attempt = 0;
        }
        let delay = backoff(attempt);
        attempt = attempt.saturating_add(1);
        tokio::select! {
            () = sleep(delay) => {}
            _ = closing.changed() => return,
        }
    }
}

async fn last_line(stderr: ChildStderr, last: Arc<Mutex<String>>) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(text)) = lines.next_line().await {
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        let mut slot = last.lock().expect("stderr line lock is not poisoned");
        text.clone_into(&mut slot);
    }
}

async fn connect_once(inner: &Inner) -> bool {
    let Some(destination) = &inner.config.connect else {
        return false;
    };
    let mut command = Command::new(&inner.ssh);
    command
        .args(["-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5"])
        .arg(destination)
        .arg(&inner.config.remote_ferry)
        .arg("peer")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(err) => {
            let message = format!("cannot start {}: {err}", inner.ssh);
            eprintln!("ferry: {message}");
            inner.note_failure(message);
            return false;
        }
    };
    let stdin = child.stdin.take().expect("stdin is piped");
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    let last = Arc::new(Mutex::new(String::new()));
    let reader = tokio::spawn(last_line(stderr, Arc::clone(&last)));

    let end = inner.attach(Role::Dialer, stdout, stdin).await;

    if timeout(EXIT_WAIT, child.wait()).await.is_err() {
        let _ = child.kill().await;
    }
    let _ = timeout(STDERR_WAIT, reader).await;
    let line = last
        .lock()
        .expect("stderr line lock is not poisoned")
        .clone();
    if !line.is_empty() {
        let message = format!("{}: {line}", end.reason);
        eprintln!("ferry: link to {destination} failed: {message}");
        inner.note_failure(message);
    }
    end.established
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_to_sixteen_then_holds_at_thirty() {
        let mut seconds = Vec::new();
        for attempt in 0..8 {
            seconds.push(backoff(attempt).as_secs());
        }
        assert_eq!(seconds, vec![1, 2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(backoff(u32::MAX).as_secs(), 30);
    }
}
