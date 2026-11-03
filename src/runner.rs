//! Running a handler as a child process and ending its run.

use std::fmt;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use rustix::process::{Pid, Signal, kill_process_group, test_kill_process_group};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};

use crate::frame::Stream;
use crate::handlers::Handler;

pub const CHUNK_BYTES: usize = 64 * 1024;
pub const SPAWN_FAILED_CODE: i32 = 126;

const EXIT_GRACE: Duration = Duration::from_secs(5);
const KILL_AFTER: Duration = Duration::from_secs(5);
const CLOSE_AFTER: Duration = Duration::from_secs(10);
const KILLED_CODE: i32 = 137;
const UNKNOWN_STATUS_CODE: i32 = 255;
const EVENT_QUEUE: usize = 8;

/// The handler file could not be started. Its code is [`SPAWN_FAILED_CODE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnError {
    pub reason: String,
}

impl fmt::Display for SpawnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for SpawnError {}

/// What a running handler produces, in the order it produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Chunk { stream: Stream, data: Vec<u8> },
    Eof(Stream),
    Exit(i32),
}

/// Starts the cancel sequence of a run. Cloning is cheap and cancelling twice is harmless.
#[derive(Debug, Clone)]
pub struct Canceller {
    notify: Arc<Notify>,
}

impl Canceller {
    pub fn cancel(&self) {
        self.notify.notify_one();
    }
}

/// A handler process. `events` ends with one `Exit` after the `Eof` of stdout and stderr.
#[derive(Debug)]
pub struct Running {
    pub stdin: Option<ChildStdin>,
    pub events: mpsc::Receiver<Event>,
    canceller: Canceller,
}

impl Running {
    #[must_use]
    pub fn canceller(&self) -> Canceller {
        self.canceller.clone()
    }

    pub fn cancel(&self) {
        self.canceller.cancel();
    }
}

/// Starts `handler` with `argv`, no shell, in `$HOME`, in its own process group.
///
/// # Errors
///
/// Fails with the OS reason when the process cannot be started.
///
/// # Panics
///
/// Panics if the OS reports a process id that is zero or does not fit an `i32`.
pub fn spawn(handler: &Handler, argv: &[String]) -> Result<Running, SpawnError> {
    let mut command = Command::new(&handler.path);
    command
        .args(argv)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    if let Some(home) = std::env::var_os("HOME") {
        command.current_dir(home);
    }
    let mut child = command.spawn().map_err(|err| SpawnError {
        reason: err.to_string(),
    })?;

    let id = child.id().expect("a child that was just spawned has an id");
    let id = i32::try_from(id).expect("process ids fit an i32");
    let pgid = Pid::from_raw(id).expect("process ids are not zero");
    let stdin = child.stdin.take();
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");

    let (tx, events) = mpsc::channel(EVENT_QUEUE);
    let notify = Arc::new(Notify::new());
    let canceller = Canceller {
        notify: Arc::clone(&notify),
    };
    tokio::spawn(supervise(child, pgid, stdout, stderr, tx, notify));

    Ok(Running {
        stdin,
        events,
        canceller,
    })
}

fn exit_code(status: ExitStatus) -> i32 {
    match (status.code(), status.signal()) {
        (Some(code), _) => code,
        (None, Some(signal)) => 128 + signal,
        (None, None) => UNKNOWN_STATUS_CODE,
    }
}

async fn pump<R: AsyncRead + Unpin>(mut pipe: R, stream: Stream, tx: mpsc::Sender<Event>) {
    let mut buf = vec![0; CHUNK_BYTES];
    loop {
        let Ok(read) = pipe.read(&mut buf).await else {
            return;
        };
        if read == 0 {
            return;
        }
        let data = buf[..read].to_vec();
        if tx.send(Event::Chunk { stream, data }).await.is_err() {
            return;
        }
    }
}

struct Sequence {
    kill_at: Option<Instant>,
    close_at: Option<Instant>,
}

fn begin_cancel(pgid: Pid) -> Sequence {
    let started = Instant::now();
    let _ = kill_process_group(pgid, Signal::TERM);
    Sequence {
        kill_at: Some(started + KILL_AFTER),
        close_at: Some(started + CLOSE_AFTER),
    }
}

async fn supervise(
    mut child: Child,
    pgid: Pid,
    stdout: ChildStdout,
    stderr: ChildStderr,
    tx: mpsc::Sender<Event>,
    cancel: Arc<Notify>,
) {
    let mut out_task: JoinHandle<()> = tokio::spawn(pump(stdout, Stream::Stdout, tx.clone()));
    let mut err_task: JoinHandle<()> = tokio::spawn(pump(stderr, Stream::Stderr, tx.clone()));
    let mut out_done = false;
    let mut err_done = false;
    let mut code = None;
    let mut grace = None;
    let mut sequence: Option<Sequence> = None;

    while code.is_none() || !out_done || !err_done {
        let kill_at = sequence.as_ref().and_then(|seq| seq.kill_at);
        let close_at = sequence.as_ref().and_then(|seq| seq.close_at);
        tokio::select! {
            status = child.wait(), if code.is_none() => {
                code = Some(status.map_or(UNKNOWN_STATUS_CODE, exit_code));
                if sequence.is_none() {
                    grace = Some(Instant::now() + EXIT_GRACE);
                }
            }
            _ = &mut out_task, if !out_done => {
                out_done = true;
                let _ = tx.send(Event::Eof(Stream::Stdout)).await;
            }
            _ = &mut err_task, if !err_done => {
                err_done = true;
                let _ = tx.send(Event::Eof(Stream::Stderr)).await;
            }
            () = cancel.notified(), if sequence.is_none() => {
                grace = None;
                sequence = Some(begin_cancel(pgid));
            }
            () = sleep_until(grace.unwrap_or_else(Instant::now)), if grace.is_some() => {
                grace = None;
                sequence = Some(begin_cancel(pgid));
            }
            () = sleep_until(kill_at.unwrap_or_else(Instant::now)), if kill_at.is_some() => {
                if let Some(seq) = sequence.as_mut() {
                    seq.kill_at = None;
                }
                if test_kill_process_group(pgid).is_ok() {
                    let _ = kill_process_group(pgid, Signal::KILL);
                }
            }
            () = sleep_until(close_at.unwrap_or_else(Instant::now)), if close_at.is_some() => {
                break;
            }
        }
    }

    if !out_done {
        out_task.abort();
        let _ = out_task.await;
        let _ = tx.send(Event::Eof(Stream::Stdout)).await;
    }
    if !err_done {
        err_task.abort();
        let _ = err_task.await;
        let _ = tx.send(Event::Eof(Stream::Stderr)).await;
    }
    let _ = tx.send(Event::Exit(code.unwrap_or(KILLED_CODE))).await;
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant as StdInstant;

    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::name::HandlerName;

    static SPAWN_LOCK: Mutex<()> = Mutex::new(());
    static NEXT_FILE: AtomicUsize = AtomicUsize::new(0);

    struct Collected {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        code: i32,
        elapsed: Duration,
    }

    fn start_with_mode(
        dir: &TempDir,
        script: &str,
        mode: u32,
        argv: &[&str],
    ) -> Result<Running, SpawnError> {
        let index = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let path = dir.path().join(format!("handler-{index}"));
        let argv: Vec<String> = argv.iter().map(|arg| (*arg).to_owned()).collect();
        let _guard = SPAWN_LOCK.lock().expect("spawn lock is not poisoned");
        fs::write(&path, script).expect("script is written");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("mode is set");
        let handler = Handler {
            name: HandlerName::parse("probe").expect("name is valid"),
            desc: String::new(),
            path,
        };
        spawn(&handler, &argv)
    }

    fn start(dir: &TempDir, script: &str, argv: &[&str]) -> Running {
        start_with_mode(dir, script, 0o755, argv).expect("handler starts")
    }

    async fn collect(running: &mut Running) -> Collected {
        let begun = StdInstant::now();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut eofs = Vec::new();
        loop {
            let event = running.events.recv().await.expect("events end with Exit");
            match event {
                Event::Chunk {
                    stream: Stream::Stdout,
                    data,
                } => stdout.extend(data),
                Event::Chunk {
                    stream: Stream::Stderr,
                    data,
                } => stderr.extend(data),
                Event::Chunk {
                    stream: Stream::Stdin,
                    ..
                } => panic!("a handler never produces stdin"),
                Event::Eof(stream) => eofs.push(stream),
                Event::Exit(code) => {
                    assert_eq!(eofs.len(), 2, "both streams end before Exit");
                    assert!(running.events.recv().await.is_none(), "Exit is last");
                    return Collected {
                        stdout,
                        stderr,
                        code,
                        elapsed: begun.elapsed(),
                    };
                }
            }
        }
    }

    #[tokio::test]
    async fn argv_arrives_without_shell_splitting() {
        let dir = TempDir::new().expect("tempdir");
        let mut running = start(&dir, "#!/bin/sh\nprintf '%s|' \"$@\"\n", &["a b", "$HOME"]);
        let done = collect(&mut running).await;
        assert_eq!(done.stdout, b"a b|$HOME|");
        assert_eq!(done.code, 0);
    }

    #[tokio::test]
    async fn runs_in_home() {
        let dir = TempDir::new().expect("tempdir");
        let mut running = start(&dir, "#!/bin/sh\npwd -P\n", &[]);
        let done = collect(&mut running).await;
        let home = std::env::var("HOME").expect("HOME is set");
        let home = fs::canonicalize(home).expect("HOME exists");
        assert_eq!(
            String::from_utf8(done.stdout).expect("utf-8").trim(),
            home.to_str().expect("utf-8 path")
        );
    }

    #[tokio::test]
    async fn stdin_is_echoed_to_stdout() {
        let dir = TempDir::new().expect("tempdir");
        let mut running = start(&dir, "#!/bin/sh\ncat\n", &[]);
        let mut stdin = running.stdin.take().expect("stdin is piped");
        stdin.write_all(b"hello\n").await.expect("stdin accepts");
        drop(stdin);
        let done = collect(&mut running).await;
        assert_eq!(done.stdout, b"hello\n");
        assert_eq!(done.code, 0);
    }

    #[tokio::test]
    async fn stderr_is_separate_and_exit_code_passes_through() {
        let dir = TempDir::new().expect("tempdir");
        let mut running = start(&dir, "#!/bin/sh\necho out\necho err >&2\nexit 3\n", &[]);
        let done = collect(&mut running).await;
        assert_eq!(done.stdout, b"out\n");
        assert_eq!(done.stderr, b"err\n");
        assert_eq!(done.code, 3);
    }

    #[tokio::test]
    async fn a_signal_maps_to_128_plus_its_number() {
        let dir = TempDir::new().expect("tempdir");
        let mut running = start(&dir, "#!/bin/sh\nkill -9 $$\n", &[]);
        let done = collect(&mut running).await;
        assert_eq!(done.code, 137);
    }

    #[tokio::test]
    async fn a_non_executable_file_fails_to_start() {
        let dir = TempDir::new().expect("tempdir");
        let Err(err) = start_with_mode(&dir, "#!/bin/sh\n", 0o644, &[]) else {
            panic!("a file without the execute bit must not start");
        };
        assert_ne!(err.reason, "");
        assert_eq!(SPAWN_FAILED_CODE, 126);
    }

    #[tokio::test]
    async fn cancel_stops_a_plain_handler() {
        let dir = TempDir::new().expect("tempdir");
        let mut running = start(&dir, "#!/bin/sh\nexec sleep 30\n", &[]);
        tokio::time::sleep(Duration::from_millis(200)).await;
        running.cancel();
        let done = collect(&mut running).await;
        assert_eq!(done.code, 143);
        assert!(done.elapsed < Duration::from_secs(3), "{:?}", done.elapsed);
    }

    #[tokio::test]
    async fn a_handler_that_traps_sigterm_is_killed_after_five_seconds() {
        let dir = TempDir::new().expect("tempdir");
        let script = "#!/bin/sh\ntrap '' TERM\nwhile :; do sleep 1; done\n";
        let mut running = start(&dir, script, &[]);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let begun = StdInstant::now();
        running.cancel();
        let done = collect(&mut running).await;
        let waited = begun.elapsed();
        assert_eq!(done.code, 137);
        assert!(waited >= Duration::from_millis(4800), "{waited:?}");
        assert!(waited < Duration::from_secs(8), "{waited:?}");
    }

    #[tokio::test]
    async fn a_child_that_traps_sigterm_is_killed_after_five_seconds_though_the_handler_died() {
        let dir = TempDir::new().expect("tempdir");
        let script = "#!/bin/sh\n(trap '' TERM; while :; do sleep 1; done) &\nexit 0\n";
        let mut running = start(&dir, script, &[]);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let begun = StdInstant::now();
        running.cancel();
        let done = collect(&mut running).await;
        let waited = begun.elapsed();
        assert_eq!(done.code, 0);
        assert!(waited >= Duration::from_millis(4800), "{waited:?}");
        assert!(waited < Duration::from_secs(8), "{waited:?}");
    }

    #[tokio::test]
    async fn a_background_child_holding_stdout_ends_the_run_five_seconds_after_exit() {
        let dir = TempDir::new().expect("tempdir");
        let script = "#!/bin/sh\necho before\n(sleep 30) &\necho done\nexit 0\n";
        let mut running = start(&dir, script, &[]);
        let done = collect(&mut running).await;
        assert_eq!(done.stdout, b"before\ndone\n");
        assert_eq!(done.code, 0);
        assert!(
            done.elapsed >= Duration::from_millis(4800),
            "{:?}",
            done.elapsed
        );
        assert!(done.elapsed < Duration::from_secs(8), "{:?}", done.elapsed);
    }

    #[tokio::test]
    async fn a_setsid_child_holding_stdout_ends_the_run_about_fifteen_seconds_after_exit() {
        let dir = TempDir::new().expect("tempdir");
        let script = "#!/bin/sh\n\
            perl -e 'use POSIX qw(setsid); setsid(); $SIG{TERM} = \"IGNORE\"; sleep 20' &\n\
            echo written\n\
            exit 0\n";
        let mut running = start(&dir, script, &[]);
        let done = collect(&mut running).await;
        assert_eq!(done.stdout, b"written\n");
        assert_eq!(done.code, 0);
        assert!(
            done.elapsed >= Duration::from_millis(14800),
            "{:?}",
            done.elapsed
        );
        assert!(done.elapsed < Duration::from_secs(18), "{:?}", done.elapsed);
    }
}
