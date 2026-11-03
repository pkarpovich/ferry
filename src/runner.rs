//! Running a handler as a child process and ending its run.

use std::fmt;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use rustix::process::{Pid, Signal, kill_process_group, test_kill_process_group};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};

use crate::frame::{MAX_CHUNK, Stream};
use crate::handlers::Handler;

pub const SPAWN_FAILED_CODE: i32 = 126;

const EXIT_GRACE: Duration = Duration::from_secs(5);
const KILL_AFTER: Duration = Duration::from_secs(5);
const CLOSE_AFTER: Duration = Duration::from_secs(10);
const KILLED_CODE: i32 = 137;
const UNKNOWN_STATUS_CODE: i32 = 255;
const WATCH_EVERY: Duration = Duration::from_millis(50);
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

/// A handler process. `events` ends with one `Exit` after the `Eof` of stdout and stderr.
#[derive(Debug)]
pub struct Running {
    pub stdin: Option<ChildStdin>,
    pub events: mpsc::Receiver<Event>,
    cancel: Arc<Notify>,
}

impl Running {
    pub fn cancel(&self) {
        self.cancel.notify_one();
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
    let cancel = Arc::new(Notify::new());
    tokio::spawn(supervise(
        child,
        pgid,
        stdout,
        stderr,
        tx,
        Arc::clone(&cancel),
    ));

    Ok(Running {
        stdin,
        events,
        cancel,
    })
}

fn exit_code(status: ExitStatus) -> i32 {
    match (status.code(), status.signal()) {
        (Some(code), _) => code,
        (None, Some(signal)) => 128 + signal,
        (None, None) => UNKNOWN_STATUS_CODE,
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PumpState {
    blocked: Duration,
    sending_since: Option<Instant>,
}

async fn pump<R: AsyncRead + Unpin>(
    mut pipe: R,
    stream: Stream,
    tx: mpsc::Sender<Event>,
    state: watch::Sender<PumpState>,
) {
    let mut buf = vec![0; MAX_CHUNK];
    let mut blocked = Duration::ZERO;
    loop {
        let Ok(read) = pipe.read(&mut buf).await else {
            return;
        };
        if read == 0 {
            return;
        }
        let data = buf[..read].to_vec();
        let since = Instant::now();
        state.send_replace(PumpState {
            blocked,
            sending_since: Some(since),
        });
        let sent = tx.send(Event::Chunk { stream, data }).await;
        blocked += since.elapsed();
        state.send_replace(PumpState {
            blocked,
            sending_since: None,
        });
        if sent.is_err() {
            return;
        }
    }
}

struct Pipe {
    done: bool,
    watching: bool,
    blocked_at_exit: Duration,
    state: watch::Receiver<PumpState>,
}

impl Pipe {
    fn mark_exit(&mut self) {
        let PumpState {
            blocked,
            sending_since: _,
        } = *self.state.borrow();
        self.blocked_at_exit = blocked;
    }
}

fn grace_deadline(exited: Instant, pipes: [&Pipe; 2]) -> Option<Instant> {
    let mut extra = Duration::ZERO;
    for pipe in pipes {
        if pipe.done {
            continue;
        }
        let PumpState {
            blocked,
            sending_since,
        } = *pipe.state.borrow();
        if sending_since.is_some() {
            return None;
        }
        let after_exit = blocked.saturating_sub(pipe.blocked_at_exit);
        if after_exit > extra {
            extra = after_exit;
        }
    }
    Some(exited + EXIT_GRACE + extra)
}

#[cfg(target_os = "linux")]
fn group_lives(pgid: Pid) -> bool {
    if test_kill_process_group(pgid).is_err() {
        return false;
    }
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return true;
    };
    let want = pgid.as_raw_nonzero().get();
    for entry in entries.flatten() {
        let Ok(stat) = std::fs::read(entry.path().join("stat")) else {
            continue;
        };
        let stat = String::from_utf8_lossy(&stat);
        let Some((_, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        let mut fields = rest.split_whitespace();
        let (Some(state), Some(_ppid), Some(group)) = (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if group.parse::<i32>() != Ok(want) {
            continue;
        }
        if state != "Z" || has_live_thread(&entry.path()) {
            return true;
        }
    }
    false
}

#[cfg(target_os = "linux")]
fn has_live_thread(process: &std::path::Path) -> bool {
    let Ok(tasks) = std::fs::read_dir(process.join("task")) else {
        return false;
    };
    for task in tasks.flatten() {
        let Ok(stat) = std::fs::read(task.path().join("stat")) else {
            continue;
        };
        let stat = String::from_utf8_lossy(&stat);
        let Some((_, rest)) = stat.rsplit_once(')') else {
            continue;
        };
        let Some(state) = rest.split_whitespace().next() else {
            continue;
        };
        if state != "Z" {
            return true;
        }
    }
    false
}

#[cfg(not(target_os = "linux"))]
fn group_lives(pgid: Pid) -> bool {
    test_kill_process_group(pgid).is_ok()
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
    let (out_state, out_watch) = watch::channel(PumpState::default());
    let (err_state, err_watch) = watch::channel(PumpState::default());
    let mut out_task: JoinHandle<()> =
        tokio::spawn(pump(stdout, Stream::Stdout, tx.clone(), out_state));
    let mut err_task: JoinHandle<()> =
        tokio::spawn(pump(stderr, Stream::Stderr, tx.clone(), err_state));
    let mut out = Pipe {
        done: false,
        watching: true,
        blocked_at_exit: Duration::ZERO,
        state: out_watch,
    };
    let mut err = Pipe {
        done: false,
        watching: true,
        blocked_at_exit: Duration::ZERO,
        state: err_watch,
    };
    let mut code = None;
    let mut exited = None;
    let mut sequence: Option<Sequence> = None;

    loop {
        let killing = match &sequence {
            Some(Sequence {
                kill_at: Some(_), ..
            }) => group_lives(pgid),
            Some(Sequence { kill_at: None, .. }) | None => false,
        };
        let finished = code.is_some() && out.done && err.done;
        if finished && !killing {
            break;
        }
        let kill_at = sequence.as_ref().and_then(|seq| seq.kill_at);
        let close_at = sequence.as_ref().and_then(|seq| seq.close_at);
        let grace = exited.and_then(|at| grace_deadline(at, [&out, &err]));
        let watch_out = exited.is_some() && out.watching && !out.done;
        let watch_err = exited.is_some() && err.watching && !err.done;
        tokio::select! {
            status = child.wait(), if code.is_none() => {
                code = Some(status.map_or(UNKNOWN_STATUS_CODE, exit_code));
                if sequence.is_none() {
                    exited = Some(Instant::now());
                    out.mark_exit();
                    err.mark_exit();
                }
            }
            _ = &mut out_task, if !out.done => {
                out.done = true;
                let _ = tx.send(Event::Eof(Stream::Stdout)).await;
            }
            _ = &mut err_task, if !err.done => {
                err.done = true;
                let _ = tx.send(Event::Eof(Stream::Stderr)).await;
            }
            changed = out.state.changed(), if watch_out => {
                if changed.is_err() {
                    out.watching = false;
                }
            }
            changed = err.state.changed(), if watch_err => {
                if changed.is_err() {
                    err.watching = false;
                }
            }
            () = cancel.notified(), if sequence.is_none() => {
                exited = None;
                sequence = Some(begin_cancel(pgid));
            }
            () = sleep_until(grace.unwrap_or_else(Instant::now)), if grace.is_some() => {
                exited = None;
                sequence = Some(begin_cancel(pgid));
            }
            () = sleep_until(kill_at.unwrap_or_else(Instant::now)), if kill_at.is_some() => {
                if let Some(seq) = sequence.as_mut() {
                    seq.kill_at = None;
                }
                if group_lives(pgid) {
                    let _ = kill_process_group(pgid, Signal::KILL);
                }
            }
            () = sleep_until(close_at.unwrap_or_else(Instant::now)), if close_at.is_some() => {
                break;
            }
            () = sleep(WATCH_EVERY), if finished => {}
        }
    }

    if !out.done {
        out_task.abort();
        let _ = out_task.await;
        let _ = tx.send(Event::Eof(Stream::Stdout)).await;
    }
    if !err.done {
        err_task.abort();
        let _ = err_task.await;
        let _ = tx.send(Event::Eof(Stream::Stderr)).await;
    }
    let _ = tx.send(Event::Exit(code.unwrap_or(KILLED_CODE))).await;
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
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
    async fn a_lingering_child_that_closed_its_pipes_on_sigterm_is_still_killed() {
        let dir = TempDir::new().expect("tempdir");
        let pid_file = dir.path().join("child.pid");
        let script = format!(
            "#!/bin/sh\n(trap 'exec >&- 2>&-' TERM; while :; do sleep 1; done) &\necho $! > {}\nexit 0\n",
            pid_file.display()
        );
        let mut running = start(&dir, &script, &[]);
        let done = collect(&mut running).await;
        assert_eq!(done.code, 0);
        assert!(
            done.elapsed >= Duration::from_millis(9800),
            "{:?}",
            done.elapsed
        );
        assert!(done.elapsed < Duration::from_secs(13), "{:?}", done.elapsed);
        let pid = fs::read_to_string(&pid_file).expect("child wrote its pid");
        let stat_path = format!("/proc/{}/stat", pid.trim());
        let mut stat = fs::read_to_string(&stat_path);
        for _ in 0..40 {
            let Ok(text) = &stat else { break };
            let state = text.rsplit_once(')').map(|(_, rest)| rest.trim_start());
            if state.is_some_and(|rest| rest.starts_with('Z')) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            stat = fs::read_to_string(&stat_path);
        }
        if let Ok(stat) = stat {
            let state = stat.rsplit_once(')').map(|(_, rest)| rest.trim_start());
            assert!(state.is_some_and(|rest| rest.starts_with('Z')), "{stat}");
        }
    }

    #[tokio::test]
    async fn a_descendant_that_keeps_writing_is_cut_off_five_seconds_after_exit() {
        let dir = TempDir::new().expect("tempdir");
        let script = "#!/bin/sh\n(while :; do echo tick; sleep 1; done) &\nexit 0\n";
        let mut running = start(&dir, script, &[]);
        let done = collect(&mut running).await;
        assert_eq!(done.code, 0);
        assert!(
            done.elapsed >= Duration::from_millis(4800),
            "{:?}",
            done.elapsed
        );
        assert!(done.elapsed < Duration::from_secs(8), "{:?}", done.elapsed);
    }

    #[tokio::test]
    async fn a_slow_receiver_after_exit_loses_no_output() {
        let dir = TempDir::new().expect("tempdir");
        let script = "#!/bin/sh\n\
            for i in 1 2 3 4 5 6 7 8 9 10; do echo line-$i; sleep 0.05; done\n\
            exit 0\n";
        let mut running = start(&dir, script, &[]);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let mut stdout = Vec::new();
        for _ in 0..2 {
            let event = running.events.recv().await.expect("a line is queued");
            let Event::Chunk {
                stream: Stream::Stdout,
                data,
            } = event
            else {
                panic!("the first events are stdout lines, got {event:?}");
            };
            stdout.extend(data);
        }
        tokio::time::sleep(Duration::from_millis(16500)).await;
        let done = collect(&mut running).await;
        stdout.extend(done.stdout);
        let mut expected = String::new();
        for i in 1..=10 {
            writeln!(expected, "line-{i}").expect("writing to a String cannot fail");
        }
        assert_eq!(String::from_utf8(stdout).expect("utf-8"), expected);
        assert_eq!(done.code, 0);
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
