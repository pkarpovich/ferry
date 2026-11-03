//! The daemon: the lock, the local socket, the loopback ports and the current link.

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rustix::fs::{FlockOperation, flock};
use rustix::io::Errno;
use rustix::process::setsid;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::config::{Config, Listen};
use crate::dial;
use crate::frame::{CallId, Frame, FrameError, MAX_CHUNK, MAX_LINE, Stream, encode, read_frame};
use crate::handlers::{self, Handler, Scan, Skipped};
use crate::local::{LinkInfo, LsReply, ReloadReply, Request, ack_line, line, refusal_line};
use crate::name::HandlerName;
use crate::paths::Paths;
use crate::session::{
    CallEvent, CallFailure, OutCall, Role, Session, SessionEnd, SessionHandle, VERSION,
};

const REFUSED_CODE: u16 = 255;
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// What the daemon reads from its surroundings.
#[derive(Debug, Clone)]
pub struct Options {
    pub ssh: String,
}

impl Options {
    /// Reads `FERRY_SSH`, which defaults to `ssh`.
    #[must_use]
    pub fn from_env() -> Self {
        let ssh = std::env::var("FERRY_SSH").unwrap_or_else(|_| "ssh".to_owned());
        Self { ssh }
    }
}

struct Current {
    handle: SessionHandle,
    generation: u64,
}

struct State {
    handlers: Vec<Handler>,
    skipped: Vec<Skipped>,
    current: Option<Current>,
    running: usize,
    generation: u64,
    last_failure: Option<String>,
}

pub(crate) struct Inner {
    pub(crate) config: Config,
    pub(crate) ssh: String,
    state: Mutex<State>,
    closing: watch::Sender<bool>,
    changed: Notify,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("daemon state lock is not poisoned")
}

fn scan_folder(config: &Config) -> Scan {
    let Some(folder) = &config.handlers else {
        return Scan::default();
    };
    handlers::scan(folder)
}

impl Inner {
    pub(crate) fn closing(&self) -> watch::Receiver<bool> {
        self.closing.subscribe()
    }

    fn is_closing(&self) -> bool {
        *self.closing.borrow()
    }

    fn up_handle(&self) -> Option<SessionHandle> {
        let state = locked(&self.state);
        let current = state.current.as_ref()?;
        if !current.handle.is_up() {
            return None;
        }
        Some(current.handle.clone())
    }

    /// Records why the last link failed, unless a link is attached again meanwhile.
    pub(crate) fn note_failure(&self, message: String) {
        let mut state = locked(&self.state);
        if state.current.is_none() {
            state.last_failure = Some(message);
        }
    }

    /// Runs one link until it ends. A link already attached is dropped as lost.
    pub(crate) async fn attach<R, W>(&self, role: Role, input: R, output: W) -> SessionEnd
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let (session, generation) = {
            let mut state = locked(&self.state);
            let session = Session::new(role, state.handlers.clone(), input, output);
            let handle = session.handle();
            state.generation += 1;
            state.running += 1;
            let generation = state.generation;
            let old = state.current.replace(Current {
                handle: handle.clone(),
                generation,
            });
            if let Some(Current { handle: old, .. }) = old {
                old.close();
            }
            if self.is_closing() {
                handle.close();
            }
            (session, generation)
        };
        let end = session.run().await;
        eprintln!("ferry: link ended: {}", end.reason);
        {
            let mut state = locked(&self.state);
            let ours = match &state.current {
                Some(Current {
                    generation: current,
                    ..
                }) => *current == generation,
                None => false,
            };
            if ours {
                state.current = None;
                state.last_failure = Some(end.reason.to_string());
            }
            state.running -= 1;
        }
        self.changed.notify_waiters();
        end
    }

    async fn wait_unlinked(&self) {
        loop {
            let notified = self.changed.notified();
            let mut notified = pin!(notified);
            notified.as_mut().enable();
            if locked(&self.state).running == 0 {
                return;
            }
            notified.await;
        }
    }

    fn reload(&self) -> ReloadReply {
        let Scan { handlers, skipped } = scan_folder(&self.config);
        let count = handlers.len();
        let mut state = locked(&self.state);
        state.handlers.clone_from(&handlers);
        state.skipped.clone_from(&skipped);
        if let Some(Current { handle, .. }) = &state.current {
            handle.set_handlers(handlers);
        }
        ReloadReply {
            ok: true,
            handlers: count,
            skipped,
        }
    }

    fn ls(&self) -> LsReply {
        let state = locked(&self.state);
        let local = crate::session::infos(&state.handlers);
        let up = match &state.current {
            Some(Current { handle, .. }) if handle.is_up() => Some(handle.clone()),
            Some(_) | None => None,
        };
        let (link, remote) = match up {
            Some(handle) => (LinkInfo::Up { version: VERSION }, handle.remote_handlers()),
            None => (
                LinkInfo::Down {
                    reason: state.last_failure.clone(),
                },
                Vec::new(),
            ),
        };
        LsReply {
            ok: true,
            link,
            local,
            remote,
            skipped: state.skipped.clone(),
        }
    }
}

/// A running daemon. Dropping it releases the lock.
pub struct Daemon {
    inner: Arc<Inner>,
    paths: Paths,
    tasks: Vec<JoinHandle<()>>,
    dialer: Option<JoinHandle<()>>,
    _lock: File,
}

fn take_lock(paths: &Paths) -> Result<Option<File>> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&paths.lock)
        .with_context(|| format!("cannot open {}", paths.lock.display()))?;
    match flock(&file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {
            file.set_len(0)
                .and_then(|()| (&file).write_all(std::process::id().to_string().as_bytes()))
                .with_context(|| format!("cannot write {}", paths.lock.display()))?;
            Ok(Some(file))
        }
        Err(Errno::WOULDBLOCK) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("cannot lock {}", paths.lock.display())),
    }
}

fn bind_socket(paths: &Paths) -> Result<UnixListener> {
    match fs::remove_file(&paths.socket) {
        Ok(()) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("cannot remove {}", paths.socket.display()));
        }
    }
    let listener = UnixListener::bind(&paths.socket)
        .with_context(|| format!("cannot bind {}", paths.socket.display()))?;
    fs::set_permissions(&paths.socket, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("cannot set mode on {}", paths.socket.display()))?;
    Ok(listener)
}

impl Daemon {
    /// Starts the daemon on the host whose home is `home`. `None` when another
    /// daemon of this host already holds the lock.
    ///
    /// # Errors
    /// Fails when the config is bad, the socket path is too long or the state
    /// directory, the lock or the socket cannot be set up.
    pub async fn start(home: &Path, options: Options) -> Result<Option<Self>> {
        let paths = Paths::new(home)?;
        paths.create_state_dir()?;
        let Some(lock) = take_lock(&paths)? else {
            return Ok(None);
        };
        let config = Config::load(home)?;
        let Scan { handlers, skipped } = scan_folder(&config);
        let listener = bind_socket(&paths)?;
        let (closing, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            ssh: options.ssh,
            config,
            state: Mutex::new(State {
                handlers,
                skipped,
                current: None,
                running: 0,
                generation: 0,
                last_failure: None,
            }),
            closing,
            changed: Notify::new(),
        });

        let mut tasks = vec![tokio::spawn(accept_local(Arc::clone(&inner), listener))];
        for listen in &inner.config.listen {
            let Listen { port, handler } = listen;
            let bound = TcpListener::bind(("127.0.0.1", *port)).await;
            match bound {
                Ok(listener) => tasks.push(tokio::spawn(accept_tcp(
                    Arc::clone(&inner),
                    listener,
                    handler.clone(),
                ))),
                Err(err) => eprintln!("ferry: cannot listen on 127.0.0.1:{port}: {err}"),
            }
        }
        let dialer = inner
            .config
            .connect
            .is_some()
            .then(|| tokio::spawn(dial::run(Arc::clone(&inner))));
        Ok(Some(Self {
            inner,
            paths,
            tasks,
            dialer,
            _lock: lock,
        }))
    }

    /// Ends every call with 255, cancels the handlers, removes the socket.
    pub async fn shutdown(self) {
        let Daemon {
            inner,
            paths,
            tasks,
            dialer,
            _lock,
        } = self;
        inner.closing.send_replace(true);
        for task in &tasks {
            task.abort();
        }
        if let Some(Current { handle, .. }) = &locked(&inner.state).current {
            handle.close();
        }
        inner.wait_unlinked().await;
        if let Some(dialer) = dialer {
            let _ = dialer.await;
        }
        let _ = fs::remove_file(&paths.socket);
    }
}

/// Runs the daemon of the current user until SIGTERM or SIGINT.
///
/// # Errors
/// Fails when `$HOME` is not set, the signal handlers cannot be installed or
/// [`Daemon::start`] fails.
pub async fn run() -> Result<()> {
    match setsid() {
        Ok(_) | Err(Errno::PERM) => {}
        Err(err) => return Err(err).context("cannot start a new session"),
    }
    let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    let home = PathBuf::from(home);
    let mut term = signal(SignalKind::terminate()).context("cannot watch SIGTERM")?;
    let mut int = signal(SignalKind::interrupt()).context("cannot watch SIGINT")?;
    let Some(daemon) = Daemon::start(&home, Options::from_env()).await? else {
        return Ok(());
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    daemon.shutdown().await;
    Ok(())
}

async fn accept_local(inner: Arc<Inner>, listener: UnixListener) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(serve_client(Arc::clone(&inner), stream));
            }
            Err(err) => {
                eprintln!("ferry: accept on the local socket failed: {err}");
                sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

async fn accept_tcp(inner: Arc<Inner>, listener: TcpListener, handler: HandlerName) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(serve_listen(Arc::clone(&inner), stream, handler.clone()));
            }
            Err(err) => {
                eprintln!("ferry: accept on a listen port failed: {err}");
                sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

enum FirstLine {
    Request(Request),
    Gone,
    Bad(String),
}

async fn read_first_line(read: &mut BufReader<OwnedReadHalf>) -> FirstLine {
    let mut bytes = Vec::new();
    let mut limited = read.take(MAX_LINE as u64 + 1);
    match limited.read_until(b'\n', &mut bytes).await {
        Ok(0) | Err(_) => return FirstLine::Gone,
        Ok(_) => {}
    }
    if bytes.last() != Some(&b'\n') {
        if bytes.len() > MAX_LINE {
            return FirstLine::Bad("request line too long".to_owned());
        }
        return FirstLine::Gone;
    }
    match serde_json::from_slice(&bytes) {
        Ok(request) => FirstLine::Request(request),
        Err(err) => FirstLine::Bad(format!("bad request: {err}")),
    }
}

async fn refuse(write: &mut OwnedWriteHalf, code: u16, error: &str) {
    let _ = write.write_all(&refusal_line(code, error)).await;
    let _ = write.shutdown().await;
}

async fn serve_client(inner: Arc<Inner>, stream: UnixStream) {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let request = match read_first_line(&mut read).await {
        FirstLine::Request(request) => request,
        FirstLine::Gone => return,
        FirstLine::Bad(error) => {
            refuse(&mut write, REFUSED_CODE, &error).await;
            return;
        }
    };
    match request {
        Request::Peer => serve_peer(&inner, read, write).await,
        Request::Call { name, argv } => serve_call(&inner, read, write, name, argv).await,
        Request::Ls => {
            let _ = write.write_all(&line(&inner.ls())).await;
        }
        Request::Reload => {
            let _ = write.write_all(&line(&inner.reload())).await;
        }
    }
}

async fn serve_peer(inner: &Inner, read: BufReader<OwnedReadHalf>, mut write: OwnedWriteHalf) {
    if inner.is_closing() {
        refuse(&mut write, REFUSED_CODE, "daemon is shutting down").await;
        return;
    }
    if write.write_all(&ack_line()).await.is_err() {
        return;
    }
    inner.attach(Role::Far, read, write).await;
}

fn local_frame(event: CallEvent) -> (Frame, bool) {
    let id = CallId(0);
    match event {
        CallEvent::Chunk { stream, data } => (Frame::Chunk { id, stream, data }, false),
        CallEvent::Eof(stream) => (Frame::Eof { id, stream }, false),
        CallEvent::Exit(code) => (Frame::Exit { id, code }, true),
        CallEvent::Error(CallFailure { code, message }) => {
            (Frame::Error { id, code, message }, true)
        }
    }
}

async fn serve_call(
    inner: &Inner,
    read: BufReader<OwnedReadHalf>,
    mut write: OwnedWriteHalf,
    name: String,
    argv: Vec<String>,
) {
    let Some(handle) = inner.up_handle() else {
        refuse(&mut write, REFUSED_CODE, "link not up").await;
        return;
    };
    let OutCall { input, mut events } = match handle.open(&name, argv).await {
        Ok(call) => call,
        Err(CallFailure { code, message }) => {
            refuse(&mut write, code, &message).await;
            return;
        }
    };
    if write.write_all(&ack_line()).await.is_err() {
        input.cancel().await;
        return;
    }
    let feeder = tokio::spawn(feed_call(read, input.clone()));
    while let Some(event) = events.next().await {
        let (frame, last) = local_frame(event);
        if write.write_all(&encode(&frame)).await.is_err() {
            input.cancel().await;
            break;
        }
        if last {
            break;
        }
    }
    feeder.abort();
}

async fn feed_call(mut read: BufReader<OwnedReadHalf>, input: crate::session::CallInput) {
    loop {
        let frame = match read_frame(&mut read).await {
            Ok(Some(frame)) => frame,
            Err(FrameError::Unknown(_)) => continue,
            Ok(None) | Err(FrameError::TooLong | FrameError::Malformed(_) | FrameError::Io(_)) => {
                input.cancel().await;
                return;
            }
        };
        match frame {
            Frame::Chunk {
                id: _,
                stream,
                data,
            } => match stream {
                Stream::Stdin => {
                    if !input.send_stdin(&data).await {
                        return;
                    }
                }
                Stream::Stdout | Stream::Stderr => {}
            },
            Frame::Eof { id: _, stream } => match stream {
                Stream::Stdin => {
                    if !input.close_stdin().await {
                        return;
                    }
                }
                Stream::Stdout | Stream::Stderr => {}
            },
            Frame::Hello { .. }
            | Frame::Handlers { .. }
            | Frame::Ping
            | Frame::Open { .. }
            | Frame::Exit { .. }
            | Frame::Error { .. }
            | Frame::Cancel { .. } => {}
        }
    }
}

async fn serve_listen(inner: Arc<Inner>, stream: TcpStream, handler: HandlerName) {
    let Some(handle) = inner.up_handle() else {
        return;
    };
    let Ok(OutCall { input, mut events }) = handle.open(handler.as_str(), Vec::new()).await else {
        return;
    };
    let (read, mut write) = stream.into_split();
    let feeder = tokio::spawn(feed_socket(read, input.clone()));
    while let Some(event) = events.next().await {
        match event {
            CallEvent::Chunk { stream, data } => match stream {
                Stream::Stdout => {
                    if write.write_all(&data).await.is_err() {
                        input.cancel().await;
                        break;
                    }
                }
                Stream::Stderr => {
                    let _ = tokio::io::stderr().write_all(&data).await;
                }
                Stream::Stdin => {}
            },
            CallEvent::Eof(stream) => match stream {
                Stream::Stdout => {
                    let _ = write.shutdown().await;
                }
                Stream::Stdin | Stream::Stderr => {}
            },
            CallEvent::Exit(_) | CallEvent::Error(_) => break,
        }
    }
    feeder.abort();
}

async fn feed_socket(mut read: tokio::net::tcp::OwnedReadHalf, input: crate::session::CallInput) {
    let mut buffer = vec![0u8; MAX_CHUNK];
    loop {
        let count = read.read(&mut buffer).await.unwrap_or(0);
        if count == 0 {
            input.cancel().await;
            return;
        }
        if !input.send_stdin(&buffer[..count]).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local::Refusal;
    use rustix::process::{Pid, test_kill_process};
    use tempfile::TempDir;
    use tokio::io::{duplex, split};
    use tokio::time::timeout;

    const WAIT: Duration = Duration::from_secs(15);

    fn temp_home() -> TempDir {
        tempfile::Builder::new()
            .prefix("fd")
            .tempdir_in("/tmp")
            .expect("temp home")
    }

    fn write_file(path: &Path, text: &str, mode: u32) {
        fs::create_dir_all(path.parent().expect("has a parent")).expect("parent dir");
        fs::write(path, text).expect("file written");
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("mode set");
    }

    fn write_config(home: &Path, text: &str) {
        write_file(&Config::path(home), text, 0o644);
    }

    fn write_handler(home: &Path, file: &str, body: &str) {
        write_file(&home.join("handlers").join(file), body, 0o755);
    }

    fn free_port() -> u16 {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        socket.local_addr().expect("has an address").port()
    }

    fn options() -> Options {
        Options {
            ssh: "ssh".to_owned(),
        }
    }

    async fn start(home: &Path) -> Daemon {
        Daemon::start(home, options())
            .await
            .expect("daemon starts")
            .expect("no other daemon runs")
    }

    async fn until<F: FnMut() -> bool>(what: &str, mut ready: F) {
        let waiting = async {
            while !ready() {
                sleep(Duration::from_millis(20)).await;
            }
        };
        assert!(
            timeout(WAIT, waiting).await.is_ok(),
            "timed out waiting for {what}"
        );
    }

    fn link(a: &Daemon, b: &Daemon) {
        let (near, far) = duplex(1 << 20);
        let (near_read, near_write) = split(near);
        let (far_read, far_write) = split(far);
        let a = Arc::clone(&a.inner);
        let b = Arc::clone(&b.inner);
        tokio::spawn(async move { a.attach(Role::Dialer, near_read, near_write).await });
        tokio::spawn(async move { b.attach(Role::Far, far_read, far_write).await });
    }

    async fn linked(a: &Daemon, b: &Daemon) {
        link(a, b);
        until("the link", || {
            a.inner.up_handle().is_some() && b.inner.up_handle().is_some()
        })
        .await;
    }

    async fn connect(home: &Path, first: &[u8]) -> BufReader<UnixStream> {
        let paths = Paths::new(home).expect("short path");
        let mut stream = UnixStream::connect(&paths.socket)
            .await
            .expect("socket connects");
        stream.write_all(first).await.expect("first line sent");
        BufReader::new(stream)
    }

    async fn reply_line(stream: &mut BufReader<UnixStream>) -> String {
        let mut text = String::new();
        timeout(WAIT, stream.read_line(&mut text))
            .await
            .expect("a reply in time")
            .expect("reply read");
        text
    }

    async fn read_pid(path: PathBuf) -> i32 {
        let mut pid = None;
        let waiting = async {
            loop {
                if let Ok(text) = fs::read_to_string(&path)
                    && let Ok(number) = text.trim().parse::<i32>()
                {
                    pid = Some(number);
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        };
        assert!(timeout(WAIT, waiting).await.is_ok(), "no pid file");
        pid.expect("pid was read")
    }

    fn gone(pid: i32) -> bool {
        let Some(pid) = Pid::from_raw(pid) else {
            return true;
        };
        test_kill_process(pid).is_err()
    }

    fn cat_handler(home: &Path) {
        write_handler(home, "cat", "#!/bin/sh\n# @ferry.name cat\nexec cat\n");
    }

    fn listen_config(port: u16, handler: &str) -> String {
        format!("[[listen]]\nport = {port}\nhandler = \"{handler}\"\n")
    }

    #[tokio::test]
    async fn a_second_daemon_exits_while_the_first_runs() {
        let home = temp_home();
        let first = start(home.path()).await;
        let second = Daemon::start(home.path(), options())
            .await
            .expect("second start is not an error");
        assert!(second.is_none());
        first.shutdown().await;
    }

    #[tokio::test]
    async fn a_second_daemon_exits_zero_even_when_the_config_is_broken() {
        let home = temp_home();
        let first = start(home.path()).await;
        fs::create_dir_all(home.path().join(".config/ferry")).expect("config dir");
        fs::write(home.path().join(".config/ferry/config.toml"), "bogus = 1\n")
            .expect("write config");
        let second = Daemon::start(home.path(), options())
            .await
            .expect("a held lock wins over a bad config");
        assert!(second.is_none());
        first.shutdown().await;
    }

    #[tokio::test]
    async fn a_socket_left_by_a_killed_daemon_does_not_stop_the_next() {
        let home = temp_home();
        let paths = Paths::new(home.path()).expect("short path");
        paths.create_state_dir().expect("state dir");
        drop(std::os::unix::net::UnixListener::bind(&paths.socket).expect("stale socket"));
        assert!(paths.socket.exists());
        let daemon = start(home.path()).await;
        let mut stream = connect(home.path(), b"{\"kind\":\"ls\"}\n").await;
        assert!(reply_line(&mut stream).await.contains("\"ok\":true"));
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn the_socket_is_private_and_shutdown_removes_it() {
        let home = temp_home();
        let daemon = start(home.path()).await;
        let paths = Paths::new(home.path()).expect("short path");
        let mode = fs::metadata(&paths.socket)
            .expect("socket exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        daemon.shutdown().await;
        assert!(!paths.socket.exists());
    }

    #[tokio::test]
    async fn a_busy_listen_port_is_logged_and_the_others_still_start() {
        let home = temp_home();
        let busy = std::net::TcpListener::bind("127.0.0.1:0").expect("a busy port");
        let busy_port = busy.local_addr().expect("has an address").port();
        let free = free_port();
        let text = format!(
            "{}\n{}",
            listen_config(busy_port, "cat"),
            listen_config(free, "cat")
        );
        write_config(home.path(), &text);
        let daemon = start(home.path()).await;
        let mut client = TcpStream::connect(("127.0.0.1", free))
            .await
            .expect("the free port listens");
        let mut rest = Vec::new();
        timeout(WAIT, client.read_to_end(&mut rest))
            .await
            .expect("closed at once")
            .expect("read ends");
        assert_eq!(rest, Vec::<u8>::new());
        let mut stream = connect(home.path(), b"{\"kind\":\"ls\"}\n").await;
        assert!(reply_line(&mut stream).await.contains("\"ok\":true"));
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn a_malformed_first_line_gets_a_refusal_and_a_close() {
        let home = temp_home();
        let daemon = start(home.path()).await;
        let mut stream = connect(home.path(), b"garbage\n").await;
        let mut text = String::new();
        timeout(WAIT, stream.read_to_string(&mut text))
            .await
            .expect("closed in time")
            .expect("read ends");
        let refusal: Refusal = serde_json::from_str(&text).expect("a refusal line");
        assert!(!refusal.ok);
        assert_eq!(refusal.code, 255);
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn a_call_is_refused_while_the_link_is_down() {
        let home = temp_home();
        let daemon = start(home.path()).await;
        let request = line(&Request::Call {
            name: "cat".to_owned(),
            argv: Vec::new(),
        });
        let mut stream = connect(home.path(), &request).await;
        let refusal: Refusal =
            serde_json::from_str(&reply_line(&mut stream).await).expect("a refusal line");
        assert_eq!((refusal.ok, refusal.code), (false, 255));
        assert_eq!(refusal.error, "link not up");
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn a_tcp_client_that_leaves_cancels_a_silent_handler() {
        let (near, far) = (temp_home(), temp_home());
        let port = free_port();
        write_config(near.path(), &listen_config(port, "silent"));
        write_config(far.path(), "handlers = \"~/handlers\"\n");
        let pid_file = far.path().join("pid");
        let body = format!(
            "#!/bin/sh\n# @ferry.name silent\necho $$ > {}\nexec sleep 300\n",
            pid_file.display()
        );
        write_handler(far.path(), "silent", &body);
        let (a, b) = (start(near.path()).await, start(far.path()).await);
        linked(&a, &b).await;
        let mut client = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connects");
        client.write_all(b"request").await.expect("request sent");
        let pid = read_pid(pid_file).await;
        drop(client);
        until("the handler to stop", || gone(pid)).await;
        a.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn a_tcp_client_that_answers_and_stays_gets_the_whole_reply() {
        let (near, far) = (temp_home(), temp_home());
        let port = free_port();
        write_config(near.path(), &listen_config(port, "reply"));
        write_config(far.path(), "handlers = \"~/handlers\"\n");
        write_handler(
            far.path(),
            "reply",
            "#!/bin/sh\n# @ferry.name reply\nhead -c 4 > /dev/null\nprintf pong\n",
        );
        let (a, b) = (start(near.path()).await, start(far.path()).await);
        linked(&a, &b).await;
        let mut client = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connects");
        client.write_all(b"ping").await.expect("request sent");
        let mut answer = Vec::new();
        timeout(WAIT, client.read_to_end(&mut answer))
            .await
            .expect("answered in time")
            .expect("read ends");
        assert_eq!(answer, b"pong");
        a.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn a_tcp_client_that_disconnects_while_the_handler_writes_cancels_the_call() {
        let (near, far) = (temp_home(), temp_home());
        let port = free_port();
        write_config(near.path(), &listen_config(port, "spew"));
        write_config(far.path(), "handlers = \"~/handlers\"\n");
        let pid_file = far.path().join("pid");
        let body = format!(
            "#!/bin/sh\n# @ferry.name spew\necho $$ > {}\nexec yes\n",
            pid_file.display()
        );
        write_handler(far.path(), "spew", &body);
        let (a, b) = (start(near.path()).await, start(far.path()).await);
        linked(&a, &b).await;
        let mut client = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connects");
        let mut some = [0u8; 16];
        client.read_exact(&mut some).await.expect("output arrives");
        let pid = read_pid(pid_file).await;
        drop(client);
        until("the handler to stop", || gone(pid)).await;
        a.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn a_handler_that_closes_stdout_early_half_closes_the_socket_but_still_reads() {
        let (near, far) = (temp_home(), temp_home());
        let port = free_port();
        write_config(near.path(), &listen_config(port, "quiet"));
        write_config(far.path(), "handlers = \"~/handlers\"\n");
        let out = far.path().join("out");
        let body = format!(
            "#!/bin/sh\n# @ferry.name quiet\nexec >&-\ncat > {}\n",
            out.display()
        );
        write_handler(far.path(), "quiet", &body);
        let (a, b) = (start(near.path()).await, start(far.path()).await);
        linked(&a, &b).await;
        let mut client = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connects");
        let mut first = [0u8; 1];
        let read = timeout(WAIT, client.read(&mut first))
            .await
            .expect("EOF in time")
            .expect("read ends");
        assert_eq!(read, 0);
        client.write_all(b"late").await.expect("can still send");
        until("the handler to read", || {
            fs::read_to_string(&out).is_ok_and(|text| text == "late")
        })
        .await;
        drop(client);
        a.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn a_local_call_round_trips_and_closing_the_socket_cancels_it() {
        let (near, far) = (temp_home(), temp_home());
        write_config(far.path(), "handlers = \"~/handlers\"\n");
        cat_handler(far.path());
        let pid_file = far.path().join("pid");
        let body = format!(
            "#!/bin/sh\n# @ferry.name hang\necho $$ > {}\nexec sleep 60\n",
            pid_file.display()
        );
        write_handler(far.path(), "hang", &body);
        let (a, b) = (start(near.path()).await, start(far.path()).await);
        linked(&a, &b).await;

        let request = line(&Request::Call {
            name: "cat".to_owned(),
            argv: Vec::new(),
        });
        let mut stream = connect(near.path(), &request).await;
        assert_eq!(reply_line(&mut stream).await, "{\"ok\":true}\n");
        let id = CallId(0);
        let chunk = Frame::Chunk {
            id,
            stream: Stream::Stdin,
            data: b"hi".to_vec(),
        };
        stream.write_all(&encode(&chunk)).await.expect("stdin sent");
        let eof = Frame::Eof {
            id,
            stream: Stream::Stdin,
        };
        stream.write_all(&encode(&eof)).await.expect("eof sent");
        let mut output = Vec::new();
        let mut code = None;
        while code.is_none() {
            let frame = timeout(WAIT, read_frame(&mut stream))
                .await
                .expect("a frame in time")
                .expect("frame decodes")
                .expect("not closed");
            match frame {
                Frame::Chunk {
                    stream: Stream::Stdout,
                    data,
                    ..
                } => output.extend(data),
                Frame::Exit { code: exit, .. } => code = Some(exit),
                Frame::Eof { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(output, b"hi");
        assert_eq!(code, Some(0));

        let request = line(&Request::Call {
            name: "hang".to_owned(),
            argv: Vec::new(),
        });
        let mut stream = connect(near.path(), &request).await;
        assert_eq!(reply_line(&mut stream).await, "{\"ok\":true}\n");
        let pid = read_pid(pid_file).await;
        drop(stream);
        until("the handler to stop", || gone(pid)).await;
        a.shutdown().await;
        b.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_ends_a_call_in_flight_with_255() {
        let (near, far) = (temp_home(), temp_home());
        write_config(far.path(), "handlers = \"~/handlers\"\n");
        write_handler(
            far.path(),
            "hang",
            "#!/bin/sh\n# @ferry.name hang\nexec sleep 60\n",
        );
        let (a, b) = (start(near.path()).await, start(far.path()).await);
        linked(&a, &b).await;
        let request = line(&Request::Call {
            name: "hang".to_owned(),
            argv: Vec::new(),
        });
        let mut stream = connect(near.path(), &request).await;
        assert_eq!(reply_line(&mut stream).await, "{\"ok\":true}\n");
        let stopping = tokio::spawn(a.shutdown());
        let frame = timeout(WAIT, read_frame(&mut stream))
            .await
            .expect("a frame in time")
            .expect("frame decodes")
            .expect("not closed");
        match frame {
            Frame::Error { code, .. } => assert_eq!(code, 255),
            other => panic!("expected an error frame, got {other:?}"),
        }
        timeout(WAIT, stopping)
            .await
            .expect("shutdown ends")
            .expect("task joins");
        b.shutdown().await;
    }

    #[tokio::test]
    async fn a_new_peer_replaces_the_connected_one() {
        let home = temp_home();
        let daemon = start(home.path()).await;
        let mut first = connect(home.path(), b"{\"kind\":\"peer\"}\n").await;
        assert_eq!(reply_line(&mut first).await, "{\"ok\":true}\n");
        assert!(reply_line(&mut first).await.contains("\"kind\":\"hello\""));
        let mut second = connect(home.path(), b"{\"kind\":\"peer\"}\n").await;
        assert_eq!(reply_line(&mut second).await, "{\"ok\":true}\n");
        let mut rest = String::new();
        timeout(WAIT, first.read_to_string(&mut rest))
            .await
            .expect("the old link closes")
            .expect("read ends");
        assert_eq!(rest, "");
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn reload_rescans_and_tells_the_other_side() {
        let (near, far) = (temp_home(), temp_home());
        write_config(far.path(), "handlers = \"~/handlers\"\n");
        let (a, b) = (start(near.path()).await, start(far.path()).await);
        linked(&a, &b).await;
        cat_handler(far.path());
        let mut stream = connect(far.path(), b"{\"kind\":\"reload\"}\n").await;
        let reply = reply_line(&mut stream).await;
        assert!(reply.contains("\"handlers\":1"), "{reply}");
        until("the new handler to show", || a.inner.ls().remote.len() == 1).await;
        a.shutdown().await;
        b.shutdown().await;
    }

    fn fake_ssh(home: &Path, record: &Path) -> String {
        let script = home.join("ssh");
        let body = format!(
            "#!/bin/sh\necho \"$@\" > {}\nprintf '{{\"kind\":\"hello\",\"version\":1,\"kinds\":[],\"handlers\":[{{\"name\":\"x\",\"desc\":\"d\"}}]}}\\n'\ncat > /dev/null\n",
            record.display()
        );
        write_file(&script, &body, 0o755);
        script.display().to_string()
    }

    #[tokio::test]
    async fn the_dialer_runs_ssh_and_brings_the_link_up() {
        let home = temp_home();
        write_config(
            home.path(),
            "connect = \"far\"\nremote_ferry = \"/opt/ferry\"\n",
        );
        let record = home.path().join("args");
        let ssh = fake_ssh(home.path(), &record);
        let daemon = Daemon::start(home.path(), Options { ssh })
            .await
            .expect("daemon starts")
            .expect("no other daemon runs");
        until("the link", || daemon.inner.up_handle().is_some()).await;
        let reply = daemon.inner.ls();
        assert_eq!(reply.link, LinkInfo::Up { version: 1 });
        assert_eq!(reply.remote.len(), 1);
        let args = fs::read_to_string(&record).expect("ssh was run");
        assert_eq!(
            args.trim(),
            "-T -o BatchMode=yes -o ConnectTimeout=5 far /opt/ferry peer"
        );
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn a_dialer_that_cannot_start_ssh_reports_why() {
        let home = temp_home();
        write_config(home.path(), "connect = \"far\"\n");
        let ssh = home.path().join("missing").display().to_string();
        let daemon = Daemon::start(home.path(), Options { ssh })
            .await
            .expect("daemon starts")
            .expect("no other daemon runs");
        until("the failure", || match daemon.inner.ls().link {
            LinkInfo::Down { reason } => reason.is_some(),
            LinkInfo::Up { .. } => false,
        })
        .await;
        daemon.shutdown().await;
    }
}
