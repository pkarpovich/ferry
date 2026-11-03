//! One ferry link: hello, ping, and the calls either side opens.

use std::collections::HashMap;
use std::io;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::ChildStdin;
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval_at, timeout};

use crate::frame::{
    CallId, Frame, FrameError, HandlerInfo, KNOWN_KINDS, MAX_CHUNK, MAX_LINE, Stream, encode,
    read_frame,
};
use crate::handlers::Handler;
use crate::runner::{self, Event};
use crate::writer::{self, CallSlot, LinkSender};

pub const VERSION: u32 = 1;
pub const MAX_CALLS: usize = 32;
pub const MAX_UNREAD: usize = 16 * 1024 * 1024;
pub const PING_EVERY: Duration = Duration::from_secs(10);
pub const SILENCE: Duration = Duration::from_secs(30);

const NO_HANDLER_CODE: u16 = 127;
const CANNOT_START_CODE: u16 = 126;
const FAILED_CODE: u16 = 255;

/// Which end of the ssh stream this host is. It decides the parity of call ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Dialer,
    Far,
}

impl Role {
    fn first_id(self) -> u64 {
        match self {
            Role::Dialer => 1,
            Role::Far => 2,
        }
    }
}

/// A call that failed before or instead of an exit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallFailure {
    pub code: u16,
    pub message: String,
}

impl CallFailure {
    fn failed(message: &str) -> Self {
        Self {
            code: FAILED_CODE,
            message: message.to_owned(),
        }
    }
}

/// What the caller of a call receives, in order. `Exit` and `Error` end it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallEvent {
    Chunk { stream: Stream, data: Vec<u8> },
    Eof(Stream),
    Exit(i32),
    Error(CallFailure),
}

/// Why a link ended.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("the link was closed")]
    Closed,
    #[error("no frame for {} s", SILENCE.as_secs())]
    Silent,
    #[error("the other side speaks version {0}, this side speaks {VERSION}")]
    Version(u32),
    #[error("the first frame was not hello")]
    NoHello,
    #[error("protocol error: {0}")]
    Protocol(#[from] FrameError),
    #[error("write failed: {0}")]
    Write(io::Error),
}

#[derive(Debug)]
pub struct SessionEnd {
    pub reason: SessionError,
    pub established: bool,
}

enum StdinMsg {
    Data(Vec<u8>),
    Eof,
}

enum Ctl {
    Cancel,
    Overflow,
    LinkLost,
}

struct Inbound {
    stdin: mpsc::UnboundedSender<StdinMsg>,
    ctl: mpsc::UnboundedSender<Ctl>,
    unread: Arc<AtomicUsize>,
    overflowed: bool,
}

struct Outbound {
    events: mpsc::UnboundedSender<CallEvent>,
    unread: Arc<AtomicUsize>,
    slot: CallSlot,
}

struct Shared {
    next_id: AtomicU64,
    local: Mutex<Vec<Handler>>,
    remote: Mutex<Vec<HandlerInfo>>,
    up: watch::Sender<bool>,
    established: AtomicBool,
    outbound: Mutex<HashMap<CallId, Outbound>>,
    inbound: Mutex<HashMap<CallId, Inbound>>,
    drained: Notify,
    stop: Notify,
    link: LinkSender,
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect("session state lock is not poisoned")
}

fn infos(handlers: &[Handler]) -> Vec<HandlerInfo> {
    let mut list = Vec::new();
    for Handler {
        name,
        desc,
        path: _,
    } in handlers
    {
        list.push(HandlerInfo {
            name: name.to_string(),
            desc: desc.clone(),
        });
    }
    list
}

type Input = Box<dyn AsyncRead + Send + Unpin>;

/// One link between two hosts. Create it, take its handle, then `run` it.
pub struct Session {
    shared: Arc<Shared>,
    input: Input,
    writer: JoinHandle<io::Result<()>>,
}

impl Session {
    /// Starts the writer task on `output`. Nothing is sent until `run`.
    #[must_use]
    pub fn new<R, W>(role: Role, handlers: Vec<Handler>, input: R, output: W) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let (link, writer) = writer::spawn(output);
        let (up, _) = watch::channel(false);
        let shared = Arc::new(Shared {
            next_id: AtomicU64::new(role.first_id()),
            local: Mutex::new(handlers),
            remote: Mutex::new(Vec::new()),
            up,
            established: AtomicBool::new(false),
            outbound: Mutex::new(HashMap::new()),
            inbound: Mutex::new(HashMap::new()),
            drained: Notify::new(),
            stop: Notify::new(),
            link,
        });
        Self {
            shared,
            input: Box::new(input),
            writer,
        }
    }

    #[must_use]
    pub fn handle(&self) -> SessionHandle {
        SessionHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Runs the link until it ends. Every call in flight has ended, and every
    /// handler this side ran has stopped, when this returns.
    pub async fn run(self) -> SessionEnd {
        let Session {
            shared,
            input,
            mut writer,
        } = self;
        let mut kinds = Vec::new();
        for kind in KNOWN_KINDS {
            kinds.push(kind.to_owned());
        }
        let hello = Frame::Hello {
            version: VERSION,
            kinds,
            handlers: infos(&locked(&shared.local)),
        };
        shared.link.send_link(hello);

        let mut input = BufReader::new(input);
        let reason = {
            let mut reading = pin!(read_loop(&shared, &mut input));
            let mut ping = interval_at(Instant::now() + PING_EVERY, PING_EVERY);
            loop {
                tokio::select! {
                    reason = &mut reading => break reason,
                    () = shared.stop.notified() => break SessionError::Closed,
                    _ = ping.tick() => {
                        shared.link.send_link(Frame::Ping);
                    }
                    result = &mut writer => break match result {
                        Ok(Ok(())) | Err(_) => SessionError::Closed,
                        Ok(Err(err)) => SessionError::Write(err),
                    },
                }
            }
        };
        writer.abort();
        lose(&shared);
        wait_drained(&shared).await;
        SessionEnd {
            reason,
            established: shared.established.load(Ordering::Relaxed),
        }
    }
}

/// Reaches into a running session from outside: open calls, change handlers, read state.
#[derive(Clone)]
pub struct SessionHandle {
    shared: Arc<Shared>,
}

impl SessionHandle {
    #[must_use]
    pub fn is_up(&self) -> bool {
        *self.shared.up.borrow()
    }

    /// Ends the link as lost. `run` returns after every call has ended.
    pub fn close(&self) {
        self.shared.stop.notify_one();
    }

    #[must_use]
    pub fn up(&self) -> watch::Receiver<bool> {
        self.shared.up.subscribe()
    }

    #[must_use]
    pub fn remote_handlers(&self) -> Vec<HandlerInfo> {
        locked(&self.shared.remote).clone()
    }

    /// Replaces this host's handlers and tells the other side when the link is up.
    pub fn set_handlers(&self, handlers: Vec<Handler>) {
        let list = infos(&handlers);
        *locked(&self.shared.local) = handlers;
        if self.is_up() {
            self.shared
                .link
                .send_link(Frame::Handlers { handlers: list });
        }
    }

    /// Opens a call to the handler `name` on the other side.
    ///
    /// # Errors
    ///
    /// 255 when the link is not up, was lost meanwhile, or the `open` frame
    /// would pass 128 KiB.
    pub async fn open(&self, name: &str, argv: Vec<String>) -> Result<OutCall, CallFailure> {
        if !self.is_up() {
            return Err(CallFailure::failed("link not up"));
        }
        let id = CallId(self.shared.next_id.fetch_add(2, Ordering::Relaxed));
        let frame = Frame::Open {
            id,
            name: name.to_owned(),
            argv,
        };
        if encode(&frame).len() > MAX_LINE {
            return Err(CallFailure::failed("arguments too large"));
        }
        let slot = self.shared.link.call_slot(id);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let unread = Arc::new(AtomicUsize::new(0));
        locked(&self.shared.outbound).insert(
            id,
            Outbound {
                events: events_tx,
                unread: Arc::clone(&unread),
                slot: slot.clone(),
            },
        );
        if !self.is_up() || !slot.send(frame).await {
            locked(&self.shared.outbound).remove(&id);
            return Err(CallFailure::failed("link lost"));
        }
        Ok(OutCall {
            input: CallInput {
                id,
                slot,
                shared: Arc::clone(&self.shared),
            },
            events: CallEvents {
                rx: events_rx,
                unread,
            },
        })
    }
}

/// A call this side opened.
pub struct OutCall {
    pub input: CallInput,
    pub events: CallEvents,
}

/// The caller's way of feeding stdin to a call and cancelling it.
#[derive(Clone)]
pub struct CallInput {
    id: CallId,
    slot: CallSlot,
    shared: Arc<Shared>,
}

impl CallInput {
    /// Sends stdin data in chunks of at most 64 KiB. False when the link is gone.
    pub async fn send_stdin(&self, data: &[u8]) -> bool {
        for piece in data.chunks(MAX_CHUNK) {
            let frame = Frame::Chunk {
                id: self.id,
                stream: Stream::Stdin,
                data: piece.to_vec(),
            };
            if !self.slot.send(frame).await {
                return false;
            }
        }
        true
    }

    pub async fn close_stdin(&self) -> bool {
        self.slot
            .send(Frame::Eof {
                id: self.id,
                stream: Stream::Stdin,
            })
            .await
    }

    /// Tells the other side to stop the handler. Nothing more is delivered to the events.
    pub async fn cancel(&self) {
        let removed = locked(&self.shared.outbound).remove(&self.id);
        if removed.is_none() {
            return;
        }
        self.slot.send(Frame::Cancel { id: self.id }).await;
    }
}

/// What a call produced. Reading it lowers the call's unread count.
pub struct CallEvents {
    rx: mpsc::UnboundedReceiver<CallEvent>,
    unread: Arc<AtomicUsize>,
}

impl CallEvents {
    pub async fn next(&mut self) -> Option<CallEvent> {
        let event = self.rx.recv().await?;
        if let CallEvent::Chunk { data, .. } = &event {
            self.unread.fetch_sub(data.len(), Ordering::Relaxed);
        }
        Some(event)
    }
}

async fn next_frame(input: &mut BufReader<Input>) -> Result<Frame, SessionError> {
    loop {
        let Ok(read) = timeout(SILENCE, read_frame(input)).await else {
            return Err(SessionError::Silent);
        };
        match read {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => return Err(SessionError::Closed),
            Err(FrameError::Unknown(_)) => {}
            Err(err @ (FrameError::TooLong | FrameError::Malformed(_) | FrameError::Io(_))) => {
                return Err(SessionError::Protocol(err));
            }
        }
    }
}

async fn read_loop(shared: &Arc<Shared>, input: &mut BufReader<Input>) -> SessionError {
    let first = match next_frame(input).await {
        Ok(frame) => frame,
        Err(err) => return err,
    };
    let Frame::Hello {
        version, handlers, ..
    } = first
    else {
        return SessionError::NoHello;
    };
    if version != VERSION {
        return SessionError::Version(version);
    }
    *locked(&shared.remote) = handlers;
    shared.established.store(true, Ordering::Relaxed);
    shared.up.send_replace(true);

    loop {
        let frame = match next_frame(input).await {
            Ok(frame) => frame,
            Err(err) => return err,
        };
        handle_frame(shared, frame);
    }
}

fn handle_frame(shared: &Arc<Shared>, frame: Frame) {
    match frame {
        Frame::Hello { .. } | Frame::Ping => {}
        Frame::Handlers { handlers } => *locked(&shared.remote) = handlers,
        Frame::Open { id, name, argv } => open_inbound(shared, id, name, argv),
        Frame::Chunk { id, stream, data } => match stream {
            Stream::Stdin => inbound_stdin(shared, id, StdinMsg::Data(data)),
            Stream::Stdout | Stream::Stderr => outbound_chunk(shared, id, stream, data),
        },
        Frame::Eof { id, stream } => match stream {
            Stream::Stdin => inbound_stdin(shared, id, StdinMsg::Eof),
            Stream::Stdout | Stream::Stderr => {
                if let Some(entry) = locked(&shared.outbound).get(&id) {
                    let _ = entry.events.send(CallEvent::Eof(stream));
                }
            }
        },
        Frame::Exit { id, code } => outbound_finish(shared, id, CallEvent::Exit(code)),
        Frame::Error { id, code, message } => {
            outbound_finish(shared, id, CallEvent::Error(CallFailure { code, message }));
        }
        Frame::Cancel { id } => {
            if let Some(entry) = locked(&shared.inbound).get(&id) {
                let _ = entry.ctl.send(Ctl::Cancel);
            }
        }
    }
}

fn outbound_finish(shared: &Shared, id: CallId, event: CallEvent) {
    let Some(Outbound { events, .. }) = locked(&shared.outbound).remove(&id) else {
        return;
    };
    let _ = events.send(event);
}

fn outbound_chunk(shared: &Shared, id: CallId, stream: Stream, data: Vec<u8>) {
    let mut outbound = locked(&shared.outbound);
    let Some(entry) = outbound.get(&id) else {
        return;
    };
    let added = data.len();
    let total = entry.unread.fetch_add(added, Ordering::Relaxed) + added;
    let _ = entry.events.send(CallEvent::Chunk { stream, data });
    if total <= MAX_UNREAD {
        return;
    }
    let Some(Outbound { events, slot, .. }) = outbound.remove(&id) else {
        return;
    };
    drop(outbound);
    let _ = events.send(CallEvent::Error(CallFailure::failed("unread limit")));
    tokio::spawn(async move {
        slot.send(Frame::Cancel { id }).await;
    });
}

fn inbound_stdin(shared: &Shared, id: CallId, msg: StdinMsg) {
    let mut inbound = locked(&shared.inbound);
    let Some(entry) = inbound.get_mut(&id) else {
        return;
    };
    let Inbound {
        stdin,
        ctl,
        unread,
        overflowed,
    } = entry;
    if *overflowed {
        return;
    }
    let added = match &msg {
        StdinMsg::Data(data) => data.len(),
        StdinMsg::Eof => 0,
    };
    let total = unread.fetch_add(added, Ordering::Relaxed) + added;
    let _ = stdin.send(msg);
    if total > MAX_UNREAD {
        *overflowed = true;
        let _ = ctl.send(Ctl::Overflow);
    }
}

fn open_inbound(shared: &Arc<Shared>, id: CallId, name: String, argv: Vec<String>) {
    let (stdin_tx, stdin_rx) = mpsc::unbounded_channel();
    let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
    let unread = Arc::new(AtomicUsize::new(0));
    let mut inbound = locked(&shared.inbound);
    if inbound.contains_key(&id) {
        return;
    }
    if inbound.len() >= MAX_CALLS {
        drop(inbound);
        let slot = shared.link.call_slot(id);
        tokio::spawn(async move {
            slot.send(Frame::Error {
                id,
                code: FAILED_CODE,
                message: "too many calls".to_owned(),
            })
            .await;
        });
        return;
    }
    inbound.insert(
        id,
        Inbound {
            stdin: stdin_tx,
            ctl: ctl_tx,
            unread: Arc::clone(&unread),
            overflowed: false,
        },
    );
    drop(inbound);

    let mut found = None;
    for handler in locked(&shared.local).iter() {
        if handler.name.as_str() == name {
            found = Some(handler.clone());
            break;
        }
    }
    let call = InboundCall {
        id,
        name,
        argv,
        handler: found,
        slot: shared.link.call_slot(id),
        stdin: stdin_rx,
        ctl: ctl_rx,
        unread,
    };
    let shared = Arc::clone(shared);
    tokio::spawn(async move {
        serve_inbound(call).await;
        locked(&shared.inbound).remove(&id);
        shared.drained.notify_waiters();
    });
}

struct InboundCall {
    id: CallId,
    name: String,
    argv: Vec<String>,
    handler: Option<Handler>,
    slot: CallSlot,
    stdin: mpsc::UnboundedReceiver<StdinMsg>,
    ctl: mpsc::UnboundedReceiver<Ctl>,
    unread: Arc<AtomicUsize>,
}

async fn feed_stdin(
    stdin: ChildStdin,
    mut rx: mpsc::UnboundedReceiver<StdinMsg>,
    unread: Arc<AtomicUsize>,
) {
    let mut stdin = Some(stdin);
    while let Some(msg) = rx.recv().await {
        match msg {
            StdinMsg::Data(data) => {
                if let Some(pipe) = stdin.as_mut()
                    && pipe.write_all(&data).await.is_err()
                {
                    stdin = None;
                }
                unread.fetch_sub(data.len(), Ordering::Relaxed);
            }
            StdinMsg::Eof => stdin = None,
        }
    }
}

async fn serve_inbound(call: InboundCall) {
    let InboundCall {
        id,
        name,
        argv,
        handler,
        slot,
        stdin,
        mut ctl,
        unread,
    } = call;
    let Some(handler) = handler else {
        slot.send(Frame::Error {
            id,
            code: NO_HANDLER_CODE,
            message: format!("no handler {name} on the other side"),
        })
        .await;
        return;
    };
    let mut running = match runner::spawn(&handler, &argv) {
        Ok(running) => running,
        Err(err) => {
            slot.send(Frame::Error {
                id,
                code: CANNOT_START_CODE,
                message: format!("cannot start {name}: {err}"),
            })
            .await;
            return;
        }
    };
    let feeder = running
        .stdin
        .take()
        .map(|pipe| tokio::spawn(feed_stdin(pipe, stdin, unread)));

    let mut silent = false;
    let mut ctl_open = true;
    loop {
        tokio::select! {
            event = running.events.recv() => {
                let Some(event) = event else { break };
                match event {
                    Event::Chunk { stream, data } => {
                        if !silent {
                            slot.send(Frame::Chunk { id, stream, data }).await;
                        }
                    }
                    Event::Eof(stream) => {
                        if !silent {
                            slot.send(Frame::Eof { id, stream }).await;
                        }
                    }
                    Event::Exit(code) => {
                        if !silent {
                            slot.send(Frame::Exit { id, code }).await;
                        }
                        break;
                    }
                }
            }
            msg = ctl.recv(), if ctl_open => match msg {
                Some(Ctl::Overflow) => {
                    if !silent {
                        let message = "unread limit".to_owned();
                        slot.send(Frame::Error { id, code: FAILED_CODE, message }).await;
                    }
                    silent = true;
                    running.cancel();
                }
                Some(Ctl::Cancel | Ctl::LinkLost) => {
                    silent = true;
                    running.cancel();
                }
                None => {
                    ctl_open = false;
                    silent = true;
                    running.cancel();
                }
            },
        }
    }
    if let Some(feeder) = feeder {
        feeder.abort();
    }
}

fn lose(shared: &Shared) {
    shared.up.send_replace(false);
    let outbound = std::mem::take(&mut *locked(&shared.outbound));
    for (_, Outbound { events, .. }) in outbound {
        let _ = events.send(CallEvent::Error(CallFailure::failed("link lost")));
    }
    for entry in locked(&shared.inbound).values() {
        let _ = entry.ctl.send(Ctl::LinkLost);
    }
}

async fn wait_drained(shared: &Shared) {
    loop {
        let notified = shared.drained.notified();
        let mut notified = pin!(notified);
        notified.as_mut().enable();
        if locked(&shared.inbound).is_empty() {
            return;
        }
        notified.await;
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use tempfile::TempDir;
    use tokio::io::{duplex, split};

    use super::*;
    use crate::name::HandlerName;

    const WAIT: Duration = Duration::from_secs(30);

    fn handler_with_mode(dir: &TempDir, name: &str, body: &str, mode: u32) -> Handler {
        let path = dir.path().join(name);
        fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("script is written");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("mode is set");
        Handler {
            name: HandlerName::parse(name).expect("name is valid"),
            desc: format!("{name} handler"),
            path,
        }
    }

    fn handler(dir: &TempDir, name: &str, body: &str) -> Handler {
        handler_with_mode(dir, name, body, 0o755)
    }

    struct Pair {
        dialer: SessionHandle,
        far: SessionHandle,
    }

    async fn wait_up(handle: &SessionHandle) {
        let mut up = handle.up();
        timeout(WAIT, up.wait_for(|up| *up))
            .await
            .expect("link comes up in time")
            .expect("session is alive");
    }

    async fn link(dialer_handlers: Vec<Handler>, far_handlers: Vec<Handler>) -> Pair {
        let (near, distant) = duplex(64 * 1024);
        let (near_read, near_write) = split(near);
        let (distant_read, distant_write) = split(distant);
        let dialer = Session::new(Role::Dialer, dialer_handlers, near_read, near_write);
        let far = Session::new(Role::Far, far_handlers, distant_read, distant_write);
        let pair = Pair {
            dialer: dialer.handle(),
            far: far.handle(),
        };
        tokio::spawn(dialer.run());
        tokio::spawn(far.run());
        wait_up(&pair.dialer).await;
        wait_up(&pair.far).await;
        pair
    }

    struct Outcome {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        eofs: usize,
        last: CallEvent,
    }

    async fn finish(call: &mut OutCall) -> Outcome {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut eofs = 0;
        loop {
            let event = timeout(WAIT, call.events.next())
                .await
                .expect("call ends in time")
                .expect("events end with Exit or Error");
            match event {
                CallEvent::Chunk {
                    stream: Stream::Stdout,
                    data,
                } => stdout.extend(data),
                CallEvent::Chunk {
                    stream: Stream::Stderr,
                    data,
                } => stderr.extend(data),
                CallEvent::Chunk {
                    stream: Stream::Stdin,
                    ..
                } => panic!("a handler never produces stdin"),
                CallEvent::Eof(_) => eofs += 1,
                last @ (CallEvent::Exit(_) | CallEvent::Error(_)) => {
                    return Outcome {
                        stdout,
                        stderr,
                        eofs,
                        last,
                    };
                }
            }
        }
    }

    async fn until<F: Fn() -> bool>(what: &str, check: F) {
        let begun = std::time::Instant::now();
        while !check() {
            assert!(begun.elapsed() < WAIT, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn inbound_empty(handle: &SessionHandle) -> bool {
        locked(&handle.shared.inbound).is_empty()
    }

    fn argv(items: &[&str]) -> Vec<String> {
        let mut list = Vec::new();
        for item in items {
            list.push((*item).to_owned());
        }
        list
    }

    fn pattern(len: usize) -> Vec<u8> {
        let mut data = Vec::new();
        for index in 0..len {
            data.push(u8::try_from(index % 251).expect("below 256"));
        }
        data
    }

    #[tokio::test]
    async fn a_call_round_trips_in_both_directions() {
        let dir = TempDir::new().expect("tempdir");
        let script = "printf '%s|' \"$@\"; cat; echo oops >&2; exit 3";
        let pair = link(
            vec![handler(&dir, "echo-d", script)],
            vec![handler(&dir, "echo-f", script)],
        )
        .await;

        let mut call = pair
            .dialer
            .open("echo-f", argv(&["a b", "$HOME"]))
            .await
            .expect("call opens");
        assert!(call.input.send_stdin(b"hello").await);
        assert!(call.input.close_stdin().await);
        let done = finish(&mut call).await;
        assert_eq!(done.stdout, b"a b|$HOME|hello");
        assert_eq!(done.stderr, b"oops\n");
        assert_eq!(done.eofs, 2);
        assert_eq!(done.last, CallEvent::Exit(3));

        let mut call = pair
            .far
            .open("echo-d", argv(&["x"]))
            .await
            .expect("call opens");
        assert!(call.input.close_stdin().await);
        let done = finish(&mut call).await;
        assert_eq!(done.stdout, b"x|");
        assert_eq!(done.last, CallEvent::Exit(3));

        let remote = pair.dialer.remote_handlers();
        assert_eq!(remote.len(), 1);
        assert_eq!(remote[0].name, "echo-f");
        assert_eq!(remote[0].desc, "echo-f handler");
    }

    #[tokio::test]
    async fn an_unknown_name_is_127() {
        let pair = link(Vec::new(), Vec::new()).await;
        let mut call = pair.dialer.open("nope", Vec::new()).await.expect("opens");
        let done = finish(&mut call).await;
        assert_eq!(
            done.last,
            CallEvent::Error(CallFailure {
                code: 127,
                message: "no handler nope on the other side".to_owned()
            })
        );
    }

    #[tokio::test]
    async fn a_handler_that_cannot_start_is_126() {
        let dir = TempDir::new().expect("tempdir");
        let broken = handler_with_mode(&dir, "broken", "exit 0", 0o644);
        let pair = link(Vec::new(), vec![broken]).await;
        let mut call = pair.dialer.open("broken", Vec::new()).await.expect("opens");
        let done = finish(&mut call).await;
        let CallEvent::Error(CallFailure { code, message }) = done.last else {
            panic!("expected an error, got {:?}", done.last);
        };
        assert_eq!(code, 126);
        assert!(message.starts_with("cannot start broken: "), "{message}");
    }

    #[tokio::test]
    async fn the_33rd_call_is_refused_with_255() {
        let dir = TempDir::new().expect("tempdir");
        let pair = link(Vec::new(), vec![handler(&dir, "wait", "cat")]).await;
        let mut calls = Vec::new();
        for _ in 0..MAX_CALLS {
            calls.push(pair.dialer.open("wait", Vec::new()).await.expect("opens"));
        }
        let mut extra = pair.dialer.open("wait", Vec::new()).await.expect("opens");
        let done = finish(&mut extra).await;
        assert_eq!(
            done.last,
            CallEvent::Error(CallFailure::failed("too many calls"))
        );
        for call in &calls {
            call.input.cancel().await;
        }
        until("handlers to stop", || inbound_empty(&pair.far)).await;
    }

    #[tokio::test]
    async fn cancel_stops_the_handler() {
        let dir = TempDir::new().expect("tempdir");
        let marker = dir.path().join("terminated");
        let body = format!(
            "trap 'echo term > {}; exit 0' TERM\necho ready\nwhile true; do sleep 0.1; done",
            marker.display()
        );
        let pair = link(Vec::new(), vec![handler(&dir, "loop", &body)]).await;
        let mut call = pair.dialer.open("loop", Vec::new()).await.expect("opens");
        let ready = timeout(WAIT, call.events.next())
            .await
            .expect("output in time")
            .expect("event");
        assert_eq!(
            ready,
            CallEvent::Chunk {
                stream: Stream::Stdout,
                data: b"ready\n".to_vec()
            }
        );
        call.input.cancel().await;
        until("the handler to get SIGTERM", || Path::new(&marker).exists()).await;
        until("the call to end", || inbound_empty(&pair.far)).await;
    }

    #[tokio::test]
    async fn a_small_call_finishes_while_a_large_one_is_running() {
        let dir = TempDir::new().expect("tempdir");
        let big =
            "i=0\nwhile [ $i -lt 16 ]; do head -c 65536 /dev/zero; sleep 0.05; i=$((i+1)); done";
        let pair = link(
            Vec::new(),
            vec![handler(&dir, "big", big), handler(&dir, "small", "echo hi")],
        )
        .await;
        let mut large = pair.dialer.open("big", Vec::new()).await.expect("opens");
        let first = timeout(WAIT, large.events.next())
            .await
            .expect("first chunk in time")
            .expect("event");
        let CallEvent::Chunk { data, .. } = first else {
            panic!("expected a chunk, got {first:?}");
        };
        let mut received = data.len();
        let mut small = pair.dialer.open("small", Vec::new()).await.expect("opens");

        let small_done = finish(&mut small).await;
        assert_eq!(small_done.stdout, b"hi\n");
        assert_eq!(small_done.last, CallEvent::Exit(0));

        let large_done = finish(&mut large).await;
        received += large_done.stdout.len();
        assert_eq!(received, 1024 * 1024);
        assert_eq!(large_done.last, CallEvent::Exit(0));
    }

    #[tokio::test]
    async fn a_handler_that_writes_1_mib_and_exits_delivers_every_byte() {
        let dir = TempDir::new().expect("tempdir");
        let pair = link(
            Vec::new(),
            vec![handler(&dir, "burst", "head -c 1048576 /dev/zero")],
        )
        .await;
        for run in 0..50 {
            let mut call = pair.dialer.open("burst", Vec::new()).await.expect("opens");
            let done = finish(&mut call).await;
            assert_eq!(done.stdout.len(), 1024 * 1024, "run {run}");
            assert_eq!(done.eofs, 2, "run {run}");
            assert_eq!(done.last, CallEvent::Exit(0), "run {run}");
        }
    }

    #[tokio::test]
    async fn eof_stdin_never_overtakes_the_last_stdin_chunk() {
        let dir = TempDir::new().expect("tempdir");
        let pair = link(Vec::new(), vec![handler(&dir, "copy", "cat")]).await;
        let data = pattern(300_000);
        for _ in 0..5 {
            let mut call = pair.dialer.open("copy", Vec::new()).await.expect("opens");
            assert!(call.input.send_stdin(&data).await);
            assert!(call.input.close_stdin().await);
            let done = finish(&mut call).await;
            assert_eq!(done.stdout, data);
            assert_eq!(done.last, CallEvent::Exit(0));
        }
    }

    #[tokio::test]
    async fn unread_output_past_16_mib_ends_the_call_on_the_callers_side() {
        let dir = TempDir::new().expect("tempdir");
        let pair = link(
            Vec::new(),
            vec![handler(&dir, "flood", "head -c 25165824 /dev/zero")],
        )
        .await;
        let mut call = pair.dialer.open("flood", Vec::new()).await.expect("opens");
        until("the unread limit to end the call", || {
            locked(&pair.dialer.shared.outbound).is_empty()
        })
        .await;
        let done = finish(&mut call).await;
        assert_eq!(
            done.last,
            CallEvent::Error(CallFailure::failed("unread limit"))
        );
        assert!(done.stdout.len() > MAX_UNREAD);
        until("the handler to stop", || inbound_empty(&pair.far)).await;
    }

    #[tokio::test]
    async fn stdin_past_16_mib_to_a_handler_that_never_reads_ends_the_call() {
        let dir = TempDir::new().expect("tempdir");
        let pair = link(Vec::new(), vec![handler(&dir, "deaf", "exec sleep 60")]).await;
        let mut call = pair.dialer.open("deaf", Vec::new()).await.expect("opens");
        let input = call.input.clone();
        let sender = tokio::spawn(async move {
            let block = vec![0_u8; 1024 * 1024];
            for _ in 0..20 {
                if !input.send_stdin(&block).await {
                    break;
                }
            }
        });
        let done = finish(&mut call).await;
        sender.abort();
        assert_eq!(
            done.last,
            CallEvent::Error(CallFailure::failed("unread limit"))
        );
        until("the handler to stop", || inbound_empty(&pair.far)).await;
    }

    #[tokio::test]
    async fn an_oversized_open_fails_locally_and_the_link_stays_up() {
        let dir = TempDir::new().expect("tempdir");
        let pair = link(Vec::new(), vec![handler(&dir, "echo", "echo ok")]).await;
        let huge = vec!["x".repeat(200 * 1024)];
        let Err(failure) = pair.dialer.open("echo", huge).await else {
            panic!("an oversized open must fail");
        };
        assert_eq!(failure, CallFailure::failed("arguments too large"));
        assert!(pair.dialer.is_up());
        let mut call = pair.dialer.open("echo", Vec::new()).await.expect("opens");
        let done = finish(&mut call).await;
        assert_eq!(done.stdout, b"ok\n");
    }

    #[tokio::test]
    async fn set_handlers_reaches_the_other_side() {
        let dir = TempDir::new().expect("tempdir");
        let pair = link(Vec::new(), Vec::new()).await;
        assert_eq!(pair.dialer.remote_handlers().len(), 0);
        pair.far
            .set_handlers(vec![handler(&dir, "late", "echo late")]);
        until("the new list", || !pair.dialer.remote_handlers().is_empty()).await;
        let mut call = pair.dialer.open("late", Vec::new()).await.expect("opens");
        assert_eq!(finish(&mut call).await.stdout, b"late\n");
    }

    #[tokio::test]
    async fn a_call_without_a_link_fails_with_255() {
        let (near, _distant) = duplex(1024);
        let (read, write) = split(near);
        let session = Session::new(Role::Dialer, Vec::new(), read, write);
        let Err(failure) = session.handle().open("x", Vec::new()).await else {
            panic!("no link, no call");
        };
        assert_eq!(failure, CallFailure::failed("link not up"));
    }

    fn hello(version: u32) -> Vec<u8> {
        encode(&Frame::Hello {
            version,
            kinds: Vec::new(),
            handlers: Vec::new(),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn thirty_seconds_of_silence_lose_the_link() {
        let (near, mut distant) = duplex(64 * 1024);
        let (read, write) = split(near);
        let session = Session::new(Role::Dialer, Vec::new(), read, write);
        distant
            .write_all(&hello(1))
            .await
            .expect("hello is written");
        let begun = Instant::now();
        let end = session.run().await;
        let SessionError::Silent = end.reason else {
            panic!("expected Silent, got {:?}", end.reason);
        };
        assert!(end.established);
        assert!(begun.elapsed() >= SILENCE);
    }

    #[tokio::test]
    async fn a_different_version_closes_the_link() {
        let (near, mut distant) = duplex(64 * 1024);
        let (read, write) = split(near);
        let session = Session::new(Role::Dialer, Vec::new(), read, write);
        distant
            .write_all(&hello(2))
            .await
            .expect("hello is written");
        let end = session.run().await;
        let SessionError::Version(2) = end.reason else {
            panic!("expected Version(2), got {:?}", end.reason);
        };
        assert!(!end.established);
    }

    #[tokio::test]
    async fn a_first_frame_that_is_not_hello_closes_the_link() {
        let (near, mut distant) = duplex(64 * 1024);
        let (read, write) = split(near);
        let session = Session::new(Role::Dialer, Vec::new(), read, write);
        distant
            .write_all(&encode(&Frame::Ping))
            .await
            .expect("ping is written");
        let end = session.run().await;
        let SessionError::NoHello = end.reason else {
            panic!("expected NoHello, got {:?}", end.reason);
        };
    }

    #[tokio::test]
    async fn losing_the_link_ends_outgoing_calls_with_255() {
        let (near, mut distant) = duplex(64 * 1024);
        let (read, write) = split(near);
        let session = Session::new(Role::Dialer, Vec::new(), read, write);
        let handle = session.handle();
        let run = tokio::spawn(session.run());
        distant
            .write_all(&hello(1))
            .await
            .expect("hello is written");
        wait_up(&handle).await;
        let mut call = handle.open("x", Vec::new()).await.expect("opens");
        drop(distant);
        let done = finish(&mut call).await;
        assert_eq!(
            done.last,
            CallEvent::Error(CallFailure::failed("link lost"))
        );
        let end = run.await.expect("run does not panic");
        assert!(end.established);
        assert!(!handle.is_up());
    }

    #[tokio::test]
    async fn losing_the_link_stops_the_handlers_this_side_runs() {
        let dir = TempDir::new().expect("tempdir");
        let started = dir.path().join("started");
        let marker = dir.path().join("terminated");
        let body = format!(
            "trap 'echo term > {}; exit 0' TERM\necho up > {}\nwhile true; do sleep 0.1; done",
            marker.display(),
            started.display()
        );
        let (near, mut distant) = duplex(64 * 1024);
        let (read, write) = split(near);
        let session = Session::new(
            Role::Dialer,
            vec![handler(&dir, "loop", &body)],
            read,
            write,
        );
        let handle = session.handle();
        let run = tokio::spawn(session.run());
        distant
            .write_all(&hello(1))
            .await
            .expect("hello is written");
        wait_up(&handle).await;
        let open = encode(&Frame::Open {
            id: CallId(2),
            name: "loop".to_owned(),
            argv: Vec::new(),
        });
        distant.write_all(&open).await.expect("open is written");
        until("the handler to start", || started.exists()).await;
        drop(distant);
        let end = run.await.expect("run does not panic");
        assert!(end.established);
        assert!(
            marker.exists(),
            "the handler got SIGTERM before run returned"
        );
        assert!(inbound_empty(&handle));
    }
}
