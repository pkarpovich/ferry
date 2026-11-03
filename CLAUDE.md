# ferry

A call bus between two hosts over one ssh stream. Rust edition 2024, a library plus a thin binary, tokio on a current-thread runtime. The full design is in `docs/plans/completed/20261103-ferry-v1.md`.

## Layout

- `src/main.rs` - dispatch of `daemon`, `peer`, `call`, `ls`, `reload`, `help` into the library.
- `src/cli.rs` - command parsing and the usage text.
- `src/name.rs` - the `HandlerName` newtype and the name rules.
- `src/config.rs` - `config.toml`, `deny_unknown_fields`.
- `src/paths.rs` - state dir, socket, lock and log paths, the 103-byte socket path check.
- `src/frame.rs` - the frame codec, JSON lines, 128 KiB line limit, `FrameError`.
- `src/handlers.rs` - handler discovery by marker comment.
- `src/runner.rs` - spawning a handler, exit mapping, the cancel sequence.
- `src/writer.rs` - the one task that owns the link's write half, with per-call slots.
- `src/session.rs` - one link: `hello`, ping, calls in both directions.
- `src/local.rs` - requests and replies on the daemon's local socket.
- `src/daemon.rs` - lock, local socket, listen ports, shutdown.
- `src/dial.rs` - the dialer's ssh child and the reconnect schedule.
- `src/client.rs` - `ferry call`, `ls`, `reload`.
- `src/peer.rs` - the bridge sshd runs on the far host.
- `tests/` - `cli.rs`, `client.rs`, and `e2e.rs` with two daemons and a fake ssh script from `tests/support/mod.rs`.

## Gate

Run after every change, all through mise:

```sh
mise exec -- cargo fmt --check
mise exec -- cargo clippy --all-targets --all-features --locked -- -D warnings
mise exec -- cargo test
```

Also `grep -rnE '\.unwrap\(\)' src/ tests/` and `grep -rnE 'matches!|#\[allow' src/ tests/` must find nothing.

## Style

- `for` loops with accumulators, not iterator chains.
- `let ... else` for early exits, one let chain instead of nested `if let`.
- Shadow through transformations. Newtypes for identifiers (`HandlerName`, `CallId`), two-variant enums instead of `bool` parameters.
- Every `match` lists all variants: no `_` arm, no `matches!`.
- No `unwrap()` anywhere, tests included. Tests use `expect("what was expected")`.
- The binary uses `anyhow` with `.context(...)`. The frame codec has a `thiserror` enum.
- `unsafe_code = "forbid"`. No `#[allow]`; an unfixable lint gets `#[expect(..., reason = "...")]` on the smallest item, after asking.
- No comments that narrate code, no section dividers, no TODOs. Public items carry `# Errors` and `# Panics` doc sections where clippy asks.
- Test temp homes are short paths under `/tmp`: macOS limits a unix socket path to 104 bytes.

## Invariants

- One stream carries every call. There is no flow control across the link.
- Call ids are `u64`: the dialer opens odd ids, the far host even ids. A local socket carries exactly one call under id 0.
- The writer sends link frames first, and every call goes through its own one-frame slot, so a call's frames keep their order and a large call gets ahead of a small one by at most one frame.
- The reader never waits for a slow consumer. Unread bytes per call are counted on both sides, and 16 MiB ends the call with 255.
- A call always goes to the other side. A host never calls its own handlers through ferry.
- Handlers are names, never paths or commands from the other side.
- A frame for an unknown or finished id is dropped. A well-formed frame of an unknown kind is skipped. A malformed line or a line over 128 KiB closes the link.
- The daemon holds an exclusive lock on `ferry.lock`. A second daemon exits 0 at once.
- `ferry call` and `ferry peer` end with `std::process::exit`, never by returning from `main`, because a pending stdin read cannot be cancelled.
- The cancel sequence is one routine: SIGTERM to the group, SIGKILL after 5 s if any member lives, close the daemon's pipe ends after 10 s.
