# ferry

[![ci](https://github.com/pkarpovich/ferry/actions/workflows/ci.yml/badge.svg)](https://github.com/pkarpovich/ferry/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

ferry lets two hosts call named programs on each other when only one of them can open a connection to the other.
One host, the dialer, runs `ssh <host> ferry peer` and keeps that one stream open, and over it either side calls a handler on the other side.
A call is a two-way byte stream with argv and an exit code, the same shape as `ssh host command`, so a caller pipes into it and reads its output like any local command.

The case it is built for: the far host accepts no inbound connections, and reaches the dialer only through an ssh reverse tunnel that the far host itself keeps up.
The dialer's key for that tunnel cannot run commands, so the far host cannot start anything on the dialer, and the dialer has to come to it.
ferry keeps that boundary: the dialer runs only the names in its own handler folder, never a path or a command the other side sends.

## Install

Homebrew, on Apple Silicon or x86_64 Linux:

```sh
brew install pkarpovich/apps/ferry
```

Or take the static tarball from the releases page: `ferry-aarch64-apple-darwin.tar.gz` or `ferry-x86_64-unknown-linux-musl.tar.gz`.
Unpack it and put `ferry` on the PATH of both hosts.

## Setup

The same binary runs on both hosts, and each host runs one `ferry daemon`.
The two daemons are peers, and they differ in one thing only: the dialer has `connect` in its config and opens the link, the far host waits for it.

Config lives in `~/.config/ferry/config.toml`.
All keys are optional, and an unknown key is an error.

| Key | Meaning |
|---|---|
| `handlers` | Folder with this host's handlers. A leading `~/` expands to `$HOME`, and a relative path is taken from `$HOME`. |
| `connect` | ssh destination. Present: this host is the dialer. |
| `remote_ferry` | Path of `ferry` on the far host, default `ferry`. Needed when sshd's PATH does not contain it. |
| `[[listen]]` | Each has `port` (1-65535) and `handler` (a name on the other side). |

The dialer:

```toml
handlers = "~/.config/ferry/handlers"
connect = "far-host"
```

The far host:

```toml
handlers = "~/.config/ferry/handlers"

[[listen]]
port = 7811
handler = "service-proxy"
```

Run `ferry daemon` on the dialer under launchd or another supervisor.
On the far host nothing needs to be started: `ferry peer`, which sshd runs, starts the daemon when it finds none.
A change to `config.toml` needs a daemon restart, and `ferry reload` re-reads the handler folder only and sends the new list to the other side.

A host keeps its state in `~/.local/state/ferry/` (mode 0700): `ferry.sock`, `ferry.lock` and `ferry.log`. A daemon started by `ferry peer` writes its stdout and stderr to `ferry.log`. Only one daemon runs per host; a second `ferry daemon` exits 0 at once. `$HOME` must be short enough that the socket path fits in 103 bytes.

The dialer runs `ssh -T -o BatchMode=yes -o ConnectTimeout=5 <connect> <remote_ferry> peer`.
Set `FERRY_SSH` to use another program than `ssh`.
When the link drops, the dialer reconnects after 1, 2, 4, 8 and 16 s, then every 30 s.

### launchd on the dialer

Save as `~/Library/LaunchAgents/dev.ferry.daemon.plist` and run `launchctl load` on it:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.ferry.daemon</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/ferry</string>
        <string>daemon</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardErrorPath</key>
    <string>/tmp/ferry.err</string>
</dict>
</plist>
```

## Writing a handler

A handler is a regular file in the handler folder with an execute bit for the user.
It carries the marker line `# @ferry.name <name>` within its first 64 lines.
An optional `# @ferry.desc <text>` after it is its one-line description.
A file without the marker is not a handler, and a compiled program becomes one through a small script with the marker that `exec`s it.

```sh
#!/bin/sh
# @ferry.name echo-upper
# @ferry.desc Upper-cases stdin
exec tr a-z A-Z
```

A name is 1 to 64 characters of `[a-z0-9._-]`, starting with a letter or digit.
The contract: argv comes from the call, stdin from the caller, stdout is the answer, stderr is the error text, and the exit code is the result.
A handler has no terminal and must not wait for a person.
The daemon starts it with no shell, in `$HOME`, in its own process group, so a cancel reaches the programs it starts too.
A handler that starts a long-lived background program redirects that program's stdio.

`ferry ls` shows the link state (up with the peer's version, or down with the last reason), this host's handlers, the other side's handlers, and the files that were skipped with the reason. `ferry ls --json` prints the same as JSON for scripts.

## Calling

```sh
echo hello | ferry call echo-upper
ferry call some-handler arg1 "arg two"
```

`ferry call <name> [args...]` works on either host and always calls the other side.
Its stdin, stdout and stderr are the call's streams, and its exit code is the handler's.
Ctrl+C stops the handler on the other side.

### Listen ports

A `[[listen]]` entry binds `127.0.0.1:<port>`, never another address.
Every accepted connection becomes one call to the named handler on the other side with no argv, with the socket's bytes as the handler's stdin and stdout, the way inetd hands a socket to a program.
When the client closes the connection, the call is cancelled and the handler stopped, the way a reverse proxy drops an upstream request whose client left. A client that half-closes its side after the request is treated as gone too and gets no answer.
This is how an HTTP client that only talks to loopback reaches a service behind the other host: the handler forwards the bytes, and ferry itself knows nothing about HTTP.
On any error the daemon closes the TCP connection.

## Errors

| Case | Exit code | stderr |
|---|---|---|
| Handler exited | the handler's code | the handler's own stderr |
| No handler with that name on the other side | 127 | `ferry: no handler <name> on the other side` |
| The handler file could not be started | 126 | `ferry: cannot start <name>: <reason>` |
| Link not up, link lost during the call, unread limit passed, 33rd concurrent call, arguments over 128 KiB | 255 | `ferry: <reason>` |

A handler killed by a signal exits with 128 plus the signal number.
A call made while the link is down fails at once, with no queue and no retry.

## Limits

- 32 handlers run at once on one host.
- A call whose unread bytes pass 16 MiB ends with 255 on both sides.
- Ping every 10 s from each side, and the link is lost after 30 s without a frame.
- A cancel sends SIGTERM, then SIGKILL to the group after 5 s, and ends the call after 10 s.
- One dialer per far host, and a new link replaces the old one.
- No call timeout, no authentication beyond ssh, no encryption beyond ssh.

## Credits

The wire framing, newline-delimited JSON with a `hello` that names the frame kinds each side speaks, borrows from the remote presentation stream of [agterm](https://github.com/umputun/agterm) (MIT).

## License

MIT, see [LICENSE](LICENSE).
