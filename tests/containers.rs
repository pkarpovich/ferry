//! Two hosts in containers, linked the way ferry is deployed: the far host keeps a reverse ssh tunnel to the dialer with a key that cannot run commands, and the dialer runs `ssh far ferry peer` back through it.
//! Run with `cargo test --test containers -- --ignored`; Docker is required.

use std::process::Command;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use testcontainers::core::{ExecCommand, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

const IMAGE: &str = "ferry-test";
const TAG: &str = "local";
const FERRY: &str = "/opt/ferry/bin/ferry";
const LINK_WAIT: Duration = Duration::from_secs(120);
const EXEC_WAIT: Duration = Duration::from_secs(10);
const BIG: usize = 20 * 1024 * 1024;

static BUILT: OnceLock<()> = OnceLock::new();
static NEXT: AtomicUsize = AtomicUsize::new(0);

fn build_image() {
    BUILT.get_or_init(|| {
        let status = Command::new("docker")
            .args(["build", "--quiet", "--tag", &format!("{IMAGE}:{TAG}")])
            .args(["--file", "tests/containers/Dockerfile", "."])
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("docker runs");
        assert!(status.success(), "the test image builds");
    });
}

struct Output {
    code: i64,
    stdout: Vec<u8>,
    stderr: String,
}

impl Output {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }
}

struct Host {
    container: Container<GenericImage>,
}

impl Host {
    fn run(&self, script: &str) -> Output {
        let command = ExecCommand::new(["su", "-", "ferry", "-c", script]);
        let mut result = self.container.exec(command).expect("exec starts");
        let stdout = result.stdout_to_vec().expect("stdout is read");
        let stderr = result.stderr_to_vec().expect("stderr is read");
        let deadline = Instant::now() + EXEC_WAIT;
        let code = loop {
            if let Some(code) = result.exit_code().expect("exec is inspected") {
                break code;
            }
            assert!(
                Instant::now() < deadline,
                "exec of {script:?} reports no exit code"
            );
            sleep(Duration::from_millis(50));
        };
        Output {
            code,
            stdout,
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        }
    }

    fn ok(&self, script: &str) -> String {
        let output = self.run(script);
        assert_eq!(output.code, 0, "{script}: {}", output.stderr);
        output.text().trim_end().to_owned()
    }
}

enum Link {
    Up,
    Down,
}

struct Topology {
    dialer: Host,
    far: Host,
    network: String,
}

impl Topology {
    fn start() -> Self {
        build_image();
        let index = NEXT.fetch_add(1, Ordering::Relaxed);
        let network = format!("ferry-test-{}-{index}", std::process::id());
        let dialer_name = format!("{network}-dialer");
        let dialer = GenericImage::new(IMAGE, TAG)
            .with_wait_for(WaitFor::message_on_stdout("ready"))
            .with_network(&network)
            .with_container_name(&dialer_name)
            .with_env_var("FERRY_ROLE", "dialer")
            .start()
            .expect("the dialer starts");
        let far = GenericImage::new(IMAGE, TAG)
            .with_wait_for(WaitFor::message_on_stdout("ready"))
            .with_network(&network)
            .with_container_name(format!("{network}-far"))
            .with_env_var("FERRY_ROLE", "far")
            .with_env_var("DIALER_HOST", &dialer_name)
            .start()
            .expect("the far host starts");
        let topology = Self {
            dialer: Host { container: dialer },
            far: Host { container: far },
            network,
        };
        topology.wait_link(&Link::Up);
        topology
    }

    fn link_state(&self) -> String {
        let output = self.far.run(&format!("{FERRY} ls --json"));
        if output.code != 0 {
            return "down".to_owned();
        }
        let value: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("ls prints JSON");
        value["link"]["state"].as_str().unwrap_or("down").to_owned()
    }

    fn wait_link(&self, want: &Link) {
        let want = match want {
            Link::Up => "up",
            Link::Down => "down",
        };
        let deadline = Instant::now() + LINK_WAIT;
        loop {
            let state = self.link_state();
            if state == want {
                return;
            }
            assert!(
                Instant::now() <= deadline,
                "the link is {state}, not {want}; dialer log:\n{}",
                self.dialer.run("tail -n 20 ~/ferry.log").text()
            );
            sleep(Duration::from_millis(500));
        }
    }

    fn call(&self, args: &str) -> Output {
        self.far.run(&format!("timeout 120 {FERRY} call {args}"))
    }

    fn docker(&self, verb: &str) {
        let status = Command::new("docker")
            .args(["network", verb, &self.network, self.far.container.id()])
            .status()
            .expect("docker runs");
        assert!(status.success(), "docker network {verb} works");
    }
}

#[test]
#[ignore = "needs docker"]
fn file_read_returns_small_empty_odd_and_large_files() {
    let hosts = Topology::start();
    hosts
        .dialer
        .ok("printf 'hello\\nworld\\n' > ~/docs/note.md && : > ~/docs/empty.md");
    let note = hosts.call("file-read note.md");
    assert_eq!(note.code, 0, "{}", note.stderr);
    assert_eq!(note.text(), "hello\nworld\n");

    let empty = hosts.call("file-read empty.md");
    assert_eq!(empty.code, 0, "{}", empty.stderr);
    assert_eq!(empty.stdout, b"");

    hosts.dialer.ok("printf odd > ~/docs/'my note $HOME;.md'");
    let odd = hosts.call("file-read 'my note $HOME;.md'");
    assert_eq!(odd.code, 0, "{}", odd.stderr);
    assert_eq!(odd.text(), "odd");

    let made = hosts.dialer.ok(&format!(
        "head -c {BIG} /dev/urandom > ~/docs/big.bin && sha256sum ~/docs/big.bin | cut -d' ' -f1"
    ));
    let read = hosts.far.ok(&format!(
        "timeout 120 {FERRY} call file-read big.bin > /tmp/big.bin && sha256sum /tmp/big.bin | cut -d' ' -f1"
    ));
    assert_eq!(read, made, "a 20 MiB binary file arrives intact");
}

#[test]
#[ignore = "needs docker"]
fn file_read_refuses_missing_files_paths_outside_and_unknown_names() {
    let hosts = Topology::start();
    let missing = hosts.call("file-read nothing-here.md");
    assert_eq!(missing.code, 1);
    assert!(
        missing.stderr.contains("No such file"),
        "{}",
        missing.stderr
    );

    let outside = hosts.call("file-read ../.ssh/dialer_key");
    assert_eq!(outside.code, 2);
    assert_eq!(outside.stdout, b"", "the dialer's private key never leaves");
    assert!(
        outside.stderr.contains("outside docs"),
        "{}",
        outside.stderr
    );

    let unknown = hosts.call("no-such-handler");
    assert_eq!(unknown.code, 127, "{}", unknown.stderr);

    let path = hosts.call("../../usr/bin/id");
    assert_ne!(path.code, 0);
    assert!(!path.text().contains("uid="), "a path is never run");
}

#[test]
#[ignore = "needs docker"]
fn file_write_needs_the_sha_it_was_read_with() {
    let hosts = Topology::start();
    hosts.dialer.ok("printf 'first\\n' > ~/docs/note.md");
    let wrote = hosts.far.run(&format!(
        "f=$(mktemp) && {FERRY} call file-read note.md > \"$f\" \
         && sha=$(sha256sum \"$f\" | cut -d' ' -f1) && printf 'second\\n' >> \"$f\" \
         && timeout 120 {FERRY} call file-write note.md \"$sha\" < \"$f\""
    ));
    assert_eq!(wrote.code, 0, "{}", wrote.stderr);
    assert_eq!(hosts.dialer.ok("cat ~/docs/note.md"), "first\nsecond");

    let sha = hosts.far.ok(&format!(
        "{FERRY} call file-read note.md | sha256sum | cut -d' ' -f1"
    ));
    hosts
        .dialer
        .ok("printf 'changed on the dialer\\n' > ~/docs/note.md");
    let stale = hosts.far.run(&format!(
        "printf 'from the far host\\n' | timeout 120 {FERRY} call file-write note.md {sha}"
    ));
    assert_eq!(stale.code, 1);
    assert!(
        stale.stderr.contains("changed since it was read"),
        "{}",
        stale.stderr
    );
    assert_eq!(
        hosts.dialer.ok("cat ~/docs/note.md"),
        "changed on the dialer"
    );

    let outside = hosts.far.run(&format!(
        "printf x | timeout 120 {FERRY} call file-write ../escaped 0"
    ));
    assert_eq!(outside.code, 2);
    assert_eq!(hosts.dialer.run("test -e ~/escaped").code, 1);
}

#[test]
#[ignore = "needs docker"]
fn file_write_carries_a_large_file() {
    let hosts = Topology::start();
    hosts.dialer.ok("printf 'old\\n' > ~/docs/big.bin");
    let sent = hosts.far.ok(&format!(
        "head -c {BIG} /dev/urandom > /tmp/new.bin \
         && sha=$({FERRY} call file-read big.bin | sha256sum | cut -d' ' -f1) \
         && timeout 120 {FERRY} call file-write big.bin \"$sha\" < /tmp/new.bin \
         && sha256sum /tmp/new.bin | cut -d' ' -f1"
    ));
    let landed = hosts.dialer.ok("sha256sum ~/docs/big.bin | cut -d' ' -f1");
    assert_eq!(landed, sent, "a 20 MiB write lands intact");
}

#[test]
#[ignore = "needs docker"]
fn parallel_calls_all_complete_intact() {
    let hosts = Topology::start();
    let made = hosts.dialer.ok(
        "head -c 8388608 /dev/urandom > ~/docs/mid.bin && sha256sum ~/docs/mid.bin | cut -d' ' -f1",
    );
    let shas = hosts.far.ok(&format!(
        "for i in 1 2 3 4 5 6 7 8; do \
           (timeout 120 {FERRY} call file-read mid.bin | sha256sum | cut -d' ' -f1 > /tmp/sha.$i) & \
         done; wait; cat /tmp/sha.*"
    ));
    let mut count = 0;
    for sha in shas.lines() {
        assert_eq!(sha, made, "every parallel read is intact");
        count += 1;
    }
    assert_eq!(count, 8);
}

#[test]
#[ignore = "needs docker"]
fn http_requests_reach_the_service_with_its_token() {
    let hosts = Topology::start();
    let answer = hosts.far.ok(
        "curl -sS --max-time 30 -X POST -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"params\":{\"q\":\"ping\"}}' \
         http://127.0.0.1:7811/mcp",
    );
    let value: serde_json::Value = serde_json::from_str(&answer).expect("the service answers JSON");
    assert_eq!(value["id"], 1);
    assert_eq!(value["result"]["echo"]["q"], "ping");

    let direct = hosts.far.run(&format!(
        "curl -sS --max-time 3 http://{}-dialer:8080/mcp",
        hosts.network
    ));
    assert_ne!(
        direct.code, 0,
        "the service is reachable only through ferry"
    );

    let download = hosts.far.ok(&format!(
        "curl -sS --max-time 120 -D /tmp/headers -o /tmp/download http://127.0.0.1:7811/big?bytes={BIG} \
         && grep -i '^x-sha256:' /tmp/headers | tr -d '\\r' | cut -d' ' -f2 \
         && sha256sum /tmp/download | cut -d' ' -f1"
    ));
    let mut lines = download.lines();
    let header = lines.next().expect("the service sends X-Sha256");
    let body = lines.next().expect("the body is hashed");
    assert_eq!(body, header, "a 20 MiB response arrives intact");

    let upload = hosts.far.ok(
        "head -c 8388608 /dev/urandom > /tmp/up.bin \
         && curl -sS --max-time 120 -H 'Expect:' --data-binary @/tmp/up.bin http://127.0.0.1:7811/upload \
         && echo && sha256sum /tmp/up.bin | cut -d' ' -f1",
    );
    let mut lines = upload.lines();
    let reply: serde_json::Value =
        serde_json::from_str(lines.next().expect("the service replies")).expect("JSON reply");
    let sent = lines.next().expect("the upload is hashed");
    assert_eq!(reply["bytes"], 8_388_608);
    assert_eq!(
        reply["sha256"], sent,
        "an 8 MiB request body arrives intact"
    );

    let parallel = hosts.far.ok(
        "for i in 1 2 3 4 5 6; do \
           (curl -sS --max-time 60 -X POST -d \"{\\\"id\\\":$i}\" http://127.0.0.1:7811/mcp > /tmp/reply.$i; echo >> /tmp/reply.$i) & \
         done; wait; cat /tmp/reply.*",
    );
    let mut count = 0;
    for line in parallel.lines() {
        let reply: serde_json::Value = serde_json::from_str(line).expect("each reply is JSON");
        assert_eq!(reply["jsonrpc"], "2.0");
        count += 1;
    }
    assert_eq!(count, 6);
}

#[test]
#[ignore = "needs docker"]
fn server_sent_events_stream_without_buffering() {
    let hosts = Topology::start();
    let events = hosts.far.ok(
        "curl -sSN --max-time 30 -X POST -H 'Accept: text/event-stream' -d '{\"id\":7}' \
         http://127.0.0.1:7811/mcp | while IFS= read -r line; do \
           case \"$line\" in data:*) echo \"$(date +%s%N) ${line#data: }\" ;; esac; \
         done",
    );
    let mut stamps = Vec::new();
    let mut seq = 0;
    for line in events.lines() {
        let (stamp, data) = line.split_once(' ').expect("a stamp and an event");
        let event: serde_json::Value = serde_json::from_str(data).expect("each event is JSON");
        assert_eq!(event["seq"], seq, "events keep their order");
        assert_eq!(event["id"], 7);
        stamps.push(stamp.parse::<u128>().expect("a nanosecond stamp"));
        seq += 1;
    }
    assert_eq!(seq, 5);
    let first = stamps.first().expect("a first event");
    let last = stamps.last().expect("a last event");
    let spread = Duration::from_nanos(u64::try_from(last - first).expect("fits"));
    assert!(
        spread >= Duration::from_millis(1500),
        "events arrive as they are sent, not at the end: {spread:?}"
    );
}

#[test]
#[ignore = "needs docker"]
fn an_aborted_stream_stops_its_handler() {
    let hosts = Topology::start();
    let aborted = hosts.far.run(
        "curl -sSN --max-time 1 -X POST -H 'Accept: text/event-stream' -d '{\"id\":1}' \
         http://127.0.0.1:7811/mcp",
    );
    assert_eq!(aborted.code, 28, "curl gave up after a second");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let left = hosts.dialer.run("pgrep -f '[s]ervice-proxy'");
        if left.code == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "service-proxy still runs: {}",
            left.text()
        );
        sleep(Duration::from_millis(250));
    }
}

#[test]
#[ignore = "needs docker"]
fn the_tunnel_key_cannot_run_commands_on_the_dialer() {
    let hosts = Topology::start();
    let id = hosts.far.run("ssh -o BatchMode=yes dialer id");
    assert_ne!(id.code, 0);
    assert!(!id.text().contains("uid="), "{}", id.text());
    let shell = hosts
        .far
        .run("ssh -o BatchMode=yes dialer -- sh -c 'echo reached'");
    assert_ne!(shell.code, 0);
    assert!(!shell.text().contains("reached"), "{}", shell.text());
    assert_eq!(
        hosts.far.run("test -e ~/.ssh/dialer_key").code,
        1,
        "the far host holds no key to the dialer but the tunnel key"
    );
}

#[test]
#[ignore = "needs docker"]
fn the_link_comes_back_after_the_far_daemon_dies() {
    let hosts = Topology::start();
    hosts.dialer.ok("printf back > ~/docs/note.md");
    hosts.far.ok("pkill -9 -f '[/]opt/ferry/bin/ferry daemon'");
    hosts.wait_link(&Link::Down);
    hosts.wait_link(&Link::Up);
    let note = hosts.call("file-read note.md");
    assert_eq!(note.code, 0, "{}", note.stderr);
    assert_eq!(note.text(), "back");
}

#[test]
#[ignore = "needs docker"]
fn a_network_partition_fails_calls_and_heals() {
    let hosts = Topology::start();
    hosts.dialer.ok("printf healed > ~/docs/note.md");
    hosts.docker("disconnect");
    let begun = Instant::now();
    let during = hosts.call("file-read note.md");
    let waited = begun.elapsed();
    assert_eq!(during.code, 255, "{}", during.stderr);
    assert!(waited < Duration::from_secs(45), "{waited:?}");
    hosts.docker("connect");
    hosts.wait_link(&Link::Up);
    let after = hosts.call("file-read note.md");
    assert_eq!(after.code, 0, "{}", after.stderr);
    assert_eq!(after.text(), "healed");
}
