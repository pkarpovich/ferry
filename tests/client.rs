use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

fn temp_home() -> TempDir {
    tempfile::Builder::new()
        .prefix("fc")
        .tempdir_in("/tmp")
        .expect("temp home")
}

fn ferry(home: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ferry"));
    command.args(args).env("HOME", home);
    command
}

fn state_dir(home: &Path) -> std::path::PathBuf {
    let dir = home.join(".local/state/ferry");
    std::fs::create_dir_all(&dir).expect("state dir");
    dir
}

fn wait_for_exit(child: &mut Child, what: &str) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("wait works") {
            return status.code().expect("exited with a code");
        }
        assert!(Instant::now() < deadline, "{what} did not exit in time");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn read_first_line(stream: &mut impl Read) -> String {
    let mut text = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("a byte");
        if byte[0] == b'\n' {
            return String::from_utf8(text).expect("utf-8");
        }
        text.push(byte[0]);
    }
}

#[test]
fn call_without_a_daemon_exits_255() {
    let home = temp_home();
    let output = ferry(home.path(), &["call", "echo"])
        .output()
        .expect("runs");
    assert_eq!(output.status.code(), Some(255));
    let text = String::from_utf8(output.stderr).expect("utf-8");
    assert_eq!(text, "ferry: no daemon on this host\n");
}

#[test]
fn call_with_200_kib_of_arguments_exits_255_without_connecting() {
    let home = temp_home();
    let piece = "a".repeat(10 * 1024);
    let output = ferry(home.path(), &["call", "echo"])
        .args([piece.as_str(); 20])
        .output()
        .expect("runs");
    assert_eq!(output.status.code(), Some(255));
    let text = String::from_utf8(output.stderr).expect("utf-8");
    assert_eq!(text, "ferry: arguments too large\n");
}

#[test]
fn ls_and_reload_without_a_daemon_fail() {
    let home = temp_home();
    for args in [&["ls"][..], &["ls", "--json"], &["reload"]] {
        let output = ferry(home.path(), args).output().expect("runs");
        assert_eq!(output.status.code(), Some(1));
        let text = String::from_utf8(output.stderr).expect("utf-8");
        assert_eq!(text, "ferry: no daemon on this host\n");
    }
}

#[test]
fn call_returns_at_once_when_the_handler_exits_without_reading_stdin() {
    let home = temp_home();
    let listener = UnixListener::bind(state_dir(home.path()).join("ferry.sock")).expect("bind");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accepted");
        let request = read_first_line(&mut stream);
        assert!(request.contains("\"kind\":\"call\""), "{request}");
        stream.write_all(b"{\"ok\":true}\n").expect("ack");
        let chunk = "{\"kind\":\"chunk\",\"id\":0,\"stream\":\"stdout\",\"data\":\"aGk=\"}\n";
        stream.write_all(chunk.as_bytes()).expect("chunk");
        stream
            .write_all(b"{\"kind\":\"exit\",\"id\":0,\"code\":7}\n")
            .expect("exit");
        let mut rest = Vec::new();
        let _ = stream.read_to_end(&mut rest);
    });
    let mut child = ferry(home.path(), &["call", "quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawns");
    let held_open = child.stdin.take().expect("stdin pipe");
    assert_eq!(wait_for_exit(&mut child, "ferry call"), 7);
    let mut out = String::new();
    child
        .stdout
        .take()
        .expect("stdout pipe")
        .read_to_string(&mut out)
        .expect("output");
    assert_eq!(out, "hi");
    drop(held_open);
    server.join().expect("server thread");
}

#[test]
fn call_reports_the_refusal_text_and_code() {
    let home = temp_home();
    let listener = UnixListener::bind(state_dir(home.path()).join("ferry.sock")).expect("bind");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accepted");
        read_first_line(&mut stream);
        stream
            .write_all(b"{\"ok\":false,\"code\":255,\"error\":\"link not up\"}\n")
            .expect("refusal");
    });
    let output = ferry(home.path(), &["call", "x"]).output().expect("runs");
    assert_eq!(output.status.code(), Some(255));
    assert_eq!(
        String::from_utf8(output.stderr).expect("utf-8"),
        "ferry: link not up\n"
    );
    server.join().expect("server thread");
}

#[test]
fn peer_exits_at_once_when_the_daemon_closes_its_socket() {
    let home = temp_home();
    let listener = UnixListener::bind(state_dir(home.path()).join("ferry.sock")).expect("bind");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accepted");
        let request = read_first_line(&mut stream);
        assert_eq!(request, "{\"kind\":\"peer\"}");
        stream.write_all(b"{\"ok\":true}\nafter").expect("ack");
    });
    let mut child = ferry(home.path(), &["peer"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawns");
    let held_open = child.stdin.take().expect("stdin pipe");
    assert_eq!(wait_for_exit(&mut child, "ferry peer"), 0);
    let mut out = String::new();
    child
        .stdout
        .take()
        .expect("stdout pipe")
        .read_to_string(&mut out)
        .expect("output");
    assert_eq!(out, "after");
    drop(held_open);
    server.join().expect("server thread");
}

#[test]
fn peer_starts_a_daemon_when_none_runs() {
    let home = temp_home();
    let mut child = ferry(home.path(), &["peer"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawns");
    let held_open = child.stdin.take().expect("stdin pipe");
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout pipe"));
    let mut hello = String::new();
    stdout.read_line(&mut hello).expect("hello line");
    assert!(hello.contains("\"kind\":\"hello\""), "{hello}");
    let pid = std::fs::read_to_string(home.path().join(".local/state/ferry/ferry.lock"))
        .expect("lock file holds the daemon pid");
    drop(held_open);
    assert_eq!(wait_for_exit(&mut child, "ferry peer"), 0);
    let status = Command::new("kill")
        .arg(pid.trim())
        .status()
        .expect("kill runs");
    assert!(status.success());
}
