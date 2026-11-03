mod support;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use std::process::Stdio;
use std::time::Duration;

use support::{Host, LinkState, free_port, pair, wait_until};

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("utf-8")
}

#[test]
fn handlers_are_called_in_both_directions() {
    let (dialer, far) = pair("");
    dialer.add_handler("dialer-say", "printf '%s|' \"$@\"; cat");
    far.add_handler("far-say", "printf '%s|' \"$@\"; cat");
    dialer.ferry(&["reload"]).output().expect("reload");
    far.ferry(&["reload"]).output().expect("reload");

    let out = dialer.call(&["far-say", "a b", "$HOME"], "from dialer");
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(text(&out.stdout), "a b|$HOME|from dialer");

    let out = far.call(&["dialer-say", "x"], "from far");
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(text(&out.stdout), "x|from far");
}

#[test]
fn handler_exit_code_and_stderr_pass_through() {
    let (dialer, far) = pair("");
    far.add_handler("fail", "echo oops >&2; exit 7");
    far.ferry(&["reload"]).output().expect("reload");
    let out = dialer.call(&["fail"], "");
    assert_eq!(out.status.code(), Some(7));
    assert_eq!(text(&out.stderr), "oops\n");
}

#[test]
fn listen_port_hands_the_socket_to_a_handler_on_the_other_side() {
    let port = free_port();
    let (dialer, far) = pair(&format!(
        "[[listen]]\nport = {port}\nhandler = \"echo-back\"\n"
    ));
    dialer.add_handler("echo-back", "exec cat");
    dialer.ferry(&["reload"]).output().expect("reload");
    far.ferry(&["reload"]).output().expect("reload");

    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("listen port is open");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    stream.write_all(b"ping over tcp").expect("request");
    stream.shutdown(Shutdown::Write).expect("half close");
    let mut answer = String::new();
    stream.read_to_string(&mut answer).expect("answer");
    assert_eq!(answer, "ping over tcp");
}

#[test]
fn unknown_name_gives_127_and_unstartable_file_gives_126() {
    let (dialer, far) = pair("");
    far.add_script(
        "broken",
        "#!/nonexistent/interpreter\n# @ferry.name broken\n",
    );
    far.ferry(&["reload"]).output().expect("reload");

    let out = dialer.call(&["nonesuch"], "");
    assert_eq!(out.status.code(), Some(127));
    assert_eq!(
        text(&out.stderr),
        "ferry: no handler nonesuch on the other side\n"
    );

    let out = dialer.call(&["broken"], "");
    assert_eq!(out.status.code(), Some(126));
    assert!(
        text(&out.stderr).starts_with("ferry: cannot start broken: "),
        "{}",
        text(&out.stderr)
    );
}

#[test]
fn call_with_the_link_down_gives_255() {
    let mut alone = Host::new();
    alone.write_config("handlers = \"~/handlers\"\n");
    alone.start_daemon(None);
    let out = alone.call(&["anything"], "");
    assert_eq!(out.status.code(), Some(255));
    assert!(text(&out.stderr).starts_with("ferry: "));
}

#[test]
fn killing_the_far_daemon_ends_a_call_with_255_and_the_link_returns() {
    let (dialer, mut far) = pair("");
    far.add_handler("slow", "exec sleep 6");
    far.ferry(&["reload"]).output().expect("reload");

    let child = dialer
        .ferry(&["call", "slow"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("call starts");
    std::thread::sleep(Duration::from_millis(500));
    far.kill_daemon();
    let out = child.wait_with_output().expect("call ends");
    assert_eq!(out.status.code(), Some(255));

    far.start_daemon(None);
    dialer.wait_link(LinkState::Up);
    far.wait_link(LinkState::Up);
    let out = dialer.call(&["slow-gone"], "");
    assert_eq!(out.status.code(), Some(127));
}

#[test]
fn killing_ferry_call_stops_the_handler_on_the_other_side() {
    let (dialer, far) = pair("");
    let pidfile = far.path().join("hang.pid");
    far.add_handler(
        "hang",
        &format!("echo $$ > '{}'\nexec sleep 60", pidfile.display()),
    );
    far.ferry(&["reload"]).output().expect("reload");

    let mut child = dialer
        .ferry(&["call", "hang"])
        .stdin(Stdio::piped())
        .spawn()
        .expect("call starts");
    wait_until("handler pid", || {
        std::fs::read_to_string(&pidfile).is_ok_and(|text| text.ends_with('\n'))
    });
    let pid = std::fs::read_to_string(&pidfile)
        .expect("pid file")
        .trim()
        .to_owned();
    child.kill().expect("call killed");
    child.wait().expect("call reaped");

    wait_until("handler gone", || {
        let status = std::process::Command::new("kill")
            .args(["-0", &pid])
            .stderr(Stdio::null())
            .status()
            .expect("kill runs");
        !status.success()
    });
}

#[test]
fn reload_makes_a_new_handler_callable_without_reconnecting() {
    let (dialer, far) = pair("");
    let out = dialer.call(&["late"], "");
    assert_eq!(out.status.code(), Some(127));

    far.add_handler("late", "echo here");
    let out = far.ferry(&["reload"]).output().expect("reload runs");
    assert_eq!(out.status.code(), Some(0));

    let out = dialer.call(&["late"], "");
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(text(&out.stdout), "here\n");
    wait_until("handlers frame", || {
        dialer.remote_names().contains(&"late".to_owned())
    });
}
