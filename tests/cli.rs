use std::process::Command;

fn ferry(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ferry"))
        .args(args)
        .output()
        .expect("ferry binary runs")
}

#[test]
fn help_exits_zero_and_names_every_command() {
    let output = ferry(&["help"]);
    assert_eq!(output.status.code(), Some(0));
    let text = String::from_utf8(output.stdout).expect("usage is utf-8");
    for word in ["daemon", "peer", "call", "ls", "reload", "help"] {
        assert!(text.contains(word), "usage lacks {word}");
    }
}

#[test]
fn unknown_command_exits_two() {
    let output = ferry(&["frobnicate"]);
    assert_eq!(output.status.code(), Some(2));
    let text = String::from_utf8(output.stderr).expect("stderr is utf-8");
    assert!(text.starts_with("usage: ferry"));
}

#[test]
fn no_command_exits_two() {
    assert_eq!(ferry(&[]).status.code(), Some(2));
}

#[test]
fn unbuilt_command_exits_two() {
    let output = ferry(&["ls"]);
    assert_eq!(output.status.code(), Some(2));
    let text = String::from_utf8(output.stderr).expect("stderr is utf-8");
    assert!(text.contains("ls is not built yet"));
}
