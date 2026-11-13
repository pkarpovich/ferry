use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

pub const FERRY: &str = env!("CARGO_BIN_EXE_ferry");

#[derive(Clone, Copy)]
pub enum LinkState {
    Up,
    Down,
}

pub struct Host {
    home: TempDir,
    daemon: Option<Child>,
}

impl Host {
    pub fn new() -> Self {
        let home = tempfile::Builder::new()
            .prefix("fe")
            .tempdir_in("/tmp")
            .expect("temp home");
        fs::create_dir_all(home.path().join("handlers")).expect("handlers dir");
        Self { home, daemon: None }
    }

    pub fn path(&self) -> &Path {
        self.home.path()
    }

    pub fn write_config(&self, text: &str) {
        let dir = self.path().join(".config/ferry");
        fs::create_dir_all(&dir).expect("config dir");
        fs::write(dir.join("config.toml"), text).expect("config file");
    }

    pub fn add_script(&self, file: &str, text: &str) {
        let path = self.path().join("handlers").join(file);
        fs::write(&path, text).expect("handler file");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("handler mode");
    }

    pub fn add_handler(&self, name: &str, body: &str) {
        let text = format!("#!/bin/sh\n# @ferry.name {name}\n{body}\n");
        self.add_script(name, &text);
    }

    pub fn ferry(&self, args: &[&str]) -> Command {
        let mut command = Command::new(FERRY);
        command.args(args).env("HOME", self.path());
        command
    }

    pub fn call(&self, args: &[&str], stdin: &str) -> Output {
        let mut full = vec!["call"];
        full.extend_from_slice(args);
        let mut child = self
            .ferry(&full)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("ferry call starts");
        let mut pipe = child.stdin.take().expect("stdin pipe");
        pipe.write_all(stdin.as_bytes()).expect("stdin written");
        drop(pipe);
        child.wait_with_output().expect("ferry call ends")
    }

    pub fn start_daemon(&mut self, ssh: Option<&Path>) {
        let log = fs::File::create(self.path().join("daemon.out")).expect("daemon log");
        let mut command = self.ferry(&["daemon"]);
        command
            .stdin(Stdio::null())
            .stdout(log.try_clone().expect("log handle"))
            .stderr(log);
        if let Some(ssh) = ssh {
            command.env("FERRY_SSH", ssh);
        }
        self.daemon = Some(command.spawn().expect("daemon starts"));
        let socket = self.path().join(".local/state/ferry/ferry.sock");
        wait_until("daemon socket", || socket.exists());
    }

    pub fn kill_daemon(&mut self) {
        let Some(mut daemon) = self.daemon.take() else {
            return;
        };
        daemon.kill().expect("daemon killed");
        daemon.wait().expect("daemon reaped");
    }

    pub fn link(&self) -> Option<LinkState> {
        let output = self.ferry(&["ls", "--json"]).output().expect("ls runs");
        let reply: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
        match reply["link"]["state"].as_str()? {
            "up" => Some(LinkState::Up),
            "down" => Some(LinkState::Down),
            _ => None,
        }
    }

    pub fn wait_link(&self, want: LinkState) {
        wait_until("link state", || {
            let Some(state) = self.link() else {
                return false;
            };
            match (state, want) {
                (LinkState::Up, LinkState::Up) | (LinkState::Down, LinkState::Down) => true,
                (LinkState::Up, LinkState::Down) | (LinkState::Down, LinkState::Up) => false,
            }
        });
    }

    pub fn remote_names(&self) -> Vec<String> {
        let output = self.ferry(&["ls", "--json"]).output().expect("ls runs");
        let reply: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("ls prints json");
        let mut names = Vec::new();
        for entry in reply["remote"].as_array().expect("remote list") {
            names.push(entry["name"].as_str().expect("name").to_owned());
        }
        names
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        self.kill_daemon();
    }
}

pub fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn write_ssh_script(dialer: &Path, far: &Path) -> PathBuf {
    let path = dialer.join("ssh.sh");
    let text = format!("#!/bin/sh\nHOME='{}' exec '{FERRY}' peer\n", far.display());
    fs::write(&path, text).expect("ssh script");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("ssh script mode");
    path
}

pub fn pair(far_config: &str) -> (Host, Host) {
    let mut dialer = Host::new();
    let mut far = Host::new();
    far.write_config(&format!("handlers = \"~/handlers\"\n{far_config}"));
    dialer.write_config("handlers = \"~/handlers\"\nconnect = \"far\"\n");
    let ssh = write_ssh_script(dialer.path(), far.path());
    far.start_daemon(None);
    dialer.start_daemon(Some(&ssh));
    dialer.wait_link(LinkState::Up);
    far.wait_link(LinkState::Up);
    (dialer, far)
}

pub fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("addr").port()
}
