//! Where a host keeps its state.

use std::fs;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

pub const MAX_SOCKET_PATH: usize = 103;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    pub state_dir: PathBuf,
    pub socket: PathBuf,
    pub lock: PathBuf,
    pub log: PathBuf,
}

impl Paths {
    /// Computes the paths under `home` without touching the disk.
    ///
    /// # Errors
    /// Fails when the socket path is longer than 103 bytes.
    pub fn new(home: &Path) -> Result<Self> {
        let state_dir = home.join(".local").join("state").join("ferry");
        let socket = state_dir.join("ferry.sock");
        let length = socket.as_os_str().len();
        if length > MAX_SOCKET_PATH {
            bail!(
                "socket path {} is {length} bytes, at most {MAX_SOCKET_PATH} fit",
                socket.display()
            );
        }
        Ok(Self {
            lock: state_dir.join("ferry.lock"),
            log: state_dir.join("ferry.log"),
            state_dir,
            socket,
        })
    }

    /// Creates the state directory with mode 0700 if it is missing.
    ///
    /// # Errors
    /// Fails when the directory cannot be created or its mode cannot be set.
    pub fn create_state_dir(&self) -> Result<()> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.state_dir)
            .with_context(|| format!("cannot create {}", self.state_dir.display()))?;
        fs::set_permissions(&self.state_dir, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("cannot set mode on {}", self.state_dir.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home_with_socket_len(length: usize) -> PathBuf {
        let suffix = "/.local/state/ferry/ferry.sock";
        let base = length - suffix.len();
        PathBuf::from(format!("/{}", "h".repeat(base - 1)))
    }

    #[test]
    fn paths_sit_in_the_state_dir() {
        let paths = Paths::new(Path::new("/home/t")).expect("short path");
        assert_eq!(paths.state_dir, Path::new("/home/t/.local/state/ferry"));
        assert_eq!(
            paths.socket,
            Path::new("/home/t/.local/state/ferry/ferry.sock")
        );
        assert_eq!(
            paths.lock,
            Path::new("/home/t/.local/state/ferry/ferry.lock")
        );
        assert_eq!(paths.log, Path::new("/home/t/.local/state/ferry/ferry.log"));
    }

    #[test]
    fn socket_check_passes_at_103_and_fails_at_104() {
        let ok = home_with_socket_len(103);
        let paths = Paths::new(&ok).expect("103 fits");
        assert_eq!(paths.socket.as_os_str().len(), 103);

        let long = home_with_socket_len(104);
        let err = Paths::new(&long).expect_err("104 does not fit");
        let text = err.to_string();
        assert!(text.contains("is 104 bytes, at most 103 fit"), "{text}");
        assert!(text.starts_with("socket path /"), "{text}");
    }

    #[test]
    fn state_dir_is_created_with_mode_0700() {
        let dir = tempfile::Builder::new()
            .tempdir_in("/tmp")
            .expect("temp dir");
        let paths = Paths::new(dir.path()).expect("paths");
        paths.create_state_dir().expect("creates");
        paths.create_state_dir().expect("idempotent");
        let mode = fs::metadata(&paths.state_dir)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }
}
