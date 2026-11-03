//! Finding the executable files a host offers as handlers.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, ErrorKind};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::name::HandlerName;

pub const MARKER_LINES: usize = 64;
pub const MAX_DESC_BYTES: usize = 200;
pub const MAX_HANDLERS: usize = 256;

const NAME_MARKER: &str = "# @ferry.name ";
const DESC_MARKER: &str = "# @ferry.desc ";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handler {
    pub name: HandlerName,
    pub desc: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Skipped {
    pub file: String,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Scan {
    pub handlers: Vec<Handler>,
    pub skipped: Vec<Skipped>,
}

struct Marker {
    name: String,
    desc: String,
}

fn read_marker(path: &Path) -> std::io::Result<Option<Marker>> {
    let reader = BufReader::new(File::open(path)?);
    let mut name = None;
    let mut desc = String::new();
    let mut lines = reader.split(b'\n');
    for _ in 0..MARKER_LINES {
        let Some(line) = lines.next() else { break };
        let line = line?;
        let line = String::from_utf8_lossy(&line);
        let line = line.trim();
        if name.is_none() {
            if let Some(found) = line.strip_prefix(NAME_MARKER) {
                name = Some(found.trim().to_owned());
            }
            continue;
        }
        if let Some(found) = line.strip_prefix(DESC_MARKER) {
            found.trim().clone_into(&mut desc);
            break;
        }
    }
    let Some(name) = name else { return Ok(None) };
    let cut = desc.floor_char_boundary(MAX_DESC_BYTES);
    desc.truncate(cut);
    Ok(Some(Marker { name, desc }))
}

/// Scans `dir` for handlers. A missing folder gives an empty scan.
#[must_use]
pub fn scan(dir: &Path) -> Scan {
    let mut result = Scan::default();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return result,
        Err(err) => {
            result.skipped.push(Skipped {
                file: dir.display().to_string(),
                reason: format!("cannot read folder: {err}"),
            });
            return result;
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else { continue };
        paths.push(entry.path());
    }
    paths.sort();

    for path in paths {
        let Some(file) = path.file_name() else {
            continue;
        };
        let file = file.to_string_lossy().into_owned();
        let Ok(meta) = fs::metadata(&path) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let marker = match read_marker(&path) {
            Ok(Some(marker)) => marker,
            Ok(None) => continue,
            Err(err) => {
                result.skipped.push(Skipped {
                    file,
                    reason: format!("cannot read: {err}"),
                });
                continue;
            }
        };
        let Marker { name, desc } = marker;
        let name = match HandlerName::parse(&name) {
            Ok(name) => name,
            Err(err) => {
                result.skipped.push(Skipped {
                    file,
                    reason: err.to_string(),
                });
                continue;
            }
        };
        if meta.permissions().mode() & 0o100 == 0 {
            result.skipped.push(Skipped {
                file,
                reason: "not executable".to_owned(),
            });
            continue;
        }
        let mut taken = false;
        for known in &result.handlers {
            if known.name == name {
                taken = true;
                break;
            }
        }
        if taken {
            result.skipped.push(Skipped {
                file,
                reason: format!("name {name} is already taken"),
            });
            continue;
        }
        if result.handlers.len() >= MAX_HANDLERS {
            result.skipped.push(Skipped {
                file,
                reason: "too many handlers".to_owned(),
            });
            continue;
        }
        result.handlers.push(Handler { name, desc, path });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, file: &str, body: &str, mode: u32) {
        let path = dir.join(file);
        fs::write(&path, body).expect("write file");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("chmod");
    }

    fn marked(name: &str) -> String {
        format!("#!/bin/sh\n# @ferry.name {name}\nexit 0\n")
    }

    #[test]
    fn finds_marked_script_with_description() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            dir.path(),
            "a.sh",
            "#!/bin/sh\n# @ferry.name echo-it\n# @ferry.desc says things\n",
            0o755,
        );
        let Scan { handlers, skipped } = scan(dir.path());
        assert_eq!(skipped, Vec::new());
        assert_eq!(handlers.len(), 1);
        let Handler { name, desc, path } = &handlers[0];
        assert_eq!(name.as_str(), "echo-it");
        assert_eq!(desc, "says things");
        assert_eq!(path, &dir.path().join("a.sh"));
    }

    #[test]
    fn file_without_marker_is_ignored_silently() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "plain", "#!/bin/sh\nexit 0\n", 0o755);
        write(dir.path(), "plain-noexec", "hello\n", 0o644);
        assert_eq!(scan(dir.path()), Scan::default());
    }

    #[test]
    fn non_executable_and_invalid_name_are_skipped_with_reason() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "a", &marked("fine"), 0o644);
        write(dir.path(), "b", &marked("Bad Name"), 0o755);
        let Scan { handlers, skipped } = scan(dir.path());
        assert_eq!(handlers, Vec::new());
        assert_eq!(skipped.len(), 2);
        assert_eq!(skipped[0].file, "a");
        assert_eq!(skipped[0].reason, "not executable");
        assert_eq!(skipped[1].file, "b");
        assert!(skipped[1].reason.contains("invalid handler name"));
    }

    #[test]
    fn duplicate_name_keeps_first_file_by_name_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "b", &marked("same"), 0o755);
        write(dir.path(), "a", &marked("same"), 0o755);
        let Scan { handlers, skipped } = scan(dir.path());
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].path, dir.path().join("a"));
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].file, "b");
    }

    #[test]
    fn marker_on_line_65_is_not_seen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut late = "#\n".repeat(64);
        late.push_str("# @ferry.name late\n");
        write(dir.path(), "late", &late, 0o755);
        let mut last = "#\n".repeat(63);
        last.push_str("# @ferry.name last\n");
        write(dir.path(), "last", &last, 0o755);
        let Scan { handlers, skipped } = scan(dir.path());
        assert_eq!(skipped, Vec::new());
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].name.as_str(), "last");
    }

    #[test]
    fn long_description_is_cut_at_a_character_boundary() {
        let dir = tempfile::tempdir().expect("tempdir");
        let body = format!("# @ferry.name a\n# @ferry.desc {}\n", "x".repeat(300));
        write(dir.path(), "a", &body, 0o755);
        let body = format!("# @ferry.name b\n# @ferry.desc {}\n", "é".repeat(150));
        write(dir.path(), "b", &body, 0o755);
        let Scan { handlers, .. } = scan(dir.path());
        assert_eq!(handlers[0].desc.len(), 200);
        assert_eq!(handlers[1].desc.len(), 200);
        assert_eq!(handlers[1].desc.chars().count(), 100);
    }

    #[test]
    fn handler_past_the_limit_is_skipped() {
        let dir = tempfile::tempdir().expect("tempdir");
        for index in 0..=MAX_HANDLERS {
            write(
                dir.path(),
                &format!("h{index:03}"),
                &marked(&format!("h{index:03}")),
                0o755,
            );
        }
        let Scan { handlers, skipped } = scan(dir.path());
        assert_eq!(handlers.len(), MAX_HANDLERS);
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].file, "h256");
        assert_eq!(skipped[0].reason, "too many handlers");
    }

    #[test]
    fn symlinks_are_followed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target = tempfile::tempdir().expect("tempdir");
        write(target.path(), "real", &marked("linked"), 0o755);
        std::os::unix::fs::symlink(target.path().join("real"), dir.path().join("link"))
            .expect("symlink");
        let Scan { handlers, .. } = scan(dir.path());
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].name.as_str(), "linked");
    }

    #[test]
    fn missing_folder_gives_empty_scan() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(scan(&dir.path().join("nope")), Scan::default());
    }
}
