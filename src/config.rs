//! This host's `config.toml`.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::name::HandlerName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listen {
    pub port: u16,
    pub handler: HandlerName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub handlers: Option<PathBuf>,
    pub connect: Option<String>,
    pub remote_ferry: String,
    pub listen: Vec<Listen>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawListen {
    port: u16,
    handler: HandlerName,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    handlers: Option<String>,
    connect: Option<String>,
    remote_ferry: Option<String>,
    #[serde(default)]
    listen: Vec<RawListen>,
}

impl Config {
    #[must_use]
    pub fn path(home: &Path) -> PathBuf {
        home.join(".config").join("ferry").join("config.toml")
    }

    /// # Errors
    /// Fails when the file exists but cannot be read or parsed.
    pub fn load(home: &Path) -> Result<Self> {
        let path = Self::path(home);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => String::new(),
            Err(err) => {
                return Err(err).with_context(|| format!("cannot read {}", path.display()));
            }
        };
        Self::parse(&text, home).with_context(|| format!("bad config {}", path.display()))
    }

    /// # Errors
    /// Fails on a TOML error, an unknown key, a port outside 1-65535 or an
    /// invalid handler name.
    pub fn parse(text: &str, home: &Path) -> Result<Self> {
        let RawConfig {
            handlers,
            connect,
            remote_ferry,
            listen,
        } = toml::from_str(text)?;
        let handlers = handlers.map(|folder| expand_home(&folder, home));
        let remote_ferry = remote_ferry.unwrap_or_else(|| "ferry".to_owned());
        let mut ports = Vec::new();
        for RawListen { port, handler } in listen {
            if port == 0 {
                bail!("listen port must be between 1 and 65535, got 0");
            }
            ports.push(Listen { port, handler });
        }
        Ok(Self {
            handlers,
            connect,
            remote_ferry,
            listen: ports,
        })
    }
}

fn expand_home(folder: &str, home: &Path) -> PathBuf {
    let Some(rest) = folder.strip_prefix("~/") else {
        return PathBuf::from(folder);
    };
    home.join(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home() -> PathBuf {
        PathBuf::from("/home/test")
    }

    #[test]
    fn full_config_parses() {
        let text = r#"
handlers = "~/.config/ferry/handlers"
connect = "far"
remote_ferry = "/opt/bin/ferry"

[[listen]]
port = 7811
handler = "service-proxy"

[[listen]]
port = 7812
handler = "other"
"#;
        let config = Config::parse(text, &home()).expect("parses");
        assert_eq!(
            config.handlers,
            Some(PathBuf::from("/home/test/.config/ferry/handlers"))
        );
        assert_eq!(config.connect.as_deref(), Some("far"));
        assert_eq!(config.remote_ferry, "/opt/bin/ferry");
        assert_eq!(config.listen.len(), 2);
        assert_eq!(config.listen[0].port, 7811);
        assert_eq!(config.listen[0].handler.as_str(), "service-proxy");
    }

    #[test]
    fn empty_text_gives_empty_config() {
        let config = Config::parse("", &home()).expect("parses");
        assert_eq!(config.handlers, None);
        assert_eq!(config.connect, None);
        assert_eq!(config.remote_ferry, "ferry");
        assert_eq!(config.listen.len(), 0);
    }

    #[test]
    fn missing_file_gives_empty_config() {
        let dir = tempfile::Builder::new()
            .tempdir_in("/tmp")
            .expect("temp dir");
        let config = Config::load(dir.path()).expect("loads");
        assert_eq!(config, Config::parse("", dir.path()).expect("parses"));
    }

    #[test]
    fn load_reads_the_file_and_names_it_on_error() {
        let dir = tempfile::Builder::new()
            .tempdir_in("/tmp")
            .expect("temp dir");
        let path = Config::path(dir.path());
        fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        fs::write(&path, "connect = \"far\"\n").expect("write");
        let config = Config::load(dir.path()).expect("loads");
        assert_eq!(config.connect.as_deref(), Some("far"));

        fs::write(&path, "bogus = 1\n").expect("write");
        let err = Config::load(dir.path()).expect_err("fails");
        assert!(format!("{err:#}").contains("config.toml"), "{err:#}");
    }

    #[test]
    fn unknown_key_fails_naming_it() {
        let err = Config::parse("conect = \"far\"\n", &home()).expect_err("fails");
        assert!(format!("{err:#}").contains("conect"), "{err:#}");
        let err = Config::parse(
            "[[listen]]\nport = 1\nhandler = \"a\"\nextra = 2\n",
            &home(),
        )
        .expect_err("fails");
        assert!(format!("{err:#}").contains("extra"), "{err:#}");
    }

    #[test]
    fn bad_ports_fail() {
        for port in ["0", "70000"] {
            let text = format!("[[listen]]\nport = {port}\nhandler = \"a\"\n");
            Config::parse(&text, &home()).expect_err("bad port");
        }
        let text = "[[listen]]\nport = 65535\nhandler = \"a\"\n";
        Config::parse(text, &home()).expect("max port is fine");
    }

    #[test]
    fn invalid_handler_name_fails() {
        let text = "[[listen]]\nport = 80\nhandler = \"Bad Name\"\n";
        let err = Config::parse(text, &home()).expect_err("fails");
        assert!(format!("{err:#}").contains("Bad Name"), "{err:#}");
    }

    #[test]
    fn tilde_expands_only_with_a_slash() {
        assert_eq!(expand_home("~/h", &home()), PathBuf::from("/home/test/h"));
        assert_eq!(expand_home("/abs/h", &home()), PathBuf::from("/abs/h"));
        assert_eq!(expand_home("~h", &home()), PathBuf::from("~h"));
    }
}
