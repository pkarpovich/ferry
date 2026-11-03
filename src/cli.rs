//! Command-line dispatch.

/// A command of the `ferry` binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Daemon,
    Peer,
    Call,
    Ls,
    Reload,
    Help,
}

impl Command {
    /// Parses the first command-line argument.
    #[must_use]
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "daemon" => Some(Self::Daemon),
            "peer" => Some(Self::Peer),
            "call" => Some(Self::Call),
            "ls" => Some(Self::Ls),
            "reload" => Some(Self::Reload),
            "help" => Some(Self::Help),
            _ => None,
        }
    }

    /// The word that selects this command.
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            Self::Daemon => "daemon",
            Self::Peer => "peer",
            Self::Call => "call",
            Self::Ls => "ls",
            Self::Reload => "reload",
            Self::Help => "help",
        }
    }
}

/// The usage text printed by `ferry help`.
pub const USAGE: &str = "\
usage: ferry <command>

commands:
  daemon                    run the daemon in the foreground
  peer                      the bridge that sshd runs on the far host
  call <name> [args...]     call a handler on the other side
  ls [--json]               show the link and the handlers of both sides
  reload                    re-scan the handler folder
  help                      show this text
";
