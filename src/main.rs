use std::process::ExitCode;

use ferry::cli::{Command, USAGE};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next().as_deref().and_then(Command::parse) else {
        eprint!("{USAGE}");
        return ExitCode::from(2);
    };
    match command {
        Command::Help => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Command::Daemon => match ferry::daemon::run().await {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("ferry: {err:#}");
                ExitCode::from(1)
            }
        },
        Command::Peer | Command::Call | Command::Ls | Command::Reload => {
            eprintln!("ferry: {} is not built yet", command.word());
            ExitCode::from(2)
        }
    }
}
