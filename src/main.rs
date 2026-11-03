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
        Command::Peer => std::process::exit(ferry::peer::run().await),
        Command::Call => {
            let Some(name) = args.next() else {
                eprint!("{USAGE}");
                return ExitCode::from(2);
            };
            let mut call_args = Vec::new();
            for arg in args {
                call_args.push(arg);
            }
            std::process::exit(ferry::client::call(name, call_args).await)
        }
        Command::Ls => {
            let json = match args.next().as_deref() {
                None => false,
                Some("--json") => true,
                Some(_) => {
                    eprint!("{USAGE}");
                    return ExitCode::from(2);
                }
            };
            report(ferry::client::ls(json).await)
        }
        Command::Reload => report(ferry::client::reload().await),
    }
}

fn report(result: anyhow::Result<()>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ferry: {err:#}");
            ExitCode::from(1)
        }
    }
}
