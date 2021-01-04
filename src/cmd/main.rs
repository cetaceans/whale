use structopt::StructOpt;

use futures::task::Spawn;
use whale::engine::server;

#[derive(StructOpt, Debug)]
#[structopt(name = "Grogu")]
struct Opts {
    /// Sets a custom config file. Could have been an Option<T> with no default too
    #[structopt(short, long, default_value = "grogu.conf")]
    config: String,
    /// A level of verbosity, and can be used multiple times
    #[structopt(short, long, parse(from_occurrences))]
    verbose: i32,

    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(StructOpt, Debug)]
enum SubCommand {
    /// server operation
    Server {
        /// Server start | stop
        #[structopt(default_value = "start")]
        server: String,
    },
}

fn main() {
    let opts: Opts = Opts::from_args();

    println!("Value for config: {}", opts.config);

    match opts.verbose {
        0 => println!("No verbose info"),
        1 => println!("Some verbose info"),
        2 => println!("Tons of verbose info"),
        3 | _ => println!("Don't be crazy"),
    }

    match opts.subcmd {
        SubCommand::Server { server } => match server.as_str() {
            "start" => {
                server::standalone::run();
            }
            "stop" => {
                println!("Server Stopped!")
            }
            _ => {}
        },
    }
}
