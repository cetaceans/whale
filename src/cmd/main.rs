use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::Instant;
use std::{fs, process};

use arrow::util::pretty;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use log::{error, info};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;

use whale::common::config::WhaleConfig;
use whale::common::logger::Logger;
use whale::engine::client::Client;
use whale::engine::server::WhaleServer;
use whale::{ABOUT, WHALE_VERSION};

#[derive(StructOpt, Debug)]
#[structopt(name = "
        ▀▄▀
      ▄███████▄     ▀██▄██▀
    ▄█████▀█████▄     ▄█
    ███████▀████████▀
    ▄▄▄▄▄▄▄███████▀

Version:", about = ABOUT)]
struct Opts {
    /// Activate debug mode
    #[structopt(short, long)]
    debug: bool,
    /// Sets a custom config file.
    #[structopt(short, long, default_value = "")]
    config: String,
    /// subcommands
    #[structopt(subcommand)]
    subcmd: SubCommand,
}

#[derive(StructOpt, Debug)]
enum SubCommand {
    /// Connect to a single node, pseudo-distributed or distributed server
    Connect {
        /// Connect Whale server `ip:port`
        #[structopt(default_value = "localhost:3697")]
        url: String,
    },
    /// Server daemon management
    Server {
        /// Server start
        #[structopt(short, long)]
        server: bool,
    },
    /// History file management
    His {
        /// History file print|clear
        #[structopt(default_value = "print")]
        his: String,
    },
}

#[paw::main]
fn main(args: paw::Args) {
    if args.len() == 1 {
        readline();
    }

    let opts: Opts = Opts::from_args();
    match opts.debug {
        true => {
            match Logger::level("debug") {
                Ok(logger) => {
                    info!("CLI opts {:?}", &opts);
                    info!("Log path [{}]", logger.log_path.display());
                }
                Err(_e) => error!("Logger setup error!"),
            };
        }
        false => {}
    }

    let mut config = WhaleConfig::default();
    match opts.config {
        String { .. } => {
            if opts.config.is_empty() {
                info!("config file path-[{}]", config.config_path.display());
            } else {
                let config_path = PathBuf::from(opts.config);
                if !config_path.exists() {
                    println!("Config file does not exists.");
                    process::exit(0x0100);
                }
                if config_path.is_file() {
                    config = WhaleConfig::new(config_path);
                    info!("config file path-[{}]", config.config_path.display());
                } else {
                    println!("Config file does not a file.");
                    process::exit(0x0100);
                }
            }
        }
    }

    match opts.subcmd {
        SubCommand::Connect { url, .. } => {
            readline_with_client(url);
        }
        SubCommand::Server { server } => match server {
            _ => {
                WhaleServer::start(config).as_ref();
            }
        },
        SubCommand::His { his } => match his.as_str() {
            "clear" => {
                let his = WhaleConfig::default().his_path;
                if his.exists() {
                    fs::remove_file(his).unwrap();
                } else {
                    println!("History file does not exists.")
                }
            }
            "print" => {
                let his = WhaleConfig::default().his_path;
                println!("History file path-[{:?}]", &his);
                if his.exists() {
                    let mut file = File::open(his).unwrap();
                    let mut contents = String::new();
                    file.read_to_string(&mut contents).unwrap();
                    print!("{}", contents.trim_end());
                } else {
                    println!("History file does not exists.")
                }
            }
            _ => {}
        },
    }
}

#[tokio::main]
async fn readline() {
    slogan();
    let history = WhaleConfig::default().his_path;
    let mut rl = Editor::<()>::new();
    rl.load_history(&history.as_path()).ok();

    let mut ctx = ExecutionContext::with_config(ExecutionConfig::new().with_batch_size(1_048_576));
    let mut query = "".to_owned();
    loop {
        let readline = rl.readline("WHALE:>");
        match readline {
            Ok(ref line) if is_exit(line) && query.is_empty() => {
                break;
            }
            Ok(ref line) if is_help(line) => {
                help_info();
            }
            Ok(ref line) if line.trim_end().ends_with(';') => {
                query.push_str(line.trim_end());
                rl.add_history_entry(query.clone().as_str());
                match exec_local_query(&mut ctx, query).await {
                    Ok(_) => {}
                    Err(err) => println!("{:?}", err),
                }
                query = "".to_owned();
            }
            Ok(ref line) => {
                query.push_str(line);
                query.push(' ');
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(_) => {
                break;
            }
        }
    }

    rl.save_history(history.as_path()).unwrap();
}

async fn exec_local_query(ctx: &mut ExecutionContext, sql: String) -> Result<(), DataFusionError> {
    let now = Instant::now();

    let df = ctx.sql(&sql)?;
    let results = df.collect().await?;

    if results.is_empty() {
        println!(
            "0 rows in set. Query took {} seconds.",
            now.elapsed().as_secs()
        );
        return Ok(());
    }

    pretty::print_batches(&results)?;

    let row_count: usize = results.iter().map(|b| b.num_rows()).sum();

    if row_count > 1 {
        println!(
            "{} row in set. Query took {} seconds.",
            row_count,
            now.elapsed().as_secs()
        );
    } else {
        println!(
            "{} rows in set. Query took {} seconds.",
            row_count,
            now.elapsed().as_secs()
        );
    }
    Ok(())
}

#[tokio::main]
async fn readline_with_client(url: String) -> Result<(), Box<dyn std::error::Error>> {
    match Client::connect(url.clone()).await {
        Ok(mut client) => {
            slogan();
            println!("Connect to server - [{}]", url.clone());
            let history = WhaleConfig::default().his_path;
            let mut rl = Editor::<()>::new();
            rl.load_history(&history.as_path()).ok();

            let mut query = "".to_owned();
            loop {
                let readline = rl.readline("WHALE:>");
                match readline {
                    Ok(ref line) if is_exit(line) && query.is_empty() => {
                        break;
                    }
                    Ok(ref line) if is_help(line) => {
                        help_info();
                    }
                    Ok(ref line) if line.trim_end().ends_with(';') => {
                        query.push_str(line.trim_end());
                        rl.add_history_entry(query.clone().as_str());
                        match client.exec_query(query).await {
                            Ok(_) => {}
                            Err(err) => println!("{}", err),
                        }
                        query = "".to_owned();
                    }
                    Ok(ref line) => {
                        query.push_str(line);
                        query.push(' ');
                    }
                    Err(ReadlineError::Interrupted) => {
                        break;
                    }
                    Err(ReadlineError::Eof) => {
                        break;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            rl.save_history(history.as_path()).unwrap();
        }
        Err(err) => println!("Failed to connect [{}]-{}", url, err),
    };
    Ok(())
}

fn slogan() {
    println!(
        "
        ▀▄▀
      ▄███████▄     ▀██▄██▀
    ▄█████▀█████▄     ▄█
    ███████▀████████▀
    ▄▄▄▄▄▄▄███████▀

Version: {}\n{}\nEnter the `help` command \
to view help information.",
        WHALE_VERSION, ABOUT
    );
}

fn is_exit(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line.starts_with("quit") || line.starts_with("exit")
}

fn is_help(line: &str) -> bool {
    let line = line.trim_end().to_lowercase();
    line.starts_with("help")
}

fn help_info() {
    println!("help infos:");
    println!("\tDDL: `create` `drop` `alter` etc..");
    println!("\tDML: `insert` `delete` `update` etc..");
    println!("\tDCL: `grant` `revoke` etc..");
}
