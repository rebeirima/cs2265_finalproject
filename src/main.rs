mod database;
use chrono::Local;
use std::{net::SocketAddr, sync::Arc};

use command::read_command;
use config::Config;
use database::Database;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    signal,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

mod command;
mod config;

/// TODO: explain how I lock levels to support threading
/// Ex: When switch the lock to the next level during range/stats, I make sure a writer cant get the lock

#[tokio::main]
async fn main() {
    let config = Config::parse_from_args();

    // TODO: could parallelize database creation since level initializations are independent
    let db = Arc::new(Database::new(config.data_dir));

    let listener = TcpListener::bind(("127.0.0.1", config.port)).await.unwrap();
    println!("Starting server on 127.0.0.1:{}!", config.port);

    let token = CancellationToken::new();
    let cloned_token = token.clone();

    let tracker = TaskTracker::new();
    tracker.spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                cloned_token.cancel();
            }
            Err(err) => {
                eprintln!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, client) = accept_result.unwrap();
                let db_clone = db.clone();
                let cloned_token = token.clone();
                tracker.spawn(async move {
                    println!("New connection with {:?}", client);
                    handle_connection(stream, client, db_clone, cloned_token).await;
                    println!("Closed connection with {:?}", client);
                });
            }
            _ = token.cancelled() => {
                break;
            }
        }
    }

    tracker.close();
    // Wait for everything to finish.
    tracker.wait().await;

    let db: Database = unsafe { Arc::try_unwrap(db).unwrap_unchecked() };
    db.cleanup();
}

async fn handle_connection(
    stream: TcpStream,
    _: SocketAddr,
    db: Arc<Database>,
    cancel_token: CancellationToken,
) {
    let now = Local::now();
    eprintln!("{}", now.format("%H:%M:%S%.6f"));

    let mut processed: usize = 0;
    let (read, mut write) = stream.into_split();
    let mut buf_read = BufReader::new(read);

    let mut out_buf = String::new();
    loop {
        tokio::select! {
            read_res = read_command(&mut buf_read) => {
                let command = if let Ok(command) = read_res {
                    command
                } else {
                    break;
                };

                processed += 1;
                // println!("Received command {:?} from {:?}, this is the {processed} command", command, addr);

                if processed % 10_000 == 0 {
                    let now = Local::now();
                    eprintln!("{}", now.format("%H:%M:%S%.6f"));
                }

                command.execute(&db, &mut out_buf).await;

                let mut bytes = out_buf.into_bytes();
                // delimiter
                bytes.push(0x00);
                write.write_all(&bytes).await.unwrap();

                bytes.clear();
                out_buf = unsafe { String::from_utf8_unchecked(bytes) };
            }
            _ = cancel_token.cancelled() => {
                break;
            }
        }
    }
}
