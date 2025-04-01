use core::str;
use std::{
    io::{self, BufRead, BufReader, Write},
    net::TcpStream,
};

use clap::{command, Parser};
use command::Command;
mod command;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1234)]
    port: u16,
}

fn main() -> io::Result<()> {
    let args = Args::parse();
    let mut input_buf = String::new();
    let mut output_buf = Vec::new();

    if let Ok(stream) = TcpStream::connect(("127.0.0.1", args.port)) {
        let mut write_half = stream;
        let mut read_half = BufReader::new(write_half.try_clone()?);

        loop {
            // prompt
            print!("127.0.0.1:{}> ", args.port);
            std::io::stdout().flush()?;

            // read
            if std::io::stdin().read_line(&mut input_buf)? == 0 {
                break;
            }

            input_buf.pop(); // \n
            if let Some(command) = Command::from_input(&input_buf) {
                // send
                command.serialize(&mut output_buf);
                write_half.write_all(&output_buf)?;
                output_buf.clear();

                // recv
                read_half.read_until(0x00, &mut output_buf)?;
                if output_buf.is_empty() || !output_buf.ends_with(b"\0") {
                    // connection was cut off
                    println!(
                        "Could not read response from server at 127.0.0.1:{}: Connection dropped",
                        args.port
                    );
                    break;
                }
                output_buf.pop(); // \0

                // print
                println!("{}", unsafe { str::from_utf8_unchecked(&output_buf) });
                output_buf.clear();
            } else {
                println!("Invalid command...");
            }

            input_buf.clear();
        }
    } else {
        println!(
            "Could not connect to server at 127.0.0.1:{}: Connection refused",
            args.port
        );
    }

    Ok(())
}
