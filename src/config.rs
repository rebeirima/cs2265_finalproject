use std::{env::args, path::PathBuf};

pub const BLOCK_SIZE_BYTES: usize = 4096;

// 466033 * 4(5^5) > 2^32 ==> the final level can fit all possible key-value pairs
pub const LEVEL1_FILE_CAPACITY: usize = 4;
pub const SIZE_MULTIPLIER: usize = 5;
pub const NUM_LEVELS: usize = 6;

pub const MAX_FILE_SIZE_BYTES: usize = 1 << 22; // 4 MB
pub const MAX_FILE_SIZE_BLOCKS: usize = MAX_FILE_SIZE_BYTES >> 12;

// Worst case (all puts, each taking 9 bytes) upper bound on number of entries in the memory level that can serialize into a single file
pub const MEM_CAPACITY: u32 = (MAX_FILE_SIZE_BLOCKS * BLOCK_SIZE_BYTES / 9) as u32;
// pub const MEM_CAPACITY: u32 = 10;

pub const BLOOM_CAPACITY: usize = 1 << 16;

const DEFAULT_DATABASE_DIRECTORY: &'static str = "/Users/noahr/dev/rust/lsm-tree/database";

#[derive(Debug)]
pub struct Config {
    pub data_dir: PathBuf,
    pub port: u16,
}

impl Config {
    pub fn parse_from_args() -> Self {
        let mut data_dir = DEFAULT_DATABASE_DIRECTORY.parse().unwrap();
        let mut port = 1234;

        let mut args = args();

        while let Some(arg) = args.next() {
            if arg.starts_with("--") {
                match &arg[2..] {
                    "data-dir" => {
                        data_dir = args.next().map(|d| d.parse().unwrap()).unwrap();
                    }
                    "port" => {
                        port = args.next().map(|d| d.parse().unwrap()).unwrap();
                    }
                    _ => unimplemented!(),
                }
            }
        }

        Config { data_dir, port }
    }
}
