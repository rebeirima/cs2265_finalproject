use std::{
    fs::{self, metadata},
    io::Read,
    path::PathBuf,
};

use bytes::BufMut;

#[derive(Clone, Debug)]
pub enum Command {
    PUT { key: i32, val: i32 },
    GET { key: i32 },
    DELETE { key: i32 },
    LOAD { file: PathBuf },
    RANGE { min_key: i32, max_key: i32 },
    STATS
}

impl Command {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Self::PUT { key, val } => {
                buf.put_u8(b'p');
                buf.put_i32(*key);
                buf.put_i32(*val);
            }
            Self::GET { key } => {
                buf.put_u8(b'g');
                buf.put_i32(*key);
            }
            Self::DELETE { key } => {
                buf.put_u8(b'd');
                buf.put_i32(*key);
            }
            Self::LOAD { file } => {
                buf.put_u8(b'l');

                let file_size = metadata(file).unwrap().len();
                let kv_pairs = file_size / 8;

                buf.put_u64(kv_pairs);
                fs::File::open(file).unwrap().read_to_end(buf).unwrap();
            }
            Self::RANGE { min_key, max_key } => {
                buf.put_u8(b'r');
                buf.put_i32(*min_key);
                buf.put_i32(*max_key);
            }
            Self::STATS => {
                buf.put_u8(b's');
            }
        }
    }

    pub fn from_input(input: &str) -> Option<Self> {
        let mut split_iter = input.split(' ');
        let tag = split_iter.next()?;

        match tag {
            "p" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                let val: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::PUT { key, val })
            }
            "g" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::GET { key })
            }
            "d" => {
                let key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::DELETE { key })
            }
            "l" => {
                let file: PathBuf = split_iter.next()?.parse().ok()?;

                if !file.is_file() {
                    return None;
                }

                Some(Command::LOAD { file })
            }
            "r" => {
                let min_key: i32 = split_iter.next()?.parse().ok()?;
                let max_key: i32 = split_iter.next()?.parse().ok()?;
                Some(Command::RANGE { min_key, max_key })
            }
            "s" => Some(Command::STATS),
            _ => None,
        }
    }
}