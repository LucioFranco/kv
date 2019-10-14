use crate::pb::{self, wal as walpb};
use bytes::{BufMut, BytesMut};
use prost::Message;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub struct Log {
    dir: PathBuf,
    file: File,

    last_entry: u64,

    buf: BytesMut,
}

impl Log {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, crate::Error> {
        fs::create_dir_all(&dir)?;

        let file_path = dir.as_ref().join(wal_name(0, 0));

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(file_path)?;

        let buf = BytesMut::with_capacity(1024 * 64);

        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            file,
            buf,
            last_entry: 0,
        })
    }

    pub fn save(&mut self, data: walpb::Record) -> Result<(), crate::Error> {
        if self.buf.remaining_mut() > data.encoded_len() {
            self.buf.clear();
        }

        data.encode(&mut self.buf)?;

        self.file.write_all(&self.buf[..])?;

        Ok(())
    }
}

fn wal_name(seq: usize, index: usize) -> String {
    format!("{:016}-{:016}", seq, index)
}
