use bytes::BytesMut;
use prost::Message;
use raft::{
    eraftpb::{ConfState, Entry, Snapshot},
    RaftState,
};
use sled::Db;
use std::path::Path;

pub use raft::storage::MemStorage;

const CONF_STATE_KEY: &'static str = "conf_state";
const HIGH_INDEX: [u8; 8] = [255, 255, 255, 255, 255, 255, 255, 255];
const LOW_INDEX: [u8; 10] = [255, 255, 255, 255, 255, 255, 255, 255, 255, 0];

pub trait Storage: raft::storage::Storage {
    fn append(&self, entries: &[Entry]) -> Result<(), crate::Error>;
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), crate::Error>;
    fn set_conf_state(&self, cs: ConfState) -> Result<(), crate::Error>;
    fn set_hard_state(&self, commit: u64, term: u64) -> Result<(), crate::Error>;
}

impl Storage for MemStorage {
    fn append(&self, entries: &[Entry]) -> Result<(), crate::Error> {
        self.wl().append(entries)?;
        Ok(())
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), crate::Error> {
        self.wl().apply_snapshot(snapshot)?;
        Ok(())
    }

    fn set_conf_state(&self, cs: ConfState) -> Result<(), crate::Error> {
        self.wl().set_conf_state(cs, None);
        Ok(())
    }

    fn set_hard_state(&self, commit: u64, term: u64) -> Result<(), crate::Error> {
        let mut me = self.wl();
        me.mut_hard_state().commit = commit;
        me.mut_hard_state().term = term;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SledStorage {
    db: Db,
}

impl SledStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, crate::Error> {
        let db = Db::open(path)?;

        Ok(Self { db })
    }

    pub async fn append(&self, entries: &[Entry]) -> Result<(), crate::Error> {
        for entry in entries {
            let key = format!("key-{}", entry.index);
            let mut buf = BytesMut::with_capacity(entry.encoded_len());
            entry.encode(&mut buf)?;
            self.db.insert(&key[..], &buf[..])?;
        }

        self.db.flush_async().await?;

        Ok(())
    }

    pub fn set_conf_state(&self, cs: ConfState) -> Result<(), crate::Error> {
        let mut buf = BytesMut::with_capacity(cs.encoded_len());
        cs.encode(&mut buf)?;
        self.db.insert(CONF_STATE_KEY.as_bytes(), &buf[..])?;
        Ok(())
    }
}

impl raft::storage::Storage for SledStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::default())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<Entry>> {
        let low = make_log_key(low);
        let high = make_log_key(high);

        let entries = if let Some(max_size) = max_size.into() {
            self.db
                .range(low..high)
                .take(max_size as usize)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        } else {
            self.db
                .range(low..high)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };

        let entries = entries
            .into_iter()
            .map(|(_idx, buf)| Entry::decode(&buf[..]))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let key = make_log_key(idx);

        if let Some(value) = self.db.get(&key[..]).unwrap() {
            let entry = Entry::decode(&value[..]).unwrap();
            Ok(entry.term)
        } else {
            Ok(1)
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        if let Some(value) = self.db.range(LOW_INDEX..).next() {
            let value = value.unwrap();
            let entry = Entry::decode(&value.1[..]).unwrap();
            Ok(entry.index)
        } else {
            Ok(1)
        }
    }

    fn last_index(&self) -> raft::Result<u64> {
        if let Some(value) = self.db.range(..HIGH_INDEX).next_back() {
            let value = value.unwrap();
            let entry = Entry::decode(&value.1[..]).unwrap();
            Ok(entry.index)
        } else {
            Ok(1)
        }
    }

    fn snapshot(&self, _request_index: u64) -> raft::Result<Snapshot> {
        unimplemented!()
    }
}

fn make_log_key(idx: u64) -> [u8; 9] {
    use bytes::BufMut;
    use std::io::Cursor;
    let mut key = [0; 9];

    {
        let mut key = Cursor::new(&mut key[..]);
        key.put_u8(1);
        key.put_u64_le(idx);
    }

    key
}
