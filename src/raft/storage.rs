use raft::raftpb::{HardState, ConfState, Entry, Snapshot};
use protobuf;
use raft::errors::Result;
use raft::errors::{Error, StorageError};


#[derive(Debug, Clone)]
pub struct RaftState {
    hard_state: HardState,
    conf_state: ConfState,
}

pub trait Storage {
    fn initial_state(&self) -> Result<RaftState>;
    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<&[Entry]>;
    fn term(&self, idx: u64) -> Result<u64>;
    fn first_index(&self) -> Result<u64>;
    fn last_index(&self) -> Result<u64>;
    fn snapshot(&self) -> Result<Snapshot>;
}

pub struct MemStorage {
    hard_state: HardState,
    snapshot: Snapshot,
    entries: Vec<Entry>,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage {
            // When starting from scratch populate the list with a dummy entry at term zero.
            entries: vec![Entry::new()],
            hard_state: HardState::new(),
            snapshot: Snapshot::new(),
        }
    }

    pub fn set_hardstate(&mut self, hs: HardState) {
        self.hard_state = hs;
    }

    fn inner_last_index(&self) -> u64 {
        return self.entries[0].Index.unwrap() + self.entries.len() as u64 - 1;
    }

    fn limit_size(entries: &[Entry], max: u64) -> Result<&[Entry]> {
        if entries.len() == 0 {
            return Ok(entries);
        }

        let mut size = protobuf::Message::compute_size(&entries[0]) as u64;
        let mut limit = 1u64;
        while limit < entries.len() as u64 {
            size += protobuf::Message::compute_size(&entries[limit as usize]) as u64;
            if size > max {
                break;
            }
            limit += 1;
        }
        let ents = &entries[..limit as usize];
        Ok(ents)
    }
}


impl Storage for MemStorage {
    fn initial_state(&self) -> Result<RaftState> {
        Ok(RaftState {
            hard_state: self.hard_state.clone(),
            conf_state: self.snapshot.get_metadata().get_conf_state().clone(),
        })
    }

    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<&[Entry]> {
        let offset = self.entries[0].Index as u64;
        if low <= offset {
            Err(Error::Store(StorageError::Compacted))
        }

        if high > self.inner_last_index() + 1 {
            panic!("index out of bound")
        }
        // only contains dummy entries.
        if self.ents.len() == 1 {
            Err(Error::Store(StorageError::Unavailable))
        }

        let ents = self.ents[low - offset..high - offset];
        Ok(self.limitSize(ents, max_size))
    }

    fn term(&self, idx: u64) -> Result<u64> {
        unimplemented!()
    }

    fn first_index(&self) -> Result<u64> {
        unimplemented!()
    }

    fn last_index(&self) -> Result<u64> {
        unimplemented!()
    }

    fn snapshot(&self) -> Result<Snapshot> {
        unimplemented!()
    }
}
