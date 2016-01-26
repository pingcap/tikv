#![allow(dead_code)]
use raft::errors::{Result, Error, StorageError};
use raft::Storage;
use protobuf::{self, RepeatedField};
use proto::raftpb::{HardState, Entry, EntryType, Message, Snapshot, MessageType, ConfChange,
                    ConfChangeType, ConfState};
use raft::raft::{Config, Raft, SoftState, INVALID_ID};

#[derive(Debug, Default)]
struct Peer {
    id: u64,
    context: Vec<u8>,
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
#[derive(Default)]
struct Ready {
    // The current volatile state of a Node.
    // SoftState will be nil if there is no update.
    // It is not required to consume or store SoftState.
    ss: Option<SoftState>,

    // The current state of a Node to be saved to stable storage BEFORE
    // Messages are sent.
    // HardState will be equal to empty state if there is no update.
    hs: Option<HardState>,

    // Entries specifies entries to be saved to stable storage BEFORE
    // Messages are sent.
    entries: Vec<Entry>,

    // Snapshot specifies the snapshot to be saved to stable storage.
    snapshot: Snapshot,

    // CommittedEntries specifies entries to be committed to a
    // store/state-machine. These have previously been committed to stable
    // store.
    committed_entries: Vec<Entry>,

    // Messages specifies outbound messages to be sent AFTER Entries are
    // committed to stable storage.
    // If it contains a MsgSnap message, the application MUST report back to raft
    // when the snapshot has been received or has failed by calling ReportSnapshot.
    messages: Option<Vec<Message>>,
}

impl Ready {
    fn new<T: Storage + Sync + Default>(raft: &Raft<T>,
                                        prev_ss: &SoftState,
                                        prev_hs: &HardState)
                                        -> Ready {
        let mut rd = Ready {
            entries: raft.raft_log.unstable_entries().unwrap_or(&vec![]).to_vec(),
            committed_entries: raft.raft_log.next_entries().unwrap_or(vec![]),
            ..Default::default()
        };
        let ss = raft.soft_state();
        if &ss != prev_ss {
            rd.ss = Some(ss);
        }
        let hs = raft.hard_state();
        if &hs != prev_hs {
            rd.hs = Some(hs);
        }
        if raft.raft_log.get_unstable().snapshot.is_some() {
            rd.snapshot = raft.raft_log.get_unstable().snapshot.clone().unwrap();
        }
        rd
    }
}

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
#[derive(Default)]
struct RawNode<T: Storage + Sync + Default> {
    raft: Raft<T>,
    prev_ss: SoftState,
    prev_hs: HardState,
}

impl<T: Storage + Sync + Default> RawNode<T> {
    // NewRawNode returns a new RawNode given configuration and a list of raft peers.
    fn new(config: &Config<T>, peers: &[Peer]) -> Result<RawNode<T>> {
        assert!(config.id != 0, "config.id must not be zero");
        let r = Raft::new(config);
        let mut rn = RawNode { raft: r, ..Default::default() };
        let last_index = rn.raft.get_store().last_index().expect("");
        if last_index == 0 {
            rn.raft.become_follower(1, INVALID_ID);
            let mut ents = Vec::with_capacity(peers.len());
            for (i, peer) in peers.iter().enumerate() {
                let mut cc = ConfChange::new();
                cc.set_Type(ConfChangeType::ConfChangeAddNode);
                cc.set_NodeID(peer.id);
                cc.set_Context(peer.context.clone());
                let data = protobuf::Message::write_to_bytes(&cc)
                               .expect("unexpected marshal error");
                let mut e = Entry::new();
                e.set_Type(EntryType::EntryConfChange);
                e.set_Term(1);
                e.set_Index(i as u64 + 1);
                e.set_Data(data);
                ents.push(e);
            }
            rn.raft.raft_log.append(&ents);
            rn.raft.raft_log.committed = ents.len() as u64;
            for peer in peers {
                rn.raft.add_node(peer.id);
            }
        }
        rn.prev_ss = rn.raft.soft_state();
        rn.prev_hs = rn.raft.hard_state();
        Ok(rn)
    }

    fn new_ready(&self) -> Ready {
        Ready::new(&self.raft, &self.prev_ss, &self.prev_hs)
    }

    fn commit_ready(&mut self, rd: Ready) {
        if rd.ss.is_some() {
            self.prev_ss = rd.ss.unwrap();
        }
        if let Some(e) = rd.hs {
            if e != HardState::new() {
                self.prev_hs = e;
            }
        }
        if self.prev_hs.get_commit() != 0 {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            self.raft.raft_log.applied_to(self.prev_hs.get_commit());
        }
        if rd.entries.len() > 0 {
            let e = rd.entries.last().unwrap();
            self.raft.raft_log.stable_to(e.get_Index(), e.get_Term());
        }
        if rd.snapshot != Snapshot::new() {
            self.raft.raft_log.stable_snap_to(rd.snapshot.get_metadata().get_index());
        }
    }

    // Tick advances the internal logical clock by a single tick.
    fn tick(&mut self) {
        self.raft.tick();
    }

    // Campaign causes this RawNode to transition to candidate state.
    fn campaign(&mut self) -> Result<()> {
        let mut m = Message::new();
        m.set_field_type(MessageType::MsgHup);
        self.raft.step(m)
    }

    // Propose proposes data be appended to the raft log.
    fn propose(&mut self, data: Vec<u8>) -> Result<()> {
        let mut m = Message::new();
        m.set_field_type(MessageType::MsgPropose);
        m.set_from(self.raft.id);
        let mut e = Entry::new();
        e.set_Data(data);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        self.raft.step(m)
    }

    // ProposeConfChange proposes a config change.
    fn propose_conf_change(&mut self, cc: ConfChange) -> Result<()> {
        let data = try!(protobuf::Message::write_to_bytes(&cc));
        let mut m = Message::new();
        m.set_field_type(MessageType::MsgPropose);
        let mut e = Entry::new();
        e.set_Type(EntryType::EntryConfChange);
        e.set_Data(data);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        self.raft.step(m)
    }

    fn apply_conf_change(&mut self, cc: ConfChange) -> ConfState {
        if cc.get_NodeID() == INVALID_ID {
            self.raft.reset_pending_conf();
            let mut cs = ConfState::new();
            cs.set_nodes(self.raft.nodes());
            return cs;
        }
        let nid = cc.get_NodeID();
        assert!(cc.has_Type(), "unexpected conf type");
        match cc.get_Type() {
            ConfChangeType::ConfChangeAddNode => self.raft.add_node(nid),
            ConfChangeType::ConfChangeRemoveNode => self.raft.remove_node(nid),
            ConfChangeType::ConfChangeUpdateNode => self.raft.reset_pending_conf(),
        }
        let mut cs = ConfState::new();
        cs.set_nodes(self.raft.nodes());
        cs
    }
}
