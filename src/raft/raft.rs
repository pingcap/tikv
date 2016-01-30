#![allow(dead_code)]
use std::cmp;
use raft::storage::Storage;
use util::DefaultRng;
use rand::Rng;
use proto::raftpb::{HardState, Entry, EntryType, Message, Snapshot, MessageType};
use protobuf::repeated::RepeatedField;
use raft::progress::{Progress, Inflights, ProgressState};
use raft::errors::{Result, Error, StorageError};
use std::collections::HashMap;
use raft::raft_log::{self, RaftLog};
use std::sync::Arc;


#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StateRole {
    Follower,
    Candidate,
    Leader,
}

impl Default for StateRole {
    fn default() -> StateRole {
        StateRole::Follower
    }
}

pub const INVALID_ID: u64 = 0;

/// Config contains the parameters to start a raft.
#[derive(Default)]
pub struct Config<T: Storage + Default> {
    /// id is the identity of the local raft. ID cannot be 0.
    pub id: u64,

    /// peers contains the IDs of all nodes (including self) in
    /// the raft cluster. It should only be set when starting a new
    /// raft cluster.
    /// Restarting raft from previous configuration will panic if
    /// peers is set.
    /// peer is private and only used for testing right now.
    pub peers: Vec<u64>,

    /// ElectionTick is the election timeout. If a follower does not
    /// receive any message from the leader of current term during
    /// ElectionTick, it will become candidate and start an election.
    /// ElectionTick must be greater than HeartbeatTick. We suggest
    /// to use ElectionTick = 10 * HeartbeatTick to avoid unnecessary
    /// leader switching.
    pub election_tick: usize,
    /// HeartbeatTick is the heartbeat usizeerval. A leader sends heartbeat
    /// message to mausizeain the leadership every heartbeat usizeerval.
    pub heartbeat_tick: usize,

    /// Storage is the storage for raft. raft generates entires and
    /// states to be stored in storage. raft reads the persisted entires
    /// and states out of Storage when it needs. raft reads out the previous
    /// state and configuration out of storage when restarting.
    pub storage: Arc<T>,
    /// Applied is the last applied index. It should only be set when restarting
    /// raft. raft will not return entries to the application smaller or equal to Applied.
    /// If Applied is unset when restarting, raft might return previous applied entries.
    /// This is a very application dependent configuration.
    pub applied: u64,

    /// MaxSizePerMsg limits the max size of each append message. Smaller value lowers
    /// the raft recovery cost(initial probing and message lost during normal operation).
    /// On the other side, it might affect the throughput during normal replication.
    /// Note: math.MaxUusize64 for unlimited, 0 for at most one entry per message.
    pub max_size_per_msg: u64,
    /// max_inflight_msgs limits the max number of in-flight append messages during optimistic
    /// replication phase. The application transportation layer usually has its own sending
    /// buffer over TCP/UDP. Setting MaxInflightMsgs to avoid overflowing that sending buffer.
    /// TODO (xiangli): feedback to application to limit the proposal rate?
    pub max_inflight_msgs: usize,

    /// check_quorum specifies if the leader should check quorum activity. Leader steps down when
    /// quorum is not active for an electionTimeout.
    pub check_quorum: bool,
}

impl<T: Storage + Default> Config<T> {
    pub fn validate(&self) -> Result<()> {
        if self.id == INVALID_ID {
            return Err(Error::ConfigInvalid("invalid node id".to_string()));
        }

        if self.heartbeat_tick <= 0 {
            return Err(Error::ConfigInvalid("heartbeat tick must greater than 0".to_string()));
        }

        if self.election_tick <= self.heartbeat_tick {
            return Err(Error::ConfigInvalid("election tick must be greater than heartbeat tick"
                                                .to_string()));
        }

        if self.max_inflight_msgs <= 0 {
            return Err(Error::ConfigInvalid("max inflight messages must be greater than 0"
                                                .to_string()));
        }

        Ok(())
    }
}

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, PartialEq)]
pub struct SoftState {
    pub lead: u64,
    pub raft_state: StateRole,
}

#[derive(Default)]
pub struct Raft<T: Default + Storage> {
    pub term: u64,
    pub vote: u64,

    pub id: u64,

    /// the log
    pub raft_log: RaftLog<T>,

    pub max_inflight: usize,
    pub max_msg_size: u64,
    pub prs: HashMap<u64, Progress>,

    pub state: StateRole,

    pub votes: HashMap<u64, bool>,

    pub msgs: Vec<Message>,

    /// the leader id
    pub lead: u64,

    /// New configuration is ignored if there exists unapplied configuration.
    pending_conf: bool,

    /// number of ticks since it reached last electionTimeout when it is leader
    /// or candidate.
    /// number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    election_elapsed: usize,

    /// number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    heartbeat_elapsed: usize,

    check_quorum: bool,

    heartbeat_timeout: usize,
    election_timeout: usize,
    /// Will be called when step** is about to be called.
    /// return false will skip step**.
    skip_step: Option<Box<FnMut() -> bool>>,
    rng: DefaultRng,
}

fn new_progress(next_idx: u64, ins_size: usize) -> Progress {
    Progress {
        next_idx: next_idx,
        ins: Inflights::new(ins_size),
        ..Default::default()
    }
}

fn new_message(to: u64, field_type: MessageType, from: Option<u64>) -> Message {
    let mut m = Message::new();
    m.set_to(to);
    if let Some(id) = from {
        m.set_from(id);
    }
    m.set_msg_type(field_type);
    m
}

impl<T: Storage + Default> Raft<T> {
    pub fn new(c: &Config<T>) -> Raft<T> {
        c.validate().expect("configuration is invalid");
        let store = c.storage.clone();
        let rs = store.initial_state().expect("");
        let raft_log = RaftLog::new(store);
        let mut peers: &[u64] = &c.peers;
        if rs.conf_state.get_nodes().len() > 0 {
            if peers.len() > 0 {
                // TODO(bdarnell): the peers argument is always nil except in
                // tests; the argument should be removed and these tests should be
                // updated to specify their nodes through a snap
                panic!("cannot specify both new(peers) and ConfState.Nodes")
            }
            peers = rs.conf_state.get_nodes();
        }
        let mut r = Raft {
            id: c.id,
            raft_log: raft_log,
            max_inflight: c.max_inflight_msgs,
            max_msg_size: c.max_size_per_msg,
            prs: HashMap::with_capacity(peers.len()),
            state: StateRole::Follower,
            check_quorum: c.check_quorum,
            heartbeat_timeout: c.heartbeat_tick,
            election_timeout: c.election_tick,
            ..Default::default()
        };
        for p in peers {
            r.prs.insert(*p, new_progress(1, r.max_inflight));
        }
        if rs.hard_state != HardState::new() {
            r.load_state(rs.hard_state);
        }
        if c.applied > 0 {
            r.raft_log.applied_to(c.applied);
        }
        let term = r.get_term();
        r.become_follower(term, INVALID_ID);
        let nodes_str = r.nodes().iter().fold(String::new(), |b, n| b + &format!("{}", n));
        info!("newRaft {:x} [peers: [{}], term: {:?}, commit: {}, applied: {}, last_index: {}, \
               last_term: {}]",
              r.id,
              nodes_str,
              r.get_term(),
              r.raft_log.committed,
              r.raft_log.get_applied(),
              r.raft_log.last_index(),
              r.raft_log.last_term());
        r
    }

    pub fn get_store(&self) -> Arc<T> {
        self.raft_log.get_store()
    }

    fn has_leader(&self) -> bool {
        self.lead != INVALID_ID
    }

    pub fn soft_state(&self) -> SoftState {
        SoftState {
            lead: self.lead,
            raft_state: self.state,
        }
    }

    pub fn hard_state(&self) -> HardState {
        let mut hs = HardState::new();
        hs.set_term(self.term);
        hs.set_vote(self.vote);
        hs.set_commit(self.raft_log.committed);
        hs
    }

    fn quorum(&self) -> usize {
        self.prs.len() / 2 + 1
    }

    pub fn nodes(&self) -> Vec<u64> {
        let mut nodes = Vec::with_capacity(self.prs.len());
        nodes.extend(self.prs.keys());
        nodes.sort();
        nodes
    }

    // send persists state to stable storage and then sends to its mailbox.
    fn send(&mut self, m: Message) {
        let mut m = m;
        m.set_from(self.id);
        // do not attach term to MsgPropose
        // proposals are a way to forward to the leader and
        // should be treated as local message.
        if m.get_msg_type() != MessageType::MsgPropose {
            m.set_term(self.get_term());
        }
        self.msgs.push(m);
    }

    fn prepare_send_snapshot(&mut self, m: &mut Message, to: u64) {
        let pr = self.prs.get_mut(&to).unwrap();
        if !pr.recent_active {
            debug!("ignore sending snapshot to {:x} since it is not recently active",
                   to);
            return;
        }

        m.set_msg_type(MessageType::MsgSnapshot);
        let snapshot_r = self.raft_log.snapshot();
        if let Err(e) = snapshot_r {
            if e == Error::Store(StorageError::SnapshotTemporarilyUnavailable) {
                debug!("{:x} failed to send snapshot to {:x} because snapshot is termporarily \
                        unavailable",
                       self.id,
                       to);
                return;
            }
            panic!(e);
        }
        let snapshot = snapshot_r.unwrap();
        if snapshot.get_metadata().get_index() == 0 {
            panic!("need non-empty snapshot");
        }
        let (sindex, sterm) = (snapshot.get_metadata().get_index(),
                               snapshot.get_metadata().get_term());
        m.set_snapshot(snapshot);
        debug!("{:x} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {:x} \
                [{:?}]",
               self.id,
               self.raft_log.first_index(),
               self.raft_log.committed,
               sindex,
               sterm,
               to,
               pr);
        pr.become_snapshot(sindex);
        debug!("{:x} paused sending replication messages to {:x} [{:?}]",
               self.id,
               to,
               pr);
    }

    fn prepare_send_entries(&mut self, m: &mut Message, to: u64, term: u64, ents: Vec<Entry>) {
        let pr = self.prs.get_mut(&to).unwrap();
        m.set_msg_type(MessageType::MsgAppend);
        m.set_index(pr.next_idx - 1);
        m.set_log_term(term);
        m.set_entries(RepeatedField::from_vec(ents));
        m.set_commit(self.raft_log.committed);
        if m.get_entries().len() != 0 {
            match pr.state {
                ProgressState::Replicate => {
                    let last = m.get_entries().last().unwrap().get_index();
                    pr.optimistic_update(last);
                    pr.ins.add(last);
                }
                ProgressState::Probe => pr.pause(),
                _ => {
                    panic!("{:x} is sending append in unhandled state {:?}",
                           self.id,
                           pr.state)
                }
            }
        }
    }

    // send_append sends RPC, with entries to the given peer.
    fn send_append(&mut self, to: u64) {
        let (term, ents) = {
            let pr = self.prs.get(&to).unwrap();
            if pr.is_paused() {
                return;
            }
            (self.raft_log.term(pr.next_idx - 1),
             self.raft_log.entries(pr.next_idx, self.max_msg_size))
        };
        let mut m = Message::new();
        m.set_to(to);
        if term.is_err() || ents.is_err() {
            // send snapshot if we failed to get term or entries
            self.prepare_send_snapshot(&mut m, to);
        } else {
            self.prepare_send_entries(&mut m, to, term.unwrap(), ents.unwrap());
        }
        self.send(m);
    }

    // send_heartbeat sends an empty MsgAppend
    fn send_heartbeat(&mut self, to: u64) {
        // Attach the commit as min(to.matched, self.raft_log.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        let mut m = Message::new();
        m.set_to(to);
        m.set_msg_type(MessageType::MsgHeartbeat);
        let commit = cmp::min(self.prs.get(&to).unwrap().matched, self.raft_log.committed);
        m.set_commit(commit);
        self.send(m);
    }

    // bcastAppend sends RPC, with entries to all peers that are not up-to-date
    // according to the progress recorded in r.prs.
    fn bcast_append(&mut self) {
        // TODO: avoid copy
        let keys: Vec<u64> = self.prs.keys().map(|x| *x).collect();
        for id in keys {
            if id == self.id {
                continue;
            }
            self.send_append(id);
        }
    }

    // bcastHeartbeat sends RPC, without entries to all the peers.
    fn bcast_heartbeat(&mut self) {
        // TODO: avoid copy
        let keys: Vec<u64> = self.prs.keys().map(|x| *x).collect();
        for id in keys {
            if id == self.id {
                continue;
            }
            self.send_heartbeat(id);
            self.prs.get_mut(&id).unwrap().resume()
        }
    }

    fn maybe_commit(&mut self) -> bool {
        // TODO: optimize
        let mut mis = Vec::with_capacity(self.prs.len());
        for p in self.prs.values() {
            mis.push(p.matched);
        }
        // reverse sort
        mis.sort_by(|a, b| b.cmp(a));
        let mci = mis[self.quorum() - 1];
        let term = self.get_term();
        self.raft_log.maybe_commit(mci, term)
    }

    fn reset(&mut self, term: u64) {
        if self.get_term() != term {
            self.term = term;
            self.vote = INVALID_ID;
        }
        self.lead = INVALID_ID;
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        self.votes = HashMap::new();
        let (last_index, max_inflight) = (self.raft_log.last_index(), self.max_inflight);
        let self_id = self.id;
        for (id, p) in self.prs.iter_mut() {
            *p = new_progress(last_index + 1, max_inflight);
            if id == &self_id {
                p.matched = last_index;
            }
        }
        self.pending_conf = false;
    }

    fn append_entry(&mut self, es: &mut [Entry]) {
        let li = self.raft_log.last_index();
        for (i, e) in es.iter_mut().enumerate() {
            e.set_term(self.get_term());
            e.set_index(li + 1 + i as u64);
        }
        self.raft_log.append(es);
        self.prs.get_mut(&self.id).unwrap().maybe_update(self.raft_log.last_index());
        self.maybe_commit();
    }

    pub fn tick(&mut self) {
        match self.state {
            StateRole::Candidate | StateRole::Follower => self.tick_election(),
            StateRole::Leader => self.tick_heartbeat(),
        }
    }

    // tick_election is run by followers and candidates after self.election_timeout.
    fn tick_election(&mut self) {
        if !self.promotable() {
            self.election_elapsed = 0;
            return;
        }
        self.election_elapsed += 1;
        if self.is_election_timeout() {
            self.election_elapsed = 0;
            let m = new_message(INVALID_ID, MessageType::MsgHup, Some(self.id));
            self.step(m).is_ok();
        }
    }

    // tick_heartbeat is run by leaders to send a MsgBeat after self.heartbeat_timeout.
    fn tick_heartbeat(&mut self) {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            if self.check_quorum {
                let m = new_message(INVALID_ID, MessageType::MsgCheckQuorum, Some(self.id));
                self.step(m).is_ok();
            }
        }

        if self.state != StateRole::Leader {
            return;
        }

        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            let m = new_message(INVALID_ID, MessageType::MsgBeat, Some(self.id));
            self.step(m).is_ok();
        }
    }

    pub fn become_follower(&mut self, term: u64, lead: u64) {
        self.reset(term);
        self.lead = lead;
        self.state = StateRole::Follower;
        info!("{:x} became follower at term {}", self.id, self.get_term());
    }

    pub fn become_candidate(&mut self) {
        assert!(self.state != StateRole::Leader,
                "invalid transition [leader -> candidate]");
        let term = self.get_term() + 1;
        self.reset(term);
        let id = self.id;
        self.vote = id;
        self.state = StateRole::Candidate;
        info!("{:x} became candidate at term {}", self.id, self.get_term());
    }

    fn become_leader(&mut self) {
        assert!(self.state != StateRole::Follower,
                "invalid transition [follower -> leader]");
        let term = self.get_term();
        self.reset(term);
        self.lead = self.id;
        self.state = StateRole::Leader;
        let begin = self.raft_log.committed + 1;
        let ents = self.raft_log
                       .entries(begin, raft_log::NO_LIMIT)
                       .expect("unexpected error getting uncommitted entries");
        for e in ents {
            if e.get_entry_type() != EntryType::EntryConfChange {
                continue;
            }
            assert!(!self.pending_conf,
                    "unexpected double uncommitted config entry");
            self.pending_conf = true;
        }
        self.append_entry(&mut [Entry::new()]);
        info!("{:x} became leader at term {}", self.id, self.get_term());
    }

    fn campaign(&mut self) {
        self.become_candidate();
        let id = self.id;
        let poll_res = self.poll(id, true);
        if self.quorum() == poll_res {
            self.become_leader();
            return;
        }
        let keys: Vec<u64> = self.prs.keys().map(|x| *x).collect();
        for id in keys {
            if id == self.id {
                continue;
            }
            info!("{:x} [logterm: {}, index: {}] sent vote request to {:x} at term {}",
                  self.id,
                  self.raft_log.last_term(),
                  self.raft_log.last_index(),
                  id,
                  self.get_term());
            let mut m = new_message(id, MessageType::MsgRequestVote, None);
            m.set_index(self.raft_log.last_index());
            m.set_log_term(self.raft_log.last_term());
            self.send(m);
        }
    }

    fn get_term(&self) -> u64 {
        self.term
    }

    fn poll(&mut self, id: u64, v: bool) -> usize {
        if v {
            info!("{:x} received vote from {:x} at term {}",
                  self.id,
                  id,
                  self.get_term())
        } else {
            info!("{:x} received vote rejection from {:x} at term {}",
                  self.id,
                  id,
                  self.get_term())
        }
        if !self.votes.contains_key(&id) {
            self.votes.insert(id, v);
        }
        self.votes.values().filter(|x| **x).count()
    }

    pub fn step(&mut self, m: Message) -> Result<()> {
        if m.get_msg_type() == MessageType::MsgHup {
            if self.state != StateRole::Leader {
                info!("{:x} is starting a new election at term {}",
                      self.id,
                      self.get_term());
                self.campaign();
            } else {
                debug!("{:x} ignoring MsgHup because already leader", self.id);
            }
            return Ok(());
        }

        if m.get_term() == 0 {
            // local message
        } else if m.get_term() > self.get_term() {
            let mut lead = m.get_from();
            if m.get_msg_type() == MessageType::MsgRequestVote {
                lead = INVALID_ID;
            }
            info!("{:x} [term: {}] received a {:?} message with higher term from {:x} [term: {}]",
                  self.id,
                  self.get_term(),
                  m.get_msg_type(),
                  m.get_from(),
                  m.get_term());
            self.become_follower(m.get_term(), lead);
        } else if m.get_term() < self.get_term() {
            // ignore
            info!("{:x} [term: {}] ignored a {:?} message with lower term from {} [term: {}]",
                  self.id,
                  self.get_term(),
                  m.get_msg_type(),
                  m.get_from(),
                  m.get_term());
            return Ok(());
        }

        if self.skip_step.is_none() || self.skip_step.as_mut().unwrap()() {
            match self.state {
                StateRole::Candidate => self.step_candidate(m),
                StateRole::Follower => self.step_follower(m),
                StateRole::Leader => self.step_leader(m),
            }
        }
        Ok(())
    }

    /// check message's progress to decide which action should be taken.
    fn check_message_with_progress(&mut self,
                                   m: &Message,
                                   send_append: &mut bool,
                                   old_paused: &mut bool,
                                   maybe_commit: &mut bool) {
        let pr = self.prs.get_mut(&m.get_from());
        if pr.is_none() {
            debug!("no progress available for {:x}", m.get_from());
            return;
        }
        let pr = pr.unwrap();
        match m.get_msg_type() {
            MessageType::MsgAppendResponse => {
                pr.recent_active = true;
                if m.get_reject() {
                    debug!("{:x} received msgApp rejection(lastindex: {}) from {:x} for index {}",
                           self.id,
                           m.get_reject_hint(),
                           m.get_from(),
                           m.get_index());
                    if pr.maybe_decr_to(m.get_index(), m.get_reject_hint()) {
                        debug!("{:x} decreased progress of {:x} to [{:?}]",
                               self.id,
                               m.get_from(),
                               pr);
                        if pr.state == ProgressState::Replicate {
                            pr.become_probe();
                        }
                        *send_append = true;
                    }
                } else {
                    *old_paused = pr.is_paused();
                    if pr.maybe_update(m.get_index()) {
                        match pr.state {
                            ProgressState::Probe => pr.become_replicate(),
                            ProgressState::Snapshot if pr.maybe_snapshot_abort() => {
                                debug!("{:x} snapshot aborted, resumed sending replication \
                                        messages to {:x} [{:?}]",
                                       self.id,
                                       m.get_from(),
                                       pr);
                                pr.become_probe();
                            }
                            ProgressState::Replicate => pr.ins.free_to(m.get_index()),
                            _ => {}
                        }
                        *maybe_commit = true;
                    }
                }
            }
            MessageType::MsgHeartbeatResponse => {
                pr.recent_active = true;

                // free one slot for the full inflights window to allow progress.
                if pr.state == ProgressState::Replicate && pr.ins.full() {
                    pr.ins.free_first_one();
                }
                if pr.matched < self.raft_log.last_index() {
                    *send_append = true;
                }
            }
            MessageType::MsgSnapStatus => {
                if pr.state != ProgressState::Snapshot {
                    return;
                }
                if !m.get_reject() {
                    pr.become_probe();
                    debug!("{:x} snapshot succeeded, resumed sending replication messages to \
                            {:x} [{:?}]",
                           self.id,
                           m.get_from(),
                           pr);
                } else {
                    pr.snapshot_failure();
                    pr.become_probe();
                    debug!("{:x} snapshot failed, resumed sending replication messages to {:x} \
                            [{:?}]",
                           self.id,
                           m.get_from(),
                           pr);
                }
                // If snapshot finish, wait for the msgAppResp from the remote node before sending
                // out the next msgApp.
                // If snapshot failure, wait for a heartbeat interval before next try
                pr.pause()
            }
            MessageType::MsgUnreachable => {
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgApp is lost.
                if pr.state == ProgressState::Replicate {
                    pr.become_probe();
                }
                debug!("{:x} failed to send message to {:x} because it is unreachable [{:?}]",
                       self.id,
                       m.get_from(),
                       pr);
            }
            _ => {}
        }
    }

    fn log_vote_reject(&self, m: &Message) {
        info!("{:x} [logterm: {}, index: {}, vote: {:x}] rejected vote from {:x} [logterm: {}, \
               index: {}] at term {}",
              self.id,
              self.raft_log.last_term(),
              self.raft_log.last_index(),
              self.vote,
              m.get_from(),
              m.get_log_term(),
              m.get_index(),
              self.get_term());
    }

    fn log_vote_approve(&self, m: &Message) {
        info!("{:x} [logterm: {}, index: {}, vote: {:x}] voted for {:x} [logterm: {}, index: {}] \
               at term {}",
              self.id,
              self.raft_log.last_term(),
              self.raft_log.last_index(),
              self.vote,
              m.get_from(),
              m.get_log_term(),
              m.get_index(),
              self.get_term());
    }

    fn step_leader(&mut self, m: Message) {
        // These message types do not require any progress for m.From.
        match m.get_msg_type() {
            MessageType::MsgBeat => {
                self.bcast_heartbeat();
                return;
            }
            MessageType::MsgCheckQuorum => {
                if !self.check_quorum_active() {
                    warn!("{:x} stepped down to follower since quorum is not active",
                          self.id);
                    let term = self.get_term();
                    self.become_follower(term, INVALID_ID);
                }
                return;
            }
            MessageType::MsgPropose => {
                if m.get_entries().len() == 0 {
                    panic!("{:x} stepped empty MsgProp", self.id);
                }
                if !self.prs.contains_key(&self.id) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    return;
                }
                let mut m = m;
                if self.pending_conf {
                    for e in m.mut_entries().iter_mut() {
                        if e.get_entry_type() == EntryType::EntryConfChange {
                            *e = Entry::new();
                            e.set_entry_type(EntryType::EntryNormal);
                        }
                    }
                }
                self.append_entry(&mut m.mut_entries());
                self.bcast_append();
                return;
            }
            MessageType::MsgRequestVote => {
                self.log_vote_reject(&m);
                let mut to_sent = Message::new();
                to_sent.set_to(m.get_to());
                to_sent.set_msg_type(MessageType::MsgRequestVoteResponse);
                to_sent.set_reject(true);
                self.send(to_sent);
                return;
            }
            _ => {}
        }

        let mut send_append = false;
        let mut maybe_commit = false;
        let mut old_paused = false;
        self.check_message_with_progress(&m, &mut send_append, &mut old_paused, &mut maybe_commit);
        if maybe_commit {
            if self.maybe_commit() {
                self.bcast_append();
            } else if old_paused {
                // update() reset the wait state on this node. If we had delayed sending
                // an update before, send it now.
                send_append = true;
            }
        }
        if send_append {
            self.send_append(m.get_from());
        }
    }

    fn step_candidate(&mut self, m: Message) {
        let term = self.get_term();
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                info!("{:x} no leader at term {}; dropping proposal",
                      self.id,
                      term);
                return;
            }
            MessageType::MsgAppend => {
                self.become_follower(term, m.get_from());
                self.handle_append_entries(m);
            }
            MessageType::MsgHeartbeat => {
                self.become_follower(term, m.get_from());
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                self.become_follower(term, m.get_from());
                self.handle_snapshot(m);
            }
            MessageType::MsgRequestVote => {
                self.log_vote_reject(&m);
                let t = MessageType::MsgRequestVoteResponse;
                let mut to_send = new_message(m.get_from(), t, None);
                to_send.set_reject(true);
                self.send(to_send);
            }
            MessageType::MsgRequestVoteResponse => {
                let gr = self.poll(m.get_from(), !m.get_reject());
                let quorum = self.quorum();
                info!("{:x} [quorum:{}] has received {} votes and {} vote rejections",
                      self.id,
                      quorum,
                      gr,
                      self.votes.len() - gr);
                if quorum == gr {
                    self.become_leader();
                    self.bcast_append();
                } else if quorum == self.votes.len() - gr {
                    self.become_follower(term, INVALID_ID);
                }
            }
            _ => {}
        }
    }

    fn step_follower(&mut self, m: Message) {
        let term = self.get_term();
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                if self.lead == INVALID_ID {
                    info!("{:x} no leader at term {}; dropping proposal",
                          self.id,
                          term);
                    return;
                }
                let mut m = m;
                m.set_to(self.lead);
                self.send(m);
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.lead = m.get_from();
                self.handle_append_entries(m);
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.lead = m.get_from();
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.handle_snapshot(m);
            }
            MessageType::MsgRequestVote => {
                let t = MessageType::MsgRequestVoteResponse;
                if (self.vote == INVALID_ID || self.vote == m.get_from()) &&
                   self.raft_log.is_up_to_date(m.get_index(), m.get_log_term()) {
                    self.log_vote_approve(&m);
                    self.election_elapsed = 0;
                    self.vote = m.get_from();
                    self.send(new_message(m.get_from(), t, None));
                } else {
                    self.log_vote_reject(&m);
                    let mut to_send = new_message(m.get_from(), t, None);
                    to_send.set_reject(true);
                    self.send(to_send);
                }
            }
            _ => {}
        }
    }

    fn handle_append_entries(&mut self, m: Message) {
        if m.get_index() < self.raft_log.committed {
            let mut to_send = Message::new();
            to_send.set_to(m.get_from());
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.set_index(self.raft_log.committed);
            self.send(to_send);
            return;
        }
        let mut to_send = Message::new();
        to_send.set_to(m.get_from());
        to_send.set_msg_type(MessageType::MsgAppendResponse);
        match self.raft_log.maybe_append(m.get_index(),
                                         m.get_log_term(),
                                         m.get_commit(),
                                         m.get_entries()) {
            Some(mlast_index) => {
                to_send.set_index(mlast_index);
                self.send(to_send);
            }
            None => {
                debug!("{:x} [logterm: {}, index: {}] rejected msgApp [logterm: {}, index: {}] \
                        from {:x}",
                       self.id,
                       self.raft_log.zero_term_on_err_compacted(self.raft_log.term(m.get_index())),
                       m.get_index(),
                       m.get_log_term(),
                       m.get_index(),
                       m.get_from());
                to_send.set_index(m.get_index());
                to_send.set_reject(true);
                to_send.set_reject_hint(self.raft_log.last_index());
                self.send(to_send);
            }
        }
    }

    fn handle_heartbeat(&mut self, m: Message) {
        self.raft_log.commit_to(m.get_commit());
        let mut to_send = Message::new();
        to_send.set_to(m.get_from());
        to_send.set_msg_type(MessageType::MsgHeartbeatResponse);
        self.send(to_send);
    }

    fn handle_snapshot(&mut self, m: Message) {
        let mut m = m;
        let (sindex, sterm) = (m.get_snapshot().get_metadata().get_index(),
                               m.get_snapshot().get_metadata().get_term());
        if self.restore(m.take_snapshot()) {
            info!("{:x} [commit: {}] restored snapshot [index: {}, term: {}]",
                  self.id,
                  self.raft_log.committed,
                  sindex,
                  sterm);
            let mut to_send = Message::new();
            to_send.set_to(m.get_from());
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.set_index(self.raft_log.last_index());
            self.send(to_send);
        } else {
            info!("{:x} [commit: {}] ignored snapshot [index: {}, term: {}]",
                  self.id,
                  self.raft_log.committed,
                  sindex,
                  sterm);
            let mut to_send = Message::new();
            to_send.set_to(m.get_from());
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.set_index(self.raft_log.committed);
            self.send(to_send);
        }
    }

    fn restore_raft(&mut self, snap: &Snapshot) -> Option<bool> {
        let meta = snap.get_metadata();
        if self.raft_log.match_term(meta.get_index(), meta.get_term()) {
            info!("{:x} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to \
                   snapshot [index: {}, term: {}]",
                  self.id,
                  self.raft_log.committed,
                  self.raft_log.last_index(),
                  self.raft_log.last_term(),
                  meta.get_index(),
                  meta.get_term());
            self.raft_log.commit_to(meta.get_index());
            return Some(false);
        }

        info!("{:x} [commit: {}, lastindex: {}, lastterm: {}] starts to restore snapshot [index: \
               {}, term: {}]",
              self.id,
              self.raft_log.committed,
              self.raft_log.last_index(),
              self.raft_log.last_term(),
              meta.get_index(),
              meta.get_term());
        self.prs = HashMap::with_capacity(meta.get_conf_state().get_nodes().len());
        for n in meta.get_conf_state().get_nodes() {
            let n = *n;
            let next_idx = self.raft_log.last_index() + 1;
            let matched = if n == self.id {
                next_idx - 1
            } else {
                0
            };
            self.set_progress(n, matched, next_idx);
            info!("{:x} restored progress of {:x} [{:?}]",
                  self.id,
                  n,
                  self.prs[&n]);
        }
        None
    }

    // restore recovers the state machine from a snapshot. It restores the log and the
    // configuration of state machine.
    fn restore(&mut self, snap: Snapshot) -> bool {
        if snap.get_metadata().get_index() < self.raft_log.committed {
            return false;
        }
        if let Some(b) = self.restore_raft(&snap) {
            return b;
        }
        self.raft_log.restore(snap);
        true
    }

    // promotable indicates whether state machine can be promoted to leader,
    // which is true when its own id is in progress list.
    fn promotable(&self) -> bool {
        self.prs.contains_key(&self.id)
    }

    pub fn add_node(&mut self, id: u64) {
        if self.prs.contains_key(&id) {
            // Ignore any redundant addNode calls (which can happen because the
            // initial bootstrapping entries are applied twice).
            return;
        }
        let last_index = self.raft_log.last_index();
        self.set_progress(id, 0, last_index + 1);
        self.pending_conf = false;
    }

    pub fn remove_node(&mut self, id: u64) {
        self.del_progress(id);
        self.pending_conf = false;
    }

    pub fn reset_pending_conf(&mut self) {
        self.pending_conf = false;
    }

    fn set_progress(&mut self, id: u64, matched: u64, next_idx: u64) {
        let mut p = new_progress(next_idx, self.max_inflight);
        p.matched = matched;
        self.prs.insert(id, p);
    }

    fn del_progress(&mut self, id: u64) {
        self.prs.remove(&id);
    }

    fn load_state(&mut self, hs: HardState) {
        if hs.get_commit() < self.raft_log.committed ||
           hs.get_commit() > self.raft_log.last_index() {
            panic!("{:x} hs.commit {} is out of range [{}, {}]",
                   self.id,
                   hs.get_commit(),
                   self.raft_log.committed,
                   self.raft_log.last_index())
        }
        self.raft_log.committed = hs.get_commit();
        self.term = hs.get_term();
        self.vote = hs.get_vote();
    }

    // is_election_timeout returns true if self.election_elapsed is greater than the
    // randomized election timeout in (electiontimeout, 2 * electiontimeout - 1).
    // Otherwise, it returns false.
    fn is_election_timeout(&mut self) -> bool {
        if self.election_elapsed < self.election_timeout {
            return false;
        }
        let d = self.election_elapsed - self.election_timeout;
        d > self.rng.gen_range(0, self.election_timeout)
    }

    // check_quorum_active returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // check_quorum_active also resets all recent_active to false.
    fn check_quorum_active(&mut self) -> bool {
        let mut act = 0;
        let self_id = self.id;
        for (id, p) in self.prs.iter_mut() {
            if id == &self_id {
                // self is always active
                act += 1;
                continue;
            }

            if p.recent_active {
                act += 1;
            }

            p.recent_active = false;
        }
        act >= self.quorum()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use raft::Storage;
    use std::sync::Arc;
    use std::collections::HashMap;
    use raft::errors::*;
    use rand;
    use raft::raft_log::{self, RaftLog};
    use protobuf::RepeatedField;
    use std::ops::Deref;
    use std::ops::DerefMut;
    use raft::storage::MemStorage;
    use proto::raftpb::{Entry, Message, MessageType};
    use raft::progress::{Progress, Inflights, ProgressState};
    use raft::log_unstable::Unstable;

    fn ltoa(raft_log: &RaftLog<MemStorage>) -> String {
        let mut s = format!("committed: {}\n", raft_log.committed);
        s = s + &format!("applied: {}\n", raft_log.applied);
        for (i, e) in raft_log.all_entries().iter().enumerate() {
            s = s + &format!("#{}: {:?}\n", i, e);
        }
        s
    }

    fn new_progress(state: ProgressState,
                    matched: u64,
                    next_idx: u64,
                    pending_snapshot: u64,
                    ins_size: usize)
                    -> Progress {
        Progress {
            state: state,
            matched: matched,
            next_idx: next_idx,
            pending_snapshot: pending_snapshot,
            ins: Inflights::new(ins_size),
            ..Default::default()
        }
    }

    fn new_test_raft<T: Storage + Default>(id: u64,
                                           peers: Vec<u64>,
                                           election: usize,
                                           heartbeat: usize,
                                           storage: Arc<T>)
                                           -> Raft<T> {
        Raft::new(&Config {
            id: id,
            peers: peers,
            election_tick: election,
            heartbeat_tick: heartbeat,
            storage: storage,
            max_size_per_msg: raft_log::NO_LIMIT,
            max_inflight_msgs: 256,
            ..Default::default()
        })
    }

    fn read_messages<T: Storage + Default>(raft: &mut Raft<T>) -> Vec<Message> {
        raft.msgs.drain(..).collect()
    }

    fn ents(terms: Vec<u64>) -> Interface {
        let store = MemStorage::new();
        for (i, term) in terms.iter().enumerate() {
            let mut e = Entry::new();
            e.set_index(i as u64 + 1);
            e.set_term(*term);
            store.wl().append(&[e]).expect("");
        }
        let mut raft = new_test_raft(1, vec![], 5, 1, Arc::new(store));
        raft.reset(0);
        Interface::new(raft)
    }

    fn next_ents(r: &mut Raft<MemStorage>, s: &MemStorage) -> Vec<Entry> {
        s.wl().append(&r.raft_log.unstable_entries().unwrap()).expect("");
        let (last_idx, last_term) = (r.raft_log.last_index(), r.raft_log.last_term());
        r.raft_log.stable_to(last_idx, last_term);
        let ents = r.raft_log.next_entries();
        let committed = r.raft_log.committed;
        r.raft_log.applied_to(committed);
        ents.unwrap()
    }

    #[derive(Default, Debug, PartialEq, Eq, Hash)]
    struct Connem {
        from: u64,
        to: u64,
    }

    /// Compare to upstream, we use struct instead of trait here.
    /// Because to be able to cast Interface later, we have to make
    /// Raft derive Any, which will require a lot of dependencies to derive Any.
    /// That's not worthy for just testing purpose.
    struct Interface {
        raft: Option<Raft<MemStorage>>,
    }

    impl Interface {
        fn new(r: Raft<MemStorage>) -> Interface {
            Interface { raft: Some(r) }
        }

        fn step(&mut self, m: Message) -> Result<()> {
            match self.raft {
                Some(_) => Raft::step(self, m),
                None => Ok(()),
            }
        }

        fn read_messages(&mut self) -> Vec<Message> {
            match self.raft {
                Some(_) => self.msgs.drain(..).collect(),
                None => vec![],
            }
        }

        fn initial(&mut self, id: u64, ids: &Vec<u64>) {
            if self.raft.is_some() {
                self.id = id;
                self.prs = HashMap::with_capacity(ids.len());
                for id in ids {
                    self.prs.insert(*id, Progress { ..Default::default() });
                }
                self.reset(0);
            }
        }
    }

    impl Deref for Interface {
        type Target = Raft<MemStorage>;
        fn deref(&self) -> &Raft<MemStorage> {
            self.raft.as_ref().unwrap()
        }
    }

    impl DerefMut for Interface {
        fn deref_mut(&mut self) -> &mut Raft<MemStorage> {
            self.raft.as_mut().unwrap()
        }
    }

    fn nop_stepper() -> Interface {
        Interface { raft: None }
    }

    fn new_message(from: u64, to: u64, t: MessageType, n: usize) -> Message {
        let mut m = Message::new();
        m.set_from(from);
        m.set_to(to);
        m.set_msg_type(t);
        if n > 0 {
            let mut ents = Vec::with_capacity(n);
            for _ in 0..n {
                ents.push(new_entry(0, 0, Some("somedata")));
            }
            m.set_entries(RepeatedField::from_vec(ents));
        }
        m
    }

    fn new_entry(term: u64, index: u64, data: Option<&str>) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(term);
        if let Some(d) = data {
            e.set_data(d.as_bytes().to_vec());
        }
        e
    }

    #[derive(Default)]
    struct Network {
        peers: HashMap<u64, Interface>,
        storage: HashMap<u64, Arc<MemStorage>>,
        dropm: HashMap<Connem, f64>,
        ignorem: HashMap<MessageType, bool>,
    }

    impl Network {
        // newNetwork initializes a network from peers.
        // A nil node will be replaced with a new *stateMachine.
        // A *stateMachine will get its k, id.
        // When using stateMachine, the address list is always [1, n].
        fn new(peers: Vec<Option<Interface>>) -> Network {
            let size = peers.len();
            let peer_addrs: Vec<u64> = (1..size as u64 + 1).collect();
            let mut nstorage: HashMap<u64, Arc<MemStorage>> = HashMap::new();
            let mut npeers: HashMap<u64, Interface> = HashMap::new();
            let mut peers = peers;
            for (p, id) in peers.drain(..).zip(peer_addrs.clone()) {
                match p {
                    None => {
                        nstorage.insert(id, Arc::new(MemStorage::new()));
                        let raft = new_test_raft(id,
                                                 peer_addrs.clone(),
                                                 10,
                                                 1,
                                                 nstorage[&id].clone());
                        npeers.insert(id, Interface::new(raft));
                    }
                    Some(p) => {
                        let mut p = p;
                        p.initial(id, &peer_addrs);
                        npeers.insert(id, p);
                    }
                }
            }
            Network {
                peers: npeers,
                storage: nstorage,
                ..Default::default()
            }
        }

        fn ignore(&mut self, t: MessageType) {
            self.ignorem.insert(t, true);
        }

        fn filter(&self, msgs: Vec<Message>) -> Vec<Message> {
            let mut msgs = msgs;
            let msgs: Vec<Message> =
                msgs.drain(..)
                    .filter(|m| {
                        if self.ignorem.get(&m.get_msg_type()).map(|x| *x).unwrap_or(false) {
                            return false;
                        }
                        // hups never go over the network, so don't drop them but panic
                        assert!(m.get_msg_type() != MessageType::MsgHup, "unexpected msgHup");
                        let perc = self.dropm
                                       .get(&Connem {
                                           from: m.get_from(),
                                           to: m.get_to(),
                                       })
                                       .map(|x| *x)
                                       .unwrap_or(0f64);
                        rand::random::<f64>() >= perc
                    })
                    .collect();
            msgs
        }

        fn send(&mut self, msgs: Vec<Message>) {
            let mut msgs = msgs;
            while msgs.len() > 0 {
                let mut new_msgs = vec![];
                for m in msgs.drain(..) {
                    let resp = {
                        let p = self.peers.get_mut(&m.get_to()).unwrap();
                        p.step(m).expect("");
                        p.read_messages()
                    };
                    new_msgs.append(&mut self.filter(resp));
                }
                msgs.append(&mut new_msgs);
            }
        }

        fn drop(&mut self, from: u64, to: u64, perc: f64) {
            self.dropm.insert(Connem {
                                  from: from,
                                  to: to,
                              },
                              perc);
        }

        fn cut(&mut self, one: u64, other: u64) {
            self.drop(one, other, 1f64);
            self.drop(other, one, 1f64);
        }

        fn isolate(&mut self, id: u64) {
            for i in 0..self.peers.len() as u64 {
                let nid = i + 1;
                if nid != id {
                    self.drop(id, nid, 1.0);
                    self.drop(nid, id, 1.0);
                }
            }
        }

        fn recover(&mut self) {
            self.dropm = HashMap::new();
            self.ignorem = HashMap::new();
        }
    }

    #[test]
    fn test_progress_become_probe() {
        let matched = 1u64;
        let mut tests = vec![(new_progress(ProgressState::Replicate, matched, 5, 0, 256),
                              2),
                             // snapshot finish
                             (new_progress(ProgressState::Snapshot, matched, 5, 10, 256),
                              11),
                             // snapshot failure
                             (new_progress(ProgressState::Snapshot, matched, 5, 0, 256), 2)];
        for (i, &mut (ref mut p, wnext)) in tests.iter_mut().enumerate() {
            p.become_probe();
            if p.state != ProgressState::Probe {
                panic!("#{}: state = {:?}, want {:?}",
                       i,
                       p.state,
                       ProgressState::Probe);
            }
            if p.matched != matched {
                panic!("#{}: match = {:?}, want {:?}", i, p.matched, matched);
            }
            if p.next_idx != wnext {
                panic!("#{}: next = {}, want {}", i, p.next_idx, wnext);
            }
        }
    }

    #[test]
    fn test_progress_become_replicate() {
        let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
        p.become_replicate();

        assert_eq!(p.state, ProgressState::Replicate);
        assert_eq!(p.matched, 1);
        assert_eq!(p.matched + 1, p.next_idx);
    }

    #[test]
    fn test_progress_become_snapshot() {
        let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
        p.become_snapshot(10);
        assert_eq!(p.state, ProgressState::Snapshot);
        assert_eq!(p.matched, 1);
        assert_eq!(p.pending_snapshot, 10);
    }

    #[test]
    fn test_progress_update() {
        let (prev_m, prev_n) = (3u64, 5u64);
        let tests = vec![
            (prev_m - 1, prev_m, prev_n, false),
            (prev_m, prev_m, prev_n, false),
            (prev_m + 1, prev_m + 1, prev_n, true),
            (prev_m + 2, prev_m + 2, prev_n + 1, true),
        ];
        for (i, &(update, wm, wn, wok)) in tests.iter().enumerate() {
            let mut p = Progress {
                matched: prev_m,
                next_idx: prev_n,
                ..Default::default()
            };
            let ok = p.maybe_update(update);
            if ok != wok {
                panic!("#{}: ok= {}, want {}", i, ok, wok);
            }
            if p.matched != wm {
                panic!("#{}: match= {}, want {}", i, p.matched, wm);
            }
            if p.next_idx != wn {
                panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
            }
        }
    }

    #[test]
    fn test_progress_maybe_decr() {
        let tests = vec![
            // state replicate and rejected is not greater than match
            (ProgressState::Replicate, 5, 10, 5, 5, false, 10),
            // state replicate and rejected is not greater than match
            (ProgressState::Replicate, 5, 10, 4, 4, false, 10),
            // state replicate and rejected is greater than match
			// directly decrease to match+1
            (ProgressState::Replicate, 5, 10, 9, 9, true, 6),
            // next-1 != rejected is always false
            (ProgressState::Probe, 0, 0, 0, 0, false, 0),
            // next-1 != rejected is always false
            (ProgressState::Probe, 0, 10, 5, 5, false, 10),
            // next>1 = decremented by 1
            (ProgressState::Probe, 0, 10, 9, 9, true, 9),
            // next>1 = decremented by 1
            (ProgressState::Probe, 0, 2, 1, 1, true, 1),
            // next<=1 = reset to 1
            (ProgressState::Probe, 0, 1, 0, 0, true, 1),
            // decrease to min(rejected, last+1)
            (ProgressState::Probe, 0, 10, 9, 2, true, 3),
            // rejected < 1, reset to 1
            (ProgressState::Probe, 0, 10, 9, 0, true, 1),
        ];
        for (i, &(state, m, n, rejected, last, w, wn)) in tests.iter().enumerate() {
            let mut p = new_progress(state, m, n, 0, 0);
            if p.maybe_decr_to(rejected, last) != w {
                panic!("#{}: maybeDecrTo= {}, want {}", i, !w, w);
            }
            if p.matched != m {
                panic!("#{}: match= {}, want {}", i, p.matched, m);
            }
            if p.next_idx != wn {
                panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
            }
        }
    }

    #[test]
    fn test_progress_is_paused() {
        let tests = vec![
            (ProgressState::Probe, false, false),
            (ProgressState::Probe, true, true),
            (ProgressState::Replicate, false, false),
            (ProgressState::Replicate, true, false),
            (ProgressState::Snapshot, false, true),
            (ProgressState::Snapshot, true, true),
        ];
        for (i, &(state, paused, w)) in tests.iter().enumerate() {
            let p = Progress {
                state: state,
                paused: paused,
                ins: Inflights::new(256),
                ..Default::default()
            };
            if p.is_paused() != w {
                panic!("#{}: shouldwait = {}, want {}", i, p.is_paused(), w)
            }
        }
    }

    // TestProgressResume ensures that progress.maybeUpdate and progress.maybeDecrTo
    // will reset progress.paused.
    #[test]
    fn test_progress_resume() {
        let mut p = Progress {
            next_idx: 2,
            paused: true,
            ..Default::default()
        };
        p.maybe_decr_to(1, 1);
        assert!(!p.paused, "paused= true, want false");
        p.paused = true;
        p.maybe_update(2);
        assert!(!p.paused, "paused= true, want false");
    }

    // TestProgressResumeByHeartbeat ensures raft.heartbeat reset progress.paused by heartbeat.
    #[test]
    fn test_progress_resume_by_heartbeat() {
        let mut raft = new_test_raft(1, vec![1, 2], 5, 1, Arc::new(MemStorage::new()));
        raft.become_candidate();
        raft.become_leader();
        raft.prs.get_mut(&2).unwrap().paused = true;
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgBeat);
        raft.step(m).expect("");
        assert!(!raft.prs[&2].paused, "paused = true, want false");
    }

    #[test]
    fn test_progress_paused() {
        let mut raft = new_test_raft(1, vec![1, 2], 5, 1, Arc::new(MemStorage::new()));
        raft.become_candidate();
        raft.become_leader();
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgPropose);
        let mut e = Entry::new();
        e.set_data("some_data".as_bytes().to_vec());
        m.set_entries(RepeatedField::from_vec(vec![e]));
        raft.step(m.clone()).expect("");
        raft.step(m.clone()).expect("");
        raft.step(m.clone()).expect("");
        let ms = read_messages(&mut raft);
        assert_eq!(ms.len(), 1);
    }

    #[test]
    fn test_leader_election() {
        let mut tests = vec![
            (Network::new(vec![None, None, None]), StateRole::Leader),
            (Network::new(vec![None, None, Some(nop_stepper())]), StateRole::Leader),
            (Network::new(vec![None, Some(nop_stepper()), Some(nop_stepper())]), StateRole::Candidate),
            (Network::new(vec![None, Some(nop_stepper()), Some(nop_stepper()), None]), StateRole::Candidate),
            (Network::new(vec![None, Some(nop_stepper()), Some(nop_stepper()), None, None]), StateRole::Leader),
            
            // three logs further along than 0
            (Network::new(vec![None, Some(ents(vec![1])), Some(ents(vec![2])), Some(ents(vec![1, 3])), None]), StateRole::Follower),
            
            // logs converge
            (Network::new(vec![Some(ents(vec![1])), None, Some(ents(vec![2])), Some(ents(vec![1])), None]), StateRole::Leader),
        ];

        for (i, &mut (ref mut network, state)) in tests.iter_mut().enumerate() {
            let mut m = Message::new();
            m.set_from(1);
            m.set_to(1);
            m.set_msg_type(MessageType::MsgHup);
            network.send(vec![m]);
            let raft = network.peers.get(&1).unwrap();
            if raft.state != state {
                panic!("#{}: state = {:?}, want {:?}", i, raft.state, state);
            }
            if raft.term != 1 {
                panic!("#{}: term = {}, want {}", i, raft.term, 1)
            }
        }
    }

    #[test]
    fn test_log_replicatioin() {
        let mut tests = vec![
            (Network::new(vec![None, None, None]), vec![new_message(1, 1, MessageType::MsgPropose, 1)], 2),
            (Network::new(vec![None, None, None]), vec![new_message(1, 1, MessageType::MsgPropose, 1), new_message(1, 2, MessageType::MsgHup, 0), new_message(1, 2, MessageType::MsgPropose, 1)], 4),
        ];

        for (i, &mut (ref mut network, ref msgs, wcommitted)) in tests.iter_mut().enumerate() {
            network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
            for m in msgs {
                network.send(vec![m.clone()]);
            }

            for (j, x) in network.peers.iter_mut() {
                if x.raft_log.committed != wcommitted {
                    panic!("#{}.{}: committed = {}, want {}",
                           i,
                           j,
                           x.raft_log.committed,
                           wcommitted);
                }

                let mut ents = next_ents(x, &network.storage[j]);
                let ents: Vec<Entry> = ents.drain(..).filter(|e| e.has_data()).collect();
                for (k, m) in msgs.iter()
                                  .filter(|m| m.get_msg_type() == MessageType::MsgPropose)
                                  .enumerate() {
                    if ents[k].get_data() != m.get_entries()[0].get_data() {
                        panic!("#{}.{}: data = {:?}, want {:?}",
                               i,
                               j,
                               ents[k].get_data(),
                               m.get_entries()[0].get_data());
                    }
                }
            }
        }
    }

    #[test]
    fn test_single_node_commit() {
        let mut tt = Network::new(vec![None]);
        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

        assert_eq!(tt.peers[&1].raft_log.committed, 3);
    }

    // TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
    // when leader changes, no new proposal comes in and ChangeTerm proposal is
    // filtered.
    #[test]
    fn test_cannot_commit_without_new_term_entry() {
        let mut tt = Network::new(vec![None, None, None, None, None]);
        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

        // 0 cannot reach 2, 3, 4
        tt.cut(1, 3);
        tt.cut(1, 4);
        tt.cut(1, 5);

        tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

        assert_eq!(tt.peers[&1].raft_log.committed, 1);

        // network recovery
        tt.recover();
        // avoid committing ChangeTerm proposal
        tt.ignore(MessageType::MsgAppend);

        // elect 2 as the new leader with term 2
        tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

        // no log entries from previous term should be committed
        assert_eq!(tt.peers[&2].raft_log.committed, 1);

        tt.recover();
        // send heartbeat; reset wait
        tt.send(vec![new_message(2, 2, MessageType::MsgBeat, 0)]);
        // append an entry at current term
        tt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);
        // expect the committed to be advanced
        assert_eq!(tt.peers[&2].raft_log.committed, 5);
    }

    // TestCommitWithoutNewTermEntry tests the entries could be committed
    // when leader changes, no new proposal comes in.
    #[test]
    fn test_commit_without_new_term_entry() {
        let mut tt = Network::new(vec![None, None, None, None, None]);
        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

        // 0 cannot reach 2, 3, 4
        tt.cut(1, 3);
        tt.cut(1, 4);
        tt.cut(1, 5);

        tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

        assert_eq!(tt.peers[&1].raft_log.committed, 1);

        // network recovery
        tt.recover();

        // elect 1 as the new leader with term 2
        // after append a ChangeTerm entry from the current term, all entries
        // should be committed
        tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

        assert_eq!(tt.peers[&1].raft_log.committed, 4);
    }

    #[test]
    fn test_dueling_candidates() {
        let a = new_test_raft(1, vec![1, 2, 3], 10, 1, Arc::new(MemStorage::new()));
        let b = new_test_raft(2, vec![1, 2, 3], 10, 1, Arc::new(MemStorage::new()));
        let c = new_test_raft(3, vec![1, 2, 3], 10, 1, Arc::new(MemStorage::new()));

        let mut nt = Network::new(vec![Some(Interface::new(a)),
                                       Some(Interface::new(b)),
                                       Some(Interface::new(c))]);
        nt.cut(1, 3);

        nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

        nt.recover();
        nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

        let store = MemStorage::new();
        let mut e = Entry::new();
        e.set_term(1);
        e.set_index(1);
        store.wl().append(&[new_entry(1, 1, None)]).expect("");
        let wlog = RaftLog {
            store: Arc::new(store),
            committed: 1,
            unstable: Unstable { offset: 2, ..Default::default() },
            ..Default::default()
        };
        let wlog2 = RaftLog::new(Arc::new(MemStorage::new()));
        let tests = vec![
            (StateRole::Follower, 2, &wlog),
            (StateRole::Follower, 2, &wlog),
            (StateRole::Follower, 2, &wlog2),
        ];

        for (i, &(state, term, raft_log)) in tests.iter().enumerate() {
            let id = i as u64 + 1;
            if nt.peers[&id].state != state {
                panic!("#{}: state = {:?}, want {:?}",
                       i,
                       nt.peers[&id].state,
                       state);
            }
            if nt.peers[&id].get_term() != term {
                panic!("#{}: term = {}, want {}", i, nt.peers[&id].get_term(), term);
            }
            let base = ltoa(raft_log);
            let l = ltoa(&nt.peers[&(1 + i as u64)].raft_log);
            if base != l {
                panic!("#{}: raft_log:\n {}, want:\n {}", i, l, base);
            }
        }
    }

    #[test]
    fn test_candidate_concede() {
        let mut tt = Network::new(vec![None, None, None]);
        tt.isolate(1);

        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        tt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

        // heal the partition
        tt.recover();
        // send heartbeat; reset wait
        tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

        // send a proposal to 3 to flush out a MsgAppend to 1
        let data = "force follower";
        let mut m = new_message(3, 3, MessageType::MsgPropose, 0);
        m.set_entries(RepeatedField::from_vec(vec![new_entry(0, 0, Some(data))]));
        tt.send(vec![m]);
        // send heartbeat; flush out commit
        tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

        assert_eq!(tt.peers[&1].state, StateRole::Follower);
        assert_eq!(tt.peers[&1].get_term(), 1);

        let store = MemStorage::new();
        store.wl().append(&[new_entry(1, 1, None), new_entry(1, 2, Some(data))]).expect("");
        let want_log = ltoa(&RaftLog {
            store: Arc::new(store),
            unstable: Unstable { offset: 3, ..Default::default() },
            committed: 2,
            ..Default::default()
        });
        for (id, p) in tt.peers.iter() {
            let l = ltoa(&p.raft_log);
            if l != want_log {
                panic!("#{}: raft_log: {}, want: {}", id, l, want_log);
            }
        }
    }

    #[test]
    fn test_single_node_candidate() {
        let mut tt = Network::new(vec![None]);
        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

        assert_eq!(tt.peers[&1].state, StateRole::Leader);
    }
}
