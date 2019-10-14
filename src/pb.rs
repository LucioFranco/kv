use raft::eraftpb;

tonic::include_proto!("kv");

pub mod wal {
    tonic::include_proto!("wal");
}

// Types suck

impl From<eraftpb::Message> for Message {
    fn from(m: eraftpb::Message) -> Message {
        Message {
            msg_type: m.msg_type,
            to: m.to,
            from: m.from,
            term: m.term,
            log_term: m.log_term,
            index: m.index,
            entries: m.entries.into_iter().map(Entry::from).collect(),
            commit: m.commit,
            snapshot: m.snapshot.map(Snapshot::from),
            request_snapshot: m.request_snapshot,
            reject: m.reject,
            context: m.context,
            reject_hint: m.reject_hint,
        }
    }
}

impl From<eraftpb::Entry> for Entry {
    fn from(e: eraftpb::Entry) -> Entry {
        Entry {
            entry_type: e.entry_type,
            term: e.term,
            index: e.index,
            data: e.data,
            context: e.context,
            sync_log: e.sync_log,
        }
    }
}

impl From<eraftpb::Snapshot> for Snapshot {
    fn from(s: eraftpb::Snapshot) -> Snapshot {
        Snapshot {
            data: s.data,
            metadata: s.metadata.map(SnapshotMetadata::from),
        }
    }
}

impl From<eraftpb::SnapshotMetadata> for SnapshotMetadata {
    fn from(sm: eraftpb::SnapshotMetadata) -> SnapshotMetadata {
        SnapshotMetadata {
            conf_state: sm.conf_state.map(ConfState::from),
            pending_membership_change: sm.pending_membership_change.map(ConfState::from),
            pending_membership_change_index: sm.pending_membership_change_index,
            index: sm.index,
            term: sm.term,
        }
    }
}

impl From<eraftpb::ConfState> for ConfState {
    fn from(c: eraftpb::ConfState) -> ConfState {
        ConfState {
            nodes: c.nodes,
            learners: c.learners,
        }
    }
}

// And now we do the reverse...

impl From<Message> for eraftpb::Message {
    fn from(m: Message) -> eraftpb::Message {
        eraftpb::Message {
            msg_type: m.msg_type,
            to: m.to,
            from: m.from,
            term: m.term,
            log_term: m.log_term,
            index: m.index,
            entries: m.entries.into_iter().map(eraftpb::Entry::from).collect(),
            commit: m.commit,
            snapshot: m.snapshot.map(eraftpb::Snapshot::from),
            request_snapshot: m.request_snapshot,
            reject: m.reject,
            context: m.context,
            reject_hint: m.reject_hint,
        }
    }
}

impl From<Entry> for eraftpb::Entry {
    fn from(e: Entry) -> eraftpb::Entry {
        eraftpb::Entry {
            entry_type: e.entry_type,
            term: e.term,
            index: e.index,
            data: e.data,
            context: e.context,
            sync_log: e.sync_log,
        }
    }
}

impl From<Snapshot> for eraftpb::Snapshot {
    fn from(s: Snapshot) -> eraftpb::Snapshot {
        eraftpb::Snapshot {
            data: s.data,
            metadata: s.metadata.map(eraftpb::SnapshotMetadata::from),
        }
    }
}

impl From<SnapshotMetadata> for eraftpb::SnapshotMetadata {
    fn from(sm: SnapshotMetadata) -> eraftpb::SnapshotMetadata {
        eraftpb::SnapshotMetadata {
            conf_state: sm.conf_state.map(eraftpb::ConfState::from),
            pending_membership_change: sm.pending_membership_change.map(eraftpb::ConfState::from),
            pending_membership_change_index: sm.pending_membership_change_index,
            index: sm.index,
            term: sm.term,
        }
    }
}

impl From<ConfState> for eraftpb::ConfState {
    fn from(c: ConfState) -> eraftpb::ConfState {
        eraftpb::ConfState {
            nodes: c.nodes,
            learners: c.learners,
        }
    }
}
