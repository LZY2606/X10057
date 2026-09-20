//! Serializable, restorable snapshots of the progress tree and monotonic merging.
//!
//! A [`model::Snapshot`] captures every task of a tree (names, stable ids, steps, bounds, units
//! and lifecycle state) together with the still-retained messages. Snapshots serve three
//! purposes:
//!
//! * [`Root::snapshot`](super::Root::snapshot) serializes an interrupted computation so another
//!   process can pick it up later.
//! * [`Root::restore`](super::Root::restore) continues displaying progress from a snapshot; every
//!   handle of the previous tree is invalidated and can no longer mutate the restored tree.
//! * [`Root::merge`](super::Root::merge) monotonically folds a snapshot into a live tree: step
//!   counters only move forward, terminal states are sticky, and snapshots of other trees are
//!   rejected to prevent id collisions between unrelated progress trees.

use std::{
    borrow::Cow,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;

use crate::{
    messages::{MessageLevel, MessageRingBuffer},
    progress::{Key, State, Task, Value},
    tree::{Item, Root, Shared},
    unit::display::Location,
};

pub mod clock;
pub mod error;
pub mod model;

#[cfg(test)]
mod codec_tests;

pub use clock::FixedClock;
pub use model::Snapshot as SnapshotData;

use error::MergeReport;
use model::{
    KeySnapshot, MessageSnapshot, ModeSnapshot, SNAPSHOT_VERSION, Snapshot, StateSnapshot, TaskSnapshot, UnitSnapshot,
    ValueSnapshot,
};

pub use error::{MergeError, RestoreError};

// ---------------------------------------------------------------------------
// Codecs
// ---------------------------------------------------------------------------

pub(crate) fn encode_key(key: Key) -> KeySnapshot {
    let [a, b, c, d, e, f] = key.components();
    KeySnapshot { a, b, c, d, e, f }
}

pub(crate) fn decode_key(key: &KeySnapshot) -> Option<Key> {
    Key::from_components([key.a, key.b, key.c, key.d, key.e, key.f])
}

pub(crate) fn decode_id(id: &[u8]) -> crate::progress::Id {
    let mut out = crate::progress::UNKNOWN;
    for (slot, byte) in out.iter_mut().zip(id.iter()) {
        *slot = *byte;
    }
    out
}

pub(crate) fn is_valid_id(id: &[u8]) -> bool {
    id.len() == 4 && id != crate::progress::UNKNOWN.as_slice()
}

fn parent_key(key: &Key) -> Option<Key> {
    let mut components = key.components();
    let level = key.level();
    if level < 1 {
        return None;
    }
    components[level as usize - 1] = None;
    Key::from_components(components)
}

fn next_child_id(snapshot: &Snapshot, parent: &Key) -> u16 {
    let level = parent.level();
    snapshot
        .tasks
        .iter()
        .filter_map(|task| {
            let key = decode_key(&task.key)?;
            (key.level() == level + 1 && &parent_key(&key)? == parent)
                .then(|| key.components()[level as usize])
                .flatten()
        })
        .max()
        .map(|id| id.saturating_add(1))
        .unwrap_or(0)
}

pub(crate) fn encode_state(state: &State) -> StateSnapshot {
    match state {
        State::Running => StateSnapshot::Running,
        State::Blocked(reason, eta) => {
            let (eta_secs, eta_nanos) = encode_eta(*eta);
            StateSnapshot::Blocked {
                reason: reason.as_ref().to_owned(),
                eta_secs,
                eta_nanos,
            }
        }
        State::Halted(reason, eta) => {
            let (eta_secs, eta_nanos) = encode_eta(*eta);
            StateSnapshot::Halted {
                reason: reason.as_ref().to_owned(),
                eta_secs,
                eta_nanos,
            }
        }
        State::Completed => StateSnapshot::Completed,
    }
}

fn encode_eta(eta: Option<SystemTime>) -> (Option<u64>, u32) {
    eta.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| (Some(duration.as_secs()), duration.subsec_nanos()))
        .unwrap_or((None, 0))
}

pub(crate) fn decode_state(state: &StateSnapshot) -> Option<State> {
    Some(match state {
        StateSnapshot::Running => State::Running,
        StateSnapshot::Blocked {
            reason,
            eta_secs,
            eta_nanos,
        } => State::Blocked(Cow::Owned(reason.clone()), decode_eta(*eta_secs, *eta_nanos)),
        StateSnapshot::Halted {
            reason,
            eta_secs,
            eta_nanos,
        } => State::Halted(Cow::Owned(reason.clone()), decode_eta(*eta_secs, *eta_nanos)),
        StateSnapshot::Completed => State::Completed,
    })
}

fn decode_eta(secs: Option<u64>, nanos: u32) -> Option<SystemTime> {
    secs.map(|secs| UNIX_EPOCH + Duration::new(secs, nanos))
}

fn encode_unit(unit: &Option<crate::unit::Unit>) -> Option<UnitSnapshot> {
    let unit = unit.as_ref()?;
    let label = unit.label_str()?;
    Some(UnitSnapshot {
        label: label.to_owned(),
        mode: unit.mode().map(|mode| {
            let (location, percent, throughput) = mode.parts();
            ModeSnapshot {
                location: match location {
                    Location::BeforeValue => 0,
                    Location::AfterUnit => 1,
                },
                percent,
                throughput,
            }
        }),
    })
}

pub(crate) fn decode_unit(unit: &Option<UnitSnapshot>) -> Option<crate::unit::Unit> {
    let unit = unit.as_ref()?;
    let label: &'static str = Box::leak(unit.label.clone().into_boxed_str());
    let mode = unit.mode.map(|mode| {
        crate::unit::display::Mode::from_parts(
            match mode.location {
                0 => Location::BeforeValue,
                _ => Location::AfterUnit,
            },
            mode.percent,
            mode.throughput,
        )
    });
    Some(crate::unit::Unit::from_label(label, mode))
}

fn encode_time(time: SystemTime) -> MessageSnapshot {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    MessageSnapshot {
        secs: duration.as_secs(),
        nanos: duration.subsec_nanos(),
        level: 0,
        origin: String::new(),
        message: String::new(),
    }
}

fn decode_time(message: &MessageSnapshot) -> Option<SystemTime> {
    Some(UNIX_EPOCH + Duration::new(message.secs, message.nanos))
}

fn decode_level(level: u8) -> Option<MessageLevel> {
    match level {
        0 => Some(MessageLevel::Info),
        1 => Some(MessageLevel::Failure),
        2 => Some(MessageLevel::Success),
        _ => None,
    }
}

fn hex_id(id: &[u8]) -> String {
    let mut out = String::with_capacity(id.len() * 2);
    for byte in id {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

// ---------------------------------------------------------------------------
// Snapshot, restore, merge
// ---------------------------------------------------------------------------

impl Root {
    /// Produce a serializable snapshot of every task and the retained messages.
    ///
    /// The snapshot identifies the tree it came from; feeding it to
    /// [`merge`](Self::merge) on an unrelated tree fails instead of colliding with its ids.
    pub fn snapshot(&self) -> Snapshot {
        let mut entries = Vec::new();
        self.sorted_snapshot(&mut entries);

        let tasks = entries
            .into_iter()
            .filter(|(key, _)| *key != Key::default())
            .map(|(key, task)| TaskSnapshot {
                key: encode_key(key),
                name: task.name,
                id: task.id.to_vec(),
                progress: task.progress.as_ref().map(|value| ValueSnapshot {
                    step: value.step.load(Ordering::SeqCst),
                    done_at: value.done_at,
                    state: encode_state(&value.state),
                    unit: encode_unit(&value.unit),
                }),
            })
            .collect();

        let mut messages = Vec::new();
        self.copy_messages(&mut messages);
        let messages = messages
            .into_iter()
            .map(|message| {
                let time = encode_time(message.time);
                MessageSnapshot {
                    secs: time.secs,
                    nanos: time.nanos,
                    level: match message.level {
                        MessageLevel::Info => 0,
                        MessageLevel::Failure => 1,
                        MessageLevel::Success => 2,
                    },
                    origin: message.origin,
                    message: message.message,
                }
            })
            .collect();

        Snapshot {
            version: SNAPSHOT_VERSION,
            tree_id: self.shared.tree_id.0.to_vec(),
            message_buffer_capacity: self.messages_capacity(),
            tasks,
            messages,
        }
    }

    /// Continue the tree described by `snapshot`.
    ///
    /// The returned root is a fresh tree carrying the snapshot's identity. Every handle still
    /// pointing at this root's previous tree (root handles as well as `tree::Item` handles) is
    /// invalidated and becomes unable to mutate the restored tree.
    pub fn restore(self: &Arc<Self>, snapshot: &Snapshot) -> Result<Arc<Root>, RestoreError> {
        snapshot.validate()?;
        if snapshot.tree_id != self.shared.tree_id.0 {
            return Err(RestoreError::TreeIdMismatch {
                snapshot: hex_id(&snapshot.tree_id),
                current: hex_id(&self.shared.tree_id.0),
            });
        }

        let shared = Arc::new(Shared::from_snapshot(snapshot, self));
        let root_item = Item {
            highest_child_id: 0,
            value: Default::default(),
            key: Key::default(),
            shared: Arc::clone(&shared),
            generation: 0,
        };
        let restored = Root::from_parts(root_item, shared);

        // Invalidate every handle of the superseded tree.
        self.shared.supersede();
        Ok(restored)
    }

    /// Monotonically fold `snapshot` into this live tree and report what changed.
    ///
    /// Replaying the same snapshot any number of times leaves the tree unchanged after the first
    /// replay. Steps only move up per item and terminal states cannot be reopened.
    pub fn merge(self: &Arc<Self>, snapshot: &Snapshot) -> Result<MergeReport, MergeError> {
        snapshot.validate()?;
        if snapshot.tree_id != self.shared.tree_id.0 {
            return Err(MergeError::TreeIdMismatch {
                snapshot: hex_id(&snapshot.tree_id),
                current: hex_id(&self.shared.tree_id.0),
            });
        }

        let mut report = MergeReport::default();

        // Hold the root lock for the whole replay: concurrent `add_child` calls serialize on it,
        // so key allocation cannot interleave with snapshot keys.
        let mut root_guard = self.inner.lock();

        for task in &snapshot.tasks {
            let key = decode_key(&task.key).ok_or_else(|| MergeError::Malformed {
                context: format!("invalid key in task {:?}", task.key),
            })?;
            if key == Key::default() {
                return Err(MergeError::Malformed {
                    context: "snapshot contains a task for the root key".into(),
                });
            }
            self.fold_task(key, task, &mut report);
        }

        // Pre-seed per-parent child counters past every snapshot key, so new children never reuse
        // a key that belongs to the snapshot. Existing counters only move forward.
        let mut counters = self.shared.next_child.lock();
        for task in &snapshot.tasks {
            let key = decode_key(&task.key).expect("validated");
            if let Some(parent) = parent_key(&key) {
                let level = key.level();
                if let Some(last) = key.components()[level as usize - 1] {
                    let next = counters.entry(parent).or_insert(0);
                    *next = (*next).max(last.saturating_add(1));
                }
            }
        }
        {
            let mut root_next = self.shared.root_next_child.lock();
            *root_next = (*root_next).max(next_child_id(snapshot, &Key::default()));
        }
        root_guard.highest_child_id = *self.shared.root_next_child.lock();
        drop(counters);
        drop(root_guard);

        merge_messages(&self.shared.messages, snapshot, &mut report);

        Ok(report)
    }

    /// Insert or monotonically merge a single task.
    fn fold_task(&self, key: Key, task: &TaskSnapshot, report: &mut MergeReport) {
        let build_value = |value: &ValueSnapshot| Value {
            step: Arc::new(AtomicUsize::new(value.step)),
            done_at: value.done_at,
            unit: decode_unit(&value.unit),
            state: decode_state(&value.state).unwrap_or(State::Running),
        };

        let mut inserted = false;

        #[cfg(not(feature = "progress-tree-hp-hashmap"))]
        {
            if !self.shared.tree.contains_key(&key) {
                self.shared.tree.insert(
                    key,
                    Task {
                        name: task.name.clone(),
                        id: decode_id(&task.id),
                        progress: task.progress.as_ref().map(build_value),
                    },
                );
                inserted = true;
            } else {
                self.shared.tree.get_mut(&key, |existing| {
                    fold_existing_task(existing, task);
                });
            }
        }

        #[cfg(feature = "progress-tree-hp-hashmap")]
        {
            use dashmap::mapref::entry::Entry;
            match self.shared.tree.entry(key) {
                Entry::Vacant(vacant) => {
                    vacant.insert(Task {
                        name: task.name.clone(),
                        id: decode_id(&task.id),
                        progress: task.progress.as_ref().map(build_value),
                    });
                    inserted = true;
                }
                Entry::Occupied(mut occupied) => fold_existing_task(occupied.get_mut(), task),
            }
        }

        if inserted {
            report.tasks_inserted += 1;
        } else {
            report.tasks_updated += 1;
        }
    }
}

/// Fold an incoming task into an already-present task, monotonically.
fn fold_existing_task(existing: &mut Task, incoming: &TaskSnapshot) {
    if existing.id == crate::progress::UNKNOWN && is_valid_id(&incoming.id) {
        existing.id = decode_id(&incoming.id);
    }
    if existing.name.is_empty() && !incoming.name.is_empty() {
        existing.name.clone_from(&incoming.name);
    }
    let Some(incoming_value) = incoming.progress.as_ref() else {
        return;
    };
    match existing.progress.as_mut() {
        Some(current) => merge_value(current, incoming_value),
        None => {
            existing.progress = Some(Value {
                step: Arc::new(AtomicUsize::new(incoming_value.step)),
                done_at: incoming_value.done_at,
                unit: decode_unit(&incoming_value.unit),
                state: decode_state(&incoming_value.state).unwrap_or(State::Running),
            });
        }
    }
}

/// Monotonically merge a serializable value into a live value.
fn merge_value(current: &mut Value, incoming: &ValueSnapshot) {
    // Steps only move forward: concurrent updates settle at the per-item maximum.
    let _ = current
        .step
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |step| Some(step.max(incoming.step)));

    // A known bound replaces an unknown one; otherwise keep the larger bound.
    match (current.done_at, incoming.done_at) {
        (Some(existing), Some(incoming)) if incoming > existing => current.done_at = Some(incoming),
        (None, Some(incoming)) => current.done_at = Some(incoming),
        _ => {}
    }

    if current.unit.is_none() {
        current.unit = decode_unit(&incoming.unit);
    }

    // Terminal state is sticky; otherwise take the freshest non-terminal state.
    if !current.state.is_terminal() {
        if let Some(state) = decode_state(&incoming.state) {
            current.state = state;
        }
    }
}

/// Append only unseen messages, in snapshot order, preserving their original timestamps.
fn merge_messages(buffer: &parking_lot::Mutex<MessageRingBuffer>, snapshot: &Snapshot, report: &mut MergeReport) {
    let mut messages = buffer.lock();
    for message in &snapshot.messages {
        let (Some(level), Some(time)) = (decode_level(message.level), decode_time(message)) else {
            continue;
        };
        let candidate = crate::messages::Message {
            time,
            level,
            origin: message.origin.clone(),
            message: message.message.clone(),
        };
        if messages.contains(&candidate) {
            report.messages_skipped += 1;
            continue;
        }
        messages.push_overwrite_at(time, level, candidate.origin, candidate.message);
        report.messages_appended += 1;
    }
}

impl Shared {
    /// Rebuild shared state from a validated snapshot, inheriting the clock of `parent`.
    pub(crate) fn from_snapshot(snapshot: &Snapshot, parent: &Root) -> Shared {
        let tree = crate::tree::HashMap::with_capacity(snapshot.tasks.len().max(16));
        let mut next_child = std::collections::HashMap::<Key, u16>::new();
        for task in &snapshot.tasks {
            let key = decode_key(&task.key).expect("validated snapshot");
            tree.insert(
                key,
                Task {
                    name: task.name.clone(),
                    id: decode_id(&task.id),
                    progress: task.progress.as_ref().map(|value| Value {
                        step: Arc::new(AtomicUsize::new(value.step)),
                        done_at: value.done_at,
                        unit: decode_unit(&value.unit),
                        state: decode_state(&value.state).unwrap_or(State::Running),
                    }),
                },
            );
            if let Some(parent_key) = parent_key(&key) {
                let level = key.level();
                if let Some(last) = key.components()[level as usize - 1] {
                    let next = next_child.entry(parent_key).or_insert(0);
                    *next = (*next).max(last.saturating_add(1));
                }
            }
        }

        let buffer = Mutex::new(MessageRingBuffer::with_capacity(snapshot.message_buffer_capacity));
        {
            let mut messages = buffer.lock();
            for message in &snapshot.messages {
                if let (Some(level), Some(time)) = (decode_level(message.level), decode_time(message)) {
                    messages.push_overwrite_at(time, level, message.origin.clone(), message.message.clone());
                }
            }
        }

        let root_next = next_child_id(snapshot, &Key::default());
        Shared {
            tree: Arc::new(tree),
            messages: Arc::new(buffer),
            generation: Arc::new(AtomicU64::new(0)),
            next_child: Arc::new(Mutex::new(next_child)),
            root_next_child: Arc::new(Mutex::new(root_next)),
            clock: Arc::clone(&parent.shared.clock),
            tree_id: crate::tree::TreeId(snapshot.tree_id.as_slice().try_into().expect("validated snapshot")),
        }
    }

    pub(crate) fn set_clock(&mut self, clock: Arc<dyn crate::tree::Clock + Send + Sync>) {
        self.clock = clock;
    }
}

impl crate::tree::Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.get()
    }
}

impl Root {
    /// Return the identity of this tree. It ties snapshots to the tree they originated from.
    pub fn tree_id(&self) -> crate::tree::TreeId {
        self.shared.tree_id
    }
}

impl crate::tree::root::Options {
    /// Create a root with an explicit, caller-chosen identity and an injectable clock.
    ///
    /// Use this when the same logical tree is reconstructed in a new process: the identity makes
    /// snapshots compatible across restarts, and the clock makes message timestamps deterministic
    /// for byte-reproducible snapshot files.
    pub fn create_with_tree_id(self, tree_id: crate::tree::TreeId, clock: FixedClock) -> std::sync::Arc<Root> {
        let mut shared = Shared::with_capacity(self.initial_capacity, self.message_buffer_capacity);
        shared.tree_id = tree_id;
        shared.set_clock(Arc::new(clock));
        let shared = Arc::new(shared);
        let root_item = Item {
            highest_child_id: 0,
            value: Default::default(),
            key: Key::default(),
            shared: Arc::clone(&shared),
            generation: 0,
        };
        Root::from_parts(root_item, shared)
    }
}
