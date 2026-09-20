//! Serializable data-transfer types for [`super::Snapshot`].

use serde::{Deserialize, Serialize};

/// Bumped whenever the on-the-wire representation changes incompatibly.
pub(crate) const SNAPSHOT_VERSION: u32 = 1;

/// A serializable, deterministic snapshot of an entire progress tree.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct Snapshot {
    /// Version of the snapshot format.
    pub version: u32,
    /// Identity of the tree the snapshot was taken from (16 bytes).
    pub tree_id: Vec<u8>,
    /// Capacity of the message ring buffer to recreate on restore.
    pub message_buffer_capacity: usize,
    /// All tasks ordered by hierarchy key for byte-reproducible output.
    pub tasks: Vec<TaskSnapshot>,
    /// Retained messages ordered from oldest to newest.
    pub messages: Vec<MessageSnapshot>,
}

/// Serializable representation of a [`crate::progress::Task`].
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct TaskSnapshot {
    /// Position in the hierarchy.
    pub key: KeySnapshot,
    /// The task name.
    pub name: String,
    /// The stable four-byte task id.
    pub id: Vec<u8>,
    /// Progress of the task, unless it is an organizational unit.
    pub progress: Option<ValueSnapshot>,
}

/// Position of a task in the hierarchy as six optional levels.
#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Default)]
pub struct KeySnapshot {
    /// Level 1 component.
    pub a: Option<u16>,
    /// Level 2 component.
    pub b: Option<u16>,
    /// Level 3 component.
    pub c: Option<u16>,
    /// Level 4 component.
    pub d: Option<u16>,
    /// Level 5 component.
    pub e: Option<u16>,
    /// Level 6 component.
    pub f: Option<u16>,
}

/// Serializable representation of a [`crate::progress::Value`].
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct ValueSnapshot {
    /// The amount of progress made.
    pub step: usize,
    /// Optional upper bound.
    pub done_at: Option<usize>,
    /// Lifecycle state.
    pub state: StateSnapshot,
    /// Optional statically-known unit.
    pub unit: Option<UnitSnapshot>,
}

/// Lifecycle state of a task.
///
/// Discriminant names form the public snapshot format and must not be renamed without bumping
/// `SNAPSHOT_VERSION`.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub enum StateSnapshot {
    /// Task is running.
    Running,
    /// Task cannot progress, with a reason and optional epoch time eta.
    Blocked {
        /// Human-readable reason.
        reason: String,
        /// Seconds since the Unix epoch, if known.
        eta_secs: Option<u64>,
        /// Nanosecond part of the eta.
        eta_nanos: u32,
    },
    /// Task is halted (interruptable), with a reason and optional eta.
    Halted {
        /// Human-readable reason.
        reason: String,
        /// Seconds since the Unix epoch, if known.
        eta_secs: Option<u64>,
        /// Nanosecond part of the eta.
        eta_nanos: u32,
    },
    /// Task completed; terminal and sticky.
    Completed,
}

/// Serializable representation of a static [`crate::unit::Unit`].
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct UnitSnapshot {
    /// The static unit label.
    pub label: String,
    /// Optional display mode.
    pub mode: Option<ModeSnapshot>,
}

/// Serializable representation of a [`crate::unit::display::Mode`].
#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, Debug)]
pub struct ModeSnapshot {
    /// Where extra information is rendered (`0` = before the value, `1` = after the unit).
    pub location: u8,
    /// Whether the percentage is shown.
    pub percent: bool,
    /// Whether throughput is shown.
    pub throughput: bool,
}

/// Serializable representation of a [`crate::messages::Message`].
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct MessageSnapshot {
    /// Seconds since the Unix epoch.
    pub secs: u64,
    /// Nanosecond part of the timestamp.
    pub nanos: u32,
    /// Severity, as a [`crate::messages::MessageLevel`] discriminant.
    pub level: u8,
    /// Name of the originating task.
    pub origin: String,
    /// The message text.
    pub message: String,
}

impl Snapshot {
    /// Validate format-level invariants that deserialization alone cannot express.
    pub(crate) fn validate(&self) -> Result<(), super::RestoreError> {
        if self.version != SNAPSHOT_VERSION {
            return Err(super::RestoreError::UnsupportedVersion {
                version: self.version,
                supported: SNAPSHOT_VERSION,
            });
        }
        if self.tree_id.len() != 16 {
            return Err(super::RestoreError::Malformed {
                context: format!("tree id must be 16 bytes, got {}", self.tree_id.len()),
            });
        }
        for task in &self.tasks {
            if task.id.len() != 4 {
                return Err(super::RestoreError::Malformed {
                    context: format!("task {:?} id must be 4 bytes, got {}", task.key, task.id.len()),
                });
            }
            super::decode_key(&task.key).ok_or_else(|| super::RestoreError::Malformed {
                context: format!("task key {:?} has a hole in its hierarchy path", task.key),
            })?;
        }
        Ok(())
    }
}
