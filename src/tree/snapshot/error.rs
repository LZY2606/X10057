//! Diagnosable errors for snapshot restore and monotonic merge.

use std::fmt;

/// Failure returned by [`crate::tree::Root::restore`].
#[derive(Clone, Eq, PartialEq, Debug)]
#[non_exhaustive]
pub enum RestoreError {
    /// The snapshot uses an unknown or unsupported format version.
    UnsupportedVersion {
        /// Version found in the snapshot.
        version: u32,
        /// Only version understood by this build.
        supported: u32,
    },
    /// Structurally invalid snapshot content (bad id lengths, holes in key paths, …).
    /// The context names the offending field.
    Malformed {
        /// Human-readable description of the invalid data.
        context: String,
    },
    /// The snapshot belongs to a different progress tree and must not be restored here,
    /// as that would mix tasks whose positional ids may collide.
    TreeIdMismatch {
        /// Hex-encoded identity of the snapshot.
        snapshot: String,
        /// Hex-encoded identity of the current tree.
        current: String,
    },
}

impl fmt::Display for RestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RestoreError::UnsupportedVersion { version, supported } => write!(
                f,
                "snapshot version {version} is not supported (only version {supported})"
            ),
            RestoreError::Malformed { context } => write!(f, "malformed snapshot: {context}"),
            RestoreError::TreeIdMismatch { snapshot, current } => write!(
                f,
                "snapshot belongs to tree {snapshot} but this tree is {current}; refusing to restore"
            ),
        }
    }
}

impl std::error::Error for RestoreError {}

/// Failure returned by [`crate::tree::Root::merge`].
#[derive(Clone, Eq, PartialEq, Debug)]
#[non_exhaustive]
pub enum MergeError {
    /// The snapshot uses an unknown or unsupported format version.
    UnsupportedVersion {
        /// Version found in the snapshot.
        version: u32,
        /// Only version understood by this build.
        supported: u32,
    },
    /// Structurally invalid snapshot content. The context names the offending field.
    Malformed {
        /// Human-readable description of the invalid data.
        context: String,
    },
    /// The snapshot belongs to a different progress tree. Merging it would interleave unrelated
    /// tasks whose positional ids can collide, so the merge is refused.
    TreeIdMismatch {
        /// Hex-encoded identity of the snapshot.
        snapshot: String,
        /// Hex-encoded identity of the current tree.
        current: String,
    },
}

impl From<RestoreError> for MergeError {
    fn from(value: RestoreError) -> Self {
        match value {
            RestoreError::UnsupportedVersion { version, supported } => {
                MergeError::UnsupportedVersion { version, supported }
            }
            RestoreError::Malformed { context } => MergeError::Malformed { context },
            RestoreError::TreeIdMismatch { snapshot, current } => MergeError::TreeIdMismatch { snapshot, current },
        }
    }
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MergeError::UnsupportedVersion { version, supported } => write!(
                f,
                "snapshot version {version} is not supported (only version {supported})"
            ),
            MergeError::Malformed { context } => write!(f, "malformed snapshot: {context}"),
            MergeError::TreeIdMismatch { snapshot, current } => write!(
                f,
                "snapshot belongs to tree {snapshot} but this tree is {current}; refusing to merge"
            ),
        }
    }
}

impl std::error::Error for MergeError {}

/// Outcome of a successful monotonic [`crate::tree::Root::merge`].
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct MergeReport {
    /// Amount of tasks newly inserted into the tree.
    pub tasks_inserted: usize,
    /// Amount of tasks already present that were folded together.
    pub tasks_updated: usize,
    /// Amount of messages appended to the ring buffer.
    pub messages_appended: usize,
    /// Amount of messages skipped because they were already present (idempotent replay).
    pub messages_skipped: usize,
}
