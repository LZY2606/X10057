use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::messages::MessageRingBuffer;

#[cfg(feature = "snapshot")]
pub mod snapshot;

/// A process-wide counter so trees created within one process cannot share an identity
/// by accident. Snapshots carry the identity of the tree they were taken from and
/// refuse to merge into trees with a different identity.
#[cfg(feature = "snapshot")]
static NEXT_TREE_SEQ: AtomicU64 = AtomicU64::new(1);

/// A 16 byte identifier uniquely separating progress trees from each other.
///
/// The first 8 bytes are a marker, the remaining 8 bytes are a process-unique sequence
/// number. Trees restored from a snapshot inherit the snapshot's identity.
#[cfg(feature = "snapshot")]
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TreeId(pub [u8; 16]);

#[cfg(feature = "snapshot")]
impl TreeId {
    /// Generate an identity that is unique within this process.
    pub(crate) fn unique() -> Self {
        let seq = NEXT_TREE_SEQ.fetch_add(1, Ordering::Relaxed);
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(b"ephemer!");
        id[8..].copy_from_slice(&seq.to_be_bytes());
        TreeId(id)
    }
}

/// A clock used to timestamp messages. It is injectable so that trees with a
/// [`snapshot::FixedClock`] produce byte-reproducible snapshots.
#[cfg(feature = "snapshot")]
pub(crate) trait Clock: std::fmt::Debug {
    fn now(&self) -> std::time::SystemTime;
}

#[cfg(feature = "snapshot")]
#[derive(Debug)]
pub(crate) struct SystemClock;

#[cfg(feature = "snapshot")]
impl Clock for SystemClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
}

/// State shared between [`Root`] and every [`Item`] handle created from it.
///
/// `generation` is bumped when a snapshot is restored, which invalidates every handle
/// still pointing at the previous instance of the shared state.
pub(crate) struct Shared {
    pub(crate) tree: Arc<HashMap<crate::progress::Key, crate::progress::Task>>,
    pub(crate) messages: Arc<parking_lot::Mutex<MessageRingBuffer>>,
    pub(crate) generation: Arc<AtomicU64>,
    /// Next child id to hand out per parent key for parents other than the root. The table is
    /// populated when a snapshot is restored or merged so that newly added children cannot reuse
    /// keys that belonged to the snapshot.
    pub(crate) next_child:
        Arc<parking_lot::Mutex<std::collections::HashMap<crate::progress::Key, crate::progress::key::Id>>>,
    /// Next child id for the root item.
    pub(crate) root_next_child: Arc<parking_lot::Mutex<crate::progress::key::Id>>,
    #[cfg(feature = "snapshot")]
    pub(crate) clock: Arc<dyn Clock + Send + Sync>,
    #[cfg(feature = "snapshot")]
    pub(crate) tree_id: TreeId,
}

impl std::fmt::Debug for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shared")
            .field("tasks", &self.tree.len())
            .finish_non_exhaustive()
    }
}

impl Shared {
    /// Create fresh shared state sized for `capacity` items and `message_buffer_capacity` messages.
    #[cfg(not(feature = "snapshot"))]
    pub(crate) fn with_capacity(capacity: usize, message_buffer_capacity: usize) -> Self {
        Shared {
            tree: Arc::new(HashMap::with_capacity(capacity)),
            messages: Arc::new(parking_lot::Mutex::new(MessageRingBuffer::with_capacity(
                message_buffer_capacity,
            ))),
            generation: Arc::new(AtomicU64::new(0)),
            next_child: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            root_next_child: Arc::new(parking_lot::Mutex::new(0)),
        }
    }

    /// Create fresh shared state sized for `capacity` items and `message_buffer_capacity` messages.
    #[cfg(feature = "snapshot")]
    pub(crate) fn with_capacity(capacity: usize, message_buffer_capacity: usize) -> Self {
        Shared {
            tree: Arc::new(HashMap::with_capacity(capacity)),
            messages: Arc::new(parking_lot::Mutex::new(MessageRingBuffer::with_capacity(
                message_buffer_capacity,
            ))),
            generation: Arc::new(AtomicU64::new(0)),
            next_child: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            root_next_child: Arc::new(parking_lot::Mutex::new(0)),
            clock: Arc::new(SystemClock),
            tree_id: TreeId::unique(),
        }
    }

    /// Invalidate every existing handle of this tree. Used when restoring a snapshot.
    #[cfg(feature = "snapshot")]
    pub(crate) fn supersede(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Create an independent copy of the shared storage, as used by
    /// [`crate::tree::Root::deep_clone`]. The copy keeps the tree identity but starts a new
    /// generation line.
    pub(crate) fn duplicate(&self) -> Shared {
        Shared {
            tree: Arc::new(self.tree.as_ref().clone()),
            messages: Arc::new(parking_lot::Mutex::new(self.messages.lock().clone())),
            generation: Arc::new(AtomicU64::new(0)),
            next_child: Arc::new(parking_lot::Mutex::new(self.next_child.lock().clone())),
            root_next_child: Arc::new(parking_lot::Mutex::new(*self.root_next_child.lock())),
            #[cfg(feature = "snapshot")]
            clock: Arc::clone(&self.clock),
            #[cfg(feature = "snapshot")]
            tree_id: self.tree_id,
        }
    }

    /// Timestamp for a new message. Uses the injected clock with the `snapshot` feature so
    /// tests can freeze time for byte-reproducible snapshots.
    #[cfg(feature = "snapshot")]
    pub(crate) fn now(&self) -> std::time::SystemTime {
        self.clock.now()
    }

    /// Timestamp for a new message, using the wall clock without the `snapshot` feature.
    #[cfg(not(feature = "snapshot"))]
    pub(crate) fn now(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
}

/// The top-level of the progress tree.
#[derive(Debug)]
pub struct Root {
    pub(crate) inner: parking_lot::Mutex<Item>,
    pub(crate) shared: Arc<Shared>,
}

/// A `Tree` represents an element of the progress tree.
///
/// It can be used to set progress and send messages.
/// ```rust
/// let tree = prodash::tree::Root::new();
/// let mut progress = tree.add_child("task 1");
///
/// progress.init(Some(10), Some("elements".into()));
/// for p in 0..10 {
///     progress.set(p);
/// }
/// progress.done("great success");
/// let mut  sub_progress = progress.add_child_with_id("sub-task 1", *b"TSK2");
/// sub_progress.init(None, None);
/// sub_progress.set(5);
/// sub_progress.fail("couldn't finish");
/// ```
pub struct Item {
    pub(crate) key: crate::progress::Key,
    pub(crate) value: crate::progress::StepShared,
    pub(crate) highest_child_id: crate::progress::key::Id,
    pub(crate) shared: Arc<Shared>,
    pub(crate) generation: u64,
}

impl Item {
    pub(crate) fn tree(&self) -> &HashMap<crate::progress::Key, crate::progress::Task> {
        &self.shared.tree
    }

    /// The shared state is only usable while the tree this handle belongs to was not
    /// superseded by a snapshot restore.
    pub(crate) fn is_attached(&self) -> bool {
        self.generation == self.shared.generation.load(Ordering::SeqCst)
    }

    /// Return true if this handle's task has reached a terminal state. Step mutations on
    /// terminal items are ignored so progress can never move backwards.
    pub(crate) fn is_terminal(&self) -> bool {
        #[cfg(feature = "progress-tree-hp-hashmap")]
        {
            self.shared
                .tree
                .get(&self.key)
                .is_some_and(|entry| entry.value().progress.as_ref().is_some_and(|p| p.state.is_terminal()))
        }
        #[cfg(not(feature = "progress-tree-hp-hashmap"))]
        {
            self.shared
                .tree
                .get(&self.key, |task| {
                    task.progress.as_ref().is_some_and(|p| p.state.is_terminal())
                })
                .unwrap_or(false)
        }
    }
}

#[cfg(feature = "dashmap")]
type HashMap<K, V> = dashmap::DashMap<K, V>;

#[cfg(not(feature = "dashmap"))]
type HashMap<K, V> = sync::HashMap<K, V>;

#[cfg(not(feature = "dashmap"))]
pub(crate) mod sync {
    pub struct HashMap<K, V>(parking_lot::Mutex<std::collections::HashMap<K, V>>);

    impl<K, V> HashMap<K, V>
    where
        K: Eq + std::hash::Hash,
    {
        pub fn with_capacity(cap: usize) -> Self {
            HashMap(parking_lot::Mutex::new(std::collections::HashMap::with_capacity(cap)))
        }
        pub fn extend_to(&self, out: &mut Vec<(K, V)>)
        where
            K: Clone,
            V: Clone,
        {
            let lock = self.0.lock();
            out.extend(lock.iter().map(|(k, v)| (k.clone(), v.clone())))
        }
        pub fn remove(&self, key: &K) -> Option<V> {
            self.0.lock().remove(key)
        }
        pub fn get<T>(&self, key: &K, cb: impl FnOnce(&V) -> T) -> Option<T> {
            self.0.lock().get(key).map(cb)
        }
        pub fn get_mut<T>(&self, key: &K, cb: impl FnOnce(&mut V) -> T) -> Option<T> {
            self.0.lock().get_mut(key).map(cb)
        }
        pub fn insert(&self, key: K, value: V) {
            self.0.lock().insert(key, value);
        }
        pub fn len(&self) -> usize {
            self.0.lock().len()
        }
        #[cfg(feature = "snapshot")]
        pub fn contains_key(&self, key: &K) -> bool {
            self.0.lock().contains_key(key)
        }
        pub fn clone(&self) -> Self
        where
            K: Clone,
            V: Clone,
        {
            HashMap(parking_lot::Mutex::new(self.0.lock().clone()))
        }
    }
}

mod item;
///
pub mod root;

#[cfg(test)]
mod tests;
