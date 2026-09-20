//! Injectable clocks used to make snapshots reproducible.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

/// A controllable clock for tests and deterministic replays.
///
/// Multiple clones share the same current time, advancing the time through one clone is visible
/// through all of them. Snapshots taken from a tree with a fixed clock contain exactly the
/// injected timestamps, which makes the serialized bytes reproducible.
#[derive(Clone, Debug)]
pub struct FixedClock {
    now: Arc<Mutex<SystemTime>>,
}

impl FixedClock {
    /// Create a clock frozen at the Unix epoch.
    pub fn epoch() -> Self {
        Self::at(SystemTime::UNIX_EPOCH)
    }

    /// Create a clock frozen at the given time.
    pub fn at(time: SystemTime) -> Self {
        FixedClock {
            now: Arc::new(Mutex::new(time)),
        }
    }

    /// Advance the shared clock by `duration`.
    pub fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().expect("fixed clock lock not poisoned");
        *now += duration;
    }

    /// Set the shared clock to an absolute time.
    pub fn set(&self, time: SystemTime) {
        *self.now.lock().expect("fixed clock lock not poisoned") = time;
    }

    pub(crate) fn get(&self) -> SystemTime {
        *self.now.lock().expect("fixed clock lock not poisoned")
    }
}
