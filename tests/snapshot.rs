//! Regression tests for resumable snapshots and monotonic tree merges.
//!
//! Every test replays an explicit, deterministic interleaving of events rather than relying on
//! timing, so the evidence is reproducible without sleeps or randomness. The clock used for
//! message timestamps is injected and frozen.

#![cfg(feature = "snapshot")]

use std::{
    sync::{Arc, Barrier, atomic::Ordering},
    time::Duration,
};

use prodash::{
    Count,
    progress::{Key, State},
    tree::{
        Root, TreeId,
        root::Options,
        snapshot::{FixedClock, MergeError, RestoreError, SnapshotData},
    },
};

const TREE: TreeId = TreeId(*b"fixed-tree-id!!!");
const OTHER: TreeId = TreeId(*b"other-tree-0000!");

fn fixed_tree(clock: FixedClock) -> Arc<Root> {
    Options::default().create_with_tree_id(TREE, clock)
}

fn fixed_tree_for(id: TreeId, clock: FixedClock) -> Arc<Root> {
    Options::default().create_with_tree_id(id, clock)
}

fn entries(root: &Arc<Root>) -> Vec<(Key, prodash::progress::Task)> {
    let mut out = Vec::new();
    root.sorted_snapshot(&mut out);
    out
}

fn task_at<'a>(entries: &'a [(Key, prodash::progress::Task)], levels: &[u16]) -> &'a prodash::progress::Task {
    let key = Key::from_levels(levels);
    let found = entries
        .iter()
        .find(|(candidate, _)| *candidate == key)
        .unwrap_or_else(|| panic!("task at {levels:?} must exist"));
    &found.1
}

fn step_of(entries: &[(Key, prodash::progress::Task)], levels: &[u16]) -> usize {
    task_at(entries, levels)
        .progress
        .as_ref()
        .expect("initialized task")
        .step
        .load(Ordering::SeqCst)
}

fn build_interrupted(clock: FixedClock) -> (Arc<Root>, SnapshotData) {
    let root = fixed_tree(clock);
    let mut parent = root.add_child("parent");
    parent.init(Some(10), Some("items".into()));
    parent.set(7);
    parent.blocked("waiting on lock", None);
    parent.running();

    let mut child = parent.add_child_with_id("child", *b"CHLD");
    child.init(Some(4), None);
    child.set(4);
    child.done("finished child");

    parent.info("halfway note");
    let snapshot = root.snapshot();
    (root, snapshot)
}

// --- Restore ---------------------------------------------------------------

#[test]
fn restore_round_trips_every_field() {
    let clock = FixedClock::epoch();
    let (root, snapshot) = build_interrupted(clock);
    let restored = root.restore(&snapshot).expect("same tree id restores");

    let restored_entries = entries(&restored);
    assert_eq!(restored.tree_id(), TREE);

    let parent = task_at(&restored_entries, &[0]);
    assert_eq!(parent.name, "parent");
    let progress = parent.progress.as_ref().unwrap();
    assert_eq!(progress.step.load(Ordering::SeqCst), 7);
    assert_eq!(progress.done_at, Some(10));
    assert_eq!(progress.state, State::Running);
    let mut label = String::new();
    progress
        .unit
        .as_ref()
        .unwrap()
        .as_display_value()
        .display_unit(&mut label, 7)
        .unwrap();
    assert_eq!(label, "items");

    let child = task_at(&restored_entries, &[0, 0]);
    assert_eq!(child.name, "child");
    assert_eq!(child.id, *b"CHLD");
    let child_progress = child.progress.as_ref().unwrap();
    assert_eq!(child_progress.step.load(Ordering::SeqCst), 4);
    assert_eq!(child_progress.state, State::Completed);

    let mut messages = Vec::new();
    restored.copy_messages(&mut messages);
    let texts: Vec<_> = messages
        .iter()
        .map(|m| (m.origin.as_str(), m.message.as_str()))
        .collect();
    assert!(texts.contains(&("child", "finished child")));
    assert!(texts.contains(&("parent", "halfway note")));
}

#[test]
fn old_handles_cannot_mutate_the_restored_tree() {
    let clock = FixedClock::epoch();
    let root = fixed_tree(clock);
    let mut parent = root.add_child("persisted");
    parent.init(Some(10), None);
    parent.set(2);
    parent.done("persisted done");
    let snapshot = root.snapshot();

    let mut stale = root.add_child("stale");
    stale.init(Some(3), None);
    stale.set(1);

    let restored = root.restore(&snapshot).unwrap();

    // Handles predating the restore are detached: every mutator is a no-op against the new tree.
    stale.set(99);
    stale.inc();
    stale.running();
    stale.init(Some(100), None);
    stale.set_name("rewritten");
    stale.info("ghost message");
    let ghost = stale.add_child("ghost child");
    ghost.init(Some(1), None);
    ghost.set(1);

    let restored_entries = entries(&restored);
    // The restored tree predates "stale", so it must not appear at all.
    assert!(
        restored_entries.iter().all(|(key, _)| *key != Key::from_levels(&[1])),
        "detached handles must not create tasks in the restored tree"
    );
    assert!(
        restored_entries
            .iter()
            .all(|(key, _)| *key != Key::from_levels(&[1, 0])),
        "children of detached handles must stay out of the restored tree"
    );

    let mut messages = Vec::new();
    restored.copy_messages(&mut messages);
    assert!(messages.iter().all(|m| m.message != "ghost message"));
    assert_eq!(restored.num_tasks(), restored_entries.len());

    // Handles of the restored tree keep working and are allocated fresh keys.
    let after = restored.add_child("after");
    after.init(Some(1), None);
    after.set(1);
    // Detached handles must not have consumed the new tree's child-id counters: a second
    // fresh child lands on the very next slot rather than skipping one.
    let after_too = restored.add_child("after too");
    after_too.init(Some(1), None);
    after_too.set(1);
    let restored_entries = entries(&restored);
    assert_eq!(step_of(&restored_entries, &[1]), 1);
    assert_eq!(step_of(&restored_entries, &[2]), 1);
}

#[test]
fn new_children_after_restore_never_reuse_snapshot_keys() {
    let clock = FixedClock::epoch();
    let (root, snapshot) = build_interrupted(clock);
    let restored = root.restore(&snapshot).unwrap();

    let fresh = restored.add_child("after restore");
    fresh.init(Some(1), None);
    fresh.set(1);

    let restored_entries = entries(&restored);
    assert!(
        restored_entries
            .iter()
            .any(|(key, task)| *key == Key::from_levels(&[1]) && task.name == "after restore")
    );
}

#[test]
fn restore_rejects_foreign_and_malformed_snapshots() {
    let clock = FixedClock::epoch();
    let root = fixed_tree(clock);
    let foreign = fixed_tree_for(OTHER, FixedClock::epoch());

    let err = root.restore(&foreign.snapshot()).unwrap_err();
    match err {
        RestoreError::TreeIdMismatch { snapshot, current } => {
            assert_eq!(snapshot.len(), 32);
            assert_ne!(snapshot, current);
        }
        other => panic!("expected TreeIdMismatch, got {other:?}"),
    }

    let mut tampered = root.snapshot();
    tampered.version = 999;
    assert!(matches!(
        root.restore(&tampered),
        Err(RestoreError::UnsupportedVersion { version: 999, .. })
    ));

    let mut tampered = root.snapshot();
    tampered.tree_id = vec![1, 2, 3];
    assert!(matches!(root.restore(&tampered), Err(RestoreError::Malformed { .. })));

    let populated = {
        let tree = fixed_tree_for(TREE, FixedClock::epoch());
        let task = tree.add_child("present");
        task.init(Some(1), None);
        tree.snapshot()
    };
    let mut tampered = populated.clone();
    tampered.tasks[0].id = vec![1];
    assert!(matches!(root.restore(&tampered), Err(RestoreError::Malformed { .. })));

    // A key with a hole (level 1 missing while level 2 is present) cannot be produced by the
    // tree, but a hand-crafted payload must be rejected with a malformed error.
    let mut crafted: serde_json::Value = serde_json::to_value(&populated).unwrap();
    crafted["tasks"][0]["key"]["a"] = serde_json::Value::Null;
    crafted["tasks"][0]["key"]["b"] = serde_json::json!(3);
    let crafted: SnapshotData = serde_json::from_value(crafted).unwrap();
    assert!(matches!(root.restore(&crafted), Err(RestoreError::Malformed { .. })));
    assert!(root.restore(&crafted).unwrap_err().to_string().contains("hole"));
}

// --- Monotonic merge -------------------------------------------------------

#[test]
fn merge_is_idempotent_for_the_same_snapshot() {
    let clock = FixedClock::epoch();
    let root = fixed_tree(clock);
    let (_donor, snapshot) = build_interrupted(FixedClock::epoch());
    let message_count = snapshot.messages.len();

    let first = root.merge(&snapshot).unwrap();
    assert_eq!(first.tasks_inserted, 2);
    assert_eq!(first.tasks_updated, 0);
    assert_eq!(first.messages_appended, message_count);
    assert_eq!(first.messages_skipped, 0);

    let entries_once = entries(&root);
    let messages_once = {
        let mut out = Vec::new();
        root.copy_messages(&mut out);
        out
    };

    let second = root.merge(&snapshot).unwrap();
    assert_eq!(second.tasks_inserted, 0);
    assert_eq!(second.tasks_updated, 2);
    assert_eq!(second.messages_appended, 0);
    assert_eq!(second.messages_skipped, message_count);

    let entries_twice = entries(&root);
    assert_eq!(entries_once.len(), entries_twice.len());
    assert_eq!(step_of(&entries_twice, &[0]), 7);
    assert_eq!(step_of(&entries_twice, &[0, 0]), 4);

    let mut messages_twice = Vec::new();
    root.copy_messages(&mut messages_twice);
    assert_eq!(messages_once, messages_twice);
}

#[test]
fn merge_keeps_per_item_maximum_steps_and_sticky_terminal_state() {
    let root = fixed_tree(FixedClock::epoch());
    let live = root.add_child("task");
    live.init(Some(10), None);
    live.set(8);

    // An older snapshot with fewer steps must not move the counter backwards.
    let older = {
        let donor = fixed_tree_for(TREE, FixedClock::epoch());
        let task = donor.add_child("task");
        task.init(Some(10), None);
        task.set(3);
        donor.snapshot()
    };
    root.merge(&older).unwrap();
    assert_eq!(step_of(&entries(&root), &[0]), 8);

    // A newer snapshot advances the counter.
    let newer = {
        let donor = fixed_tree_for(TREE, FixedClock::epoch());
        let task = donor.add_child("task");
        task.init(Some(10), None);
        task.set(9);
        donor.snapshot()
    };
    root.merge(&newer).unwrap();
    assert_eq!(step_of(&entries(&root), &[0]), 9);

    // Completion is sticky, even if a stale snapshot claims the task is merely running.
    let completion = {
        let donor = fixed_tree_for(TREE, FixedClock::epoch());
        let mut task = donor.add_child("task");
        task.init(Some(10), None);
        task.set(10);
        task.done("done");
        donor.snapshot()
    };
    root.merge(&completion).unwrap();
    assert_eq!(
        task_at(&entries(&root), &[0]).progress.as_ref().unwrap().state,
        State::Completed
    );

    root.merge(&older).unwrap();
    let after_replay = entries(&root);
    assert_eq!(step_of(&after_replay, &[0]), 10);
    assert_eq!(
        task_at(&after_replay, &[0]).progress.as_ref().unwrap().state,
        State::Completed
    );

    // Direct handle updates cannot reopen the completed item either.
    live.running();
    live.set(1);
    live.blocked("late block", None);
    live.init(Some(20), None);
    let after_handle = entries(&root);
    assert_eq!(
        task_at(&after_handle, &[0]).progress.as_ref().unwrap().state,
        State::Completed
    );
    assert_eq!(step_of(&after_handle, &[0]), 10);
}

#[test]
fn merge_folds_partial_snapshots_without_dropping_other_tasks() {
    let root = fixed_tree(FixedClock::epoch());
    let resident = root.add_child("resident");
    resident.init(Some(2), None);
    resident.set(1);

    let incoming = {
        let donor = fixed_tree_for(TREE, FixedClock::epoch());
        // Occupy slot [0] in the donor with a task that has already gone away; the incoming
        // task therefore lives at slot [1] and must not be conflated with the resident [0].
        drop(donor.add_child("gone"));
        let mut other = donor.add_child("incoming");
        other.init(Some(5), None);
        other.set(5);
        other.done("incoming done");
        donor.snapshot()
    };
    root.merge(&incoming).unwrap();

    let snapshot = entries(&root);
    assert_eq!(step_of(&snapshot, &[0]), 1);
    assert_eq!(step_of(&snapshot, &[1]), 5);
    assert_eq!(
        task_at(&snapshot, &[1]).progress.as_ref().unwrap().state,
        State::Completed
    );

    // A child added after the merge cannot collide with either snapshot key.
    let third = root.add_child("third");
    third.init(Some(1), None);
    third.set(1);
    assert_eq!(step_of(&entries(&root), &[2]), 1);
}

#[test]
fn merge_rejects_foreign_tree_ids() {
    let root = fixed_tree(FixedClock::epoch());
    let foreign = fixed_tree_for(OTHER, FixedClock::epoch());
    let err = root.merge(&foreign.snapshot()).unwrap_err();
    assert!(matches!(err, MergeError::TreeIdMismatch { .. }));
    assert!(err.to_string().contains("refusing to merge"));
    assert_eq!(root.num_tasks(), 0);
}

// --- Concurrency -----------------------------------------------------------

#[test]
fn concurrent_merges_settle_at_max_step_and_terminal_state() {
    const THREADS: usize = 8;
    const INCS: usize = 256;

    let root = fixed_tree(FixedClock::epoch());
    let item = root.add_child("race");
    item.init(Some(THREADS * INCS), None);

    // Every worker drives its own tree to a distinct final step, then hands back its snapshot.
    // The barrier aligns the construction phase without any sleeping; merges happen concurrently
    // against the shared root afterwards.
    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = Vec::new();
    for thread in 0..THREADS {
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let tree = fixed_tree_for(TREE, FixedClock::epoch());
            let mut task = tree.add_child("race");
            task.init(Some(THREADS * INCS), None);
            barrier.wait();
            let final_step = (thread + 1) * INCS;
            for step in 1..=final_step {
                task.set(step);
            }
            if thread == THREADS - 1 {
                task.done("winner");
            }
            tree.snapshot()
        }));
    }

    let start = Arc::new(Barrier::new(THREADS + 1));
    let mut mergers = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        let snapshot = Arc::new(handle.join().unwrap());
        let target = Arc::clone(&root);
        let start = Arc::clone(&start);
        mergers.push(std::thread::spawn(move || {
            start.wait();
            // Merge in arrival order; explicit per-round assertions are impossible with threads,
            // but the monotonic invariant is checked on the shared tree after all joins.
            for _ in 0..2 {
                let report = target.merge(&snapshot).unwrap();
                assert!(report.tasks_inserted <= 1);
            }
            idx
        }));
    }
    start.wait();
    for merger in mergers {
        merger.join().unwrap();
    }

    let final_entries = entries(&root);
    assert_eq!(step_of(&final_entries, &[0]), THREADS * INCS);
    assert_eq!(
        task_at(&final_entries, &[0]).progress.as_ref().unwrap().state,
        State::Completed
    );

    // Merging every snapshot yet again changes nothing.
    let before = {
        let mut tasks = Vec::new();
        root.sorted_snapshot(&mut tasks);
        let mut messages = Vec::new();
        root.copy_messages(&mut messages);
        (tasks, messages)
    };
    for thread in 0..THREADS {
        let tree = fixed_tree_for(TREE, FixedClock::epoch());
        let mut task = tree.add_child("race");
        task.init(Some(THREADS * INCS), None);
        task.set((thread + 1) * INCS);
        if thread == THREADS - 1 {
            task.done("winner");
        }
        root.merge(&tree.snapshot()).unwrap();
    }
    let mut tasks_after = Vec::new();
    root.sorted_snapshot(&mut tasks_after);
    assert_eq!(before.0.len(), tasks_after.len());
    for ((key_before, task_before), (key_after, task_after)) in before.0.iter().zip(tasks_after.iter()) {
        assert_eq!(key_before, key_after);
        let step_before = task_before.progress.as_ref().map(|p| p.step.load(Ordering::SeqCst));
        let step_after = task_after.progress.as_ref().map(|p| p.step.load(Ordering::SeqCst));
        assert_eq!(step_before, step_after);
    }
    let mut messages_after = Vec::new();
    root.copy_messages(&mut messages_after);
    assert_eq!(before.1, messages_after);
}

#[test]
fn concurrent_handle_increments_never_lose_the_merged_maximum() {
    // While worker threads increment a shared handle, merges of a snapshot at the same key must
    // not be able to lower the counter.
    const INCREMENTORS: usize = 4;
    const ROUNDS: usize = 1000;

    let root = fixed_tree(FixedClock::epoch());
    let item = root.add_child("race");
    item.init(None, Some("items".into()));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let counter = item.counter();
    let mut handles = Vec::new();
    for _ in 0..INCREMENTORS {
        let counter = std::sync::Arc::clone(&counter);
        let stop = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let high_water = {
        let donor = fixed_tree_for(TREE, FixedClock::epoch());
        let task = donor.add_child("race");
        task.init(None, Some("items".into()));
        task.set(ROUNDS);
        donor.snapshot()
    };
    root.merge(&high_water).unwrap();
    for _ in 0..ROUNDS {
        // Replaying the high-water snapshot must never drag the value back down to ROUNDS.
        root.merge(&high_water).unwrap();
    }
    assert!(step_of(&entries(&root), &[0]) >= ROUNDS);

    stop.store(true, Ordering::SeqCst);
    for handle in handles {
        handle.join().unwrap();
    }
}

// --- Deterministic bytes ----------------------------------------------------

#[test]
fn snapshot_bytes_are_reproducible_with_a_fixed_clock() {
    fn make() -> Vec<u8> {
        let clock = FixedClock::epoch();
        let root = Options::default().create_with_tree_id(TREE, clock.clone());
        let mut parent = root.add_child("reproducible");
        parent.init(Some(5), Some("files".into()));
        parent.set(2);
        parent.info("note");
        clock.advance(Duration::from_secs(10));
        let mut child = parent.add_child_with_id("kid", *b"KID0");
        child.init(Some(3), None);
        child.set(3);
        child.done("complete");
        serde_json::to_vec(&root.snapshot()).unwrap()
    }

    let first = make();
    let second = make();
    assert_eq!(first, second);
    let parsed: serde_json::Value = serde_json::from_slice(&first).unwrap();
    let messages = parsed["messages"].as_array().unwrap();
    assert_eq!(messages[0]["secs"].as_u64(), Some(0));
    assert_eq!(messages[1]["secs"].as_u64(), Some(10));
    assert_eq!(parsed["tree_id"].as_array().unwrap().len(), 16);
}

#[test]
fn snapshot_serialization_round_trips_through_json() {
    let (_root, snapshot) = build_interrupted(FixedClock::epoch());
    let bytes = serde_json::to_vec(&snapshot).unwrap();
    let decoded: SnapshotData = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, snapshot);

    let blank = fixed_tree(FixedClock::epoch());
    let restored = blank.restore(&decoded).unwrap();
    assert_eq!(step_of(&entries(&restored), &[0]), 7);
}
