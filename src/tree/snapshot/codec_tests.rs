//! Unit tests for snapshot encoding/decoding round-trips and malformed input rejection.

use super::*;
use crate::progress::State;

#[test]
fn key_codec_round_trips_all_levels() {
    for levels in [
        vec![],
        vec![0],
        vec![1, 7],
        vec![3, 4, 5, 6],
        vec![9, 8, 7, 6, 5],
        vec![1, 2, 3, 4, 5, 6],
    ] {
        let key = Key::from_levels(&levels);
        let encoded = encode_key(key);
        let decoded = decode_key(&encoded).expect("dense path decodes");
        assert_eq!(key, decoded);
        assert_eq!(key.levels(), levels);
    }
}

#[test]
fn decode_rejects_key_paths_with_holes() {
    let hole = KeySnapshot {
        a: None,
        b: Some(1),
        c: None,
        d: None,
        e: None,
        f: None,
    };
    assert!(decode_key(&hole).is_none());
}

#[test]
fn state_codec_round_trips() {
    let eta = Some(UNIX_EPOCH + Duration::new(42, 7));
    for state in [
        State::Running,
        State::Blocked("waiting".into(), eta),
        State::Halted("paused".into(), None),
        State::Completed,
    ] {
        let encoded = encode_state(&state);
        let decoded = decode_state(&encoded).unwrap();
        assert_eq!(state, decoded);
    }
}

#[test]
fn static_unit_codec_round_trips() {
    let unit = crate::unit::label_and_mode("files", crate::unit::display::Mode::with_percentage().and_throughput());
    let encoded = encode_unit(&Some(unit));
    let decoded = decode_unit(&encoded).expect("static units decode");
    let encoded_again = encode_unit(&Some(decoded));
    assert_eq!(encoded, encoded_again);
}

#[test]
fn dynamic_units_are_omitted_from_snapshots() {
    // Dynamic units carry closures that cannot cross process boundaries; snapshots must
    // degrade gracefully rather than fail serialization.
    struct CountUnit;
    impl crate::unit::DisplayValue for CountUnit {
        fn dyn_hash(&self, state: &mut dyn std::hash::Hasher) {
            state.write(b"count");
        }
        fn display_unit(&self, w: &mut dyn std::fmt::Write, _value: usize) -> std::fmt::Result {
            w.write_str("counts")
        }
    }
    let dynamic = crate::unit::dynamic(CountUnit);
    assert!(encode_unit(&Some(dynamic)).is_none());
}

#[test]
fn merge_value_takes_max_step_without_reordering_terminal_state() {
    let live = std::sync::Arc::new(AtomicUsize::new(5));
    let mut current = Value {
        step: std::sync::Arc::clone(&live),
        done_at: Some(10),
        unit: None,
        state: State::Running,
    };

    merge_value(
        &mut current,
        &ValueSnapshot {
            step: 3,
            done_at: Some(10),
            state: StateSnapshot::Running,
            unit: None,
        },
    );
    assert_eq!(current.step.load(Ordering::SeqCst), 5);

    merge_value(
        &mut current,
        &ValueSnapshot {
            step: 9,
            done_at: Some(10),
            state: StateSnapshot::Completed,
            unit: None,
        },
    );
    assert_eq!(current.step.load(Ordering::SeqCst), 9);
    assert_eq!(current.state, State::Completed);

    // A stale running snapshot cannot reopen the completed item.
    merge_value(
        &mut current,
        &ValueSnapshot {
            step: 4,
            done_at: Some(10),
            state: StateSnapshot::Running,
            unit: None,
        },
    );
    assert_eq!(current.state, State::Completed);
    assert_eq!(current.step.load(Ordering::SeqCst), 9);
}

#[test]
fn message_merge_is_deduplicating_by_full_content() {
    let buffer = parking_lot::Mutex::new(MessageRingBuffer::with_capacity(4));
    let mut snapshot = Snapshot {
        version: SNAPSHOT_VERSION,
        tree_id: vec![0u8; 16],
        message_buffer_capacity: 4,
        tasks: Vec::new(),
        messages: Vec::new(),
    };
    for index in 0..3 {
        snapshot.messages.push(MessageSnapshot {
            secs: index,
            nanos: 0,
            level: 0,
            origin: "task".into(),
            message: format!("m{index}"),
        });
    }

    let mut report = MergeReport::default();
    merge_messages(&buffer, &snapshot, &mut report);
    assert_eq!(report.messages_appended, 3);
    assert_eq!(report.messages_skipped, 0);

    let mut report = MergeReport::default();
    merge_messages(&buffer, &snapshot, &mut report);
    assert_eq!(report.messages_appended, 0);
    assert_eq!(report.messages_skipped, 3);
}
