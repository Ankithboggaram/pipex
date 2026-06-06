#![no_main]

use libfuzzer_sys::fuzz_target;
use pipex::metrics::StageMetrics;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }

    let metrics = StageMetrics::new("fuzz");

    // Each 9-byte chunk: 8 bytes duration_ns + 1 byte failed flag.
    for chunk in data.chunks(9) {
        if chunk.len() == 9 {
            let duration_ns = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let failed = chunk[8] & 1 == 1;
            metrics.record(duration_ns, failed);
        }
    }

    // Snapshot must never panic regardless of input values.
    let snapshot = metrics.snapshot();

    // Invariants that must always hold.
    assert!(snapshot.p50_ns <= snapshot.p95_ns);
    assert!(snapshot.p95_ns <= snapshot.p99_ns);
    assert!(snapshot.p99_ns <= snapshot.p999_ns);
    if snapshot.count > 0 {
        assert!(snapshot.min_ns <= snapshot.max_ns);
    }
});
