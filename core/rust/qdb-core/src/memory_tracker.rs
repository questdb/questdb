/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! Per-workload native-memory counter, shared with Java and across both JNI
//! libraries by *data layout* rather than the (unstable) Rust ABI. `qdbr` and
//! `qdb-ent` each statically link their own copy of this struct, yet they
//! operate on the same Java-allocated 16-byte `{used, limit}` block by pointer,
//! so the `#[repr(C)]` layout is the contract -- pinned by the `const _`
//! assertion below and by `Unsafe.MEMORY_TRACKER_*_OFFSET` on the Java side.
//! Only the pointer crosses between libraries; each calls its own copy's
//! methods, so the Rust ABI is never relied on.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Ordering for the limit-gate loads (the `used` / `limit` reads that decide a
/// breach). `SeqCst` matches the OSS allocator's `RSS_ORDERING`.
const GATE_ORDERING: Ordering = Ordering::SeqCst;

/// Ordering for the `used` read-modify-writes (the `fetch_add` on charge, the
/// `fetch_sub` + corrective `fetch_add` on credit). `AcqRel` matches the OSS
/// allocator's `COUNTER_ORDERING`.
const RMW_ORDERING: Ordering = Ordering::AcqRel;

/// A rejected charge: the configured `limit` and the `used` value observed at
/// the gate, for the caller to surface in an out-of-memory error.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Breach {
    pub limit: usize,
    pub used: usize,
}

/// Per-workload memory counter. Wraps the 16-byte `{used, limit}` block backing
/// a `MemoryTracker` Java object; the layout must match
/// `Unsafe.MEMORY_TRACKER_*_OFFSET` and is identical in both JNI libraries that
/// overlay it.
#[repr(C)]
pub struct MemoryTracker {
    /// Bytes charged against this tracker. All parallel workers hammer this one
    /// atomic, so it is the scaling ceiling -- fine for an opt-in, default-off
    /// limit; stripe it if per-query limits ever go default-on.
    used: AtomicUsize,

    /// Configured byte limit. `0` means unlimited.
    limit: AtomicUsize,
}

// Pin the ABI shared with Java (`Unsafe.MEMORY_TRACKER_*_OFFSET`) and across the
// two JNI libraries' separately-linked copies: a drift here is silent
// cross-`.so` memory corruption.
const _: () = assert!(
    size_of::<MemoryTracker>() == 16
        && std::mem::offset_of!(MemoryTracker, used) == 0
        && std::mem::offset_of!(MemoryTracker, limit) == 8
);

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self {
            used: AtomicUsize::new(0),
            limit: AtomicUsize::new(0),
        }
    }

    /// Bytes currently charged against this tracker.
    pub fn used(&self) -> usize {
        self.used.load(GATE_ORDERING)
    }

    /// Configured byte limit; `0` means unlimited.
    pub fn limit(&self) -> usize {
        self.limit.load(GATE_ORDERING)
    }

    /// Set the configured byte limit. Production drives the limit from Java (a
    /// direct write to the shared block at `init()`); Rust callers set it only
    /// in tests.
    pub fn set_limit(&self, limit: usize) {
        self.limit.store(limit, GATE_ORDERING);
    }

    /// Charge `bytes` after checking the limit. On success `used += bytes` and
    /// `Ok` is returned; on breach the counter is untouched and the gate values
    /// come back in [`Breach`]. The check-then-add is deliberately not atomic
    /// (it mirrors the OSS allocator): concurrent charges can overshoot the
    /// limit, which is acceptable for a runaway guard rather than exact
    /// bookkeeping. A `0`-byte charge is a no-op that always succeeds.
    pub fn try_charge(&self, bytes: usize) -> Result<(), Breach> {
        if bytes == 0 {
            return Ok(());
        }
        let limit = self.limit.load(GATE_ORDERING);
        if limit > 0 {
            let used = self.used.load(GATE_ORDERING);
            if used.saturating_add(bytes) > limit {
                return Err(Breach { limit, used });
            }
        }
        self.used.fetch_add(bytes, RMW_ORDERING);
        Ok(())
    }

    /// Charge `bytes` without the limit check, for the allocator's own add path
    /// where `check_alloc_limit` already gated the request.
    pub fn charge_unchecked(&self, bytes: usize) {
        self.used.fetch_add(bytes, RMW_ORDERING);
    }

    /// Credit `bytes` previously charged, clamping at zero on the impossible
    /// underflow and returning the previous `used`. The common path is a single
    /// `fetch_sub` (one `lock xadd` rather than a CAS loop); only an underflow
    /// takes the corrective `fetch_add`. That leaves a tiny window where a
    /// concurrent reader could observe the wrapped value, acceptable because the
    /// underflow cannot occur under correct charge/credit pairing -- which the
    /// returned `prev` lets callers `debug_assert!`.
    pub fn credit(&self, bytes: usize) -> usize {
        let prev = self.used.fetch_sub(bytes, RMW_ORDERING);
        if prev < bytes {
            self.used.fetch_add(bytes - prev, RMW_ORDERING);
        }
        prev
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_charge_honors_limit_and_adds_on_success() {
        let t = MemoryTracker::new();
        t.set_limit(1024);
        assert_eq!(t.used(), 0);
        assert_eq!(t.limit(), 1024);

        // Under the limit: charged.
        assert!(t.try_charge(512).is_ok());
        assert_eq!(t.used(), 512);

        // Exactly at the boundary: still charged.
        assert!(t.try_charge(512).is_ok());
        assert_eq!(t.used(), 1024);

        // One byte over: rejected, counter untouched, gate values reported.
        assert_eq!(
            t.try_charge(1),
            Err(Breach {
                limit: 1024,
                used: 1024
            })
        );
        assert_eq!(t.used(), 1024);
    }

    #[test]
    fn credit_decrements_and_clamps_at_zero() {
        let t = MemoryTracker::new();
        t.charge_unchecked(1024);

        // Normal credit returns the previous value and subtracts.
        assert_eq!(t.credit(512), 1024);
        assert_eq!(t.used(), 512);

        // An oversized credit (the impossible underflow a release build must
        // survive) clamps at zero rather than wrapping to ~usize::MAX.
        assert_eq!(t.credit(1000), 512);
        assert_eq!(t.used(), 0);
    }

    #[test]
    fn unlimited_tracker_never_breaches() {
        let t = MemoryTracker::new();
        // limit == 0 means unlimited: a large charge still succeeds.
        assert!(t.try_charge(1 << 40).is_ok());
        assert_eq!(t.used(), 1 << 40);
    }

    #[test]
    fn zero_byte_charge_and_credit_are_noops() {
        let t = MemoryTracker::new();
        t.set_limit(8);
        // A zero-byte charge succeeds without touching the counter, even when
        // already at the limit.
        t.charge_unchecked(8);
        assert!(t.try_charge(0).is_ok());
        assert_eq!(t.used(), 8);
        // A zero-byte credit returns the current value and leaves it put.
        assert_eq!(t.credit(0), 8);
        assert_eq!(t.used(), 8);
    }

    #[test]
    fn charge_credit_through_a_raw_pointer_overlay() {
        // The cross-`.so` usage: one library charges through a `*const` view of
        // another's block. A local tracker stands in for the Java-allocated one.
        let t = MemoryTracker::new();
        t.set_limit(1024);
        let addr = &t as *const MemoryTracker as usize;

        let view = unsafe { &*(addr as *const MemoryTracker) };
        view.try_charge(512).expect("under the limit");
        assert_eq!(
            t.used(),
            512,
            "charge through the overlay reaches the block"
        );

        view.credit(512);
        assert_eq!(t.used(), 0, "credit through the overlay reaches the block");
    }
}
