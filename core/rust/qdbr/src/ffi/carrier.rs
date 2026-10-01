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

//! Carrier identity primitive used by Java-side `CarrierLocal`.
//!
//! Stores a small integer per OS thread in a const-initialized
//! `thread_local!`. The exported symbols are bound from Java via
//! the Foreign Function & Memory API (`Linker.Option.critical`) so that
//! C2 cannot fold the lookup with a hoisted `Thread.currentThread()`
//! across `Continuation.yield`/`run` boundaries.

use std::cell::Cell;

use qdb_core::memory_tracker::{
    detach_thread_local, detach_thread_local_if, publish_thread_local, MemoryScope, MemoryTracker,
};

thread_local! {
    static CARRIER_ID: Cell<i32> = const { Cell::new(-1) };
}

#[no_mangle]
pub extern "C" fn qdb_carrier_bind(id: i32) {
    CARRIER_ID.with(|c| c.set(id));
}

#[no_mangle]
pub extern "C" fn qdb_carrier_current() -> i32 {
    CARRIER_ID.with(|c| c.get())
}

/// Credits `bytes` to the tracker through the thread-local Resource Group delta.
#[no_mangle]
pub extern "C" fn qdb_memory_tracker_credit(tracker_address: i64, bytes: i64) {
    tracker(tracker_address).credit(bytes as usize);
}

/// A zero `tracker_address` detaches whatever binding the thread holds;
/// otherwise only a binding to that tracker and generation is detached.
#[no_mangle]
pub extern "C" fn qdb_memory_tracker_detach(tracker_address: i64, generation: i64) {
    if tracker_address == 0 {
        detach_thread_local();
    } else {
        detach_thread_local_if(tracker_address as usize, generation as usize);
    }
}

/// Charges `bytes` to the tracker. Returns 0 on success, otherwise the code of
/// the breached scope: 1 query, 2 process, 3 group, 4 tracker configuration.
#[no_mangle]
pub extern "C" fn qdb_memory_tracker_try_charge(tracker_address: i64, bytes: i64) -> i32 {
    match tracker(tracker_address).try_charge(bytes as usize) {
        Ok(()) => 0,
        Err(breach) => match breach.scope {
            MemoryScope::Query => 1,
            MemoryScope::Process => 2,
            MemoryScope::Group => 3,
            MemoryScope::Configuration | MemoryScope::Global => 4,
        },
    }
}

#[no_mangle]
pub extern "C" fn qdb_memory_tracker_publish() {
    publish_thread_local();
}

/// Tracker blocks stay mapped until engine shutdown; Java only passes the
/// address of a live block.
fn tracker<'a>(tracker_address: i64) -> &'a MemoryTracker {
    unsafe { &*(tracker_address as usize as *const MemoryTracker) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_and_read_round_trip() {
        assert_eq!(qdb_carrier_current(), -1);
        qdb_carrier_bind(7);
        assert_eq!(qdb_carrier_current(), 7);
        qdb_carrier_bind(0);
        assert_eq!(qdb_carrier_current(), 0);
    }

    #[test]
    fn distinct_threads_have_distinct_slots() {
        qdb_carrier_bind(1);
        let other = std::thread::spawn(|| {
            assert_eq!(qdb_carrier_current(), -1);
            qdb_carrier_bind(2);
            qdb_carrier_current()
        })
        .join()
        .unwrap();
        assert_eq!(other, 2);
        assert_eq!(qdb_carrier_current(), 1);
    }
}
