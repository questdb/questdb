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
//! `thread_local!`. The two `extern "C"` symbols are bound from Java via
//! the Foreign Function & Memory API (`Linker.Option.critical`) so that
//! C2 cannot fold the lookup with a hoisted `Thread.currentThread()`
//! across `Continuation.yield`/`run` boundaries.

use std::cell::Cell;

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
