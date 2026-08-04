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

//! Millisecond sleep backing `io.questdb.std.Os.sleep`.
//!
//! Java's `Thread.sleep` allocates a `jdk.internal.event.ThreadSleepEvent` on
//! every call since JDK 25 -- `Thread.beforeSleep` constructs the event before
//! testing `isEnabled()`, so the 40 bytes are paid even with JFR off wherever
//! the JIT does not eliminate the dead event (the Graal JIT, C1 and the
//! interpreter do not; a hot C2-compiled call site does). QuestDB's worker
//! back-off ladder calls it often enough for that to dominate the young
//! generation on GraalVM builds, hence this replacement.
//!
//! `Thread.interrupt` never signals a thread parked in a plain downcall, so a
//! Java-interrupted caller sleeps the full duration; `OsTest.testSleepEnds`
//! asserts that. Genuine POSIX signals cannot cut the sleep short either:
//! `std::thread::sleep` re-issues the underlying syscall with the remaining
//! duration after EINTR.
//!
//! Java binds this symbol through the Foreign Function & Memory API WITHOUT
//! `Linker.Option.critical`. The plain binding transitions the calling thread to
//! `_thread_in_native`, which is what lets the VM reach a safepoint while a
//! worker sleeps. Marking a blocking function critical instead stalls every
//! safepoint for the full sleep duration.

use std::thread;
use std::time::Duration;

#[no_mangle]
pub extern "C" fn qdb_sleep_millis(millis: i64) {
    if millis > 0 {
        thread::sleep(Duration::from_millis(millis as u64));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Instant;

    #[test]
    fn sleeps_at_least_the_requested_duration() {
        let start = Instant::now();
        qdb_sleep_millis(50);
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(50));
        assert!(elapsed < Duration::from_secs(5));
    }

    #[test]
    fn non_positive_returns_immediately() {
        // Run on a helper thread: dropping the guard turns `-1 as u64` into a
        // 584-million-year sleep, which must fail this test, not hang the harness.
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            qdb_sleep_millis(0);
            qdb_sleep_millis(-1);
            qdb_sleep_millis(i64::MIN);
            let _ = tx.send(());
        });
        assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok());
    }
}
