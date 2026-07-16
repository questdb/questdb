/*******************************************************************************
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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.FuzzRunner;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Randomized adaptive crash-fuzz (SP-D increment D2). Builds on {@link AbstractAdaptiveCrashSweepTest}'s
 * exhaustive per-op sweep driver with a randomized/fuzzed workload harness (see
 * {@link io.questdb.test.cairo.fuzz.FuzzRunner} / {@link io.questdb.test.fuzz.FuzzTransaction}) — this
 * task (1 of 7) establishes only the base class and the two primitives every later task in this file
 * relies on: a canonical committed-state {@link #fingerprint} and a {@link #lastMatch} membership check
 * over a recorded fingerprint history.
 */
public class RandomizedAdaptiveCrashFuzzTest extends AbstractAdaptiveCrashSweepTest {

    private final FuzzRunner fuzzer = new FuzzRunner();

    // Canonical committed-state fingerprint: full ordered dump to a String.
    private String fingerprint(String table) throws SqlException {
        StringSink fp = new StringSink();
        printSql("select * from " + table + " order by ts", fp);
        return fp.toString();
    }

    // Largest index whose recorded fingerprint equals `state`; -1 if none (conservative on coincident txns).
    private static int lastMatch(ObjList<String> history, CharSequence state) {
        for (int i = history.size() - 1; i >= 0; i--) {
            if (TestUtils.equals(history.getQuick(i), state)) {
                return i;
            }
        }
        return -1;
    }

    @Test
    public void testFingerprintMembershipPrimitive() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table fp (ts timestamp, v long) timestamp(ts) partition by day wal");
            ObjList<String> history = new ObjList<>();
            history.add(fingerprint("fp"));                          // fp[0] empty
            for (int i = 0; i < 3; i++) {
                execute("insert into fp values (" + (i * 1_000_000L) + ", " + i + ")");
                drainWalQueue();
                history.add(fingerprint("fp"));                      // fp[1..3]
            }
            // the current (full) state must match the last snapshot
            Assert.assertEquals(3, lastMatch(history, fingerprint("fp")));
            // an intermediate snapshot is found at its own index
            Assert.assertEquals(1, lastMatch(history, history.getQuick(1)));
            // a fabricated state matches nothing
            Assert.assertEquals(-1, lastMatch(history, "not a real dump"));
        });
    }
}
