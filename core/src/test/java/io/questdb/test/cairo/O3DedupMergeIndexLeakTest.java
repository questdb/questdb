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

package io.questdb.test.cairo;

import io.questdb.std.str.Utf8s;
import io.questdb.std.str.LPSZ;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A DEDUP commit with additional key columns builds its timestamp merge index in
 * a temporary buffer, then reallocs it down to the deduplicated row count:
 *
 * <pre>
 *     tempIndexAddr = Unsafe.malloc(tempIndexSize, NATIVE_O3);
 *     dedupRows = getDedupRows(..., tempIndexAddr);      // opens the key column
 *     timestampMergeIndexAddr = Unsafe.realloc(tempIndexAddr, ...);
 * </pre>
 * <p>
 * {@code getDedupRows} opens the partition's dedup key column, so it can throw.
 * Until the realloc, the buffer is reachable only through the local - the
 * enclosing handler frees {@code timestampMergeIndexAddr}, which is still 0 - so
 * the throw leaks {@code mergeRowCount * 16} bytes of NATIVE_O3 per failed
 * commit, permanently, for the life of the process.
 * <p>
 * Found by fault-injection fuzzing, where it surfaced as a ~7% flake attributed
 * to whichever test happened to be running. It is not specific to covering
 * indexes; any DEDUP table with a non-timestamp key hits it whenever a merge
 * commit fails while reading that key column - a full disk, a hit fd limit, or
 * an I/O error.
 */
public class O3DedupMergeIndexLeakTest extends AbstractCairoTest {

    /**
     * The leak is detected by {@code assertMemoryLeak}'s NATIVE_O3 tag check, so
     * the test body only has to make the failing merge happen; there is no
     * explicit leak assertion to get wrong.
     */
    @Test
    public void testFailedDedupKeyColumnOpenDoesNotLeakMergeIndex() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicBoolean fired = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // The dedup key column read inside the merge. Only this one:
                // failing every sym.d open would break the seed as well.
                // The dedup key column of the PARTITION, read inside the merge.
                // Scoped to the partition directory so a WAL segment's sym.d -
                // which is read while building the commit, before the merge even
                // starts - cannot consume the one-shot fault instead.
                if (armed.get() && name != null
                        && Utf8s.containsAscii(name, "2024-01-02")
                        && Utf8s.endsWithAscii(name, "sym.d")) {
                    armed.set(false);
                    fired.set(true);
                    return -1;
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL, value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            // Day 1 and day 3 first, so day 2 is a MID partition. A mid-partition
            // O3 write always goes through the merge; a write into the LAST
            // partition can be absorbed by the lag path and never merge at all,
            // which is why the first version of this test reached nothing.
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 'S0', 1.0)");
            execute("INSERT INTO t VALUES ('2024-01-03T00:00:00.000000Z', 'S0', 3.0)");
            execute("INSERT INTO t SELECT dateadd('u', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),"
                    + " 'S' || (x % 4), x::DOUBLE FROM long_sequence(5000)");
            drainWalQueue();

            armed.set(true);
            // Lands INSIDE the seed's range, so this is a merge, and the dedup
            // key (sym) forces the additional-keys path that reads the partition
            // column. On a WAL table the failure suspends the table rather than
            // throwing from INSERT.
            execute("INSERT INTO t SELECT dateadd('u', (2000 + x)::INT, '2024-01-02T00:00:00Z'::TIMESTAMP),"
                    + " 'S' || (x % 4), (100000 + x)::DOUBLE FROM long_sequence(500)");
            drainWalQueue();
            Assert.assertTrue("the injected fault never fired, so nothing was tested", fired.get());

            // Drop the distressed writer so every other allocation it owns is
            // released; whatever the tag check then reports is the leak itself.
            engine.releaseAllWriters();
        });
    }
}
