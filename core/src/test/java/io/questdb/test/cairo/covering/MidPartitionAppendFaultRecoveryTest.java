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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Deterministic counterpart to {@code MidPartitionAppendFailureFuzzTest}: ONE
 * commit appends into an existing mid partition, ONE filesystem op fails during
 * the covered publish, and the table then has to recover.
 * <p>
 * This is the scenario that makes the covered append path structurally
 * different from the reseal it replaces. The reseal rewrote the partition's
 * sidecars wholesale every commit, so a failed attempt was repaired by the next
 * one. The append path is now the ONLY writer of those postings, and a failure
 * between {@code index()} and the generation publish leaves the chain head's
 * MAX_VALUE advanced over postings that were never written - so a seal that
 * trusted {@code getMaxValue()} would skip re-indexing them and the rows would
 * be missing from every index scan, permanently and silently.
 * <p>
 * The assertion is deliberately index-vs-base on the SAME table rather than a
 * comparison against a control table: it localises the damage (an index scan
 * that finds fewer rows than the column does) instead of merely reporting that
 * two tables differ.
 */
public class MidPartitionAppendFaultRecoveryTest extends AbstractCairoTest {

    private static final long DAY = 24L * 60 * 60 * 1_000_000L;
    private static final int ROWS = 200;
    private static final long T0 = 1_700_000_000_000_000L / DAY * DAY;
    private final AtomicBoolean armed = new AtomicBoolean();
    // fd of the file the read fault is aimed at, latched at open time
    private final java.util.concurrent.atomic.AtomicLong targetFd = new java.util.concurrent.atomic.AtomicLong(-1);

    @Before
    public void enableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
    }

    @After
    public void disableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
    }

    @Test
    public void testFaultOnCoveredSidecarOpenRecovers() throws Exception {
        runOne(".pc", false);
    }

    @Test
    public void testFaultOnPostingValuesOpenRecovers() throws Exception {
        runOne(".pv", false);
    }

    @Test
    public void testFaultOnSymbolColumnOpenRecovers() throws Exception {
        runOne("sym.d", false);
    }

    /**
     * Faults the READ rather than the open, which is the only thing that
     * exercises SymbolColumnIndexer#index's short-read guard on this path.
     */
    @Test
    public void testFaultOnSymbolColumnReadRecovers() throws Exception {
        runOne("sym.d", true);
    }

    private void assertIndexAgreesWithColumn() throws Exception {
        // Every symbol: what the index scan finds must equal what a scan of the
        // column itself finds. A tail the seal failed to index shows up here as
        // a smaller count, which comparing covered values alone would miss.
        assertSqlCursors(
                "SELECT /*+ no_index */ sym, count(*), sum(value) FROM t ORDER BY sym",
                "SELECT sym, count(*), sum(value) FROM t ORDER BY sym"
        );
        // ... and the covered values themselves must match the base column.
        assertSqlCursors(
                "SELECT /*+ no_covering */ ts, sym, value FROM t WHERE sym = 'S1' ORDER BY ts",
                "SELECT ts, sym, value FROM t WHERE sym = 'S1' ORDER BY ts"
        );
    }

    private void insertBatch(long v0) throws Exception {
        execute("INSERT INTO t SELECT (" + (T0 + DAY + 1 + v0 * 1000) + " + x * 1000)::TIMESTAMP,"
                + " 'S' || ((" + v0 + " + x) % 4), (" + v0 + " + x)::DOUBLE FROM long_sequence(" + ROWS + ")");
    }

    private void runOne(String target, boolean faultRead) throws Exception {
        ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                // Descriptor numbers are recycled; a stale latch would fault an
                // unrelated file.
                targetFd.compareAndSet(fd, -1);
                return super.close(fd);
            }

            @Override
            public long openRO(LPSZ name) {
                if (faultRead) {
                    if (matches(name) && targetFd.get() < 0) {
                        long fd = super.openRO(name);
                        if (fd >= 0) {
                            targetFd.set(fd); // fault its first read instead
                        }
                        return fd;
                    }
                } else if (shouldFail(name)) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (!faultRead && shouldFail(name)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public long read(long fd, long buf, long len, long offset) {
                if (faultRead && armed.get() && fd == targetFd.get() && fd >= 0) {
                    armed.set(false); // one shot
                    targetFd.set(-1);
                    return 0; // short read: the tail would be silently unindexed
                }
                return super.read(fd, buf, len, offset);
            }

            private boolean matches(LPSZ name) {
                // Never fault WAL segments: the transaction log must stay
                // durable, so the commit is guaranteed to be replayed.
                return armed.get() && name != null
                        && !Utf8s.containsAscii(name, "wal")
                        && Utf8s.containsAscii(name, target);
            }

            private boolean shouldFail(LPSZ name) {
                if (matches(name)) {
                    armed.set(false); // one shot
                    return true;
                }
                return false;
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later day anchors the table max, so the writes below append into
            // a partition that is not the last one.
            execute("INSERT INTO t SELECT (" + (T0 + 3 * DAY) + ")::TIMESTAMP, 'S0', 1.0 FROM long_sequence(1)");
            insertBatch(0);
            drainWalQueue();

            // The commit that fails mid-publish.
            armed.set(true);
            insertBatch(ROWS);
            try {
                drainWalQueue();
            } catch (Throwable ignore) {
                // surfaces as a failed apply; recovery is what this test asserts
            }
            // The fault must actually have fired, otherwise this is a plain
            // append test wearing a fault test's name.
            Assert.assertFalse("fault never fired", armed.get());
            armed.set(false);
            targetFd.set(-1);

            final TableToken tt = engine.verifyTableName("t");
            for (int i = 0; i < 50 && engine.getTableSequencerAPI().isSuspended(tt); i++) {
                engine.releaseInactive();
                try {
                    execute("ALTER TABLE t RESUME WAL");
                } catch (Throwable ignore) {
                    // retry
                }
                try {
                    drainWalQueue();
                } catch (Throwable ignore) {
                    // retry
                }
            }
            engine.releaseInactive();
            drainWalQueue();

            Assert.assertFalse("table must recover, not stay suspended",
                    engine.getTableSequencerAPI().isSuspended(tt));
            assertIndexAgreesWithColumn();

            // WAL durability means the failed commit must eventually land in
            // full - 1 anchor row + two batches.
            printSql("SELECT count() FROM t");
            Assert.assertEquals("count\n" + (1 + 2 * ROWS) + "\n", sink.toString());

            // And the partition must still be usable for further appends.
            PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
            insertBatch(2L * ROWS);
            drainWalQueue();
            assertIndexAgreesWithColumn();
            // ... via the path under test, not by silently falling back to the
            // reseal for the rest of the table's life.
            Assert.assertTrue("the append path must resume after recovery",
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
        });
    }
}
