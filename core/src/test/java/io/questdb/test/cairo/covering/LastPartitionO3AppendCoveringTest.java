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

import io.questdb.PropertyKey;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The LAST partition's covering index when the WAL fast-lag gate does NOT apply.
 * <p>
 * Fast-lag handles a pure append into the last partition, but only when the
 * commit qualifies. DEDUP disqualifies both halves of the gate - the block-apply
 * gate tests {@code isCommitPlainInsert()} and the single-txn gate tests
 * {@code !isCommitDedupMode()} - so a deduped table's appends fall back to the O3
 * route while remaining pure appends (the timestamps here are unique and
 * ascending, so nothing is actually deduplicated). Concurrent clients hit the
 * same fallback through multi-segment blocks, without any dedup configuration.
 * <p>
 * On that route the index was maintained during the O3 copy phase, which had two
 * consequences: every commit then paid a full {@code rebuildSidecars()} of the
 * partition, AND the covered fragment written during the copy was speculative -
 * the indexed column's task can run before the covered column's own append lands,
 * so the reseal was what repaired it. Deferring the covering index to the seal,
 * which runs after every column task has joined, removes both: the fragment is
 * built once, from final column data.
 */
public class LastPartitionO3AppendCoveringTest extends AbstractCairoTest {

    private static final int COMMITS = 12;
    private static final int ROWS_PER_COMMIT = 500;
    private static final int SEED_ROWS = 5000;

    @Before
    public void enableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);
        PostingIndexWriter.COVERING_SEAL_APPEND_PENDING_DECLINE_COUNT.set(0);
    }

    @After
    public void disableCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = false;
    }

    /**
     * Correctness must not depend on the new path: the same stream with the path
     * switched off has to produce the same reads.
     */
    @Test
    public void testDedupLastPartitionCorrectOnResealPath() throws Exception {
        PostingIndexWriter.COVERING_SEAL_APPEND_DISABLED = true;
        assertMemoryLeak(() -> {
            createTables(true);
            appendAndAssert();
        });
    }

    @Test
    public void testDedupLastPartitionTakesAppendPath() throws Exception {
        assertMemoryLeak(() -> {
            createTables(true);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
            PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);

            appendAndAssert();

            Assert.assertEquals("a pure append into the last partition must not full-reseal",
                    0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            Assert.assertTrue("the covered append path must fire (appends="
                            + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
        });
    }

    /**
     * With real duplicates the commit is no longer a pure append (rows are
     * replaced), so it must fall back to the rebuild and still read correctly.
     */
    @Test
    public void testDedupWithRealDuplicatesStillCorrect() throws Exception {
        assertMemoryLeak(() -> {
            createTables(true);
            for (int c = 0; c < COMMITS; c++) {
                // half the batch repeats timestamps already written
                final long base = SEED_ROWS + (long) c * ROWS_PER_COMMIT;
                insertBoth(base, ROWS_PER_COMMIT);
                insertBoth(base - ROWS_PER_COMMIT / 2, ROWS_PER_COMMIT / 2);
                drainWalQueue();
            }
            assertNotSuspended();
            assertCoveredMatchesControl();
        });
    }

    /**
     * The O3PartitionJob half of the deferral (the merge-collapses-to-append
     * degradation that yields OPEN_LAST_PARTITION_FOR_APPEND) is a DIFFERENT
     * production path from the writer's inline append branch, and the other tests
     * here all take the inline one. Route A requires the batch to start strictly
     * after the partition max under dedup, so a batch whose first row repeats the
     * max timestamp is refused by it and goes through the merge instead - where
     * dedup drops the duplicate and the merge collapses to an append.
     */
    @Test
    public void testDedupMergeCollapseTakesAppendPath() throws Exception {
        assertMemoryLeak(() -> {
            createTables(true);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
            PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.set(0);

            for (int c = 0; c < COMMITS; c++) {
                final long base = SEED_ROWS + (long) c * ROWS_PER_COMMIT;
                // first row repeats the current max timestamp (a real duplicate),
                // the rest extend past it
                insertBoth(base - 1, ROWS_PER_COMMIT + 1);
                drainWalQueue();
            }

            assertNotSuspended();
            assertCoveredMatchesControl();
            Assert.assertTrue("the merge-collapse route must reach the covered append path (appends="
                            + PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() + ')',
                    PostingIndexWriter.COVERING_SEAL_APPEND_COUNT.get() > 0);
        });
    }

    /**
     * Deferred compaction has to actually fire on this route too: at the shipped
     * threshold COMMITS would never reach it, so this would otherwise prove only
     * that nothing happened. Also checks the superseded files are reclaimed once
     * the purge job runs.
     */
    @Test
    public void testLastPartitionAppendCompactsAndReclaims() throws Exception {
        setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 3);
        assertMemoryLeak(() -> {
            createTables(true);
            appendAndAssert();

            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                for (int i = 0; i < 64 && purgeJob.run(); i++) {
                    // drain
                }
            }
            assertCoveredMatchesControl();
            // The guard that protects the live indexer's unflushed entries must not
            // be silently firing: if it is, this route is falling back to the
            // rebuild and the append path is not being exercised at all.
            Assert.assertEquals("covered append declined for pending entries",
                    0, PostingIndexWriter.COVERING_SEAL_APPEND_PENDING_DECLINE_COUNT.get());
        });
    }

    private void appendAndAssert() throws Exception {
        for (int c = 0; c < COMMITS; c++) {
            insertBoth(SEED_ROWS + (long) c * ROWS_PER_COMMIT, ROWS_PER_COMMIT);
            drainWalQueue();
        }
        assertNotSuspended();
        assertCoveredMatchesControl();

        // and again after a reopen, so the published fragments are proven durable
        engine.releaseInactive();
        assertCoveredMatchesControl();
    }

    private void assertCoveredMatchesControl() throws Exception {
        for (int s = 0; s < 8; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, value FROM ctl WHERE sym = '" + sym + "' ORDER BY ts",
                    "SELECT ts, sym, value FROM t WHERE sym = '" + sym + "' ORDER BY ts"
            );
        }
        assertSqlCursors(
                "SELECT sym, count(*), sum(value), min(value), max(value) FROM ctl ORDER BY sym",
                "SELECT sym, count(*), sum(value), min(value), max(value) FROM t ORDER BY sym"
        );
        // index scan vs the column itself, which localises a missing tail
        assertSqlCursors(
                "SELECT /*+ no_index */ sym, count(*) FROM t ORDER BY sym",
                "SELECT sym, count(*) FROM t ORDER BY sym"
        );
    }

    private void assertNotSuspended() {
        Assert.assertFalse("t suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
        Assert.assertFalse("ctl suspended", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("ctl")));
    }

    private void createTables(boolean dedup) throws Exception {
        final String dedupClause = dedup ? " DEDUP UPSERT KEYS(ts, sym)" : "";
        execute("CREATE TABLE t (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL" + dedupClause);
        execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY WAL" + dedupClause);
        insertBoth(0, SEED_ROWS);
        drainWalQueue();
    }

    private void insertBoth(long base, int rows) throws Exception {
        final String tail = " SELECT dateadd('u', (" + base + " + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),"
                + " 'S' || ((" + base + " + x) % 8), (" + base + " + x)::DOUBLE"
                + " FROM long_sequence(" + rows + ")";
        execute("INSERT INTO t" + tail);
        execute("INSERT INTO ctl" + tail);
    }
}
