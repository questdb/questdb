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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * A commit that only APPENDS rows to an existing, non-last partition (partitions
 * A and C exist, the rows land at the end of B) takes the append-only O3 path
 * for the data columns - {@code OPEN_MID_PARTITION_FOR_APPEND}, no partition
 * copy. The covering POSTING index must follow suit: publish an incremental
 * covered fragment for the appended rows and defer compaction, instead of
 * resealing the whole partition's .pv/.pc on every commit
 * ({@code COVERING_FULL_RESEAL_COUNT} must stay 0).
 * <p>
 * On unmodified code {@code TableWriter#sealPostingIndexForPartition} calls
 * {@code rebuildSidecars()} unconditionally on the covering branch, so every
 * commit full-reseals and cost grows with the size of the partition.
 */
public class MidPartitionAppendCoveringSealTest extends AbstractCairoTest {

    private static final int COMMITS = 12;
    private static final int ROWS_PER_COMMIT = 200;
    private static final int SEED_ROWS = 4000;

    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.set(0);
    }

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
        // Static kill-switch: a test that sets it and throws before restoring it
        // would silently disable the append path for every test that runs after.
        PostingIndexWriter.COVERING_MIDPART_APPEND_DISABLED = false;
    }

    /**
     * Correctness guard for the O3 paths the append fast path must NOT change:
     * rows that land BEFORE the partition's max timestamp force a merge, which
     * still rebuilds the index from the column data.
     */
    @Test
    public void testMidPartitionOutOfOrderStillCorrect() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            seedMidPartition();

            // Interleave: every batch writes rows BEHIND the current partition
            // max, so the commit is a genuine O3 merge into the mid partition.
            for (int c = 0; c < COMMITS; c++) {
                final long base = 10L + c;
                insert("blk", "SELECT dateadd('u', (" + base + " + x * 7)::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                        " 'S' || (x % 8), (1000000 + " + base + " + x)::DOUBLE FROM long_sequence(50)");
                insert("ctl", "SELECT dateadd('u', (" + base + " + x * 7)::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                        " 'S' || (x % 8), (1000000 + " + base + " + x)::DOUBLE FROM long_sequence(50)");
                drainWalQueue();
            }

            assertNotSuspended();
            assertCoveredMatchesControl();
        });
    }

    @Test
    public void testMidPartitionPureAppendDoesNotFullReseal() throws Exception {
        // Cross the deferred-compaction threshold several times during the run,
        // so this covers the append -> compaction-seal -> append transition and
        // not just a run short enough that compaction never fires.
        setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 3);
        assertMemoryLeak(() -> {
            createTables();
            seedMidPartition();

            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
            PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.set(0);
            appendToMidPartition();

            final long fullReseals = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();
            final long appends = PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.get();
            Assert.assertEquals(
                    "a pure append into an existing mid partition must not full-reseal the covered sidecar",
                    0, fullReseals);
            // Assert the path FIRED rather than infer it from the absence of
            // reseals, which is also true when nothing happened at all.
            Assert.assertEquals(
                    "every commit must take the covered append path",
                    COMMITS, appends);

            assertNotSuspended();
            assertCoveredMatchesControl();
        });
    }

    @Test
    public void testMidPartitionPureAppendSurvivesReopen() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            seedMidPartition();
            appendToMidPartition();

            // Drop every cached writer/reader so the next read re-opens the
            // posting index from disk: proves the incrementally published
            // chain entry and its covered fragments are durable, not just
            // consistent in the writer's memory.
            engine.releaseInactive();

            assertNotSuspended();
            assertCoveredMatchesControl();
        });
    }

    /**
     * The append path publishes into the LIVE .pv/.pc instead of rotating them,
     * so the superseded-file purge that the reseal path drives never runs for
     * these commits. That is only safe if nothing accumulates: assert the
     * partition's posting-index file set stays bounded no matter how many
     * commits land, and that the reseal path's own bookkeeping (deferred
     * compaction at the gen threshold) keeps it that way.
     */
    @Test
    public void testMidPartitionPureAppendDoesNotAccumulateSealFiles() throws Exception {
        // The default gen threshold is 16, so COMMITS appends would never reach
        // the deferred compaction and this test would prove only that nothing
        // happened. Lower it so the run crosses it several times: the point is
        // that compaction reclaims what the appends published, not that appends
        // publish little.
        setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 3);
        assertMemoryLeak(() -> {
            createTables();
            seedMidPartition();
            final int afterSeed = countPostingFiles("blk", "2024-01-02");

            PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.set(0);
            appendToMidPartition();
            // Discriminate: the post-purge steady state below would also hold on
            // the reseal path (rotate every commit, purge reclaims), so pin that
            // these commits actually took the append path.
            Assert.assertEquals("every commit must take the covered append path",
                    COMMITS, PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.get());
            final int beforePurge = countPostingFiles("blk", "2024-01-02");

            // Deferred compaction DOES rotate .pv/.pc when it fires, superseding
            // the previous versions; reclaiming those is the purge job's job, and
            // it does not run in this harness. Drive it, then assert the steady
            // state - that is the claim worth making: appends publish in place,
            // periodic compaction rotates, and nothing accumulates.
            try (PostingSealPurgeJob purgeJob = new PostingSealPurgeJob(engine)) {
                for (int i = 0; i < 64 && purgeJob.run(); i++) {
                    // drain
                }
            }
            final int afterPurge = countPostingFiles("blk", "2024-01-02");

            Assert.assertTrue(
                    "posting index files must not grow per commit [afterSeed=" + afterSeed
                            + ", after" + COMMITS + "commits=" + beforePurge
                            + ", afterPurge=" + afterPurge + ']',
                    afterPurge <= afterSeed + 4);
            assertCoveredMatchesControl();
        });
    }

    /**
     * The append path extends the chain head in place, and
     * {@link PostingIndexWriter#getHeadTxnAtSeal()} is what tells a head left by
     * an attempt that never committed from one a commit produced. Entry-level
     * txnAtSeal is slot[0]'s and extendHead never rewrites slot[0], so reading
     * it would report the txn of the partition's FIRST generation forever -
     * making that guard silently inert. Pin the semantic: after appends, the
     * value must track the latest commit, not the seed.
     */
    @Test
    public void testHeadTxnAtSealTracksTheLatestAppend() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            seedMidPartition();
            final long seedTxn = headTxnAtSeal();

            PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.set(0);
            appendToMidPartition();
            final long afterAppendTxn = headTxnAtSeal();

            // Pin the path FIRST. The reseal rotates the head every commit, so
            // txnAtSeal advances there too and the assertion below is green even
            // with the append path switched off - i.e. it would pass without ever
            // exercising the in-place extend whose semantics it exists to pin.
            Assert.assertEquals("every commit must take the mid-partition append path",
                    COMMITS, PostingIndexWriter.COVERING_MIDPART_APPEND_COUNT.get());
            Assert.assertTrue("head txnAtSeal must advance with in-place appends [seed=" + seedTxn
                            + ", afterAppends=" + afterAppendTxn + ']',
                    afterAppendTxn > seedTxn);
        });
    }

    private long headTxnAtSeal() {
        TableToken tt = engine.verifyTableName("blk");
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tt).concat("2024-01-02");
            try (PostingIndexWriter w = new PostingIndexWriter(configuration)) {
                w.of(path, "sym", TableUtils.COLUMN_NAME_TXN_NONE);
                return w.getHeadTxnAtSeal();
            }
        }
    }

    private void appendToMidPartition() throws Exception {
        for (int c = 0; c < COMMITS; c++) {
            final long base = SEED_ROWS + (long) c * ROWS_PER_COMMIT;
            final String tail = "SELECT dateadd('u', (" + base + " + x)::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                    " 'S' || ((" + base + " + x) % 8), (" + base + " + x)::DOUBLE" +
                    " FROM long_sequence(" + ROWS_PER_COMMIT + ")";
            insert("blk", tail);
            insert("ctl", tail);
            drainWalQueue();
        }
    }

    /**
     * Compares every covered read against the non-covering control table, which
     * holds identical data behind a plain POSTING index. `value` is globally
     * unique, so ORDER BY value is deterministic.
     */
    private void assertCoveredMatchesControl() throws Exception {
        // Pin that these comparisons actually go THROUGH the covering index. A
        // plan change that stopped using it would make every assertion below
        // compare two non-covering scans and quietly lose the point of the test.
        assertQuery("SELECT ts, sym, value FROM blk WHERE sym = 'S1' ORDER BY value")
                .noLeakCheck()
                // "with:" is what makes it a COVERED read - the plan lists the
                // covered columns (the designated timestamp is covered too).
                .assertsPlanContaining("CoveringIndex on: sym with: ts, value");
        for (int s = 0; s < 8; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, value FROM ctl WHERE sym = '" + sym + "' ORDER BY value",
                    "SELECT ts, sym, value FROM blk WHERE sym = '" + sym + "' ORDER BY value"
            );
        }
        assertSqlCursors(
                "SELECT sym, count(value), sum(value), min(value), max(value) FROM ctl ORDER BY sym",
                "SELECT sym, count(value), sum(value), min(value), max(value) FROM blk ORDER BY sym"
        );
        assertSqlCursors("SELECT count() FROM ctl", "SELECT count() FROM blk");
    }

    private int countPostingFiles(String table, String day) {
        TableToken tt = engine.verifyTableName(table);
        File tableDir = new File(configuration.getDbRoot().toString(), tt.getDirName());
        File[] dirs = tableDir.listFiles((d, n) -> n.startsWith(day));
        int count = 0;
        Assert.assertNotNull("partition dir for " + day + " must exist", dirs);
        for (File dir : dirs) {
            File[] files = dir.listFiles((d, n) -> n.contains(".pv") || n.contains(".pc") || n.contains(".pk"));
            if (files != null) {
                count += files.length;
            }
        }
        return count;
    }

    private void assertNotSuspended() throws Exception {
        assertQuery("SELECT name, suspended FROM wal_tables() WHERE name IN ('blk','ctl') ORDER BY name")
                .noLeakCheck()
                .returns("name\tsuspended\nblk\tfalse\nctl\tfalse\n");
    }

    private void createTables() throws Exception {
        execute("""
                CREATE TABLE blk (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING INCLUDE (value),
                    value DOUBLE
                ) TIMESTAMP(ts) PARTITION BY DAY WAL
                """);
        execute("""
                CREATE TABLE ctl (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING,
                    value DOUBLE
                ) TIMESTAMP(ts) PARTITION BY DAY WAL
                """);
    }

    private void insert(String table, String selectTail) throws Exception {
        execute("INSERT INTO " + table + " " + selectTail);
    }

    /**
     * Partition A (day 1) and partition C (day 4) bracket the target partition
     * B (day 2), so every subsequent write into day 2 is an O3 commit into an
     * existing, non-last partition.
     */
    private void seedMidPartition() throws Exception {
        for (String t : new String[]{"blk", "ctl"}) {
            execute("INSERT INTO " + t + " VALUES ('2024-01-01T00:00:00.000000Z', 'S0', -1.0)");
            execute("INSERT INTO " + t + " VALUES ('2024-01-04T00:00:00.000000Z', 'S0', -2.0)");
            insert(t, "SELECT dateadd('u', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                    " 'S' || (x % 8), x::DOUBLE FROM long_sequence(" + SEED_ROWS + ")");
        }
        drainWalQueue();
    }
}
