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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the covering-POSTING-index behaviour under WAL block-apply of proven
 * in-order data. Part 1 broadens the block-apply in-order detection to recognise
 * multi-timestamp globally-in-order single-segment blocks; A2 then routes those
 * blocks through the fast-lag mechanism (append to the last partition + fast-lag
 * index/covered publish + deferred compaction), bypassing the O3
 * copy/index/seal (which full-reseals the covered sidecar every block). Covers:
 * correctness of covered reads, that the seal is never a full reseal
 * (COVERING_FULL_RESEAL_COUNT stays 0 while COVERING_FASTLAG_COMMIT_COUNT
 * advances), the partition-boundary split, an out-of-order negative control,
 * NULLs, and MAX_GEN_COUNT threshold auto-seal under sustained load.
 */
public class CoveringIndexBlockApplySealTest extends AbstractCairoTest {

    /**
     * The covering counters are production-hot-path @TestOnly observability
     * gated by PostingIndexWriter.COVERING_COUNTERS_ENABLED (false in production
     * so the JIT elides the increments). Enable it and zero the counters before
     * each test; disable it after so the flag and the shared static counters
     * cannot leak into other test classes. These counters are single-fork
     * test-only state (forkCount must be 1 for this class).
     */
    @Before
    public void enableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = true;
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
    }

    @After
    public void disableCoveringCounters() {
        PostingIndexWriter.COVERING_COUNTERS_ENABLED = false;
    }

    /**
     * Keeps many small WAL transactions inside a single segment so the applier
     * batches them into one block apply (block starts at a non-zero segment row
     * offset).
     */
    private void configureForBlockApply() {
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 10_000_000);
        // This class is about the WAL fast-lag block-apply, which a merge-append table refuses: every
        // one of its commits is a merge-append, with its covering indexes published by that path instead
        // (see CoveringIndexMergeAppendTest). Tests default to merge-append; ask for master's behaviour.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
    }

    @Test
    public void testCoveredReadsCorrectAfterBlockApplyInOrder() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            // Covering POSTING index on sym, covering a double `value`. NULLs in
            // the covered column are exercised via nullif so the appended rows'
            // fragments must carry the NULL sentinel too. Single partition
            // (PARTITION BY YEAR) so successive blocks reseal/append the same
            // growing partition -- the pathology the append path targets.
            execute("""
                    CREATE TABLE blk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (value),
                        value DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
                    """);
            // Non-covering control table with identical data, indexed with a
            // plain (non-covering) POSTING index. Cross-check target.
            execute("""
                    CREATE TABLE ctl (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        value DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
                    """);

            // Ingest over many separate WAL transactions WITHOUT draining
            // between them, so the single drain applies them as one in-order
            // block. Each transaction spans MANY distinct ascending timestamps
            // (multi-timestamp), and the transactions are globally sequential --
            // the case Part 1's broadened non-overlap in-order detection must
            // recognise (previously forced through the O3 sort). `value` is
            // globally unique for a deterministic ORDER BY; sym cycles all 8 keys.
            final int batches = 40;
            final int rowsPerBatch = 250;
            for (int b = 0; b < batches; b++) {
                final long base = (long) b * rowsPerBatch;
                final String selectTail = " SELECT " +
                        "  dateadd('s', (" + base + " + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                        "  'S' || ((" + base + " + x) % 8), " +
                        "  (" + base + " + x)::DOUBLE " +
                        "FROM long_sequence(" + rowsPerBatch + ")";
                execute("INSERT INTO blk" + selectTail);
                execute("INSERT INTO ctl" + selectTail);
            }
            drainWalQueue();

            // No suspension on either table.
            assertQuery("SELECT name, suspended FROM wal_tables() WHERE name IN ('blk','ctl') ORDER BY name")
                    .noLeakCheck()
                    .returns("name\tsuspended\nblk\tfalse\nctl\tfalse\n");

            // Per-symbol covered lookups (served by the covering index) match the
            // control, ordered by the unique `value` for a deterministic compare.
            for (int s = 0; s < 8; s++) {
                final String sym = "S" + s;
                assertSqlCursors(
                        "SELECT ts, sym, value FROM ctl WHERE sym = '" + sym + "' ORDER BY value",
                        "SELECT ts, sym, value FROM blk WHERE sym = '" + sym + "' ORDER BY value"
                );
            }

            // A covered aggregate that reads the covered `value`.
            assertSqlCursors(
                    "SELECT sym, sum(value), count(value), min(value), max(value) FROM ctl ORDER BY sym",
                    "SELECT sym, sum(value), count(value), min(value), max(value) FROM blk ORDER BY sym"
            );
        });
    }

    /**
     * TDD.2 (behaviour): a pure in-order WAL block-apply into the last partition
     * is routed through the fast-lag mechanism (A2) instead of O3
     * copy/index/seal, so the covered sidecar is published incrementally and
     * NEVER full-resealed: {@code COVERING_FULL_RESEAL_COUNT} stays 0 while
     * {@code COVERING_FASTLAG_COMMIT_COUNT} advances. On unmodified code the same
     * blocks route through {@code finishO3Commit -> rebuildSidecars} (full reseal
     * every block), so this fails.
     */
    @Test
    public void testCoveringSealTakesAppendPathNotFullReseal() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE blk (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (value),
                        value DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
                    """);
            execute("""
                    CREATE TABLE ctl (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING,
                        value DOUBLE
                    ) TIMESTAMP(ts) PARTITION BY YEAR WAL
                    """);

            // Bootstrap the last partition first: the very first block on an
            // empty table has no last partition to append to, so it legitimately
            // creates it via O3. A2 (fast-lag append) applies to every subsequent
            // in-order block. Measure only after bootstrap.
            execute("INSERT INTO blk VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);

            // Each iteration queues several MULTI-timestamp (distinct ascending
            // per row) globally-in-order WAL transactions with NO drain between
            // them, then drains them as one in-order block appended to the growing
            // last partition -- exercising Part 1 (broadened in-order detection)
            // + A2 (fast-lag append).
            final int drains = 8;
            final int batchesPerDrain = 5;
            final int rowsPerBatch = 250;
            long base = rowsPerBatch;
            for (int d = 0; d < drains; d++) {
                for (int b = 0; b < batchesPerDrain; b++) {
                    final String selectTail = " SELECT " +
                            "  dateadd('s', (" + base + " + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                            "  'S' || ((" + base + " + x) % 8), " +
                            "  (" + base + " + x)::DOUBLE " +
                            "FROM long_sequence(" + rowsPerBatch + ")";
                    execute("INSERT INTO blk" + selectTail);
                    execute("INSERT INTO ctl" + selectTail);
                    base += rowsPerBatch;
                }
                drainWalQueue();
            }

            final long fastLag = PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get();
            final long fullReseals = PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get();
            Assert.assertTrue(
                    "in-order block-apply must route the covered index through the fast-lag path "
                            + "(fastLag=" + fastLag + ", fullReseals=" + fullReseals + ")",
                    fastLag > 0);
            Assert.assertEquals(
                    "in-order block-apply must NOT full-reseal the covered sidecar "
                            + "(fastLag=" + fastLag + ", fullReseals=" + fullReseals + ")",
                    0, fullReseals);

            // The fast-lag path must also be correct: covered reads match control
            // (per-symbol, ordered by the unique `value`).
            assertCoveredMatchesControl();
        });
    }

    /**
     * Partition-boundary split: an in-order multi-timestamp block that straddles
     * a DAY boundary must fast-append the within-partition PREFIX (fast-lag) and
     * route the OVERFLOW through O3 (creating the new partition), with correct
     * covered reads across both partitions.
     */
    @Test
    public void testCoveredReadsCorrectStraddlingPartitionBoundary() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("DAY");
            // Bootstrap day 1 so the straddling block has a last partition to
            // fast-append its prefix into.
            execute("INSERT INTO blk VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);

            // One block of 5 ascending multi-timestamp transactions starting at
            // 23:59:01 on day 1 and crossing midnight into day 2 (straddle).
            final int batchesPerDrain = 5;
            final int rowsPerBatch = 200;
            long base = 0;
            for (int b = 0; b < batchesPerDrain; b++) {
                final String selectTail = " SELECT " +
                        "  dateadd('s', (" + base + " + x)::INT, '2024-01-01T23:59:01Z'::TIMESTAMP), " +
                        "  'S' || ((" + base + " + x) % 8), " +
                        "  (" + base + " + x)::DOUBLE " +
                        "FROM long_sequence(" + rowsPerBatch + ")";
                execute("INSERT INTO blk" + selectTail);
                execute("INSERT INTO ctl" + selectTail);
                base += rowsPerBatch;
            }
            drainWalQueue();

            // The prefix used the fast-lag path; both partitions got data (proven
            // by the per-partition counts matching the control, which has the
            // identical data via the plain O3 path). Assert the BLOCK-APPLY
            // counter, not COVERING_FASTLAG_COMMIT_COUNT: the latter also counts
            // the pre-existing single-txn fast-lag publish, so it cannot tell this
            // path from the one that was already on master.
            Assert.assertTrue("straddle must fast-append the within-partition prefix",
                    PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get() > 0);
            assertSqlCursors(
                    "SELECT ts::date d, count(*), sum(value) FROM ctl GROUP BY d ORDER BY d",
                    "SELECT ts::date d, count(*), sum(value) FROM blk GROUP BY d ORDER BY d"
            );
            assertCoveredMatchesControl();
        });
    }

    /**
     * Phase-3 negative control: a SORTED (interleaved / out-of-order) block whose
     * min is BELOW the committed max -- genuine LATE DATA that merges into
     * existing partition rows -- must NOT take the sorted pure-append fast path.
     * It must sort + full-reseal (the reseal is legitimately required for a
     * merge), and covered reads stay exact vs the non-covering control.
     * <p>
     * (Replaces the A2-era {@code testOutOfOrderBlockStillFullReseals}, whose
     * block was a pure append merely out of order and now -- correctly -- fast
     * paths; see docs/task-1-report.md and docs/task-2-report.md.)
     */
    @Test
    public void testSortedLateDataBlockStillFullReseals() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("DAY");
            // Establish committed rows at 02:00:00.. so the committed max is ~02:04.
            execute("INSERT INTO blk SELECT dateadd('s', x::INT, '2024-01-01T02:00:00Z'::TIMESTAMP), 'S' || (x % 8), (100000 + x)::DOUBLE FROM long_sequence(250)");
            execute("INSERT INTO ctl SELECT dateadd('s', x::INT, '2024-01-01T02:00:00Z'::TIMESTAMP), 'S' || (x % 8), (100000 + x)::DOUBLE FROM long_sequence(250)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);

            // A block that must SORT (two interleaved batches: even/odd seconds)
            // whose timestamps are all at 01:00:.. -- BEFORE the committed 02:00
            // max -> late data / real merge -> the sorted fast path must NOT fire.
            final int n = 300;
            final String even = " SELECT dateadd('s', (2 * x)::INT, '2024-01-01T01:00:00Z'::TIMESTAMP), 'S' || ((2 * x) % 8), (2 * x)::DOUBLE FROM long_sequence(" + n + ")";
            final String odd = " SELECT dateadd('s', (2 * x - 1)::INT, '2024-01-01T01:00:00Z'::TIMESTAMP), 'S' || ((2 * x - 1) % 8), (2 * x - 1)::DOUBLE FROM long_sequence(" + n + ")";
            execute("INSERT INTO blk" + even);
            execute("INSERT INTO blk" + odd);
            execute("INSERT INTO ctl" + even);
            execute("INSERT INTO ctl" + odd);
            drainWalQueue();

            Assert.assertEquals("late-data sorted block must not fast-lag",
                    0, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertTrue("late-data sorted block must full-reseal (fast path must NOT fire)",
                    PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() > 0);
            assertCoveredMatchesControl();
        });
    }

    /**
     * NULLs in the covered column across an in-order block-apply routed through
     * A2: covered reads (including the NULL sentinel) stay correct, no reseal.
     */
    @Test
    public void testCoveredNullsInOrderBlockApply() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("YEAR");
            execute("INSERT INTO blk VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);

            final int batchesPerDrain = 5;
            final int rowsPerBatch = 250;
            long base = rowsPerBatch;
            for (int b = 0; b < batchesPerDrain; b++) {
                // nullif nulls the covered value for one row per batch.
                final String selectTail = " SELECT " +
                        "  dateadd('s', (" + base + " + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                        "  'S' || ((" + base + " + x) % 8), " +
                        "  nullif((" + base + " + x)::DOUBLE, " + (base + 7) + ") " +
                        "FROM long_sequence(" + rowsPerBatch + ")";
                execute("INSERT INTO blk" + selectTail);
                execute("INSERT INTO ctl" + selectTail);
                base += rowsPerBatch;
            }
            drainWalQueue();

            Assert.assertTrue(PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            // count(value) excludes NULLs; the covered aggregate must match.
            assertSqlCursors(
                    "SELECT sym, sum(value), count(value), count(*) FROM ctl ORDER BY sym",
                    "SELECT sym, sum(value), count(value), count(*) FROM blk ORDER BY sym"
            );
            assertCoveredMatchesControl();
        });
    }

    /**
     * Sustained in-order block-apply that accumulates more than MAX_GEN_COUNT
     * deferred fast-lag generations, forcing the covered index's threshold
     * auto-seal (compaction) -- covered reads stay correct and the covering
     * seal is still never a full reseal (compaction is seal(), not
     * rebuildSidecars).
     */
    @Test
    public void testSustainedInOrderBlockApplyAutoSeal() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("YEAR");
            execute("INSERT INTO blk VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);

            // 24 drains > MAX_GEN_COUNT (16): the fast-lag defers each block's gen
            // until the threshold auto-seal compacts.
            final int drains = 24;
            final int rowsPerBatch = 200;
            long base = rowsPerBatch;
            for (int d = 0; d < drains; d++) {
                for (int b = 0; b < 3; b++) {
                    final String selectTail = " SELECT " +
                            "  dateadd('s', (" + base + " + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                            "  'S' || ((" + base + " + x) % 8), " +
                            "  (" + base + " + x)::DOUBLE " +
                            "FROM long_sequence(" + rowsPerBatch + ")";
                    execute("INSERT INTO blk" + selectTail);
                    execute("INSERT INTO ctl" + selectTail);
                    base += rowsPerBatch;
                }
                drainWalQueue();
            }

            Assert.assertTrue(PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);
            Assert.assertEquals("sustained in-order block-apply must never full-reseal",
                    0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesControl();
        });
    }

    /**
     * Phase-3 Task 1 spike: a block that MUST sort (its transactions' timestamp
     * ranges interleave, so {@code getAllTxnDataInOrder()} is false and the O3
     * sort path is taken) but whose sorted result is a pure APPEND to the last
     * partition (all timestamps &gt; the committed max). Today such a block
     * reseals the covered sidecar (O3 path); the spike routes it through the
     * fast-lag append instead. Asserts covered reads exact AND no full reseal.
     */
    @Test
    public void testCoveredReadsCorrectAfterSortedPureAppendBlock() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("YEAR");
            // Bootstrap: establish the last partition + committed max at 00:00:00.
            execute("INSERT INTO blk VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);

            // Two interleaved transactions, all AFTER the committed max (pure
            // append) but out of order relative to each other -> the block must
            // sort. Batch A = even seconds (value even), batch B = odd seconds
            // (value odd); B's min second (1) < A's max second (2N) so the block
            // is not in order. Combined `value` = 1..2N unique (deterministic
            // ORDER BY). Withheld drain forms a single block.
            final int n = 400;
            final String batchA = " SELECT " +
                    "  dateadd('s', (2 * x)::INT, '2024-06-01T01:00:00Z'::TIMESTAMP), " +
                    "  'S' || ((2 * x) % 8), (2 * x)::DOUBLE " +
                    "FROM long_sequence(" + n + ")";
            final String batchB = " SELECT " +
                    "  dateadd('s', (2 * x - 1)::INT, '2024-06-01T01:00:00Z'::TIMESTAMP), " +
                    "  'S' || ((2 * x - 1) % 8), (2 * x - 1)::DOUBLE " +
                    "FROM long_sequence(" + n + ")";
            execute("INSERT INTO blk" + batchA);
            execute("INSERT INTO blk" + batchB);
            execute("INSERT INTO ctl" + batchA);
            execute("INSERT INTO ctl" + batchB);
            drainWalQueue();

            // Covered reads must be exact vs the non-covering control.
            assertCoveredMatchesControl();
            assertSqlCursors(
                    "SELECT sym, sum(value), count(value), count(*) FROM ctl ORDER BY sym",
                    "SELECT sym, sum(value), count(value), count(*) FROM blk ORDER BY sym"
            );
            // The sorted pure-append block must NOT full-reseal the covered index.
            Assert.assertEquals(
                    "sorted pure-append block must not full-reseal (fullReseals="
                            + PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() + ")",
                    0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
        });
    }

    /**
     * Phase-3 Task 3: a SORTED (interleaved / out-of-order) pure-append block
     * whose ascending timestamp range crosses a DAY boundary. The
     * within-last-partition prefix must fast-lag (no reseal for it) and the
     * overflow must create the new partition via the unchanged O3 path, with
     * per-partition counts and covered reads exact vs the non-covering control.
     */
    @Test
    public void testSortedBlockStraddlingPartitionBoundary() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("DAY");
            // Bootstrap day 1 (last partition + committed max at 00:00:00).
            execute("INSERT INTO blk VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-01-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);

            // Two INTERLEAVED batches (even/odd seconds) -> the block must SORT.
            // Starts at 23:59:01 on day 1 and crosses midnight into day 2, all
            // after the committed max (pure append) -> the sorted-block fast path
            // fast-lags the day-1 prefix and O3s the day-2 overflow. value = 1..2N
            // unique (deterministic ORDER BY).
            final int n = 300;
            final String even = " SELECT dateadd('s', (2 * x)::INT, '2024-01-01T23:59:01Z'::TIMESTAMP), 'S' || ((2 * x) % 8), (2 * x)::DOUBLE FROM long_sequence(" + n + ")";
            final String odd = " SELECT dateadd('s', (2 * x - 1)::INT, '2024-01-01T23:59:01Z'::TIMESTAMP), 'S' || ((2 * x - 1) % 8), (2 * x - 1)::DOUBLE FROM long_sequence(" + n + ")";
            execute("INSERT INTO blk" + even);
            execute("INSERT INTO blk" + odd);
            execute("INSERT INTO ctl" + even);
            execute("INSERT INTO ctl" + odd);
            drainWalQueue();

            // Prefix fast-lagged, overflow created day 2 via O3. Assert the
            // BLOCK-APPLY counter rather than COVERING_FASTLAG_COMMIT_COUNT: the
            // latter also counts the pre-existing single-txn fast-lag publish, so
            // it cannot tell this path from the one already on master.
            Assert.assertTrue("straddling sorted block must fast-append the day-1 prefix",
                    PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get() > 0);
            // Both partitions exist with the right rows (vs the plain-O3 control).
            assertSqlCursors(
                    "SELECT ts::date d, count(*), sum(value) FROM ctl GROUP BY d ORDER BY d",
                    "SELECT ts::date d, count(*), sum(value) FROM blk GROUP BY d ORDER BY d"
            );
            assertCoveredMatchesControl();
        });
    }

    /**
     * Phase-3 Task 4: NULLs in the covered column scattered across a SORTED
     * (interleaved) pure-append block. Covered reads -- including the NULL
     * sentinel and an {@code IS NULL} covered predicate -- must be exact vs the
     * non-covering control, with no full reseal.
     */
    @Test
    public void testSortedPureAppendNulls() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("YEAR");
            execute("INSERT INTO blk VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);

            // Interleaved even/odd seconds -> sorts; all after 00:00:00 -> pure
            // append. Some covered values are NULL (every 5th / 4th row).
            final int n = 300;
            final String even = " SELECT dateadd('s', (2 * x)::INT, '2024-06-01T01:00:00Z'::TIMESTAMP), 'S' || ((2 * x) % 8), case when x % 5 = 0 then cast(null as double) else (2 * x)::double end FROM long_sequence(" + n + ")";
            final String odd = " SELECT dateadd('s', (2 * x - 1)::INT, '2024-06-01T01:00:00Z'::TIMESTAMP), 'S' || ((2 * x - 1) % 8), case when x % 4 = 0 then cast(null as double) else (2 * x - 1)::double end FROM long_sequence(" + n + ")";
            execute("INSERT INTO blk" + even);
            execute("INSERT INTO blk" + odd);
            execute("INSERT INTO ctl" + even);
            execute("INSERT INTO ctl" + odd);
            drainWalQueue();

            Assert.assertTrue(PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);
            Assert.assertEquals(0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());

            // Per-symbol covered reads (incl NULLs), ordered by the unique ts.
            for (int s = 0; s < 8; s++) {
                assertSqlCursors(
                        "SELECT ts, sym, value FROM ctl WHERE sym = 'S" + s + "' ORDER BY ts",
                        "SELECT ts, sym, value FROM blk WHERE sym = 'S" + s + "' ORDER BY ts");
            }
            // Covered predicate that returns only NULL-valued rows.
            assertSqlCursors(
                    "SELECT ts, sym FROM ctl WHERE sym = 'S0' AND value IS NULL ORDER BY ts",
                    "SELECT ts, sym FROM blk WHERE sym = 'S0' AND value IS NULL ORDER BY ts");
            assertSqlCursors(
                    "SELECT sym, sum(value), count(value), count(*) FROM ctl ORDER BY sym",
                    "SELECT sym, sum(value), count(value), count(*) FROM blk ORDER BY sym");
        });
    }

    /**
     * Phase-3 Task 4/6: many sorted pure-append blocks accumulate more than
     * MAX_GEN_COUNT = 128 deferred fast-lag generations, so the covered index's
     * threshold auto-seal MUST fire. Every block fast-lags
     * ({@code fastLag == blockCount}); the compaction fires as {@code seal()}
     * inside flushAllPending, NOT a per-block {@code rebuildSidecars}
     * ({@code fullReseals == 0}); final covered reads exact.
     * <p>
     * Task 6 hardening: assert the 128-gen hard cap is REACHED AND ENFORCED on
     * the fast-lag path directly, not merely inferred from correct reads. If the
     * fast-lag path ever bypassed flushAllPending's inline
     * {@code if (genCount >= MAX_GEN_COUNT) seal()}, genCount would run past 128
     * and overflow the 128-slot {@code .pc} header. So we assert
     * {@code COVERING_AUTOSEAL_COUNT >= 1} (the cap-seal actually fired via the
     * fast-lag path) and {@code COVERING_MAX_GENCOUNT_OBSERVED <= MAX_GEN_COUNT}
     * (genCount never exceeded the cap). The run REACHES the cap; it never
     * exceeds it.
     */
    @Test
    public void testSortedSustainedAutoSeal() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("YEAR");
            execute("INSERT INTO blk VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();

            PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
            PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
            PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
            PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);

            // 135 > MAX_GEN_COUNT (128). Each block = two interleaved (even/odd)
            // transactions -> sorts; ascending across blocks -> pure append. One
            // deferred gen per block until the threshold auto-seal compacts.
            final int blocks = 135;
            final int perBatch = 10;
            long base = 0;
            for (int b = 0; b < blocks; b++) {
                final String even = " SELECT dateadd('s', (" + base + " + 2 * x)::INT, '2024-06-01T01:00:00Z'::TIMESTAMP), 'S' || ((" + base + " + 2 * x) % 8), (" + base + " + 2 * x)::DOUBLE FROM long_sequence(" + perBatch + ")";
                final String odd = " SELECT dateadd('s', (" + base + " + 2 * x - 1)::INT, '2024-06-01T01:00:00Z'::TIMESTAMP), 'S' || ((" + base + " + 2 * x - 1) % 8), (" + base + " + 2 * x - 1)::DOUBLE FROM long_sequence(" + perBatch + ")";
                execute("INSERT INTO blk" + even);
                execute("INSERT INTO blk" + odd);
                execute("INSERT INTO ctl" + even);
                execute("INSERT INTO ctl" + odd);
                drainWalQueue();
                base += 2L * perBatch;
            }

            Assert.assertEquals("every sorted pure-append block must fast-lag",
                    blocks, PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get());
            Assert.assertEquals("threshold compaction is seal(), never a per-block rebuildSidecars",
                    0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            // The 128-gen hard cap must be REACHED and ENFORCED on the fast-lag
            // path: the inline auto-seal fired at least once...
            Assert.assertTrue("MAX_GEN_COUNT auto-seal must fire on the fast-lag path "
                            + "(COVERING_AUTOSEAL_COUNT=" + PostingIndexWriter.COVERING_AUTOSEAL_COUNT.get() + ")",
                    PostingIndexWriter.COVERING_AUTOSEAL_COUNT.get() >= 1);
            // ...and genCount never ran past the 128-slot .pc header.
            Assert.assertTrue("genCount must never exceed MAX_GEN_COUNT "
                            + "(observed=" + PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.get() + ")",
                    PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.get() <= PostingIndexUtils.MAX_GEN_COUNT);
            assertCoveredMatchesControl();
        });
    }

    /**
     * Phase-3 Task 8: a GENUINELY multi-segment block (two WAL writers = two
     * segments, held open together so their txns batch into one apply block)
     * whose merged data is a pure append (all timestamps after the committed
     * max, interleaved across the two segments) must take the sorted fast path.
     * Multi-segment blocks go through the O3-sort branch (copiedToMemory=true)
     * which gathers the merge-ordered rows without indexing; a pure-append
     * result then fast-lags via tryFastAppendSortedBlock with NO segmentCount
     * gate. We prove the block was really multi-segment
     * ({@code COVERING_MAX_SEGCOUNT_OBSERVED > 1}) before asserting the fast path
     * fired ({@code fastLag > 0}, {@code fullReseals == 0}) and covered reads
     * are byte-exact vs the non-covering control.
     */
    @Test
    public void testCoveredReadsCorrectAfterMultiSegmentPureAppendBlock() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("YEAR");
            // Seed the committed max at 2024-06-01T00:00:00.
            execute("INSERT INTO blk VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();
            resetCoveringCounters();

            // Two segments, both starting one hour AFTER the committed max, with
            // interleaved timestamps (segment 1 = even offsets, segment 2 = odd
            // offsets) so the merged result is fully ascending -> pure append.
            final long base = MicrosTimestampDriver.floor("2024-06-01T01:00:00");
            writeTwoSegmentBlock("blk", base, 400, false);
            writeTwoSegmentBlock("ctl", base, 400, false);
            drainWalQueue();

            Assert.assertTrue("block must be genuinely multi-segment "
                            + "(COVERING_MAX_SEGCOUNT_OBSERVED=" + PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.get() + ")",
                    PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.get() > 1);
            Assert.assertTrue("multi-segment pure-append must fast-lag "
                            + "(fastLag=" + PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() + ")",
                    PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.get() > 0);
            Assert.assertEquals("multi-segment pure-append must not full-reseal",
                    0, PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get());
            assertCoveredMatchesControl();
            assertSqlCursors(
                    "SELECT sym, sum(value), count(value), count(*) FROM ctl ORDER BY sym",
                    "SELECT sym, sum(value), count(value), count(*) FROM blk ORDER BY sym");
        });
    }

    /**
     * Phase-3 Task 8 negative control: a multi-segment block where the second
     * segment carries LATE data (min &lt; committed max), forcing a real O3
     * merge into the existing partition. The sorted fast path must NOT fire; the
     * commit full-reseals the covered sidecar ({@code fullReseals > 0}) and
     * covered reads stay exact.
     */
    @Test
    public void testMultiSegmentMergeStillFullReseals() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            createTables("YEAR");
            execute("INSERT INTO blk VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            execute("INSERT INTO ctl VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0)");
            drainWalQueue();
            resetCoveringCounters();

            // Segment 1 appends after the committed max; segment 2 back-dates
            // well before the seed (2024-05-01), so blockMin < committed max ->
            // genuine merge, not a pure append.
            final long base = MicrosTimestampDriver.floor("2024-06-01T01:00:00");
            writeTwoSegmentBlock("blk", base, 400, true);
            writeTwoSegmentBlock("ctl", base, 400, true);
            drainWalQueue();

            Assert.assertTrue("block must be genuinely multi-segment "
                            + "(COVERING_MAX_SEGCOUNT_OBSERVED=" + PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.get() + ")",
                    PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.get() > 1);
            Assert.assertTrue("a real multi-segment merge must full-reseal, not fast-lag "
                            + "(fullReseals=" + PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() + ")",
                    PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.get() > 0);
            assertCoveredMatchesControl();
            assertSqlCursors(
                    "SELECT sym, sum(value), count(value), count(*) FROM ctl ORDER BY sym",
                    "SELECT sym, sum(value), count(value), count(*) FROM blk ORDER BY sym");
        });
    }

    private void resetCoveringCounters() {
        PostingIndexWriter.COVERING_FASTLAG_COMMIT_COUNT.set(0);
        PostingIndexWriter.COVERING_FULL_RESEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_AUTOSEAL_COUNT.set(0);
        PostingIndexWriter.COVERING_MAX_GENCOUNT_OBSERVED.set(0);
        PostingIndexWriter.COVERING_MAX_SEGCOUNT_OBSERVED.set(0);
    }

    /**
     * Writes {@code rowsPerSeg} rows to each of TWO WalWriters on {@code table}
     * (held open simultaneously so the pool cannot reuse one segment), producing
     * two distinct WAL segments whose txns batch into one apply block on the next
     * drain. Segment 1 carries even offsets, segment 2 odd offsets, interleaved
     * so the merged block is sorted. When {@code seg2Late} the second segment is
     * back-dated before the committed max to force a merge; otherwise both
     * segments sit after {@code base} for a pure append. {@code value} is globally
     * unique (== offset) for a deterministic ORDER BY.
     */
    private void writeTwoSegmentBlock(String table, long base, int rowsPerSeg, boolean seg2Late) {
        final long sec = 1_000_000L;
        final TableToken tt = engine.verifyTableName(table);
        try (
                WalWriter w1 = engine.getWalWriter(tt);
                WalWriter w2 = engine.getWalWriter(tt)
        ) {
            for (int i = 0; i < rowsPerSeg; i++) {
                final long off1 = 2L * i;                 // 0,2,4,... after base
                appendRow(w1, base + off1 * sec, off1);
                final long off2 = seg2Late
                        ? -(3700L + 2L * i)               // before the 2024-06-01T00:00:00 seed
                        : (2L * i + 1);                   // 1,3,5,... after base
                appendRow(w2, base + off2 * sec, off2);
            }
            w1.commit();
            w2.commit();
        }
    }

    /**
     * Variable-size columns on the block-apply fast path. The shared append body
     * copies EVERY column, and a var-size one carries an aux vector whose offsets
     * must be rebased against the source. The fast path feeds that body two
     * different sources -- the WAL-mapped columns at a non-zero block row offset
     * (in-order block) and the O3 gather buffer at row 0 (sorted block) -- so both
     * are driven here. Widths vary per row and NULLs are scattered, so a
     * mis-rebased aux vector shifts payloads instead of silently matching.
     */
    @Test
    public void testFastPathCopiesVarSizeColumns() throws Exception {
        configureForBlockApply();
        assertMemoryLeak(() -> {
            execute("CREATE TABLE blk (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value),"
                    + " value DOUBLE, txt VARCHAR, str STRING) TIMESTAMP(ts) PARTITION BY YEAR WAL");
            execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING,"
                    + " value DOUBLE, txt VARCHAR, str STRING) TIMESTAMP(ts) PARTITION BY YEAR WAL");

            final String seed = " VALUES ('2024-06-01T00:00:00Z', 'S0', -1.0, 'seed', 'seed')";
            execute("INSERT INTO blk" + seed);
            execute("INSERT INTO ctl" + seed);
            drainWalQueue();

            // Two ascending, non-overlapping transactions batched into one block
            // -> the IN-ORDER fast path (single segment, no sort).
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
            insertVarSizeRows(" SELECT dateadd('s', x::INT, '2024-06-01T01:00:00Z'::TIMESTAMP),");
            insertVarSizeRows(" SELECT dateadd('s', x::INT, '2024-06-01T01:30:00Z'::TIMESTAMP),");
            drainWalQueue();
            Assert.assertTrue(
                    "in-order block must take the fast path",
                    PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get() > 0
            );
            assertVarSizeMatchesControl();

            // Interleaved even/odd timestamps -> the block needs sorting, so the
            // SORTED fast path runs and its source is the O3 gather buffer.
            PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.set(0);
            insertVarSizeRows(" SELECT dateadd('s', (2 * x)::INT, '2024-06-01T02:00:00Z'::TIMESTAMP),");
            insertVarSizeRows(" SELECT dateadd('s', (2 * x - 1)::INT, '2024-06-01T02:00:00Z'::TIMESTAMP),");
            drainWalQueue();
            Assert.assertTrue(
                    "sorted block must take the fast path",
                    PostingIndexWriter.COVERING_BLOCK_FASTPATH_COUNT.get() > 0
            );
            assertVarSizeMatchesControl();
        });
    }

    private void appendRow(WalWriter w, long tsMicros, long value) {
        final TableWriter.Row row = w.newRow(tsMicros);
        row.putSym(1, "S" + Math.floorMod(value, 8));
        row.putDouble(2, (double) value);
        row.append();
    }

    // Full-row compare including both var-size columns, plus a covered lookup per
    // symbol so the covering cursor -- not just a table scan -- returns them.
    private void assertVarSizeMatchesControl() throws Exception {
        assertSqlCursors(
                "SELECT ts, sym, value, txt, str FROM ctl ORDER BY ts, sym",
                "SELECT ts, sym, value, txt, str FROM blk ORDER BY ts, sym"
        );
        for (int s = 0; s < 8; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, value, txt, str FROM ctl WHERE sym = '" + sym + "' ORDER BY ts",
                    "SELECT ts, sym, value, txt, str FROM blk WHERE sym = '" + sym + "' ORDER BY ts"
            );
        }
    }

    // Rows whose VARCHAR / STRING widths vary per row, with NULLs on different
    // cycles so the two aux vectors cannot mask each other.
    private void insertVarSizeRows(String tsExpr) throws Exception {
        final String tail = tsExpr
                + " 'S' || (x % 8), x::DOUBLE,"
                + " CASE WHEN x % 7 = 0 THEN NULL ELSE rpad(x::VARCHAR, ((x % 13) + 1)::INT, 'v') END,"
                + " CASE WHEN x % 5 = 0 THEN NULL ELSE rpad(x::STRING, ((x % 11) + 1)::INT, 's') END"
                + " FROM long_sequence(400)";
        execute("INSERT INTO blk" + tail);
        execute("INSERT INTO ctl" + tail);
    }

    private void assertCoveredMatchesControl() throws Exception {
        for (int s = 0; s < 8; s++) {
            final String sym = "S" + s;
            assertSqlCursors(
                    "SELECT ts, sym, value FROM ctl WHERE sym = '" + sym + "' ORDER BY value",
                    "SELECT ts, sym, value FROM blk WHERE sym = '" + sym + "' ORDER BY value"
            );
        }
    }

    private void createTables(String partitionBy) throws Exception {
        execute("CREATE TABLE blk (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (value), value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY " + partitionBy + " WAL");
        execute("CREATE TABLE ctl (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING, value DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY " + partitionBy + " WAL");
    }
}
