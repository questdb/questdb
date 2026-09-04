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

import io.questdb.PropertyKey;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Composite single-cell fast-append (composite-partitioning fast-append spec 1, Task 2 -- the crux:
 * an eligible commit actually FAST-APPENDS, instead of merely being counted as in Task 1). Flag ON.
 * <p>
 * Differential-vs-plain-twin is the correctness oracle: a composite table {@code c} (fast-append) must
 * equal a plain twin {@code p} fed the identical rows AND (single-cell ordered case) a third composite
 * table {@code c1} fed the same rows in ONE commit, across scan / count / per-cell / {@code LATEST ON} /
 * {@code SAMPLE BY}. The fast-append must ALSO actually fire ({@link
 * TableWriter#getCompositeFastAppendCommittedCount()} -- a distinct "actually fast-appended" counter,
 * not Task 1's broader "eligible" counter which also fires for brand-new cells this task deliberately
 * routes to the full path), so a passing differential test cannot be vacuous.
 */
public class CompositeFastAppendTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testPerSymbolInterleavedSingleCellCommitsMatchTwinAndFastAppend() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Warm both cells so the INTERLEAVED phase below lands in already-populated, cache-warm
            // cells (the shape this task fast-appends). Commit 1 routes the table + creates cellA;
            // commit 2 warms cellA's cache (cold-cache, full path); commits 3/4 create + warm cellB.
            insBatch("A", 10);   // routes table, creates cellA (full path)
            drainWalQueue();
            insBatch("A", 11);   // cellA populated, cache cold -> full path, warms cache[A]=11
            drainWalQueue();
            insBatch("B", 12);   // cellB empty -> full path (creates B), warms cache[B]=12
            drainWalQueue();
            insBatch("B", 13);   // cellB populated + warm -> fast-append (pre-bracket)
            drainWalQueue();

            // Interleaved phase: each symbol committed as its OWN single-cell, internally-ordered batch,
            // but globally OUT OF ORDER (B's rows fall before cellA's already-committed max). A plain
            // table MUST O3 these; a composite table fast-appends each single-cell commit -- the
            // differentiated, composite-specific win.
            long before = TableWriter.getCompositeFastAppendCommittedCount();
            insBatch("A", 14, 16, 18);  // append-only into cellA (14 > cache[A]=11)
            drainWalQueue();
            insBatch("B", 15, 17, 19);  // append-only into cellB (15 > cache[B]=13); 15,17 < 18 -> p O3s
            drainWalQueue();
            insBatch("A", 20, 22);      // append-only into cellA (20 > 18)
            drainWalQueue();
            insBatch("B", 21, 23);      // append-only into cellB (21 > 19); 21 < 22 -> p O3s
            drainWalQueue();
            long after = TableWriter.getCompositeFastAppendCommittedCount();
            Assert.assertEquals(
                    "each per-symbol single-cell interleaved commit (into a warm, populated cell) must fast-append",
                    before + 4, after);

            engine.releaseInactive();
            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");

            // Composite (fast-append) == plain twin (which O3'd the globally-out-of-order rows).
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select exch, count() from p group by exch order by exch",
                    "select exch, count() from c group by exch order by exch");
            assertSqlCursors("select ts, exch, px from p latest on ts partition by exch", "select ts, exch, px from c latest on ts partition by exch");
            assertSqlCursors("select ts, sum(px) from p sample by 1h ALIGN TO CALENDAR", "select ts, sum(px) from c sample by 1h ALIGN TO CALENDAR");
        });
    }

    @Test
    public void testSingleCellOrderedStreamMatchesTwinAndSingleCommitAndFastAppends() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
            execute("create table c1 (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            // Warm-up: commit 1 routes the table (interns cellA); commit 2 is a real single-cell append
            // that (cold cache) still takes the full path but warms cellA's per-cell max cache.
            insTwo("c", "p", 0, 1.0);
            drainWalQueue();
            insTwo("c", "p", 1, 1.01);
            drainWalQueue();

            // Stream: many small single-cell (exch='A') commits, strictly increasing ts. Each is
            // eligible AND lands in the already-populated, warm cellA -> must FAST-APPEND.
            final int streamRows = 20;
            long before = TableWriter.getCompositeFastAppendCommittedCount();
            for (int h = 2; h < 2 + streamRows; h++) {
                insTwo("c", "p", h, 1.0 + h * 0.01);
                drainWalQueue();
            }
            long after = TableWriter.getCompositeFastAppendCommittedCount();
            Assert.assertEquals(
                    "every strictly-increasing single-cell commit into the warm cell must fast-append",
                    before + streamRows, after);

            // c1: identical rows fed in ONE commit (a full-path oracle -- its own first commit is not
            // eligible, so it never fast-appends; it exists only to cross-check the fast-appended c).
            StringBuilder oneCommit = new StringBuilder();
            for (int h = 0; h < 2 + streamRows; h++) {
                if (h > 0) {
                    oneCommit.append(", ");
                }
                oneCommit.append("('").append(tsOf(h)).append("','A',").append(1.0 + h * 0.01).append(')');
            }
            execute("insert into c1 values " + oneCommit);
            drainWalQueue();

            engine.releaseInactive();
            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            assertWalTableNotSuspended("c1");

            // Differential oracle: composite (fast-append) == plain twin == single-commit composite.
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select ts, exch, px from c1 order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors("select count() from c1", "select count() from c");
            assertSqlCursors("select ts, exch, px from p where exch='A' order by ts", "select ts, exch, px from c where exch='A' order by ts");
            assertSqlCursors("select ts, exch, px from p latest on ts partition by exch", "select ts, exch, px from c latest on ts partition by exch");
            assertSqlCursors("select ts, sum(px) from p sample by 1h ALIGN TO CALENDAR", "select ts, sum(px) from c sample by 1h ALIGN TO CALENDAR");
        });
    }

    @Test
    public void testIndexedCompositeTableFallsBackToFullPathAndStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            // 'exch' is the composite dimension; 'sub' is a separate INDEXED symbol column. A fast-append
            // would append rows to sub's column file without updating its index -- so an indexed table
            // must fall back to the full path (which re-indexes). Proven by the committed counter staying
            // flat AND an index-driven query matching the plain twin.
            execute("create table c (ts timestamp, exch symbol, sub symbol index, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, sub symbol index, px double) timestamp(ts) partition by day wal");

            insIndexed(0, 1.0);
            drainWalQueue();
            insIndexed(1, 1.01);
            drainWalQueue();

            long before = TableWriter.getCompositeFastAppendCommittedCount();
            for (int h = 2; h < 8; h++) {
                insIndexed(h, 1.0 + h * 0.01);
                drainWalQueue();
            }
            Assert.assertEquals(
                    "an indexed composite table must NOT fast-append (its symbol index would silently desync)",
                    before, TableWriter.getCompositeFastAppendCommittedCount());

            engine.releaseInactive();
            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            assertSqlCursors("select ts, exch, sub, px from p order by ts, exch", "select ts, exch, sub, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
            // Read THROUGH the index too. The fast-append gate exists so the index cannot silently
            // desync from the data; that claim is only testable now that an indexed read is served,
            // and a full scan alone would pass even with a completely stale index.
            assertSqlCursors("select ts, exch, sub, px from p where sub = 'A' order by ts",
                    "select ts, exch, sub, px from c where sub = 'A' order by ts");
        });
    }

    /**
     * Part A (composite-partitioning fast-append spec 2, Task 2 -- N-cell handle cache): alternating
     * single-cell fast-append commits into two DIFFERENT cells ({@code A}, {@code B}, {@code A}) each
     * take the spec-1 fast path, and BOTH cells' column-file handles stay cached open across the
     * alternation -- proven via {@link TableWriter#getCompositeFastAppendOpenCellCount()} == 2. Spec-1's
     * single scalar handle held at most ONE open cell, re-opening on every A->B->A switch; the bounded
     * cache keeps them both open. Correctness oracle throughout: composite {@code c} == plain twin
     * {@code p}.
     */
    @Test
    public void testAlternatingSingleCellCommitsKeepBothCellHandlesCached() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Warm both cells (create on the first commit -> full path; warm the per-cell max cache on
            // the second -> fast-append) so the alternating phase below fast-appends every commit.
            insBatch("A", 10);
            drainWalQueue();
            insBatch("A", 11);
            drainWalQueue();
            insBatch("B", 12);
            drainWalQueue();
            insBatch("B", 13);
            drainWalQueue();

            // Alternating single-cell fast-appends: A, B, A -- each a separate ordered single-cell commit.
            long before = TableWriter.getCompositeFastAppendCommittedCount();
            insBatch("A", 14);
            drainWalQueue();
            insBatch("B", 15);
            drainWalQueue();
            insBatch("A", 16);
            drainWalQueue();
            long after = TableWriter.getCompositeFastAppendCommittedCount();
            Assert.assertEquals(
                    "each alternating single-cell commit into a warm cell must fast-append",
                    before + 3, after);

            Assert.assertEquals(
                    "both cells' handles must stay cached open across the A/B/A alternation (spec-1's single"
                            + " scalar handle would have held only one)",
                    2, openCellCount("c"));

            engine.releaseInactive();
            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
            // order by exch: LATEST ON output row-order differs between plain (by latest ts) and composite
            // (by cell) when the two cells' latest timestamps are cell-order-inverted -- compare by cell.
            assertSqlCursors(
                    "select ts, exch, px from p latest on ts partition by exch order by exch",
                    "select ts, exch, px from c latest on ts partition by exch order by exch");
        });
    }

    /**
     * Part A LRU eviction: with {@code cairo.wal.composite.fastappend.max.open.cells=1}, alternating
     * single-cell fast-appends into {@code A}, {@code B}, {@code A} must still each fast-append while the
     * cache never holds more than one open cell (the least-recently-used cell is evicted on each switch,
     * NON-TRUNCATING so its committed rows survive). Correctness oracle: {@code c} == {@code p} across the
     * eviction cycles.
     */
    @Test
    public void testMaxOpenCellsCapOneEvictsLruAndStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_MAX_OPEN_CELLS, "1");

            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            insBatch("A", 10);
            drainWalQueue();
            insBatch("A", 11);
            drainWalQueue();
            insBatch("B", 12);
            drainWalQueue();
            insBatch("B", 13);
            drainWalQueue();

            long before = TableWriter.getCompositeFastAppendCommittedCount();
            insBatch("A", 14);
            drainWalQueue();
            Assert.assertTrue("cap=1 must keep at most one cell open", openCellCount("c") <= 1);
            insBatch("B", 15);
            drainWalQueue();
            Assert.assertTrue("cap=1 must keep at most one cell open", openCellCount("c") <= 1);
            insBatch("A", 16);
            drainWalQueue();
            Assert.assertTrue("cap=1 must keep at most one cell open", openCellCount("c") <= 1);
            long after = TableWriter.getCompositeFastAppendCommittedCount();
            Assert.assertEquals(
                    "each alternating commit must still fast-append despite LRU eviction",
                    before + 3, after);

            engine.releaseInactive();
            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
        });
    }

    // Reads the composite WAL-apply writer's live open-cell-handle count. drainWalQueue() parks that same
    // pooled writer (cache intact) after applying; getWriterUnsafe returns that instance. Read the count
    // BEFORE the try-with-resources close (a pool-return may roll back, which drops the cache).
    private int openCellCount(String table) {
        try (TableWriter w = engine.getWriterUnsafe(engine.verifyTableName(table), "test")) {
            return w.getCompositeFastAppendOpenCellCount();
        }
    }

    private static String tsOf(int hour) {
        return String.format("2020-01-01T%02d:00:00.000000Z", hour);
    }

    // One single-cell (exch='A') row carrying the indexed symbol sub='X', into both c and p.
    private void insIndexed(int hour, double px) throws SqlException {
        String v = "('" + tsOf(hour) + "','A','X'," + px + ")";
        execute("insert into c values " + v);
        execute("insert into p values " + v);
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    // One single-cell commit (all rows share exch) of one or more strictly-increasing-ts rows, inserted
    // into BOTH the composite table c and the plain twin p (identical rows) -- the caller drains.
    private void insBatch(String exch, int... hours) throws SqlException {
        StringBuilder v = new StringBuilder();
        for (int i = 0; i < hours.length; i++) {
            if (i > 0) {
                v.append(", ");
            }
            v.append("('").append(tsOf(hours[i])).append("','").append(exch).append("',").append(1.0 + hours[i] * 0.01).append(')');
        }
        execute("insert into c values " + v);
        execute("insert into p values " + v);
    }

    // One single-row commit (exch='A') into two named tables with the identical literal.
    private void insTwo(String t1, String t2, int hour, double px) throws SqlException {
        String v = "('" + tsOf(hour) + "','A'," + px + ")";
        execute("insert into " + t1 + " values " + v);
        execute("insert into " + t2 + " values " + v);
    }
}
