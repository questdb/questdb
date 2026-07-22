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
 * Composite MULTI-cell fast-append (composite-partitioning fast-append spec 2, Task 3 -- the crux: an
 * eligible multi-cell commit actually FAST-APPENDS every touched cell into its own kept-open segment
 * and folds all N {@code (ts, cellKey)} size bumps into ONE {@code _txn} commit, instead of merely
 * being counted as in Task 1). Flag ON.
 * <p>
 * Correctness oracles (a passing differential test cannot be vacuous, because the fast-append counter
 * must also fire):
 * <ul>
 *     <li>a plain twin {@code p} fed identical rows (the plain twin O3-sorts a globally out-of-order
 *     commit), and</li>
 *     <li>a full-path composite twin {@code c1} fed every row in ONE commit (its first commit spans
 *     brand-new cells, so it never fast-appends -- a pure full-O3-path composite oracle). {@code c ==
 *     c1} directly proves the multi-cell fast-append is byte-for-byte equivalent to the proven full
 *     path.</li>
 * </ul>
 * across scan / per-cell / count / group-by / {@code LATEST ON} / {@code SAMPLE BY}, AND {@link
 * TableWriter#getCompositeMultiCellFastAppendCommittedCount()} (the "actually multi-cell fast-appended"
 * counter) advancing by exactly the number of eligible commits.
 * <p>
 * NOTE on {@code LATEST ON}: its output ROW ORDER legitimately differs between a plain table (rows
 * ordered by their latest timestamp) and a composite table (rows ordered by cell), so the comparison
 * adds an explicit {@code ORDER BY exch} -- exactly as spec-1's {@link CompositeFastAppendTest} does for
 * the identical reason. This is a query-result-ordering property of composite tables, unrelated to the
 * write path under test.
 */
public class CompositeMultiCellFastAppendTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_FASTAPPEND_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testGlobalOrderMultiCellCommitsMatchTwinAndSingleCommitAndFastAppend() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
            execute("create table c1 (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            // Commit 0: route the table (interns the first real cell; isRoutedComposite() is false until
            // a real per-cell dispatch runs). Not itself a fast-append.
            execute("insert into c values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");

            // c1 one-commit oracle: accumulate EVERY row (routing row + seeds + multi commits) to feed in
            // one full-path commit at the end, so c1 == c == p regardless of how c/p got there.
            StringBuilder c1All = new StringBuilder();
            appendRow(c1All, "2020-01-01T00:00:00.000000Z", "R0", 0.0);

            // Seed + warm cells A, B, C (each pre-existing, non-empty, cache-warm at 00:05) so a later
            // multi-cell commit into all three is append-only-eligible.
            seedCell("A", "2020-01-01T00:05:00.000000Z", 1.0);
            seedCell("B", "2020-01-01T00:05:00.000000Z", 2.0);
            seedCell("C", "2020-01-01T00:05:00.000000Z", 3.0);
            appendRow(c1All, "2020-01-01T00:05:00.000000Z", "A", 1.0);
            appendRow(c1All, "2020-01-01T00:05:00.000000Z", "B", 2.0);
            appendRow(c1All, "2020-01-01T00:05:00.000000Z", "C", 3.0);

            // N globally-ordered multi-cell commits, each spanning A,B,C in increasing ts, each commit
            // strictly after the previous. Every one must multi-cell fast-append.
            final int commits = 5;
            long before = TableWriter.getCompositeMultiCellFastAppendCommittedCount();
            for (int k = 0; k < commits; k++) {
                final String tA = "2020-01-01T00:" + two(10 + k) + ":00.000000Z";
                final String tB = "2020-01-01T00:" + two(10 + k) + ":01.000000Z";
                final String tC = "2020-01-01T00:" + two(10 + k) + ":02.000000Z";
                final double vA = 1.1 + k;
                final double vB = 2.1 + k;
                final double vC = 3.1 + k;
                final String rows = "('" + tA + "','A'," + vA + "),"
                        + "('" + tB + "','B'," + vB + "),"
                        + "('" + tC + "','C'," + vC + ")";
                execute("insert into c values " + rows);
                execute("insert into p values " + rows);
                drainWalQueue();
                assertWalTableNotSuspended("c");
                appendRow(c1All, tA, "A", vA);
                appendRow(c1All, tB, "B", vB);
                appendRow(c1All, tC, "C", vC);
            }
            long after = TableWriter.getCompositeMultiCellFastAppendCommittedCount();
            Assert.assertEquals(
                    "every globally-ordered append-only commit spanning cells A,B,C must multi-cell fast-append",
                    before + commits, after);

            // c1: identical rows, one full-path commit (a pure full-O3-path composite oracle).
            execute("insert into c1 values " + c1All);
            drainWalQueue();

            engine.releaseInactive();
            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            assertWalTableNotSuspended("c1");

            // c (multi-cell fast-append) == plain twin == single-commit composite, every way.
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select ts, exch, px from c1 order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors("select count() from c1", "select count() from c");
            assertSqlCursors("select ts, exch, px from p where exch='B' order by ts", "select ts, exch, px from c where exch='B' order by ts");
            assertSqlCursors(
                    "select exch, count() from p group by exch order by exch",
                    "select exch, count() from c group by exch order by exch");
            assertSqlCursors(
                    "select ts, exch, px from p latest on ts partition by exch order by exch",
                    "select ts, exch, px from c latest on ts partition by exch order by exch");
            assertSqlCursors("select ts, sum(px) from p sample by 1h ALIGN TO CALENDAR", "select ts, sum(px) from c sample by 1h ALIGN TO CALENDAR");
        });
    }

    @Test
    public void testPerSymbolInterleavedMultiCellCommitsMatchTwinAndFastAppend() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
            execute("create table c1 (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            execute("insert into c values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            execute("insert into p values ('2020-01-01T00:00:00.000000Z','R0',0.0)");
            drainWalQueue();
            assertWalTableNotSuspended("c");
            StringBuilder c1All = new StringBuilder();
            appendRow(c1All, "2020-01-01T00:00:00.000000Z", "R0", 0.0);

            seedCell("A", "2020-01-01T00:05:00.000000Z", 1.0);
            seedCell("B", "2020-01-01T00:05:00.000000Z", 2.0);
            appendRow(c1All, "2020-01-01T00:05:00.000000Z", "A", 1.0);
            appendRow(c1All, "2020-01-01T00:05:00.000000Z", "B", 2.0);

            // Each commit interleaves A and B so GLOBAL order is non-monotonic (a ts drop from A's block
            // to B's block) but each symbol is internally ordered (A@hh:40 < hh:41, B@hh:20 < hh:21). The
            // plain twin O3-sorts each commit; the composite multi-cell-fast-appends each (A's two rows
            // to cell A, B's two rows to cell B, one _txn). Three commits, each strictly after the
            // previous per cell, exercise the N-cell handle cache reuse across commits.
            final int commits = 3;
            long before = TableWriter.getCompositeMultiCellFastAppendCommittedCount();
            for (int h = 1; h <= commits; h++) {
                final String aLo = "2020-01-01T" + two(h) + ":40:00.000000Z";
                final String bLo = "2020-01-01T" + two(h) + ":20:00.000000Z";
                final String aHi = "2020-01-01T" + two(h) + ":41:00.000000Z";
                final String bHi = "2020-01-01T" + two(h) + ":21:00.000000Z";
                // Buffer order A,B,A,B with ts 40,20,41,21 -> globally OOO, per-cell ordered.
                final String rows = "('" + aLo + "','A'," + (1.1 + h) + "),"
                        + "('" + bLo + "','B'," + (2.1 + h) + "),"
                        + "('" + aHi + "','A'," + (1.2 + h) + "),"
                        + "('" + bHi + "','B'," + (2.2 + h) + ")";
                execute("insert into c values " + rows);
                execute("insert into p values " + rows);
                drainWalQueue();
                assertWalTableNotSuspended("c");
                appendRow(c1All, aLo, "A", 1.1 + h);
                appendRow(c1All, bLo, "B", 2.1 + h);
                appendRow(c1All, aHi, "A", 1.2 + h);
                appendRow(c1All, bHi, "B", 2.2 + h);
            }
            long after = TableWriter.getCompositeMultiCellFastAppendCommittedCount();
            Assert.assertEquals(
                    "every globally-out-of-order but per-cell-ordered multi-cell commit must fast-append",
                    before + commits, after);

            // c1: identical rows fed in ONE commit (a pure full-O3-path composite oracle -- it O3-sorts
            // the interleaved rows into the same per-cell layout the fast-append built incrementally).
            execute("insert into c1 values " + c1All);
            drainWalQueue();

            engine.releaseInactive();
            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            assertWalTableNotSuspended("c1");

            // c (multi-cell fast-append, each cell physically appended in ts order) == plain twin (O3'd)
            // AND == full-path composite twin c1 (the direct fast-append-equals-full-path proof).
            assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select ts, exch, px from c1 order by ts, exch", "select ts, exch, px from c order by ts, exch");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors("select count() from c1", "select count() from c");
            assertSqlCursors("select ts, exch, px from p where exch='A' order by ts", "select ts, exch, px from c where exch='A' order by ts");
            assertSqlCursors("select ts, exch, px from p where exch='B' order by ts", "select ts, exch, px from c where exch='B' order by ts");
            assertSqlCursors(
                    "select exch, count() from p group by exch order by exch",
                    "select exch, count() from c group by exch order by exch");
            assertSqlCursors(
                    "select ts, exch, px from p latest on ts partition by exch order by exch",
                    "select ts, exch, px from c latest on ts partition by exch order by exch");
            assertSqlCursors("select ts, sum(px) from p sample by 1h ALIGN TO CALENDAR", "select ts, sum(px) from c sample by 1h ALIGN TO CALENDAR");
        });
    }

    private static void appendRow(StringBuilder sb, String ts, String exch, double px) {
        if (sb.length() > 0) {
            sb.append(',');
        }
        sb.append("('").append(ts).append("','").append(exch).append("',").append(px).append(')');
    }

    private static String two(int v) {
        return v < 10 ? "0" + v : Integer.toString(v);
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    /**
     * Inserts one row for a brand-new {@code exch} cell into both {@code c} and its plain twin {@code
     * p}, then drains the WAL so the cell pre-exists (non-empty, real {@code _txn} entry) and its max
     * timestamp is warmed into the shared per-cell max cache before any later multi-cell commit into it
     * is exercised.
     */
    private void seedCell(String exch, String ts, double px) throws SqlException {
        execute("insert into c values ('" + ts + "','" + exch + "'," + px + ")");
        execute("insert into p values ('" + ts + "','" + exch + "'," + px + ")");
        drainWalQueue();
        assertWalTableNotSuspended("c");
    }
}
