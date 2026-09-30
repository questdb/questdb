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

package io.questdb.test.cairo;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Twin equivalence across a READER RELOAD: a composite table must return exactly what its plain
 * (day-only) twin returns at EVERY point in an INSERT / read / INSERT / read sequence, not just at
 * the end.
 * <p>
 * The bug this pins: a read performed BETWEEN two commits leaves a warm, pooled reader behind. On
 * the next read that reader takes {@code TableReader#reconcileOpenPartitions}' fast path, which
 * refreshes the size of the LAST open partition only. On a plain table that is sound -- the only
 * partition an in-order append can grow is the last one in timestamp order, and anything else bumps
 * the partition table version and forces the full {@code reconcileOpenPartitions0} merge. On a
 * COMPOSITE table it is not: partitions are ordered by {@code (ts, cellKey)}, so a commit that
 * appends in-cell-order rows to a NON-last cell of the current day grows a partition the fast path
 * never looks at. The stale cell keeps serving its pre-extension row count, silently.
 * <p>
 * The signature is {@code count()} and the scan DISAGREEING: {@code count()} is served from the
 * {@code _txn} row count and stays correct, while the scan walks the reader's open partitions and
 * comes up short. Every test here asserts both, plus the full ordered scan against the twin --
 * asserting {@code count()} alone would have passed throughout.
 * <p>
 * Note what these tests must NOT do: no {@code engine.releaseInactive()} between the commits. That
 * discards the pooled reader, forces a cold open, and hides the bug entirely -- which is exactly why
 * the existing write-then-read composite suites all passed.
 */
public class CompositeReaderReloadTest extends AbstractCairoTest {

    /**
     * Shape 1 -- the original minimal repro. Two cells in one day; the read between the commits is
     * the TRIGGER; the second commit extends the EXISTING, non-last BTC cell with a row that is
     * in-order WITHIN that cell (00:00:01 &gt; 00:00:00) even though it is out-of-order for the day
     * as a whole (00:00:01 &lt; 00:00:02). Pre-fix: {@code count()} returned 3, the scan returned 2.
     */
    @Test(timeout = 60_000)
    public void testExtendExistingCellAfterReadMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:02.000000Z','ETH',2.0)");
            // THE TRIGGER: opens (and pools) a reader positioned on the pre-extension cell sizes.
            assertTwinEquivalence(2);

            insertIntoBothAndDrain("('2023-01-01T00:00:01.000000Z','BTC',3.0)");
            assertTwinEquivalence(3);
        });
    }

    /**
     * Shape 1, twice over: the same non-last cell is extended by two successive commits, each with a
     * triggering read in front of it. Catches a fix that happens to repair only the first reload.
     */
    @Test(timeout = 60_000)
    public void testExtendExistingCellRepeatedlyMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:09.000000Z','ETH',2.0)");
            assertTwinEquivalence(2);

            insertIntoBothAndDrain("('2023-01-01T00:00:01.000000Z','BTC',3.0)");
            assertTwinEquivalence(3);

            insertIntoBothAndDrain("('2023-01-01T00:00:02.000000Z','BTC',4.0)");
            assertTwinEquivalence(4);

            insertIntoBothAndDrain("('2023-01-01T00:00:03.000000Z','BTC',5.0)");
            assertTwinEquivalence(5);
        });
    }

    /**
     * Shape 2 -- a brand-new cell in an existing day. Creating a cell adds a partition, which bumps
     * the partition table version and therefore takes the FULL reconcile path; expected to have been
     * correct already. Asserted so a future fix cannot regress it.
     */
    @Test(timeout = 60_000)
    public void testNewCellInExistingDayAfterReadMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:02.000000Z','ETH',2.0)");
            assertTwinEquivalence(2);

            insertIntoBothAndDrain("('2023-01-01T00:00:03.000000Z','SOL',3.0)");
            assertTwinEquivalence(3);
        });
    }

    /**
     * Shape 3 -- a new day. The plain twin's own "append a new partition" path; expected correct
     * already on both sides.
     */
    @Test(timeout = 60_000)
    public void testNewDayAfterReadMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:02.000000Z','ETH',2.0)");
            assertTwinEquivalence(2);

            insertIntoBothAndDrain("('2023-01-02T00:00:00.000000Z','BTC',3.0)");
            assertTwinEquivalence(3);

            // ... and then extend a non-last cell of the NEW day, with a read in between.
            insertIntoBothAndDrain("('2023-01-02T00:00:05.000000Z','ETH',4.0)");
            assertTwinEquivalence(4);

            insertIntoBothAndDrain("('2023-01-02T00:00:01.000000Z','BTC',5.0)");
            assertTwinEquivalence(5);
        });
    }

    /**
     * Shape 4 -- several existing cells extended by ONE commit, with a read in front of it. Every
     * cell but the last is invisible to the fast path, so this loses more than one row pre-fix.
     */
    @Test(timeout = 60_000)
    public void testExtendSeveralCellsAtOnceAfterReadMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:01.000000Z','ETH',2.0)," +
                            "('2023-01-01T00:00:02.000000Z','SOL',3.0)");
            assertTwinEquivalence(3);

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:03.000000Z','BTC',4.0)," +
                            "('2023-01-01T00:00:04.000000Z','ETH',5.0)," +
                            "('2023-01-01T00:00:05.000000Z','SOL',6.0)");
            assertTwinEquivalence(6);
        });
    }

    /**
     * A dimension-pruned read (WHERE exch = 'BTC') between the commits. Pruning selects cells, so it
     * can mask or worsen the staleness -- and it is the read shape composite partitioning exists to
     * make fast, so it must be twin-equivalent too.
     */
    @Test(timeout = 60_000)
    public void testDimensionPrunedReadBetweenCommitsMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:02.000000Z','ETH',2.0)");

            // The ONLY read between the commits is the pruned one -- it opens a reader that has
            // touched the BTC cell and nothing else.
            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'BTC' order by ts",
                    "select ts, exch, px from c where exch = 'BTC' order by ts");

            insertIntoBothAndDrain("('2023-01-01T00:00:01.000000Z','BTC',3.0)");

            assertSqlCursors(
                    "select ts, exch, px from p where exch = 'BTC' order by ts",
                    "select ts, exch, px from c where exch = 'BTC' order by ts");
            assertTwinEquivalence(3);
        });
    }

    /**
     * The same reload hazard on a HASH dimension -- a different cellKey derivation, same
     * {@code (ts, cellKey)} partition ordering. 'BTC' and 'ETH' need not land in different buckets,
     * so this asserts twin equivalence rather than a particular cell layout.
     */
    @Test(timeout = 60_000)
    public void testExtendExistingCellAfterReadOnHashDimension() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, hash(exch, 4) wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','BTC',1.0)," +
                            "('2023-01-01T00:00:02.000000Z','ETH',2.0)," +
                            "('2023-01-01T00:00:04.000000Z','SOL',3.0)," +
                            "('2023-01-01T00:00:06.000000Z','XRP',4.0)");
            assertTwinEquivalence(4);

            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:01.000000Z','BTC',5.0)," +
                            "('2023-01-01T00:00:03.000000Z','ETH',6.0)");
            assertTwinEquivalence(6);
        });
    }

    /**
     * Two dimensions. The cell tuple is wider, but the reader still merges on a single interned
     * {@code cellKey}, so the same hazard applies to the non-last tuple.
     */
    @Test(timeout = 60_000)
    public void testExtendExistingCellAfterReadOnTwoDimensions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exch symbol, side symbol, px double) timestamp(ts) partition by day, exch, side wal");
            execute("create table p (ts timestamp, exch symbol, side symbol, px double) timestamp(ts) partition by day wal");

            execute("insert into c values ('2023-01-01T00:00:00.000000Z','BTC','BUY',1.0),('2023-01-01T00:00:04.000000Z','ETH','SELL',2.0)");
            execute("insert into p values ('2023-01-01T00:00:00.000000Z','BTC','BUY',1.0),('2023-01-01T00:00:04.000000Z','ETH','SELL',2.0)");
            drainWalQueue();
            assertSqlCursors("select ts, exch, side, px from p order by ts", "select ts, exch, side, px from c order by ts");

            execute("insert into c values ('2023-01-01T00:00:01.000000Z','BTC','BUY',3.0)");
            execute("insert into p values ('2023-01-01T00:00:01.000000Z','BTC','BUY',3.0)");
            drainWalQueue();

            assertSqlCursors("select ts, exch, side, px from p order by ts", "select ts, exch, side, px from c order by ts");
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors("select count(px) from p", "select count(px) from c");
        });
    }

    /**
     * Control: the identical sequence on a table with NO dimension must be untouched by any fix
     * here. This is the plain path asserted against itself.
     */
    @Test(timeout = 60_000)
    public void testPlainTableUnaffectedByReadBetweenCommits() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            execute("insert into p values ('2023-01-01T00:00:00.000000Z','BTC',1.0),('2023-01-01T00:00:02.000000Z','ETH',2.0)");
            drainWalQueue();
            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n2\n");

            execute("insert into p values ('2023-01-01T00:00:01.000000Z','BTC',3.0)");
            drainWalQueue();

            assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            assertQuery("select count(px) from p").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
            // Single plain execution path on purpose: assertQuery(...)'s battery also asserts cursor
            // METADATA (declared timestamp, random access, size), which is orthogonal to this
            // control's subject and rejects a designated-timestamp cursor here. The control only
            // needs to show the plain twin returns all three rows after the same interleaving.
            printSql("select ts, exch, px from p order by ts");
            TestUtils.assertEquals(
                    "ts\texch\tpx\n" +
                            "2023-01-01T00:00:00.000000Z\tBTC\t1.0\n" +
                            "2023-01-01T00:00:01.000000Z\tBTC\t3.0\n" +
                            "2023-01-01T00:00:02.000000Z\tETH\t2.0\n",
                    sink);
        });
    }

    /**
     * Asserts the composite table matches its plain twin AND that its own {@code count()} agrees
     * with a scan of the same rows.
     * <p>
     * {@code count()} is served from the {@code _txn} row count and is correct throughout this bug;
     * {@code count(px)} must read the {@code px} column out of every open partition, so it is a
     * genuine scan. Their disagreement -- with {@code px} never null in these tests -- IS the
     * signature. {@code expectedRows} is stated explicitly so a fix that broke BOTH sides
     * symmetrically (or a vacuous twin) still fails.
     */
    private void assertTwinEquivalence(long expectedRows) throws Exception {
        final String expectedCount = "count\n" + expectedRows + "\n";

        // _txn row count.
        assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns(expectedCount);
        // Scan of the open partitions -- must agree with the above.
        assertQuery("select count(px) from c").noLeakCheck().noRandomAccess().expectSize().returns(expectedCount);
        assertQuery("select count() from p").noLeakCheck().noRandomAccess().expectSize().returns(expectedCount);
        assertQuery("select count(px) from p").noLeakCheck().noRandomAccess().expectSize().returns(expectedCount);

        // Full ordered scan, against the twin.
        assertSqlCursors("select ts, exch, px from p order by ts, exch", "select ts, exch, px from c order by ts, exch");
        assertSqlCursors("select ts, exch, px from p order by ts desc, exch", "select ts, exch, px from c order by ts desc, exch");
        assertSqlCursors("select exch, count() from p order by exch", "select exch, count() from c order by exch");
        assertSqlCursors(
                "select ts, exch, px from p where exch = 'BTC' order by ts",
                "select ts, exch, px from c where exch = 'BTC' order by ts");
        assertSqlCursors(
                "select ts, exch, px from p latest on ts partition by exch order by exch",
                "select ts, exch, px from c latest on ts partition by exch order by exch");
    }

    private void createTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
    }

    private void insertIntoBothAndDrain(String valuesTuples) throws SqlException {
        execute("insert into c values " + valuesTuples);
        execute("insert into p values " + valuesTuples);
        drainWalQueue();
    }
}
