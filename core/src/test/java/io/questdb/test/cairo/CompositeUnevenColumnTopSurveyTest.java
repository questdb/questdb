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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every supported composite operation, run on cells with DIFFERENT COLUMN TOPS, against a plain twin.
 * <p>
 * <b>Why this class exists.</b> A cell-blind resolver returns the WRONG answer only when the instances
 * disagree. If a fixture makes every cell's column top equal, cellKey 0's blind answer is correct for
 * every cell and the test passes while the defect is live. That is not hypothetical: it is exactly how
 * {@code CONVERT PARTITION TO PARQUET} destroyed data in non-zero cells while four green tests covered
 * it -- all four inserted once and never ran {@code ADD COLUMN}, so every top was 0.
 * <p>
 * So this sweeps the composite feature surface under the condition that actually discriminates. Every
 * table here runs {@code ADD COLUMN} while its cells hold DIFFERENT row counts (BTC 3, ETH 1), giving
 * tops of 3 and 1. Each operation is then compared against a plain table carrying identical rows.
 * <p>
 * <b>The oracle is the twin, never a hand-written string.</b> The failure modes in this area are a
 * missing row, a NULLed value and a wrong order; an expected-string written from belief can encode any
 * of them. {@link #assertMatchesTwin} additionally asserts the twin still carries the late-added
 * column's values, so a fixture that stopped producing them fails loudly rather than passing
 * vacuously.
 */
public class CompositeUnevenColumnTopSurveyTest extends AbstractCairoTest {

    @Test
    public void testAddThenDropColumn() throws Exception {
        runTwinned("add_drop", (t) -> {
            execute("ALTER TABLE " + t + " ADD COLUMN extra INT");
            execute("ALTER TABLE " + t + " DROP COLUMN px");
        }, "SELECT ts, exch, tag, extra FROM %s ORDER BY ts");
    }

    @Test
    public void testDedupUpsertKeys() throws Exception {
        runTwinned("dedup", (t) -> {
            execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts, exch)");
            // a duplicate (ts, exch) that must REPLACE, carrying a new tag value
            execute("INSERT INTO " + t + " VALUES ('2023-10-01T06:00:00.000000Z','ETH',66.0,'E9')");
        }, DEFAULT_QUERY);
    }

    @Test
    public void testDropOnePartition() throws Exception {
        runTwinned("drop_part", (t) ->
                execute("ALTER TABLE " + t + " DROP PARTITION LIST '2023-10-02'"), DEFAULT_QUERY);
    }

    @Test
    public void testIndexedWhereOnLateAddedColumn() throws Exception {
        runTwinned("idx_where", (t) -> {
            execute("ALTER TABLE " + t + " ALTER COLUMN tag ADD INDEX");
            drainWalQueue();
        }, "SELECT ts, exch, tag FROM %s WHERE tag = 'E1' ORDER BY ts");
    }

    @Test
    public void testLatestOn() throws Exception {
        runTwinned("latest", (t) -> {
        }, "SELECT ts, exch, tag FROM %s LATEST ON ts PARTITION BY exch");
    }

    @Test
    public void testO3InsertIntoExistingCell() throws Exception {
        runTwinned("o3", (t) ->
                // BEFORE the cell's existing rows, carrying a value for the late-added column
                execute("INSERT INTO " + t + " VALUES ('2023-10-01T00:30:00.000000Z','ETH',9.0,'E0')"),
                DEFAULT_QUERY);
    }

    @Test
    public void testReindexAfterAddIndex() throws Exception {
        runTwinned("reindex", (t) -> {
            execute("ALTER TABLE " + t + " ALTER COLUMN tag ADD INDEX");
            drainWalQueue();
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            execute("REINDEX TABLE " + t + " COLUMN tag LOCK EXCLUSIVE");
        }, "SELECT ts, exch, tag FROM %s WHERE tag = 'E1' ORDER BY ts");
    }

    @Test
    public void testDropIndexOnLateAddedColumn() throws Exception {
        runTwinned("drop_idx", (t) -> {
            execute("ALTER TABLE " + t + " ALTER COLUMN tag ADD INDEX");
            drainWalQueue();
            execute("ALTER TABLE " + t + " ALTER COLUMN tag DROP INDEX");
        }, DEFAULT_QUERY);
    }

    @Test
    public void testSampleByOverLateAddedColumn() throws Exception {
        runTwinned("sample", (t) -> {
        }, "SELECT ts, count() c FROM %s WHERE tag IS NOT NULL SAMPLE BY 1d ORDER BY ts");
    }

    @Test
    public void testSquashPartitions() throws Exception {
        runTwinned("squash", (t) ->
                execute("ALTER TABLE " + t + " SQUASH PARTITIONS"), DEFAULT_QUERY);
    }

    // ------------------------------------------------------------------------------------------
    // PARQUET variants. The same operations, applied to cells that have been tiered to parquet --
    // the cold-storage state. Two of this branch's six cell-blindness defects lived on parquet paths
    // (the CONVERT encoder and the ALTER TYPE column-top propagation), and the native survey above
    // cannot reach either, so each operation is repeated with a CONVERT in front of it.
    // ------------------------------------------------------------------------------------------

    @Test
    public void testParquetAddThenDropColumn() throws Exception {
        runTwinnedOverParquet("pq_add_drop", (t) -> {
            execute("ALTER TABLE " + t + " ADD COLUMN extra INT");
            execute("ALTER TABLE " + t + " DROP COLUMN px");
        }, "SELECT ts, exch, tag, extra FROM %s ORDER BY ts");
    }

    @Test
    public void testParquetDropOnePartition() throws Exception {
        runTwinnedOverParquet("pq_drop_part", (t) ->
                execute("ALTER TABLE " + t + " DROP PARTITION LIST '2023-10-01'"), DEFAULT_QUERY);
    }

    @Test
    public void testParquetLatestOn() throws Exception {
        runTwinnedOverParquet("pq_latest", (t) -> {
        }, "SELECT ts, exch, tag FROM %s LATEST ON ts PARTITION BY exch");
    }

    @Test
    public void testParquetSampleBy() throws Exception {
        runTwinnedOverParquet("pq_sample", (t) -> {
        }, "SELECT ts, count() c FROM %s WHERE tag IS NOT NULL SAMPLE BY 1d ORDER BY ts");
    }

    @Test
    public void testParquetSelectWithFilterOnLateAddedColumn() throws Exception {
        runTwinnedOverParquet("pq_filter", (t) -> {
        }, "SELECT ts, exch, tag FROM %s WHERE tag = 'E1' ORDER BY ts");
    }

    @Test
    public void testParquetDedupUpsertKeys() throws Exception {
        runTwinnedOverParquet("pq_dedup", (t) -> {
            execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts, exch)");
            execute("INSERT INTO " + t + " VALUES ('2023-10-01T06:00:00.000000Z','ETH',66.0,'E9')");
        }, DEFAULT_QUERY);
    }

    /**
     * Same as {@link #runTwinned} but tiers the first day to PARQUET on both tables before applying
     * {@code op}. Asserts the twin match BEFORE the operation too, so a failure afterwards is
     * attributable to {@code op} rather than to the conversion.
     */
    private void runTwinnedOverParquet(String name, Op op, String query) throws Exception {
        assertMemoryLeak(() -> {
            final String c = "c_" + name;
            final String p = "p_" + name;
            createUnevenCells(c, ", exch");
            createUnevenCells(p, "");

            execute("ALTER TABLE " + c + " CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE " + p + " CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            assertLive(c);
            // PRECONDITION -- otherwise a later failure could be blamed on op when CONVERT caused it.
            assertMatchesTwin(c, p, DEFAULT_QUERY);

            op.apply(c);
            op.apply(p);
            drainWalQueue();

            assertLive(c);
            assertMatchesTwin(c, p, query);
        });
    }

    private static final String DEFAULT_QUERY = "SELECT ts, exch, tag FROM %s ORDER BY ts";

    // ------------------------------------------------------------------------------------------
    // SECOND VACUITY AXIS: the DIRECTION of the top skew, and the NUMBER of cells.
    //
    // createUnevenCells gives cell 0 the LARGER top (BTC 3, ETH 1), so a cell-blind read yields a
    // too-LARGE top for cell 1 and its rows read as absent -- data goes missing. The opposite skew is
    // a different failure mode: a too-SMALL top makes a cell read rows that predate its column, which
    // surfaces as garbage or duplicated values rather than absence. Neither the tests above nor any
    // earlier test in this branch covers that direction.
    //
    // And every fixture so far has exactly TWO cells, so cellKey is only ever 0 or 1. A resolver that
    // is off by one, or that reads "the last cell" rather than cellKey 0, would answer correctly for
    // both. Three cells distinguishes those.
    // ------------------------------------------------------------------------------------------

    @Test
    public void testReversedSkewConvertToParquet() throws Exception {
        assertMemoryLeak(() -> {
            createReversedSkewCells("c_rev_pq", ", exch");
            createReversedSkewCells("p_rev_pq", "");
            execute("ALTER TABLE c_rev_pq CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p_rev_pq CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            assertLive("c_rev_pq");
            assertMatchesTwin("c_rev_pq", "p_rev_pq", DEFAULT_QUERY);
        });
    }

    @Test
    public void testReversedSkewIndexedWhere() throws Exception {
        assertMemoryLeak(() -> {
            createReversedSkewCells("c_rev_idx", ", exch");
            createReversedSkewCells("p_rev_idx", "");
            execute("ALTER TABLE c_rev_idx ALTER COLUMN tag ADD INDEX");
            execute("ALTER TABLE p_rev_idx ALTER COLUMN tag ADD INDEX");
            drainWalQueue();
            assertLive("c_rev_idx");
            assertMatchesTwin("c_rev_idx", "p_rev_idx",
                    "SELECT ts, exch, tag FROM %s WHERE tag = 'B1' ORDER BY ts");
        });
    }

    @Test
    public void testThreeCellsConvertToParquet() throws Exception {
        assertMemoryLeak(() -> {
            createThreeCells("c_3_pq", ", exch");
            createThreeCells("p_3_pq", "");
            execute("ALTER TABLE c_3_pq CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p_3_pq CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            assertLive("c_3_pq");
            assertMatchesTwin("c_3_pq", "p_3_pq", DEFAULT_QUERY);
        });
    }

    @Test
    public void testThreeCellsIndexedWhereOnLastCell() throws Exception {
        assertMemoryLeak(() -> {
            createThreeCells("c_3_idx", ", exch");
            createThreeCells("p_3_idx", "");
            execute("ALTER TABLE c_3_idx ALTER COLUMN tag ADD INDEX");
            execute("ALTER TABLE p_3_idx ALTER COLUMN tag ADD INDEX");
            drainWalQueue();
            assertLive("c_3_idx");
            // 'S1' lives in the THIRD cell (cellKey 2) -- unreachable by any two-cell fixture.
            assertMatchesTwin("c_3_idx", "p_3_idx",
                    "SELECT ts, exch, tag FROM %s WHERE tag = 'S1' ORDER BY ts");
        });
    }

    @Test
    public void testThreeCellsO3IntoMiddleCell() throws Exception {
        assertMemoryLeak(() -> {
            createThreeCells("c_3_o3", ", exch");
            createThreeCells("p_3_o3", "");
            execute("INSERT INTO c_3_o3 VALUES ('2023-10-01T00:30:00.000000Z','ETH',9.0,'E0')");
            execute("INSERT INTO p_3_o3 VALUES ('2023-10-01T00:30:00.000000Z','ETH',9.0,'E0')");
            drainWalQueue();
            assertLive("c_3_o3");
            assertMatchesTwin("c_3_o3", "p_3_o3", DEFAULT_QUERY);
        });
    }

    /** Cell 0 gets the SMALLER top: BTC 1 row before ADD COLUMN, ETH 3. */
    private void createReversedSkewCells(String name, String dimension) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY" + dimension + " WAL");
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T01:00:00.000000Z','BTC',1.0),"
                + "('2023-10-01T02:00:00.000000Z','ETH',2.0),"
                + "('2023-10-01T03:00:00.000000Z','ETH',3.0),"
                + "('2023-10-01T04:00:00.000000Z','ETH',4.0)");
        drainWalQueue();
        execute("ALTER TABLE " + name + " ADD COLUMN tag SYMBOL");
        drainWalQueue();
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T05:00:00.000000Z','BTC',5.0,'B1'),"
                + "('2023-10-01T06:00:00.000000Z','ETH',6.0,'E1')");
        drainWalQueue();
    }

    /** THREE cells with three DIFFERENT tops: BTC 3, ETH 2, SOL 1. */
    private void createThreeCells(String name, String dimension) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY" + dimension + " WAL");
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T01:00:00.000000Z','BTC',1.0),"
                + "('2023-10-01T02:00:00.000000Z','BTC',2.0),"
                + "('2023-10-01T03:00:00.000000Z','BTC',3.0),"
                + "('2023-10-01T04:00:00.000000Z','ETH',4.0),"
                + "('2023-10-01T05:00:00.000000Z','ETH',5.0),"
                + "('2023-10-01T06:00:00.000000Z','SOL',6.0)");
        drainWalQueue();
        execute("ALTER TABLE " + name + " ADD COLUMN tag SYMBOL");
        drainWalQueue();
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T07:00:00.000000Z','BTC',7.0,'B1'),"
                + "('2023-10-01T08:00:00.000000Z','ETH',8.0,'E1'),"
                + "('2023-10-01T09:00:00.000000Z','SOL',9.0,'S1')");
        drainWalQueue();
    }

    /**
     * Builds a composite table and a plain twin with identical rows and UNEVEN column tops, applies
     * {@code op} to each, then asserts the two answer {@code query} identically.
     */
    private void runTwinned(String name, Op op, String query) throws Exception {
        assertMemoryLeak(() -> {
            final String c = "c_" + name;
            final String p = "p_" + name;
            createUnevenCells(c, ", exch");
            createUnevenCells(p, "");

            op.apply(c);
            op.apply(p);
            drainWalQueue();

            assertLive(c);
            assertMatchesTwin(c, p, query);
        });
    }

    /**
     * Asserts the table is live, and on failure reports the WAL error message rather than just the
     * fact of suspension -- a bare "suspended" tells you nothing about which gate or defect fired.
     */
    private void assertLive(String table) throws Exception {
        if (!engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(table))) {
            return;
        }
        final StringSink why = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext,
                "SELECT errorMessage FROM wal_tables() WHERE name = '" + table + "'", why);
        Assert.fail(table + " suspended: " + why.toString().replace('\n', ' '));
    }

    private void assertMatchesTwin(String composite, String plain, String query) throws Exception {
        final StringSink cs = new StringSink();
        final StringSink ps = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, String.format(query, composite), cs);
        TestUtils.printSql(engine, sqlExecutionContext, String.format(query, plain), ps);
        // Non-vacuous: the twin's answer must have more than a header row, or every comparison below
        // is trivially satisfied.
        Assert.assertTrue("twin produced no rows -- the comparison would be vacuous: " + query,
                ps.toString().indexOf('\n') < ps.length() - 1);
        TestUtils.assertEquals(composite + " differs from plain twin " + plain + " for: " + query,
                ps, cs);
    }

    /**
     * BTC holds 3 rows and ETH 1 when {@code tag} is added, so their column tops are 3 and 1. A second
     * day gives DROP PARTITION and SQUASH something to act on.
     */
    private void createUnevenCells(String name, String dimension) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY" + dimension + " WAL");
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T01:00:00.000000Z','BTC',1.0),"
                + "('2023-10-01T02:00:00.000000Z','BTC',2.0),"
                + "('2023-10-01T03:00:00.000000Z','BTC',3.0),"
                + "('2023-10-01T04:00:00.000000Z','ETH',4.0)");
        drainWalQueue();
        execute("ALTER TABLE " + name + " ADD COLUMN tag SYMBOL");
        drainWalQueue();
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T05:00:00.000000Z','BTC',5.0,'B1'),"
                + "('2023-10-01T06:00:00.000000Z','ETH',6.0,'E1'),"
                + "('2023-10-02T01:00:00.000000Z','BTC',7.0,'B2'),"
                + "('2023-10-02T02:00:00.000000Z','ETH',8.0,'E2')");
        drainWalQueue();
    }

    @FunctionalInterface
    private interface Op {
        void apply(String table) throws Exception;
    }
}
