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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.security.ReadOnlySecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * ALTER MATERIALIZED VIEW ... DROP PARTITION removes view rows and nothing else: the view stays valid and keeps
 * its refresh position and cached refresh intervals. The view's WAL keeps the statement, and the partitions it
 * names drop when the WAL applies. A dropped range gets rows again only where a refresh recomputes it.
 * <p>
 * The base holds two rows a day, at 00:10 and 01:10, so the view, which keeps the last price of each symbol per
 * hour, holds two rows a day, one per partition hour.
 */
public class MatViewDropPartitionTest extends AbstractCairoTest {
    private static final String ALL_ROWS = """
            ts\tsym\tprice
            2024-01-01T00:00:00.000000Z\ta\t1.0
            2024-01-01T01:00:00.000000Z\tb\t2.0
            2024-01-02T00:00:00.000000Z\ta\t3.0
            2024-01-02T01:00:00.000000Z\tb\t4.0
            2024-01-03T00:00:00.000000Z\ta\t5.0
            2024-01-03T01:00:00.000000Z\tb\t6.0
            """;

    @Test
    public void testAlterTableDropPartitionFails() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView();
            for (String sql : new String[]{
                    "ALTER TABLE price_1h DROP PARTITION LIST '2024-01-01'",
                    "ALTER TABLE price_1h DROP PARTITION WHERE ts < '2024-01-02'"
            }) {
                assertExceptionNoLeakCheck(sql, sql.indexOf("price_1h"), "cannot modify materialized view [view=price_1h]");
            }
            drainWalQueue();
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testBindVariableSelectsPartitionsOnApply() throws Exception {
        // The view's WAL keeps the statement with the values bound to it, and the partitions the bound value selects
        // drop when the WAL applies.
        assertMemoryLeak(() -> {
            createBaseAndView();
            try {
                bindVariableService.clear();
                bindVariableService.setInt(0, 1);
                execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION WHERE ts < dateadd('d', -$1, '2024-01-03T00:00:00.000000Z'::timestamp)");
                // a value bound later belongs to another statement
                bindVariableService.setInt(0, 0);
                assertViewRows(ALL_ROWS);
                drainWalQueue();
                assertViewRows("""
                        ts\tsym\tprice
                        2024-01-02T00:00:00.000000Z\ta\t3.0
                        2024-01-02T01:00:00.000000Z\tb\t4.0
                        2024-01-03T00:00:00.000000Z\ta\t5.0
                        2024-01-03T01:00:00.000000Z\tb\t6.0
                        """);
            } finally {
                bindVariableService.clear();
            }
            assertViewValid();
        });
    }

    @Test
    public void testDropNewestPartitionThenNewBaseRows() throws Exception {
        // Dropping the view's newest partition moves its newest rows back to the day before. Later base rows refresh
        // the buckets they touch, there and in a new day.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-03'");
            drainWalQueue();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-02T01:00:00.000000Z\tb\t4.0
                    """);
            assertPartitions("""
                    name\tnumRows
                    2024-01-01\t2
                    2024-01-02\t2
                    """);

            execute("""
                    INSERT INTO base VALUES
                    ('c', 7.0, '2024-01-03T02:10:00.000000Z'),
                    ('d', 8.0, '2024-01-04T00:10:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-02T01:00:00.000000Z\tb\t4.0
                    2024-01-03T02:00:00.000000Z\tc\t7.0
                    2024-01-04T00:00:00.000000Z\td\t8.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testDropPartitionKeepsRefreshState() throws Exception {
        // LIST and WHERE remove the rows of the partitions they name and leave the view valid, at the same refresh
        // position and with the same cached refresh intervals, as the view's TTL does. A MANUAL view keeps the base
        // changes it has not refreshed in its cache.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
            insertBaseRows();
            drainWalAndMatViewQueues();
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows(ALL_ROWS);
            execute("INSERT INTO base VALUES ('c', 7.0, '2024-01-03T02:10:00.000000Z')");
            drainWalQueue();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
            drainWalAndMatViewQueues();
            final StateSnapshot before = new StateSnapshot(viewToken);
            Assert.assertTrue("the cache must hold the unrefreshed base change", before.memoryRefreshIntervals.size() > 0);

            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
            // the executing node drops nothing; the WAL apply does
            assertViewRows(ALL_ROWS);
            drainWalQueue();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-02T01:00:00.000000Z\tb\t4.0
                    2024-01-03T00:00:00.000000Z\ta\t5.0
                    2024-01-03T01:00:00.000000Z\tb\t6.0
                    """);
            before.assertSame(viewToken);

            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION WHERE ts < '2024-01-03'");
            drainWalQueue();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-03T00:00:00.000000Z\ta\t5.0
                    2024-01-03T01:00:00.000000Z\tb\t6.0
                    """);
            assertPartitions("""
                    name\tnumRows
                    2024-01-03\t2
                    """);
            before.assertSame(viewToken);
            assertViewValid();

            // the next refresh recomputes the cached change only
            execute("REFRESH MATERIALIZED VIEW price_1h INCREMENTAL");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-03T00:00:00.000000Z\ta\t5.0
                    2024-01-03T01:00:00.000000Z\tb\t6.0
                    2024-01-03T02:00:00.000000Z\tc\t7.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testErrorPositions() throws Exception {
        // ALTER TABLE's grammar and errors. DETACH, ATTACH, CONVERT, SQUASH and FORCE DROP stay unsupported.
        assertMemoryLeak(() -> {
            createBaseAndView();
            final String expected = "'add', 'alter', 'as', 'drop', 'rename', 'resume', 'set', 'suspend' or 'with' expected";
            final String dropWhat = "'column', 'partition' or 'expire' expected";
            assertError("ALTER MATERIALIZED VIEW price_1h DROP", null, dropWhat);
            assertError("ALTER MATERIALIZED VIEW price_1h DROP foo", "foo", dropWhat);
            assertError("ALTER MATERIALIZED VIEW price_1h DROP STORAGE POLICY", "STORAGE", dropWhat);
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION", null, "'list' or 'where' expected");
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION foo", "foo", "'list' or 'where' expected");
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST", null, "partition name expected");
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-no'", "'2024-01-no'", "'yyyy-MM-dd' expected, found [ts=2024-01-no]");
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01' '2024-01-02'", "'2024-01-02'", "',' expected");
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01',", null, "partition name expected");
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION WHERE sym = 'a'", "sym", "Invalid column: sym");
            assertError("ALTER MATERIALIZED VIEW price_1h DROP PARTITION WHERE ts > 0 foo", "foo", "unexpected token");
            assertError("ALTER MATERIALIZED VIEW price_1h DETACH PARTITION LIST '2024-01-01'", "DETACH", expected);
            assertError("ALTER MATERIALIZED VIEW price_1h ATTACH PARTITION LIST '2024-01-01'", "ATTACH", expected);
            assertError("ALTER MATERIALIZED VIEW price_1h CONVERT PARTITION TO PARQUET LIST '2024-01-01'", "CONVERT", expected);
            assertError("ALTER MATERIALIZED VIEW price_1h SQUASH PARTITIONS", "SQUASH", expected);
            assertError("ALTER MATERIALIZED VIEW price_1h FORCE DROP PARTITION LIST '2024-01-01'", "FORCE", expected);
            drainWalQueue();
            assertViewRows(ALL_ROWS);
        });
    }

    @Test
    public void testFrozenZoneRowsDropped() throws Exception {
        // DROP PARTITION removes rows below a view's REFRESH LIMIT too, backfilled ones included, as the view's TTL
        // does. The frozen zone's boundary stays where the REFRESH LIMIT puts it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO trades VALUES ('a', 2.0, '2024-09-10T11:00')");
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW price_1h REFRESH MANUAL DEFERRED AS (SELECT sym, last(price) AS price, ts FROM trades SAMPLE BY 1h) PARTITION BY DAY");
            execute("ALTER MATERIALIZED VIEW price_1h SET REFRESH LIMIT 1 HOUR");
            drainWalAndMatViewQueues();

            // the anchor is min(11:00, now) = 11:00, so rows up to 10:00 lie in the frozen zone
            currentMicros = parseFloorPartialTimestamp("2024-09-10T12:30:00.000000Z");
            execute("""
                    INSERT INTO price_1h VALUES
                    ('a', 1.5, '2024-09-09T09:00:00.000000Z'),
                    ('a', 1.6, '2024-09-10T09:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            final String backfillMaxTs = """
                    backfill_max_ts
                    2024-09-10T10:00:00.000000Z
                    """;
            assertQuery("SELECT backfill_max_ts FROM materialized_views()").noLeakCheck().noRandomAccess().returns(backfillMaxTs);

            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-09-09'");
            drainWalAndMatViewQueues();
            assertQuery("price_1h").timestamp("ts").noLeakCheck().expectSize().returns("""
                    sym\tprice\tts
                    a\t1.6\t2024-09-10T09:00:00.000000Z
                    """);
            assertQuery("SELECT backfill_max_ts FROM materialized_views()").noLeakCheck().noRandomAccess().returns(backfillMaxTs);
        });
    }

    @Test
    public void testIncrementalRefreshLeavesDroppedRangeEmpty() throws Exception {
        // A refresh that recomputes other buckets leaves a dropped range alone.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('c', 7.0, '2024-01-03T01:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-02T01:00:00.000000Z\tb\t4.0
                    2024-01-03T00:00:00.000000Z\ta\t5.0
                    2024-01-03T01:00:00.000000Z\tb\t6.0
                    2024-01-03T01:00:00.000000Z\tc\t7.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testLateBaseRowRestoresItsBucketOnly() throws Exception {
        // A late base row in a dropped range makes the next refresh recompute its bucket from every row the base
        // holds in it. The other buckets of the dropped range stay empty.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
            drainWalQueue();
            execute("INSERT INTO base VALUES ('c', 7.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T00:00:00.000000Z\tc\t7.0
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-02T01:00:00.000000Z\tb\t4.0
                    2024-01-03T00:00:00.000000Z\ta\t5.0
                    2024-01-03T01:00:00.000000Z\tb\t6.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testMissingPartitionSkipsTransaction() throws Exception {
        // The executing node does not check the partitions, the WAL apply does: a partition the view lacks, and a
        // WHERE that matches none, skip the view's WAL transaction without suspending the view's WAL.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-09'");
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION WHERE ts > '2030-01-01'");
            drainWalQueue();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken));
            assertQuery("SELECT writerTxn = sequencerTxn applied FROM wal_tables() WHERE name = 'price_1h'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("applied\ntrue\n");
            assertViewRows(ALL_ROWS);

            // the view's WAL goes on
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(viewToken));
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-02T00:00:00.000000Z\ta\t3.0
                    2024-01-02T01:00:00.000000Z\tb\t4.0
                    2024-01-03T00:00:00.000000Z\ta\t5.0
                    2024-01-03T01:00:00.000000Z\tb\t6.0
                    """);
            assertViewValid();
        });
    }

    @Test
    public void testRangeAndFullRestoreDroppedRange() throws Exception {
        // RANGE recomputes a dropped range where the base still holds rows, and FULL rebuilds the whole view.
        assertMemoryLeak(() -> {
            createBaseAndView();
            execute("ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            execute("REFRESH MATERIALIZED VIEW price_1h RANGE FROM '2024-01-01T00:00' TO '2024-01-01T23:59'");
            drainWalAndMatViewQueues();
            assertViewRows("""
                    ts\tsym\tprice
                    2024-01-01T00:00:00.000000Z\ta\t1.0
                    2024-01-01T01:00:00.000000Z\tb\t2.0
                    2024-01-03T00:00:00.000000Z\ta\t5.0
                    2024-01-03T01:00:00.000000Z\tb\t6.0
                    """);

            execute("REFRESH MATERIALIZED VIEW price_1h FULL");
            drainWalAndMatViewQueues();
            assertViewRows(ALL_ROWS);
            assertViewValid();
        });
    }

    @Test
    public void testReadOnlyContextRefused() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndView();
            try (SqlExecutionContext readOnly = new SqlExecutionContextImpl(engine, 1).with(ReadOnlySecurityContext.INSTANCE)) {
                for (String sql : new String[]{
                        "ALTER MATERIALIZED VIEW price_1h DROP PARTITION LIST '2024-01-01'",
                        "ALTER MATERIALIZED VIEW price_1h DROP PARTITION WHERE ts < '2024-01-02'"
                }) {
                    try {
                        execute(sql, readOnly);
                        Assert.fail("a read-only context must not drop view partitions: " + sql);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isAuthorizationError());
                        TestUtils.assertContains(e.getFlyweightMessage(), "Write permission denied");
                        Assert.assertEquals(sql.indexOf("PARTITION"), e.getPosition());
                    }
                }
            }
            drainWalQueue();
            assertViewRows(ALL_ROWS);
        });
    }

    // Asserts that the statement fails with the message at the first occurrence of offendingText, or at the end of
    // the statement when offendingText is null.
    private static void assertError(String sql, String offendingText, String message) throws Exception {
        final int position = offendingText != null ? sql.indexOf(offendingText) : sql.length();
        Assert.assertTrue(sql, position > -1);
        try {
            execute(sql);
            Assert.fail("the statement must fail: " + sql);
        } catch (SqlException e) {
            TestUtils.assertEquals(sql, message, e.getFlyweightMessage());
            Assert.assertEquals(sql, position, e.getPosition());
        }
    }

    private static void createBase() throws SqlException {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private static void insertBaseRows() throws SqlException {
        execute("""
                INSERT INTO base VALUES
                ('a', 1.0, '2024-01-01T00:10:00.000000Z'),
                ('b', 2.0, '2024-01-01T01:10:00.000000Z'),
                ('a', 3.0, '2024-01-02T00:10:00.000000Z'),
                ('b', 4.0, '2024-01-02T01:10:00.000000Z'),
                ('a', 5.0, '2024-01-03T00:10:00.000000Z'),
                ('b', 6.0, '2024-01-03T01:10:00.000000Z')
                """);
    }

    private void assertPartitions(String expected) throws Exception {
        assertQuery("SELECT name, numRows FROM table_partitions('price_1h')")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(expected);
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("price_1h ORDER BY ts, sym").timestamp("ts").noLeakCheck().expectSize().returns(expected);
    }

    private void assertViewValid() throws Exception {
        assertQuery("SELECT view_status, invalidation_reason FROM materialized_views() WHERE view_name = 'price_1h'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\nvalid\t\n");
    }

    private void createBaseAndView() throws Exception {
        createBase();
        execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
        insertBaseRows();
        drainWalAndMatViewQueues();
        assertViewRows(ALL_ROWS);
    }

    // The view's refresh state in memory and in its state file, which DROP PARTITION leaves as it is.
    private static class StateSnapshot {
        private final long fileLastPeriodHi;
        private final long fileLastRefreshBaseTxn;
        private final long fileLastRefreshTimestampUs;
        private final LongList fileRefreshIntervals = new LongList();
        private final long fileRefreshIntervalsBaseTxn;
        private final long memoryLastPeriodHi;
        private final long memoryLastRefreshBaseTxn;
        private final LongList memoryRefreshIntervals = new LongList();
        private final long memoryRefreshIntervalsBaseTxn;

        private StateSnapshot(TableToken viewToken) {
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            Assert.assertNotNull(state);
            Assert.assertFalse(state.isInvalid());
            memoryLastRefreshBaseTxn = state.getLastRefreshBaseTxn();
            memoryLastPeriodHi = state.getLastPeriodHi();
            memoryRefreshIntervals.addAll(state.getRefreshIntervals());
            memoryRefreshIntervalsBaseTxn = state.getRefreshIntervalsBaseTxn();
            try (Path path = new Path(); BlockFileReader reader = new BlockFileReader(configuration)) {
                reader.of(path.of(configuration.getDbRoot()).concat(viewToken).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME).$());
                final MatViewStateReader fileState = new MatViewStateReader().of(reader, viewToken);
                Assert.assertFalse(fileState.isInvalid());
                fileLastRefreshBaseTxn = fileState.getLastRefreshBaseTxn();
                fileLastRefreshTimestampUs = fileState.getLastRefreshTimestampUs();
                fileLastPeriodHi = fileState.getLastPeriodHi();
                if (fileState.getRefreshIntervals() != null) {
                    fileRefreshIntervals.addAll(fileState.getRefreshIntervals());
                }
                fileRefreshIntervalsBaseTxn = fileState.getRefreshIntervalsBaseTxn();
            }
        }

        private void assertSame(TableToken viewToken) {
            final StateSnapshot now = new StateSnapshot(viewToken);
            Assert.assertEquals(memoryLastRefreshBaseTxn, now.memoryLastRefreshBaseTxn);
            Assert.assertEquals(memoryLastPeriodHi, now.memoryLastPeriodHi);
            TestUtils.assertEquals(memoryRefreshIntervals, now.memoryRefreshIntervals);
            Assert.assertEquals(memoryRefreshIntervalsBaseTxn, now.memoryRefreshIntervalsBaseTxn);
            Assert.assertEquals(fileLastRefreshBaseTxn, now.fileLastRefreshBaseTxn);
            Assert.assertEquals(fileLastRefreshTimestampUs, now.fileLastRefreshTimestampUs);
            Assert.assertEquals(fileLastPeriodHi, now.fileLastPeriodHi);
            TestUtils.assertEquals(fileRefreshIntervals, now.fileRefreshIntervals);
            Assert.assertEquals(fileRefreshIntervalsBaseTxn, now.fileRefreshIntervalsBaseTxn);
        }
    }
}
