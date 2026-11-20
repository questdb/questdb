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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnIndexerJob;
import io.questdb.cairo.ColumnTaskJob;
import io.questdb.cairo.O3CopyJob;
import io.questdb.cairo.O3OpenColumnJob;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * An O3 commit into a parquet partition decodes the designated timestamp of each row group it merges once. The
 * column top row limit of the output needs the stored timestamps of the row group where the first late row lands,
 * see O3PartitionJob.computeColumnTopRowLimit(): it takes them from the first stage of the merge of that row group,
 * which the UPSERT identity check and the merge loop complete. The parquet table {@code pt} takes each commit, its
 * native twin {@code tw} the same SQL, and the twin decides the rows.
 * <p>
 * Partition 2024-01-01 holds rows {@code x} 1..1000, one per second, in row groups of 100: row group {@code k}
 * holds rows {@code 100 * k + 1} to {@code 100 * (k + 1)}. 2024-01-02 holds one row, so that 2024-01-01 is not
 * the last partition. The DEDUP tests run on WAL tables only.
 */
@RunWith(Parameterized.class)
public class O3ParquetMergeTimestampDecodeTest extends AbstractCairoTest {
    private static final String COLUMNS = "v, d, i, flag, ts";
    // 2024-01-01T00:00:00Z
    private static final long DAY_MICROS = 1_704_067_200_000_000L;
    private static final int PARTITION_ROWS = 1_000;
    private final boolean isWal;

    public O3ParquetMergeTimestampDecodeTest(WalMode walMode) {
        this.isWal = walMode == WalMode.WITH_WAL;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{WalMode.WITH_WAL}, {WalMode.NO_WAL}});
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        // the twin keeps the column tops the O3 merge of O3OpenColumnJob keeps, the merge production runs by default
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
    }

    @Test
    public void testColumnTopAtMergedRowGroupStart() throws Exception {
        // the top of b ends at the first row of row group 1, and the late row lands inside it
        assertColumnTop(100, 150);
    }

    @Test
    public void testColumnTopInsideMergedRowGroup() throws Exception {
        assertColumnTop(150, 170);
    }

    @Test
    public void testFirstMergeStageOutOfMemory() throws Exception {
        // The first stage of the merge of row group 4 decodes the dedup key k, 10 MB of VARCHAR in that row group,
        // before the output opens: the decode runs out of memory, and the commit fails as a whole
        Assume.assumeTrue(isWal);
        // the backoff after the out of memory error, in milliseconds
        node1.setProperty(PropertyKey.CAIRO_WRITE_BACK_OFF_TIMEOUT_ON_MEM_PRESSURE, 2);
        assertMemoryLeak(() -> {
            final String bigValue = "rpad('k', 100_000, 'x')";
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v LONG, k VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL"
                        + " DEDUP UPSERT KEYS(ts, k)");
                execute("INSERT INTO " + table + " SELECT x, CASE WHEN x BETWEEN 401 AND 500 THEN " + bigValue
                        + " ELSE 'k' END, " + tsExpr() + " FROM long_sequence(" + PARTITION_ROWS + ")");
                execute("INSERT INTO " + table + " VALUES (0, 'k', '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("pt");
            final String rowsBefore = selectRows("pt");
            final String lateRow = " VALUES (9001, 'k', " + (tsOf(450) + 500_000) + "::TIMESTAMP)";
            execute("INSERT INTO pt" + lateRow);
            final long savedLimit = Unsafe.getRssMemLimit();
            try {
                // room for the commit, not for the decode of the keys of row group 4
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + 2 * Numbers.SIZE_1MB);
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                }, 60);
            } finally {
                Unsafe.setRssMemLimit(savedLimit);
            }
            assertQuery("SELECT suspended, errorTag, errorMessage FROM wal_tables() WHERE name = 'pt'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            suspended\terrorTag\terrorMessage
                            true\tOUT OF MEMORY\tbulk update failed and will be rolled back
                            """);
            // the partition keeps its rows
            Assert.assertEquals(rowsBefore, selectRows("pt"));

            execute("ALTER TABLE pt RESUME WAL");
            // the out of memory error backed the table off, RESUME WAL keeps the backoff
            TestUtils.assertEventually(
                    () -> Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(tableToken).getMemPressureControl().isReadyToProcess()),
                    60
            );
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            execute("INSERT INTO tw" + lateRow);
            drainWalQueue();
            assertSqlCursors("tw", "pt");
            // the next commit merges as any other
            assertCommitDecodes(1, "pt", "tw", " VALUES (9002, 'k', " + (tsOf(470) + 500_000) + "::TIMESTAMP)");
        });
    }

    @Test
    public void testIdenticalRowWithColumnTopNullsThenChangedRow() throws Exception {
        // An UPSERT commit resends row 120 unchanged and changes row 450. Row group 1 holds NULLs of the BOOLEAN
        // column b, below its top, so the merge loop merges it, and then merges row group 4 as well: the identity
        // check does not keep what it prepared of row group 4 when the loop merges an action before it. Both
        // row groups decode twice, the tradeoff the identity check makes for the NULLs of a column without a NULL
        // sentinel: once in the identity check, once in the merge loop. The first, of row group 1, is the one
        // that the column top row limit prepared.
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTopTables(150, " DEDUP UPSERT KEYS(ts)");
            assertCommitDecodes(
                    4,
                    "(120, 60.0, 1, 0, " + tsOf(120) + "::TIMESTAMP, NULL), (9001, 0.25, 3, 1, " + tsOf(450) + "::TIMESTAMP, true)",
                    COLUMNS + ", b"
            );
        });
    }

    @Test
    public void testLateRowAfterColumnTypeChange() throws Exception {
        // The parquet file keeps i as INT: the merge rewrites the partition and copies row groups 0 to 3 through
        // ParquetRowGroupMaterializer before it merges row group 4. The first stage of that merge, which the column
        // top row limit prepared before the output opened, lives in buffers that the copies do not use.
        assertMemoryLeak(() -> {
            createTables("INT", "");
            alterBoth("ALTER TABLE {t} ALTER COLUMN i TYPE LONG");
            assertCommitDecodes(1, lateRow(9001, tsOf(450) + 500_000));
        });
    }

    @Test
    public void testLateRowAtRowGroupStart() throws Exception {
        // row 401 starts row group 4, the late row of its timestamp lands after it, in row group 4
        assertMemoryLeak(() -> {
            createTables("INT", "");
            assertCommitDecodes(1, lateRow(9001, tsOf(401)));
        });
    }

    @Test
    public void testLateRowInFirstRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createTables("INT", "");
            assertCommitDecodes(1, lateRow(9001, tsOf(50) + 500_000));
        });
    }

    @Test
    public void testLateRowInRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            createTables("INT", "");
            assertCommitDecodes(1, lateRow(9001, tsOf(450) + 500_000));
        });
    }

    @Test
    public void testLateRowIntoSmallLastRowGroup() throws Exception {
        // The last row group holds 10 rows, fewer than a quarter of the row group size, and a row after every stored
        // row of the partition merges into it. Every stored row keeps its place, so the column top row limit needs no
        // stored timestamps, and the merge loop decodes the row group.
        assertMemoryLeak(() -> {
            createTables("INT", "", PARTITION_ROWS + 10);
            assertCommitDecodes(1, lateRow(9001, DAY_MICROS + 20 * 3_600_000_000L));
        });
    }

    @Test
    public void testLateRowsAcrossRowGroups() throws Exception {
        // 50 late rows, one every 12 rows from row 150 to row 738, merge into row groups 1 to 7
        assertMemoryLeak(() -> {
            createTables("INT", "");
            final StringSink rows = new StringSink();
            for (int r = 0; r < 50; r++) {
                if (r > 0) {
                    rows.put(", ");
                }
                rows.put(lateRow(10_000 + r, tsOf(150 + r * 12L) + 250_000));
            }
            assertCommitDecodes(7, rows.toString());
        });
    }

    @Test
    public void testLateRowsIntoSeveralPartitions() throws Exception {
        // One commit merges a late row into each of three parquet partitions. The O3 jobs of a worker pool merge
        // the partitions in parallel, each in the merge context of the worker that runs it.
        assertMemoryLeak(() -> {
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v LONG, d DOUBLE, i INT, flag INT, ts TIMESTAMP) TIMESTAMP(ts)"
                        + " PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"));
                // three days of 1000 rows, one every 86.4 seconds, and one row on the fourth day
                execute("INSERT INTO " + table + " SELECT x, x * 0.5, (x % 7)::INT, (x % 2)::INT, "
                        + DAY_MICROS + "::TIMESTAMP + (x - 1) * 86_400_000L FROM long_sequence(3_000)");
                execute("INSERT INTO " + table + " (" + COLUMNS + ") VALUES (0, 0.0, 0, 0, '2024-01-04T00:00:00.000000Z')");
            }
            drainWalQueueIfWal();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02', '2024-01-03'");
            drainWalQueueIfWal();
            final long dayMicros = 86_400_000_000L;
            final String rows = lateRow(9001, DAY_MICROS + 450 * 86_400_000L + 1)
                    + ", " + lateRow(9002, DAY_MICROS + dayMicros + 450 * 86_400_000L + 1)
                    + ", " + lateRow(9003, DAY_MICROS + 2 * dayMicros + 450 * 86_400_000L + 1);
            final long before = getTimestampDecodeCount();
            final WorkerPool pool = new TestWorkerPool(4);
            try {
                pool.assign(new O3PartitionJob(engine.getMessageBus()));
                pool.assign(new O3OpenColumnJob(engine.getMessageBus()));
                pool.assign(new O3CopyJob(engine.getMessageBus()));
                pool.assign(new ColumnTaskJob(engine.getMessageBus()));
                pool.assign(new ColumnIndexerJob(engine.getMessageBus()));
                pool.start(LOG);
                insertBoth(rows, COLUMNS);
            } finally {
                pool.halt();
            }
            Assert.assertEquals(3, getTimestampDecodeCount() - before);
            assertSqlCursors("tw", "pt");
        });
    }

    @Test
    public void testSkipDuplicateOnly() throws Exception {
        // the dedup keeps the stored row 450 and drops the late one: the partition stays as it is
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTables("INT", " DEDUP SKIP KEYS(ts)");
            assertCommitDecodes(1, lateRow(9001, tsOf(450)));
        });
    }

    @Test
    public void testSkipNewRow() throws Exception {
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTables("INT", " DEDUP SKIP KEYS(ts)");
            assertCommitDecodes(1, lateRow(9001, tsOf(450) + 500_000));
        });
    }

    @Test
    public void testUpsertAfterColumnTypeChangeWithBooleanColumn() throws Exception {
        // The identity check takes over the first stage of the merge of row group 4 and finds the row changed. The
        // rewrite copies row groups 0 to 3 through ParquetRowGroupMaterializer, which decodes into the buffers of
        // the other columns, so the check cannot keep them, and the merge loop decodes row group 4 again: the
        // tradeoff of an UPSERT commit after ALTER COLUMN TYPE on a table with a BOOLEAN, BYTE or SHORT column.
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTables("BOOLEAN", " DEDUP UPSERT KEYS(ts)");
            alterBoth("ALTER TABLE {t} ALTER COLUMN i TYPE LONG");
            assertCommitDecodes(2, "(9001, 0.25, 3, true, " + tsOf(450) + "::TIMESTAMP)");
        });
    }

    @Test
    public void testUpsertBoundaryTie() throws Exception {
        // Rows 400 and 401 share a timestamp, so row group 3 ends and row group 4 starts on it. A new key of that
        // timestamp merges both row groups as one run, which the merge decodes in one go.
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            for (String table : new String[]{"pt", "tw"}) {
                execute("CREATE TABLE " + table + " (v LONG, d DOUBLE, i INT, flag INT, ts TIMESTAMP) TIMESTAMP(ts)"
                        + " PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                execute("INSERT INTO " + table + " SELECT x, x * 0.5, (x % 7)::INT, (x % 2)::INT, " + DAY_MICROS
                        + "::TIMESTAMP + (CASE WHEN x = 401 THEN 399 ELSE x - 1 END) * 1_000_000L FROM long_sequence("
                        + PARTITION_ROWS + ")");
                execute("INSERT INTO " + table + " (" + COLUMNS + ") VALUES (0, 0.0, 0, 0, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertCommitDecodes(2, lateRow(9001, tsOf(400)));
        });
    }

    @Test
    public void testUpsertIdenticalRowWithBooleanColumn() throws Exception {
        // the identity check takes over the first stage of the merge of row group 4 and finds row 450 resent
        // unchanged: the commit leaves the partition as it is
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTables("BOOLEAN", " DEDUP UPSERT KEYS(ts)");
            assertCommitDecodes(1, "(450, 225.0, 2, true, " + tsOf(450) + "::TIMESTAMP)");
        });
    }

    @Test
    public void testUpsertNewRowIntoLegacyRequiredFile() throws Exception {
        // The 9.4.2 file of LegacyRequiredParquetRewriteTest stores its BOOLEAN, BYTE, SHORT and CHAR columns
        // Required in 15 row groups of 10 rows: the merge re-encodes every row group, and copies the row groups
        // before the merge through ParquetRowGroupMaterializer. A new key between two stored rows of row group 5
        // makes the identity check keep the first stage of the merge, in buffers of its own, for the merge loop.
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            LegacyRequiredParquetRewriteTest.loadFixture();
            execute("ALTER TABLE legacy_all DEDUP ENABLE UPSERT KEYS(ts)");
            execute("CREATE TABLE tw AS (SELECT * FROM legacy_all) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE tw DEDUP ENABLE UPSERT KEYS(ts)");
            drainWalQueue();
            final long timestampLo;
            final long timestampHi;
            try (
                    RecordCursorFactory factory = select("SELECT ts FROM legacy_all ORDER BY ts LIMIT 54, 56", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                timestampLo = cursor.getRecord().getTimestamp(0);
                Assert.assertTrue(cursor.hasNext());
                timestampHi = cursor.getRecord().getTimestamp(0);
            }
            Assert.assertTrue(timestampHi > timestampLo + 1);
            final long before;
            try (TableWriter writer = getWriter("legacy_all")) {
                before = writer.getParquetMergeTimestampDecodeCount();
            }
            for (String table : new String[]{"legacy_all", "tw"}) {
                execute("INSERT INTO " + table + " (ts, c_bool, c_int) VALUES (" + (timestampLo + 1) + "::TIMESTAMP, true, 42)");
            }
            drainWalQueue();
            try (TableWriter writer = getWriter("legacy_all")) {
                Assert.assertEquals(1, writer.getParquetMergeTimestampDecodeCount() - before);
            }
            assertSqlCursors("tw", "legacy_all");
        });
    }

    @Test
    public void testUpsertRowAfterStoredRows() throws Exception {
        // A row after every stored row of the partition, which is not the last one, goes into a new row group after
        // the full last row group: the commit merges no row group, and every stored row keeps its place, so the
        // column top row limit needs no stored timestamps either
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTables("BOOLEAN", " DEDUP UPSERT KEYS(ts)");
            assertCommitDecodes(0, "(9001, 0.25, 3, true, " + (DAY_MICROS + 20 * 3_600_000_000L) + "::TIMESTAMP)");
        });
    }

    @Test
    public void testUpsertValueUpdate() throws Exception {
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTables("INT", " DEDUP UPSERT KEYS(ts)");
            assertCommitDecodes(1, lateRow(9001, tsOf(450)));
        });
    }

    @Test
    public void testUpsertValueUpdateWithBooleanColumn() throws Exception {
        // the identity check takes over the first stage of the merge of row group 4, finds the row changed and
        // keeps the merge for the merge loop
        Assume.assumeTrue(isWal);
        assertMemoryLeak(() -> {
            createTables("BOOLEAN", " DEDUP UPSERT KEYS(ts)");
            assertCommitDecodes(1, "(9001, 0.25, 3, true, " + tsOf(450) + "::TIMESTAMP)");
        });
    }

    private static long getTimestampDecodeCount() {
        try (TableWriter writer = getWriter("pt")) {
            return writer.getParquetMergeTimestampDecodeCount();
        }
    }

    private static String lateRow(long v, long timestampMicros) {
        return "(" + v + ", 0.25, 3, 1, " + timestampMicros + "::TIMESTAMP)";
    }

    private static String selectRows(String table) throws Exception {
        final StringSink sink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, table, sink);
        return sink.toString();
    }

    private static String tsExpr() {
        return DAY_MICROS + "::TIMESTAMP + (x - 1) * 1_000_000L";
    }

    // the timestamp of row x of partition 2024-01-01
    private static long tsOf(long x) {
        return DAY_MICROS + (x - 1) * 1_000_000L;
    }

    private void alterBoth(String alterSql) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute(alterSql.replace("{t}", table));
        }
        drainWalQueueIfWal();
    }

    private void assertColumnTop(int top, int lateRowX) throws Exception {
        assertMemoryLeak(() -> {
            createTopTables(top, "");
            assertCommitDecodes(1, "(9001, 0.25, 3, 1, " + (tsOf(lateRowX) + 500_000) + "::TIMESTAMP, true)", COLUMNS + ", b");
            // CONVERT TO PARQUET writes the rows below the top as NULLs, which BOOLEAN does not have: after a change
            // to a type with NULL they read NULL or 0, where the native twin keeps the top and reads NULL
            alterBoth("ALTER TABLE {t} ALTER COLUMN b TYPE INT");
            assertTwinWithTopOfB(top);
            assertColumnTopNullOrZeroCount(top);
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueueIfWal();
            assertTwinWithTopOfB(top);
            assertColumnTopNullOrZeroCount(top);
        });
    }

    // the rows below the top of b, v 1..top of 2024-01-01, read NULL or 0
    private void assertColumnTopNullOrZeroCount(int top) throws Exception {
        assertQuery("SELECT count() FROM pt WHERE coalesce(b, 0) = 0 AND v BETWEEN 1 AND " + top + " AND ts IN '2024-01-01'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + top + "\n");
    }

    private void assertCommitDecodes(long expectedDecodes, String rows) throws Exception {
        assertCommitDecodes(expectedDecodes, rows, COLUMNS);
    }

    private void assertCommitDecodes(long expectedDecodes, String rows, String columns) throws Exception {
        final long before = getTimestampDecodeCount();
        insertBoth(rows, columns);
        Assert.assertEquals(expectedDecodes, getTimestampDecodeCount() - before);
        assertSqlCursors("tw", "pt");
    }

    private void assertCommitDecodes(long expectedDecodes, String parquetTable, String twinTable, String values) throws Exception {
        final long before = getTimestampDecodeCount();
        execute("INSERT INTO " + parquetTable + values);
        execute("INSERT INTO " + twinTable + values);
        drainWalQueueIfWal();
        Assert.assertEquals(expectedDecodes, getTimestampDecodeCount() - before);
        assertSqlCursors(twinTable, parquetTable);
    }

    // pt reads as tw, but b of the rows below its top, v 1..top of 2024-01-01, reads NULL or 0 on either
    private void assertTwinWithTopOfB(int top) throws Exception {
        final String sql = "SELECT v, d, i, flag, ts, CASE WHEN v BETWEEN 1 AND " + top + " THEN coalesce(b, 0) ELSE b END b FROM {t}";
        assertSqlCursors(sql.replace("{t}", "tw"), sql.replace("{t}", "pt"));
    }

    // creates pt and its twin tw, see the class javadoc, and converts 2024-01-01 of pt to parquet
    private void createTables(String flagType, String dedup) throws Exception {
        createTables(flagType, dedup, PARTITION_ROWS);
    }

    private void createTables(String flagType, String dedup, int partitionRows) throws Exception {
        final boolean isBoolean = "BOOLEAN".equals(flagType);
        for (String table : new String[]{"pt", "tw"}) {
            execute("CREATE TABLE " + table + " (v LONG, d DOUBLE, i INT, flag " + flagType + ", ts TIMESTAMP) TIMESTAMP(ts)"
                    + " PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL") + dedup);
            execute("INSERT INTO " + table + " SELECT x, x * 0.5, (x % 7)::INT, " + (isBoolean ? "x % 2 = 0" : "(x % 2)::INT")
                    + ", " + tsExpr() + " FROM long_sequence(" + partitionRows + ")");
            execute("INSERT INTO " + table + " (" + COLUMNS + ") VALUES (0, 0.0, 0, " + (isBoolean ? "false" : "0")
                    + ", '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueueIfWal();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueueIfWal();
    }

    // creates pt and tw with the BOOLEAN column b added after the first `top` rows of 2024-01-01, and converts
    // 2024-01-01 of pt to parquet
    private void createTopTables(int top, String dedup) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute("CREATE TABLE " + table + " (v LONG, d DOUBLE, i INT, flag INT, ts TIMESTAMP) TIMESTAMP(ts)"
                    + " PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL") + dedup);
            execute("INSERT INTO " + table + " SELECT x, x * 0.5, (x % 7)::INT, (x % 2)::INT, " + tsExpr()
                    + " FROM long_sequence(" + top + ")");
        }
        drainWalQueueIfWal();
        alterBoth("ALTER TABLE {t} ADD COLUMN b BOOLEAN");
        for (String table : new String[]{"pt", "tw"}) {
            execute("INSERT INTO " + table + " SELECT x, x * 0.5, (x % 7)::INT, (x % 2)::INT, " + tsExpr()
                    + ", x % 3 = 0 FROM long_sequence(" + PARTITION_ROWS + ") WHERE x > " + top);
            execute("INSERT INTO " + table + " (" + COLUMNS + ") VALUES (0, 0.0, 0, 0, '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueueIfWal();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueueIfWal();
    }

    private void drainWalQueueIfWal() {
        if (isWal) {
            drainWalQueue();
        }
    }

    private void insertBoth(String rows, String columns) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute("INSERT INTO " + table + " (" + columns + ") VALUES " + rows);
        }
        drainWalQueueIfWal();
    }
}
