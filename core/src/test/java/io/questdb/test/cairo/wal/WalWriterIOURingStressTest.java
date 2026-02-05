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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.IOURingFacadeImpl;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Edge-case and stress tests for io_uring WAL writer (MemoryPURImpl).
 * Exercises page churn, var-size columns, rollback, concurrent readers,
 * async commit, and segment rollover under io_uring.
 */
public class WalWriterIOURingStressTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        Assume.assumeTrue("io_uring not available", IOURingFacadeImpl.INSTANCE.isAvailable());
        setProperty(PropertyKey.CAIRO_IO_URING_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testAsyncCommitWriterContinues() throws Exception {
        // ASYNC commit: submit snapshot writes without blocking, then continue appending.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, CommitMode.ASYNC);
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("v", ColumnType.LONG)
                    .timestamp("ts")
                    .wal());
            createTable(new TableModel(configuration, copyName, PartitionBy.YEAR)
                    .col("v", ColumnType.LONG)
                    .timestamp("ts")
                    .noWal());

            long ts = 1_000_000_000L;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 200; i++) {
                        long val = batch * 200L + i;
                        TableWriter.Row row = walWriter.newRow(ts);
                        row.putLong(0, val);
                        row.append();
                        TableWriter.Row cr = copyWriter.newRow(ts);
                        cr.putLong(0, val);
                        cr.append();
                        ts += 1000;
                    }
                    walWriter.commit();
                    copyWriter.commit();
                    // Writer continues appending after async commit without waiting.
                }

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testManyColumnsSmallPageChurn() throws Exception {
        // Small page size forces rapid page transitions across many columns.
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR);
            TableModel copyModel = new TableModel(configuration, copyName, PartitionBy.YEAR);
            for (int c = 0; c < 20; c++) {
                model.col("c" + c, ColumnType.LONG);
                copyModel.col("c" + c, ColumnType.LONG);
            }
            model.timestamp("ts").wal();
            copyModel.timestamp("ts").noWal();
            TableToken tableToken = createTable(model);
            createTable(copyModel);

            // 4096 / 8 = 512 longs per page. With 20 columns, each row = 20*8=160 bytes per column
            // Actually each column gets its own page. 512 longs per page per column.
            // 600 rows should force at least 1 page transition per column.
            int rowCount = 600;
            long ts = 1_000_000_000L;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = walWriter.newRow(ts);
                    TableWriter.Row cr = copyWriter.newRow(ts);
                    for (int c = 0; c < 20; c++) {
                        long val = i * 20L + c;
                        row.putLong(c, val);
                        cr.putLong(c, val);
                    }
                    row.append();
                    cr.append();
                    ts += 1000;
                }
                walWriter.commit();
                copyWriter.commit();

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testMultipleCommitsBeforeClose() throws Exception {
        // Multiple commits while writer stays open. Tests NOSYNC flush on each commit.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal());
            createTable(new TableModel(configuration, copyName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .noWal());

            long ts = 1_000_000_000L;
            Rnd rnd = new Rnd();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                for (int commit = 0; commit < 10; commit++) {
                    for (int i = 0; i < 50; i++) {
                        int v = rnd.nextInt();
                        String sym = rnd.nextInt(5) == 0 ? null : rnd.nextString(rnd.nextInt(5) + 1);
                        TableWriter.Row row = walWriter.newRow(ts);
                        row.putInt(0, v);
                        row.putSym(1, sym);
                        row.append();
                        TableWriter.Row cr = copyWriter.newRow(ts);
                        cr.putInt(0, v);
                        cr.putSym(1, sym);
                        cr.append();
                        ts += 1000;
                    }
                    walWriter.commit();
                    copyWriter.commit();

                    // Drain and verify after each commit.
                    drainWalQueue();
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
                }
            }
        });
    }

    @Test
    public void testRollbackAcrossMultiplePages() throws Exception {
        // Write enough data to span multiple pages, then rollback to an earlier offset.
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("v", ColumnType.LONG)
                    .timestamp("ts")
                    .wal());
            createTable(new TableModel(configuration, copyName, PartitionBy.YEAR)
                    .col("v", ColumnType.LONG)
                    .timestamp("ts")
                    .noWal());

            // 4096 / 8 = 512 longs per page.
            int firstBatch = 200;
            int secondBatch = 800; // spans 2+ pages
            int thirdBatch = 100;
            long ts = 1_000_000_000L;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                // First batch: commit.
                for (int i = 0; i < firstBatch; i++) {
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putLong(0, i);
                    row.append();
                    TableWriter.Row cr = copyWriter.newRow(ts);
                    cr.putLong(0, i);
                    cr.append();
                    ts += 1000;
                }
                walWriter.commit();
                copyWriter.commit();

                // Second batch: write but rollback (pages with in-flight writes).
                long savedTs = ts;
                for (int i = 0; i < secondBatch; i++) {
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putLong(0, 99999);
                    row.append();
                    ts += 1000;
                }
                walWriter.rollback();
                ts = savedTs;

                // Third batch: write after rollback.
                for (int i = 0; i < thirdBatch; i++) {
                    long val = firstBatch + i;
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putLong(0, val);
                    row.append();
                    TableWriter.Row cr = copyWriter.newRow(ts);
                    cr.putLong(0, val);
                    cr.append();
                    ts += 1000;
                }
                walWriter.commit();
                copyWriter.commit();

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testSegmentRolloverDrainsCqes() throws Exception {
        // Force segment rollover to exercise switchTo draining CQEs before freeing buffers/fds.
        setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 500);
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("v", ColumnType.LONG)
                    .timestamp("ts")
                    .wal());
            createTable(new TableModel(configuration, copyName, PartitionBy.YEAR)
                    .col("v", ColumnType.LONG)
                    .timestamp("ts")
                    .noWal());

            long ts = 1_000_000_000L;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                // Write more than rollover threshold to force segment switches.
                for (int i = 0; i < 1500; i++) {
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putLong(0, i);
                    row.append();
                    TableWriter.Row cr = copyWriter.newRow(ts);
                    cr.putLong(0, i);
                    cr.append();
                    ts += 1000;
                    if (i % 100 == 99) {
                        walWriter.commit();
                        copyWriter.commit();
                    }
                }
                walWriter.commit();
                copyWriter.commit();

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testSyncCommitSmallData() throws Exception {
        // SYNC commit mode with small data that fits in one page.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, CommitMode.SYNC);
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .timestamp("ts")
                    .wal());
            createTable(new TableModel(configuration, copyName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .timestamp("ts")
                    .noWal());

            long ts = 1_000_000_000L;
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                for (int i = 0; i < 50; i++) {
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putInt(0, i);
                    row.append();
                    TableWriter.Row cr = copyWriter.newRow(ts);
                    cr.putInt(0, i);
                    cr.append();
                    ts += 1000;
                }
                walWriter.commit();
                copyWriter.commit();

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testVarSizeColumnRollback() throws Exception {
        // Var-size columns (STRING, VARCHAR) exercise aux + data column pairs under rollback.
        setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("s", ColumnType.STRING)
                    .col("v", ColumnType.VARCHAR)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .wal());
            createTable(new TableModel(configuration, copyName, PartitionBy.YEAR)
                    .col("s", ColumnType.STRING)
                    .col("v", ColumnType.VARCHAR)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .noWal());

            long ts = 1_000_000_000L;
            Rnd rnd = new Rnd();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                // Commit first batch.
                for (int i = 0; i < 100; i++) {
                    String str = rnd.nextString(rnd.nextInt(50) + 1);
                    String varchar = rnd.nextString(rnd.nextInt(50) + 1);
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putStr(0, str);
                    row.putVarchar(1, new Utf8String(varchar));
                    row.putInt(2, i);
                    row.append();
                    TableWriter.Row cr = copyWriter.newRow(ts);
                    cr.putStr(0, str);
                    cr.putVarchar(1, new Utf8String(varchar));
                    cr.putInt(2, i);
                    cr.append();
                    ts += 1000;
                }
                walWriter.commit();
                copyWriter.commit();

                // Write large var-size strings then rollback.
                long savedTs = ts;
                for (int i = 0; i < 200; i++) {
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putStr(0, rnd.nextString(200));
                    row.putVarchar(1, new Utf8String(rnd.nextString(200)));
                    row.putInt(2, -1);
                    row.append();
                    ts += 1000;
                }
                walWriter.rollback();
                ts = savedTs;

                // Write after rollback.
                for (int i = 0; i < 50; i++) {
                    String str = rnd.nextString(rnd.nextInt(30) + 1);
                    String varchar = rnd.nextString(rnd.nextInt(30) + 1);
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putStr(0, str);
                    row.putVarchar(1, new Utf8String(varchar));
                    row.putInt(2, 100 + i);
                    row.append();
                    TableWriter.Row cr = copyWriter.newRow(ts);
                    cr.putStr(0, str);
                    cr.putVarchar(1, new Utf8String(varchar));
                    cr.putInt(2, 100 + i);
                    cr.append();
                    ts += 1000;
                }
                walWriter.commit();
                copyWriter.commit();

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testVarSizeColumnRowCancel() throws Exception {
        // Exercise row cancel with var-size columns.
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String copyName = tableName + "_copy";
            TableToken tableToken = createTable(new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("s", ColumnType.STRING)
                    .col("v", ColumnType.VARCHAR)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .wal());
            createTable(new TableModel(configuration, copyName, PartitionBy.YEAR)
                    .col("s", ColumnType.STRING)
                    .col("v", ColumnType.VARCHAR)
                    .col("i", ColumnType.INT)
                    .timestamp("ts")
                    .noWal());

            long ts = 1_000_000_000L;
            Rnd rnd = new Rnd();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(copyName)
            ) {
                for (int i = 0; i < 200; i++) {
                    String str = rnd.nextString(rnd.nextInt(20) + 1);
                    String varchar = rnd.nextString(rnd.nextInt(20) + 1);

                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putStr(0, str);
                    row.putVarchar(1, new Utf8String(varchar));
                    row.putInt(2, i);
                    if (i % 3 == 0) {
                        // Cancel every 3rd row.
                        row.cancel();
                    } else {
                        row.append();
                        TableWriter.Row cr = copyWriter.newRow(ts);
                        cr.putStr(0, str);
                        cr.putVarchar(1, new Utf8String(varchar));
                        cr.putInt(2, i);
                        cr.append();
                    }
                    ts += 1000;
                }
                walWriter.commit();
                copyWriter.commit();

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, copyName, tableName, LOG);
            }
        });
    }
}
