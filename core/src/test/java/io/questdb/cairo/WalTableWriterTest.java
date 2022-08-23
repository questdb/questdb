/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WalTableWriterTest extends AbstractGriffinTest {

    @Before
    public void setUp() {
        super.setUp();
        currentMicros = 0L;
    }

    @After
    public void tearDown() {
        super.tearDown();
        currentMicros = -1L;
    }

    @Test
    public void testPartitionOverflowAppend() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);


                start += rowCount * tsIncrement + 1;
                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testPartitionOverflowMerge() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);


                start += rowCount * tsIncrement - 2 * Timestamps.SECOND_MICROS;
                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testRandomInOutOfOrderMultipleWalInserts() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement;
            long now = Os.currentTimeMicros();
            LOG.info().$("now :").$(now).$();
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter3 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = now;
                WalWriter[] writers = new WalWriter[]{walWriter1, walWriter2, walWriter3, null};

                for (int i = 0; i < 5; i++) {
                    boolean inOrder = rnd.nextBoolean();
                    int walIndex = rnd.nextInt(writers.length);
                    WalWriter walWriter = writers[walIndex];
                    int rowCount = rnd.nextInt(1000) + 2;
                    tsIncrement = rnd.nextLong(Timestamps.MINUTE_MICROS);

                    LOG.infoW().$("generating wall [")
                            .$("iteration:").$(i)
                            .$(", walIndex: ").$(walIndex)
                            .$(", inOrder: ").$(inOrder)
                            .$(" rowCount: ").$(rowCount)
                            .$(" tsIncrement: ").$(tsIncrement)
                            .I$();

                    addRowsToWalAndApplyToTable(i, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, inOrder);

                    LOG.info().$("verifying wall [").$("iteration:").$(i).I$();
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    start += rowCount * tsIncrement + 1;
                }
            }
        });
    }

    @Test
    public void testRandomInOutOfOrderOverlappingInserts() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement;
            long now = Os.currentTimeMicros();
            LOG.info().$("now :").$(now).$();
            long ts = 1657816077895390L;//now;
            Rnd rnd = new Rnd(300984185140125L, 1657816077895L);// TestUtils.generateRandom(LOG);

            int releaseWriterSeed = 3;
            int overlapSeed = 3;

            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter3 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = ts;
                WalWriter[] writers = new WalWriter[]{walWriter1, walWriter2, walWriter3, null};

                for (int i = 0; i < 20; i++) {
                    boolean inOrder = rnd.nextBoolean();
                    int walIndex = rnd.nextInt(writers.length);
                    WalWriter walWriter = writers[walIndex];
                    int rowCount = rnd.nextInt(10000) + 1;
                    int partitions = rnd.nextInt(3) + 1;
                    tsIncrement = partitions * Timestamps.HOUR_MICROS / rowCount;
                    long tsOffset = rnd.nextLong(2 * Timestamps.HOUR_MICROS);
                    int sign = rnd.nextInt(overlapSeed);
                    tsOffset *= sign == 0 ? -1 : 1;
                    start += tsOffset;

                    LOG.infoW().$("generating wall [")
                            .$("iteration:").$(i)
                            .$(", walIndex: ").$(walIndex)
                            .$(", inOrder: ").$(inOrder)
                            .$(" rowCount: ").$(rowCount)
                            .$(", tsIncrement: ").$(tsIncrement)
                            .$(", tsOffset: ").$(tsOffset)
                            .I$();

                    if (rnd.nextInt(releaseWriterSeed) == 0) {
                        // Close writer
                        LOG.info().$("=== releasing writers ===").$();
                        engine.releaseInactive();
                    }

                    addRowsToWalAndApplyToTable(i, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, inOrder);

                    LOG.info().$("verifying wall [").$("iteration:").$(i).I$();
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                    start += rowCount * tsIncrement + 1;
                }
            }
        });
    }

    @Test
    public void testReadAndWriteAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testReadAndWriteFrom2WallsInOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts, rnd, walWriter1, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts + rowsToInsertTotal * tsIncrement, rnd, walWriter2, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testReadAndWriteFrom2WallsOutOfOrder() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (
                    WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName);
                    WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, start, rnd, walWriter1, false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start = ts + rowsToInsertTotal * tsIncrement;
                addRowsToWalAndApplyToTable(1, tableName, tableCopyName, 2 * rowsToInsertTotal, tsIncrement, start, rnd, walWriter2, false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                start = ts + 3 * rowsToInsertTotal * tsIncrement;
                addRowsToWalAndApplyToTable(2, tableName, tableCopyName, rowsToInsertTotal, tsIncrement, start, rnd, walWriter1, false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testWalWriterWithExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            long tsIncrement = Timestamps.SECOND_MICROS;
            long ts = IntervalUtils.parseFloorPartialDate("2022-07-14T00:00:00");
            int rowCount = (int) (Files.PAGE_SIZE / 32);
            ts += (Timestamps.SECOND_MICROS * (60 * 60 - rowCount - 10));
            Rnd rnd = TestUtils.generateRandom(LOG);

            try (
                    WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
            ) {

                long start = ts;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                try (
                        WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)
                ) {
                    rnd.reset();
                    start += rowCount * tsIncrement - Timestamps.HOUR_MICROS / 2 + 1;
                    addRowsToWalAndApplyToTable(1, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter2, true);
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
                }

                start += rowCount * tsIncrement - Timestamps.HOUR_MICROS / 2 + 3;
                addRowsToWalAndApplyToTable(0, tableName, tableCopyName, rowCount, tsIncrement, start, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    private void addRowRwAllTypes(int iteration, TableWriter.Row row, int i, CharSequence symbol, String rndStr) {
        int col = 0;
        row.putInt(col++, i);
        row.putByte(col++, (byte) i);
        row.putLong(col++, i);
        row.putLong256(col++, i, i + 1, i + 2, i + 3);
        row.putDouble(col++, i + .5);
        row.putFloat(col++, i + .5f);
        row.putShort(col++, (short) iteration);
        row.putTimestamp(col++, i);
        row.putChar(col++, (char) (65 + i % 26));
        row.putBool(col++, i % 2 == 0);
        row.putDate(col++, i);
        row.putStr(col++, rndStr);
        row.putGeoHash(col++, i); // geo byte
        row.putGeoHash(col++, i); // geo int
        row.putGeoHash(col++, i); // geo short
        row.putGeoHash(col++, i); // geo long
        row.putStr(col++, (char) (65 + i % 26));
        row.putSym(col, symbol);
        row.append();
    }

    @SuppressWarnings("SameParameterValue")
    private void addRowsToWalAndApplyToTable(int iteration, String tableName, String tableCopyName, int rowsToInsertTotal, long tsIncrement, long startTs, Rnd rnd, WalWriter walWriter, boolean inOrder) {
        try (
                TableWriter copyWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableCopyName, "test");
                TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply wal")
        ) {
            if (!inOrder) {
                startTs += (rowsToInsertTotal - 1) * tsIncrement;
                tsIncrement = -tsIncrement;
            }

            for (int i = 0; i < rowsToInsertTotal; i++) {
                String symbol = rnd.nextInt(10) == 5 ? null : rnd.nextString(rnd.nextInt(9) + 1);
                String rndStr = rnd.nextInt(10) == 5 ? null : rnd.nextString(20);

                long rowTs = startTs;
                if (!inOrder && i > 3 && i < rowsToInsertTotal - 3) {
                    // Add jitter to the timestamps to randomise things even more.
                    rowTs += rnd.nextLong(2 * tsIncrement);
                }

                TableWriter.Row walRow = walWriter != null ? walWriter.newRow(rowTs) : tableWriter.newRow(rowTs);
                addRowRwAllTypes(iteration, walRow, i, symbol, rndStr);
                addRowRwAllTypes(iteration, copyWriter.newRow(rowTs), i, symbol, rndStr);
                startTs += tsIncrement;
            }

            copyWriter.commit();
            if (walWriter != null) {
                walWriter.commit();


                assertEquals("WalWriter{name=" + tableName + "}", walWriter.toString());

                try (
                        Path path = new Path();
                        WalReaderEvents wre = new WalReaderEvents(configuration.getFilesFacade())
                ) {
                    path.of(configuration.getRoot()).concat(tableWriter.getTableName()).concat(walWriter.getWalName());
                    WalEventCursor waleCursor = wre.of(path, path.length(), 0, WalWriter.WAL_FORMAT_VERSION);

                    while (waleCursor.tryHasNext()) {
                        Assert.assertTrue(waleCursor.hasNext());
                    }

                    WalEventCursor.DataInfo dataInfo = waleCursor.getDataInfo();
                    Assert.assertEquals(inOrder, !dataInfo.isOutOfOrder());

                    tableWriter.processWalCommit(
                            path,
                            walWriter.getSegment(),
                            !dataInfo.isOutOfOrder(),
                            dataInfo.getStartRowID(),
                            dataInfo.getEndRowID(),
                            dataInfo.getMinTimestamp(),
                            dataInfo.getMaxTimestamp() + 1,
                            dataInfo
                    );
                }
            } else {
                tableWriter.commit();
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void createTableAndCopy(String tableName, String tableCopyName) {
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("int", ColumnType.INT)
                .col("byte", ColumnType.BYTE)
                .col("long", ColumnType.LONG)
                .col("long256", ColumnType.LONG256)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("short", ColumnType.SHORT)
                .col("timestamp", ColumnType.TIMESTAMP)
                .col("char", ColumnType.CHAR)
                .col("boolean", ColumnType.BOOLEAN)
                .col("date", ColumnType.DATE)
                .col("string", ColumnType.STRING)
                .col("geoByte", ColumnType.getGeoHashTypeWithBits(5))
                .col("geoInt", ColumnType.getGeoHashTypeWithBits(20))
                .col("geoShort", ColumnType.getGeoHashTypeWithBits(10))
                .col("geoLong", ColumnType.getGeoHashTypeWithBits(30))
                .col("stringc", ColumnType.STRING)
                .col("label", ColumnType.SYMBOL)
                .timestamp("ts")
        ) {
            engine.createTableUnsafe(
                    AllowAllCairoSecurityContext.INSTANCE,
                    model.getMem(),
                    model.getPath(),
                    model
            );
            model.setName(tableCopyName);
            engine.createTableUnsafe(
                    AllowAllCairoSecurityContext.INSTANCE,
                    model.getMem(),
                    model.getPath(),
                    model
            );
        }
    }
}
