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

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class WalTableWriterTest extends AbstractGriffinTest {

    @Before
    public void setUp() {
        super.setUp();
        currentMillis = 0L;
    }

    @After
    public void tearDown() {
        super.tearDown();
        currentMillis = -1L;
    }

    @Test
    public void testReadAndWriteAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAllTypes";
            final String tableCopyName = tableName + "_copy";
            createTableAndCopy(tableName, tableCopyName);

            final int rowsToInsertTotal = 100;
            final long tsIncrement = 1000;
            long ts = Os.currentTimeMicros();

            Rnd rnd = new Rnd();
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                addRowsToWalAndApplyToTable(tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts, ts, rnd, walWriter, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testReadAndWriteFrom2Walls() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAllTypes";
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

                addRowsToWalAndApplyToTable(tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts, ts, rnd, walWriter1, true);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);

                addRowsToWalAndApplyToTable(tableName, tableCopyName, rowsToInsertTotal, tsIncrement, ts + rowsToInsertTotal * tsIncrement, ts, rnd, walWriter2, false);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

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
            TableUtils.createTable(
                    configuration,
                    model.getMem(),
                    model.getPath(),
                    model,
                    1
            );
            model.setName(tableCopyName);
            TableUtils.createTable(
                    configuration,
                    model.getMem(),
                    model.getPath(),
                    model,
                    1
            );
        }
    }

    private void addRowsToWalAndApplyToTable(String tableName, String tableCopyName, int rowsToInsertTotal, long tsIncrement, long ts, long minTs, Rnd rnd, WalWriter walWriter1, boolean inOrder) {
        try (
                TableWriter copyWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableCopyName, "test")
        ) {
            assertEquals(tableName, walWriter1.getTableName());
            HashSet<CharSequence> symbols = new HashSet<>();
            for (int i = 0; i < rowsToInsertTotal; i++) {

                String symbol = rnd.nextInt(10) == 5 ? null : rnd.nextString(rnd.nextInt(9) + 1);
                symbols.add(symbol);

                addRowRwAllTypes(walWriter1.newRow(ts), i, symbol);
                addRowRwAllTypes(copyWriter.newRow(ts), i, symbol);
                ts += tsIncrement;

            }

            copyWriter.commit();
            assertEquals(rowsToInsertTotal, walWriter1.size());

            assertEquals("WalWriter{name=" + tableName + "}", walWriter1.toString());

            try (
                    TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "apply wal");
                    Path path = new Path()
            ) {
                path.of(configuration.getRoot()).concat(tableWriter.getTableName()).concat(walWriter1.getWalName());
                long maxTs = minTs + (rowsToInsertTotal - 1) * tsIncrement;
                LongList symbolCounts = new LongList();
                symbolCounts.add(Numbers.encodeLowHighInts(0, symbols.size()));
                tableWriter.processWalCommit(path, "0", inOrder, 0, rowsToInsertTotal, minTs, maxTs + 1, symbolCounts);
            }
        }
    }

    private void addRowRwAllTypes(TableWriter.Row row, int i, CharSequence symbol) {
        row.putInt(0, i);
        row.putByte(1, (byte) i);
        row.putLong(2, i);
        row.putLong256(3, i, i + 1, i + 2, i + 3);
        row.putDouble(4, i + .5);
        row.putFloat(5, i + .5f);
        row.putShort(6, (short) i);
        row.putTimestamp(7, i);
        row.putChar(8, (char) (65 + i % 26));
        row.putBool(9, i % 2 == 0);
        row.putDate(10, i);
        row.putStr(11, String.valueOf(i));
        row.putGeoHash(12, i); // geo byte
        row.putGeoHash(13, i); // geo int
        row.putGeoHash(14, i); // geo short
        row.putGeoHash(15, i); // geo long
        row.putStr(16, (char) (65 + i % 26));
        row.putSym(17, symbol);
        row.append();
    }
}
