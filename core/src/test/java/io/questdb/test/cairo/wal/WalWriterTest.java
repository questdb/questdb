/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.SymbolMapDiffEntry;
import io.questdb.cairo.wal.WalDataRecord;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Long256Impl;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.questdb.cairo.wal.WalUtils.*;
import static org.junit.Assert.*;

public class WalWriterTest extends AbstractCairoTest {

    @Test
    public void apply1RowCommits1Writer() throws Exception {
        testApply1RowCommitManyWriters(Micros.SECOND_MICROS, 1_000_000, 1);
    }

    @Test
    public void apply1RowCommitsManyWriters() throws Exception {
        testApply1RowCommitManyWriters(Micros.SECOND_MICROS, 1_000_000, 16);
    }

    @Test
    public void apply1RowCommitsManyWritersExceedsBlockSortRanges() throws Exception {
        testApply1RowCommitManyWriters(Micros.YEAR_10000 / 300, 265, 16);
    }

    @Test
    public void test1RowCommitEqualSize() throws Exception {
        // Force 1 by 1 commit application
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1);
        setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 1);

        assertMemoryLeak(() -> {
            execute("create table sm (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL dedup upsert keys (ts, id)");
            TableToken tableToken = engine.verifyTableName("sm");

            execute("insert into " + tableToken.getTableName() + "(id, ts) values (1, '2022-02-24')");
            execute("insert into " + tableToken.getTableName() + "(id, ts) values (2, '2022-02-24')");
            drainWalQueue();

            assertSqlCursors("sm", "select * from sm order by id");
            assertSql(
                    """
                            count\tmin\tmax
                            2\t2022-02-24T00:00:00.000000Z\t2022-02-24T00:00:00.000000Z
                            """, "select count(*), min(ts), max(ts) from sm"
            );
        });
    }

    @Test
    public void testAddColumnRollsUncommittedRowsToNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    walName1 = walWriter1.getWalName();
                    walName2 = walWriter2.getWalName();

                    TableWriter.Row row = walWriter1.newRow(0);
                    row.putByte(0, (byte) 1);
                    row.append();
                    walWriter1.commit();

                    row = walWriter2.newRow(0);
                    row.putByte(0, (byte) 10);
                    row.append();
                    walWriter2.commit();

                    row = walWriter2.newRow(0);
                    row.putByte(0, (byte) 100);
                    row.append();

                    addColumn(walWriter1, "c", ColumnType.INT);

                    walWriter2.commit();

                    row = walWriter1.newRow(0);
                    row.putByte(0, (byte) 110);
                    row.append();
                    walWriter1.commit();
                }
            }

            TableModel model = defaultModel(tableToken.getTableName());
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(10, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            model.col("c", ColumnType.INT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 1, 1)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(110, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 1, 1)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(100, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testAddColumnsRollLargeSegment() throws Exception {
        assertMemoryLeak(() -> {
            // This test reproduces a bug where rolling a large segment file sized over 2GB
            // resulted in int overflow and commit exception.

            // The test is a bit slow writing a column over 2Gb to WAL
            node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 3000_000);
            node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_SIZE, 3 * Numbers.SIZE_1GB);

            TableToken tableToken = createTable(new TableModel(configuration, testName.getMethodName(), PartitionBy.HOUR)
                    .col("desk", ColumnType.VARCHAR)
                    .timestamp("instanceName")
                    .wal()
            );

            long initialTimestamp = MicrosTimestampDriver.floor("2022-02-24T00:40:00.000Z");
            long tsIncrement = 1000_0000L;

            int varcharSize = 20 * Numbers.SIZE_1MB;
            long buffer = Unsafe.malloc(varcharSize, MemoryTag.NATIVE_DEFAULT);
            Vect.memset(buffer, varcharSize, (byte) 'a');
            DirectUtf8String longVarchar = new DirectUtf8String();
            longVarchar.of(buffer, buffer + varcharSize);

            try (WalWriter writer = engine.getWalWriter(tableToken)) {
                // Add rows so that total size of varchar column is > 2Gb
                int rowCount = (int) ((Numbers.SIZE_1GB * 2 + Numbers.SIZE_1MB * 20) / varcharSize);
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = writer.newRow(initialTimestamp);
                    initialTimestamp += tsIncrement;
                    row.putVarchar(0, longVarchar);
                    row.append();
                }
                writer.commit();

                // Add few more rows and then add a column
                for (int i = 0; i < 1; i++) {
                    TableWriter.Row row = writer.newRow(initialTimestamp);
                    initialTimestamp += tsIncrement;
                    row.putVarchar(0, longVarchar);
                    row.append();
                }
                writer.addColumn("newColumn", ColumnType.DOUBLE);
                writer.commit();
            } finally {
                Path.clearThreadLocals();
                Unsafe.free(buffer, varcharSize, MemoryTag.NATIVE_DEFAULT);
            }
        });

    }

    @Test
    public void testAddManyColumnsExistingSegments() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = TestUtils.generateRandom(LOG);
            int threadCount = 2 + rnd.nextInt(1);
            int columnAddLimit = 10 + rnd.nextInt(5);

            node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 30);

            TableToken tableToken = createTable(new TableModel(configuration, testName.getMethodName(), PartitionBy.HOUR)
                    .col("cluster", ColumnType.SYMBOL)
                    .col("hostName", ColumnType.SYMBOL)
                    .col("desk", ColumnType.SYMBOL)
                    .timestamp("instanceName")
                    .wal()
            );

            ObjList<Thread> writerThreads = new ObjList<>();

            AtomicInteger error = new AtomicInteger();

            long initialTimestamp = MicrosTimestampDriver.floor("2022-02-24T00:40:00.000Z");
            long tsIncrement = rnd.nextLong(1000_0000L);
            for (int th = 0; th < threadCount; th++) {
                Rnd threadRnd = new Rnd(rnd.nextLong(), rnd.nextLong());

                writerThreads.add(new Thread(() -> {
                    try (WalWriter writer = engine.getWalWriter(tableToken)) {
                        int columnNum = 1;
                        long timestamp = initialTimestamp;

                        while (columnNum < columnAddLimit) {
                            int rowCount = 10 + threadRnd.nextInt(10);
                            boolean addColumn = threadRnd.nextInt(20) == 0;
                            if (addColumn) {
                                rowCount = 30 + threadRnd.nextInt(10);
                            }
                            int initialRows = addColumn ? rnd.nextInt(rowCount) : Integer.MAX_VALUE;

                            for (int r = 0; r < rowCount; r++) {
                                generateRow(writer, threadRnd, timestamp);
                                timestamp += tsIncrement;
                                if (r == initialRows) {
                                    while (true) {
                                        String columnName = rnd.nextBoolean() ? "d" : "D" + (columnNum++);
                                        try {
                                            int typeRnd = rnd.nextInt(3);
                                            int columnType = switch (typeRnd) {
                                                case 0, 1 -> ColumnType.DOUBLE;
                                                default -> ColumnType.STRING;
                                            };
                                            addColumn(writer, columnName, columnType);
                                            break;
                                        } catch (CairoException e) {
                                            int columnWriterIndex = writer.getMetadata().getColumnIndexQuiet(columnName);
                                            if (columnWriterIndex < 0) {
                                                // the column is still not there, something must be wrong
                                                throw e;
                                            }
                                            // all good, someone added the column concurrently
                                            columnNum++;
                                        }
                                    }
                                }
                            }
                            writer.commit();

                            if (rnd.nextInt(20) == 0) {
                                String columnName = rnd.nextBoolean() ? "d" : "D" + (1 + rnd.nextInt(columnNum));
                                try {
                                    AlterOperationBuilder removeColumnOperation = new AlterOperationBuilder().ofDropColumn(0, writer.getTableToken(), 0);
                                    removeColumnOperation.ofDropColumn(columnName);
                                    writer.apply(removeColumnOperation.build(), true);
                                } catch (CairoException ex) {
                                    if (ex.getMessage().contains("column does not exist")) {
                                        // all good, someone removed the column concurrently
                                        continue;
                                    }
                                    throw ex;
                                }
                            }
                        }
                    } catch (Throwable e) {
                        error.incrementAndGet();
                        throw e;
                    } finally {
                        Path.clearThreadLocals();
                    }
                }));
                writerThreads.getLast().start();
            }

            for (int i = 0; i < writerThreads.size(); i++) {
                writerThreads.getQuick(i).join();
            }

            Assert.assertEquals(0, error.get());
        });

    }

    @Test
    public void testAddingColumnClosesSegment() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();

                addColumn(walWriter, "c", ColumnType.INT);
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 10);
                row.append();
                walWriter.commit();

                addColumn(walWriter, "d", ColumnType.SHORT);
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 100);
                row.putShort(4, (short) 1000);
                row.append();
                walWriter.commit();
            }

            TableModel model = defaultModel(tableToken.getTableName());
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            model.col("c", ColumnType.INT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 1, 1)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(10, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(Integer.MIN_VALUE, record.getInt(3));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            model.col("d", ColumnType.SHORT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 2, 1)) {
                assertEquals(5, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(100, record.getByte(0));
                assertEquals(1000, record.getShort(4));
                assertNull(record.getStrA(1));
                assertEquals(Integer.MIN_VALUE, record.getInt(3));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testAddingColumnOverlapping() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    walName1 = walWriter1.getWalName();
                    walName2 = walWriter2.getWalName();
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                }
            }

            TableModel model = defaultModel(tableToken.getTableName());
            model.col("c", ColumnType.INT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 0)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(0, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertFalse(eventCursor.hasNext());
            }

            model.col("d", ColumnType.INT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 0, 0)) {
                assertEquals(5, reader.getColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableToken.getTableName(), reader.getTableName());
                assertEquals(0, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testAddingColumnOverlappingAndAddRow() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    walName1 = walWriter1.getWalName();
                    walName2 = walWriter2.getWalName();
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);

                    TableWriter.Row row = walWriter1.newRow();
                    row.putByte(0, (byte) 1);
                    row.append();
                    walWriter1.commit();
                }
            }

            TableModel model = defaultModel(tableName);
            model.col("c", ColumnType.INT);
            model.col("d", ColumnType.INT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 1)) {
                assertEquals(5, reader.getColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 0, 0)) {
                assertEquals(5, reader.getColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(0, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testAddingDuplicateColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();

                addColumn(walWriter, "c", ColumnType.INT);
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 10);
                row.append();
                walWriter.commit();

                try {
                    addColumn(walWriter, "c", ColumnType.SHORT);
                    assertExceptionNoLeakCheck("Should not be able to add duplicate column");
                } catch (CairoException e) {
                    assertEquals("[-100] duplicate column [name=c]", e.getMessage());
                }

                row = walWriter.newRow(0);
                row.putByte(0, (byte) 100);
                row.append();
                walWriter.commit();
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            model.col("c", ColumnType.INT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 1, 2)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(2, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(10, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(Integer.MIN_VALUE, record.getInt(3));
                assertEquals(0, record.getRowId());
                assertTrue(cursor.hasNext());
                assertEquals(100, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(Integer.MIN_VALUE, record.getInt(3));
                assertEquals(1, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(1, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(1, dataInfo.getStartRowID());
                assertEquals(2, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }

            try {
                engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 2, 1);
                assertExceptionNoLeakCheck("Segment 2 should not exist");
            } catch (CairoException e) {
                assertTrue(e.getMessage().endsWith("could not open, file does not exist: " + engine.getConfiguration().getDbRoot() +
                        File.separatorChar + tableName + TableUtils.SYSTEM_TABLE_NAME_SUFFIX + "1" +
                        File.separatorChar + walName +
                        File.separatorChar + "2" +
                        File.separatorChar + TableUtils.META_FILE_NAME + "]"));
            }
        });
    }

    @Test
    public void testAddingSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAddSymbolCol";
            TableToken tableToken = createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 125);
                row.append();
                walWriter.commit();
                addColumn(walWriter, "c", ColumnType.SYMBOL);
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(125, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testAlterAddChangeLag() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());
            execute("alter table " + tableToken.getTableName() + " SET PARAM o3MaxLag = 20s");
            execute("alter table " + tableToken.getTableName() + " add i2 int");
            execute("insert into " + tableToken.getTableName() + "(ts, i2) values ('2022-02-24', 2)");

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    """
                            a\tb\tts\ti2
                            0\t\t2022-02-24T00:00:00.000000Z\t2
                            """, tableToken.getTableName()
            );
        });
    }

    @Test
    public void testAlterAddChangeMaxUncommitted() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());
            execute("alter table " + tableToken.getTableName() + " set PARAM maxUncommittedRows = 20000");
            execute("alter table " + tableToken.getTableName() + " add i2 int");
            execute("insert into " + tableToken.getTableName() + "(ts, i2) values ('2022-02-24', 2)");

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    """
                            a\tb\tts\ti2
                            0\t\t2022-02-24T00:00:00.000000Z\t2
                            """, tableToken.getTableName()
            );
        });
    }

    @Test
    public void testAlterAddDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());
            execute("alter table " + tableToken.getTableName() + " add sym2 symbol");
            execute("alter table " + tableToken.getTableName() + " alter column sym2 add index");
            execute("alter table " + tableToken.getTableName() + " alter column sym2 drop index");
            execute("alter table " + tableToken.getTableName() + " add i2 int");

            drainWalQueue();

            execute("insert into " + tableToken.getTableName() + "(ts, i2) values ('2022-02-24', 2)");

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    """
                            a\tb\tts\tsym2\ti2
                            0\t\t2022-02-24T00:00:00.000000Z\t\t2
                            """, tableToken.getTableName()
            );
        });
    }

    @Test
    public void testAlterTableAllowedWhenDataTransactionPending() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                // no commit intentional
                addColumn(walWriter, "c", ColumnType.INT);
                walWriter.commit();
            }

            drainWalQueue();


            assertSql(
                    """
                            a\tb\tts\tc
                            1\t\t1970-01-01T00:00:00.000000Z\tnull
                            """, tableToken.getTableName()
            );
        });
    }

    @Test
    public void testAlterWithParallelTableRename() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table alter_rename0 (i int, s symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create table alter_rename1 (i int, s symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
            TableToken token0 = engine.verifyTableName("alter_rename0");
            TableToken token1 = engine.verifyTableName("alter_rename1");

            AtomicBoolean stop = new AtomicBoolean(false);
            CountDownLatch countDownLatch = new CountDownLatch(2);
            AtomicInteger errors = new AtomicInteger();

            Thread alterThread = new Thread(() -> {
                try {
                    countDownLatch.countDown();
                    for (int i = 0; i < 100; i++) {
                        try {
                            execute("alter table alter_rename0 alter column s symbol capacity 1024");
                        } catch (SqlException e) {
                            if (!e.isTableDoesNotExist()) {
                                throw e;
                            }
                            i--;
                        } catch (CairoException e) {
                            if (!e.isTableDoesNotExist()) {
                                throw e;
                            }
                            i--;
                        }
                        drainWalQueue();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                    throw new RuntimeException(e);
                } finally {
                    Path.clearThreadLocals();
                    stop.set(true);
                }
            });
            alterThread.start();

            Thread renameThread = new Thread(() -> {
                try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                        .with(AllowAllSecurityContext.INSTANCE);
                     SqlCompiler compiler = engine.getSqlCompiler()) {
                    countDownLatch.countDown();
                    while (!stop.get()) {
                        execute(compiler, "rename table alter_rename0 to alter_rename_tmp", sqlExecutionContext);
                        execute(compiler, "rename table alter_rename1 to alter_rename0", sqlExecutionContext);
                        execute(compiler, "rename table alter_rename_tmp to alter_rename1", sqlExecutionContext);
                    }
                } catch (Throwable th) {
                    errors.incrementAndGet();
                    throw new RuntimeException(th);
                } finally {
                    Path.clearThreadLocals();
                    stop.set(true);
                }
            });
            renameThread.start();


            alterThread.join();
            renameThread.join();

            Assert.assertEquals(0, errors.get());
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token0));
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token1));

        });
    }

    @Test
    public void testApplyManySmallCommits2Writers() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table sm (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("sm");
            long startTs = MicrosTimestampDriver.floor("2022-02-24");
            long tsIncrement = Micros.MINUTE_MICROS;

            long ts = startTs;
            int totalRows = 2000;
            int iterations = 20;
            int symbolCount = 75;

            Utf8StringSink sink = new Utf8StringSink();
            StringSink stringSink = new StringSink();
            for (int c = 0; c < iterations; c++) {
                try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                    try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {

                        int n = totalRows * (c + 1);
                        for (int i = c * totalRows; i < n; i += 2) {
                            TableWriter.Row row = walWriter1.newRow(ts);
                            row.putInt(0, i);
                            row.putLong(2, i + 1);
                            stringSink.clear();
                            stringSink.put(i);
                            row.putStr(3, stringSink);
                            sink.clear();
                            sink.put(i);
                            row.putVarchar(4, sink);
                            stringSink.clear();
                            stringSink.put(i % symbolCount);
                            row.putSym(5, stringSink);
                            row.append();
                            walWriter1.commit();

                            TableWriter.Row row2 = walWriter2.newRow(ts);
                            row2.putInt(0, i + 1);
                            row2.putLong(2, i + 2);
                            stringSink.clear();
                            stringSink.put(i + 1);
                            row2.putStr(3, stringSink);
                            sink.clear();
                            sink.put(i + 1);
                            row2.putVarchar(4, sink);
                            stringSink.clear();
                            stringSink.put((i + 1) % symbolCount);
                            row2.putSym(5, stringSink);
                            row2.append();
                            walWriter2.commit();

                            ts += tsIncrement;
                        }
                    }

                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                    assertSql(
                            "count\tmin\tmax\n" +
                                    (c + 1) * totalRows + "\t2022-02-24T00:00:00.000000Z\t" + Micros.toUSecString(ts - tsIncrement) + "\n", "select count(*), min(ts), max(ts) from sm"
                    );
                    assertSqlCursors("sm", "select * from sm order by id");
                    assertSql("id\tts\ty\ts\tv\tm\n", "select * from sm WHERE id <> cast(s as int)");
                    assertSql("id\tts\ty\ts\tv\tm\n", "select * from sm WHERE id <> cast(v as int)");
                    assertSql("id\tts\ty\ts\tv\tm\n", "select * from sm WHERE id % " + symbolCount + " <> cast(m as int)");
                }
            }
        });
    }

    @Test
    public void testCancelRowDoesNotStartsNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                assertEquals(0, walWriter.getSegmentRowCount());
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                assertEquals(1, walWriter.getSegmentRowCount());
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 11);
                row.append();
                assertEquals(2, walWriter.getSegmentRowCount());
                row = walWriter.newRow(0);

                row.putByte(0, (byte) 111);
                assertEquals(2, walWriter.getSegmentRowCount());
                row.cancel();

                assertEquals(2, walWriter.getSegmentRowCount());
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 112);
                row.append();
                assertEquals(3, walWriter.getSegmentRowCount());
                walWriter.commit();
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 3)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(3, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());

                assertTrue(cursor.hasNext());
                assertEquals(11, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(1, record.getRowId());

                assertTrue(cursor.hasNext());
                assertEquals(112, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(2, record.getRowId());

                assertFalse(cursor.hasNext());
                assertColumnMetadata(model, reader);
            }

            try (Path path = new Path().of(configuration.getDbRoot())) {
                assertWalFileExist(path, tableToken, walName, 0, "_meta");
                assertWalFileExist(path, tableToken, walName, 0, "_event");
                assertWalFileExist(path, tableToken, walName, 0, "a.d");
                assertWalFileExist(path, tableToken, walName, 0, "b.d");
                assertWalFileExist(path, tableToken, walName, 0, "b.i");
            }
        });
    }

    @Test
    public void testCommit() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < 18; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                for (int i = 0; i < 6; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.commit(); //should not create new txn, this is noop
                walWriter.commit(); //should not create new txn, this is noop
                for (int i = 0; i < 20; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.rollSegment();
                for (int i = 0; i < 7; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }

                walWriter.commit();
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 44)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(44, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i > 23 ? i - 24 : (i > 17 ? i - 18 : i), record.getByte(0));
                    assertNull(record.getStrA(1));
                    assertEquals(i, record.getRowId());
                    i++;
                }
                assertEquals(44, i);

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(18, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(1, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(18, dataInfo.getStartRowID());
                assertEquals(24, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(2, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(24, dataInfo.getStartRowID());
                assertEquals(44, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 1, 7)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(7, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertNull(record.getStrA(1));
                    assertEquals(i, record.getRowId());
                    i++;
                }
                assertEquals(7, i);

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(7, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testConcurrentAddRemoveColumnDifferentColNamePerThread() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final int numOfRows = 11;
            final int numOfThreads = 10;
            final CountDownLatch alterFinished = new CountDownLatch(numOfThreads);
            final SOCountDownLatch writeFinished = new SOCountDownLatch(numOfThreads);
            final AtomicInteger columnNumber = new AtomicInteger();

            // map<walId, Error|Exception>
            final ConcurrentMap<Integer, Throwable> errors = new ConcurrentHashMap<>(numOfThreads);
            // map<walId, numOfThreadsUsedThisWalWriter>
            final ConcurrentMap<Integer, AtomicInteger> counters = new ConcurrentHashMap<>(numOfThreads);
            for (int i = 0; i < numOfThreads; i++) {
                new Thread(() -> {
                    final String colName = "col" + columnNumber.incrementAndGet();
                    TableWriter.Row row;
                    int walId = -1;
                    boolean countedDown = false;
                    try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                        walId = walWriter.getWalId();
                        final AtomicInteger counter = counters.computeIfAbsent(walId, name -> new AtomicInteger());
                        counter.incrementAndGet();

                        addColumn(walWriter, colName, ColumnType.LONG);
                        removeColumn(walWriter, colName);
                        addColumn(walWriter, colName, ColumnType.STRING);
                        removeColumn(walWriter, colName);
                        addColumn(walWriter, colName, ColumnType.BYTE);
                        removeColumn(walWriter, colName);

                        alterFinished.countDown();
                        countedDown = true;

                        alterFinished.await();
                        for (int n = 0; n < numOfRows; n++) {
                            row = walWriter.newRow(0);
                            row.putByte(0, (byte) 1);
                            row.putStr(1, "test" + n);
                            row.append();
                        }
                        walWriter.commit();
                        walWriter.rollSegment();
                    } catch (Throwable th) {
                        errors.put(walId, th);
                    } finally {
                        Path.clearThreadLocals();
                        if (!countedDown) {
                            alterFinished.countDown();
                        }
                        writeFinished.countDown();
                    }
                }).start();
            }
            writeFinished.await();

            if (!errors.isEmpty()) {
                for (Throwable th : errors.values()) {
                    th.printStackTrace(System.out);
                }
                Assert.fail("Write failed");
            }

            final LongHashSet txnSet = new LongHashSet(numOfThreads);
            final IntList symbolCounts = new IntList();
            symbolCounts.add(numOfRows);
            TableModel model = defaultModel(tableName);
            for (Map.Entry<Integer, AtomicInteger> counterEntry : counters.entrySet()) {
                final int walId = counterEntry.getKey();
                final int count = counterEntry.getValue().get();
                final String walName = WAL_NAME_BASE + walId;

                for (int i = 0; i < count; i++) {
                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 2 * i, numOfRows)) {
                        assertEquals(3, reader.getRealColumnCount());
                        assertEquals(walName, reader.getWalName());
                        assertEquals(tableName, reader.getTableName());
                        assertEquals(numOfRows, reader.size());

                        final RecordCursor cursor = reader.getDataCursor();
                        final Record record = cursor.getRecord();
                        int n = 0;
                        while (cursor.hasNext()) {
                            assertEquals(1, record.getByte(0));
                            TestUtils.assertEquals("test" + n, record.getStrA(1));
                            assertEquals(n, record.getRowId());
                            n++;
                        }
                        assertEquals(numOfRows, n);

                        assertColumnMetadata(model, reader);

                        final WalEventCursor eventCursor = reader.getEventCursor();
                        assertTrue(eventCursor.hasNext());
                        assertEquals(WalTxnType.DATA, eventCursor.getType());
                        txnSet.add(Numbers.encodeLowHighInts(walId * 10 + i, (int) eventCursor.getTxn()));

                        final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                        assertEquals(0, dataInfo.getStartRowID());
                        assertEquals(numOfRows, dataInfo.getEndRowID());
                        assertEquals(0, dataInfo.getMinTimestamp());
                        assertEquals(0, dataInfo.getMaxTimestamp());
                        assertFalse(dataInfo.isOutOfOrder());

                        assertNull(dataInfo.nextSymbolMapDiff());

                        assertFalse(eventCursor.hasNext());
                    }

                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 2 * i + 1, 0)) {
                        assertEquals(3, reader.getRealColumnCount());
                        assertEquals(walName, reader.getWalName());
                        assertEquals(tableName, reader.getTableName());
                        assertEquals(0, reader.size());

                        final RecordCursor cursor = reader.getDataCursor();
                        assertFalse(cursor.hasNext());

                        assertColumnMetadata(model, reader);

                        final WalEventCursor eventCursor = reader.getEventCursor();
                        assertFalse(eventCursor.hasNext());
                    }

                    try (Path path = new Path().of(configuration.getDbRoot())) {
                        assertWalFileExist(path, tableToken, walName, 0, "_meta");
                        assertWalFileExist(path, tableToken, walName, 0, "_event");
                        assertWalFileExist(path, tableToken, walName, 0, "a.d");
                        assertWalFileExist(path, tableToken, walName, 0, "b.d");
                        assertWalFileExist(path, tableToken, walName, 1, "_meta");
                        assertWalFileExist(path, tableToken, walName, 1, "_event");
                        assertWalFileExist(path, tableToken, walName, 1, "a.d");
                        assertWalFileExist(path, tableToken, walName, 1, "b.d");
                    }
                }
            }

            assertEquals(numOfThreads, txnSet.size());
        });
    }

    @Test
    public void testConcurrentInsert() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            final int numOfRows = 4000;
            final int maxRowCount = 500;
            node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, maxRowCount);
            assertEquals(maxRowCount, configuration.getWalSegmentRolloverRowCount());
            final int numOfSegments = numOfRows / maxRowCount;
            final int numOfThreads = 10;
            final int numOfTxn = numOfThreads * numOfSegments;
            final SOCountDownLatch writeFinished = new SOCountDownLatch(numOfThreads);

            // map<walId, Error|Exception>
            final ConcurrentMap<Integer, Throwable> errors = new ConcurrentHashMap<>(numOfThreads);
            // map<walId, numOfThreadsUsedThisWalWriter>
            final ConcurrentMap<Integer, AtomicInteger> counters = new ConcurrentHashMap<>(numOfThreads);
            for (int i = 0; i < numOfThreads; i++) {
                new Thread(() -> {
                    TableWriter.Row row;
                    int walId = -1;
                    try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                        walId = walWriter.getWalId();
                        final AtomicInteger counter = counters.computeIfAbsent(walId, name -> new AtomicInteger());
                        assertEquals(counter.get() > 0 ? maxRowCount : 0, walWriter.getSegmentRowCount());
                        counter.incrementAndGet();
                        for (int n = 0; n < numOfRows; n++) {
                            row = walWriter.newRow(0);
                            row.putInt(0, n);
                            row.putSym(1, "test" + n);
                            row.append();
                            if ((n + 1) % maxRowCount == 0) {
                                walWriter.commit();
                            }
                        }
                        walWriter.commit();
                        assertWalExistence(true, tableName, walId);
                        for (int n = 0; n < counter.get() * numOfSegments; n++) {
                            assertSegmentExistence(true, tableName, walId, n);
                        }
                        assertSegmentExistence(false, tableName, walId, counter.get() * numOfSegments);
                    } catch (Throwable th) {
                        errors.put(walId, th);
                    } finally {
                        Path.clearThreadLocals();
                        writeFinished.countDown();
                    }
                }).start();
            }
            writeFinished.await();

            if (!errors.isEmpty()) {
                for (Throwable th : errors.values()) {
                    th.printStackTrace(System.out);
                }
                Assert.fail("Write failed");
            }

            final LongHashSet txnSet = new LongHashSet(numOfTxn);
            final IntList symbolCounts = new IntList();
            symbolCounts.add(numOfRows);
            model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal();
            for (Map.Entry<Integer, AtomicInteger> counterEntry : counters.entrySet()) {
                final int walId = counterEntry.getKey();
                final int count = counterEntry.getValue().get();
                final String walName = WAL_NAME_BASE + walId;

                for (int segmentId = 0; segmentId < count * numOfSegments; segmentId++) {
                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, segmentId, maxRowCount)) {
                        assertEquals(3, reader.getColumnCount());
                        assertEquals(walName, reader.getWalName());
                        assertEquals(tableName, reader.getTableName());
                        assertEquals(maxRowCount, reader.size());

                        final RecordCursor cursor = reader.getDataCursor();
                        final Record record = cursor.getRecord();
                        int n = 0;
                        while (cursor.hasNext()) {
                            assertEquals((segmentId % numOfSegments) * maxRowCount + n, record.getInt(0));
                            assertEquals(n, record.getInt(1)); // New symbol value every row
                            assertEquals("test" + ((segmentId % numOfSegments) * maxRowCount + n), record.getSymA(1));
                            assertEquals(n, record.getRowId());
                            n++;
                        }
                        assertEquals(maxRowCount, n);

                        assertColumnMetadata(model, reader);

                        final WalEventCursor eventCursor = reader.getEventCursor();
                        assertTrue(eventCursor.hasNext());
                        assertEquals(WalTxnType.DATA, eventCursor.getType());
                        txnSet.add(Numbers.encodeLowHighInts(walId, segmentId * 1000 + (int) eventCursor.getTxn()));

                        final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                        assertEquals(0, dataInfo.getStartRowID());
                        assertEquals(maxRowCount, dataInfo.getEndRowID());
                        assertEquals(0, dataInfo.getMinTimestamp());
                        assertEquals(0, dataInfo.getMaxTimestamp());
                        assertFalse(dataInfo.isOutOfOrder());

                        final SymbolMapDiff symbolMapDiff = dataInfo.nextSymbolMapDiff();
                        assertEquals(1, symbolMapDiff.getColumnIndex());
                        int expectedKey = 0;
                        SymbolMapDiffEntry entry;
                        while ((entry = symbolMapDiff.nextEntry()) != null) {
                            assertEquals("test" + ((segmentId % numOfSegments) * maxRowCount + expectedKey), entry.getSymbol().toString());
                            expectedKey++;
                        }
                        assertEquals(maxRowCount, expectedKey);
                        assertNull(dataInfo.nextSymbolMapDiff());

                        assertFalse(eventCursor.hasNext());
                    }

                    try (Path path = new Path().of(configuration.getDbRoot())) {
                        assertWalFileExist(path, tableToken, walName, segmentId, "_meta");
                        assertWalFileExist(path, tableToken, walName, segmentId, "_event");
                        assertWalFileExist(path, tableToken, walName, segmentId, "a.d");
                        assertWalFileExist(path, tableToken, walName, segmentId, "b.d");
                        assertWalFileExist(path, tableToken, walName, segmentId, "ts.d");
                    }
                }
            }

            assertEquals(numOfTxn, txnSet.size());
            for (Map.Entry<Integer, AtomicInteger> counterEntry : counters.entrySet()) {
                final int walId = counterEntry.getKey();
                final int count = counterEntry.getValue().get();
                for (int segmentId = 0; segmentId < count * numOfSegments; segmentId++) {
                    txnSet.remove(Numbers.encodeLowHighInts(walId, segmentId * 1000));
                }
            }
            assertEquals(0, txnSet.size());
        });
    }

    @Test
    public void testDesignatedTimestampIncludesSegmentRowNumber_NotOOO() throws Exception {
        testDesignatedTimestampIncludesSegmentRowNumber(new int[]{1000, 1200}, false);
    }

    @Test
    public void testDesignatedTimestampIncludesSegmentRowNumber_OOO() throws Exception {
        testDesignatedTimestampIncludesSegmentRowNumber(new int[]{1500, 1200}, true);
    }

    @Test
    public void testDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());
            execute("ALTER TABLE " + tableName + " ADD COLUMN sym SYMBOL INDEX");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym DROP INDEX");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN SYM DROP INDEX");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN SYM Add INDEX");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN sym DROP INDEX");
            execute("ALTER TABLE " + tableName + " ALTER COLUMN SYm DROP INDEX");

            drainWalQueue();

            Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(tableToken));
        });
    }

    @Test
    public void testExceptionThrownIfSequencerCannotBeOpened() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long getPageSize() {
                RuntimeException e = new RuntimeException("Test failure");
                e.fillInStackTrace();
                final StackTraceElement[] stackTrace = e.getStackTrace();
                if (stackTrace[4].getClassName().endsWith("TableSequencerImpl")) {
                    throw e;
                }
                return Files.PAGE_SIZE;
            }
        };

        assertMemoryLeak(
                ff, () -> {
                    try {
                        createTable(testName.getMethodName());
                        assertExceptionNoLeakCheck("Exception expected");
                    } catch (Exception e) {
                        // this exception will be handled in ILP/PG/HTTP
                        assertEquals("Test failure", e.getMessage());
                    }
                }
        );
    }

    @Test
    public void testExceptionThrownIfSequencerCannotCreateDir() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return 999;
            }

            @Override
            public int mkdirs(Path path, int mode) {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[1].getClassName().endsWith("TableSequencerImpl") && stackTrace[1].getMethodName().equals("createSequencerDir")) {
                        return 1;
                    }
                }
                return Files.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(
                ff, () -> {
                    try {
                        createTable(testName.getMethodName());
                        assertExceptionNoLeakCheck("Exception expected");
                    } catch (Exception e) {
                        // this exception will be handled in ILP/PG/HTTP
                        assertTrue(e.getMessage().startsWith("[999] Cannot create sequencer directory:"));
                    }
                }
        );
    }

    @Test
    public void testExtractNewWalEvents() throws Exception {
        assertMemoryLeak(() -> {
            String expected = """
                    a\tb
                    0\tsym0
                    1\tsym1
                    2\tsym2
                    3\tsym3
                    4\tsym4
                    5\tsym5
                    6\tsym6
                    7\tsym7
                    8\tsym8
                    9\tsym9
                    """;
            // old format only
            final String tableName = "testExtractNoNewWalEvents";
            final long refreshTxn = 42;
            TableToken tableToken = createPopulateTable(tableName, refreshTxn, false);

            checkWalEvents(tableToken, refreshTxn, false);

            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(expected, "select a,b from " + tableName);

            // mix with new format
            final String tableName1 = "testExtractNewWalEvents";
            TableToken tableToken1 = createPopulateTable(tableName1, refreshTxn, true);

            checkWalEvents(tableToken1, refreshTxn, true);

            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken1));
            assertSql(expected, "select a,b from " + tableName1);
        });
    }

    @Test
    public void testFileOpenExceptionThrownIfSequencerCannotBeOpened() throws Exception {
        assertMemoryLeak(() -> {

            createTable(testName.getMethodName());
            TableToken tableToken = engine.verifyTableName(testName.getMethodName());

            engine.execute("alter table " + tableToken.getTableName() + " set type bypass wal");
            engine.load();

            try {
                engine.getTableSequencerAPI().lastTxn(tableToken);
                assertExceptionNoLeakCheck("Exception expected");
            } catch (CairoException e) {
                // The table is not dropped in the table registry, the exception should not be table dropped exception
                Assert.assertFalse(e.isTableDropped());
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
            }
        });
    }

    @Test
    public void testIgnoreNewWalEvents() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "new_wal_events";
            final String tableCopyName = tableName + "_copy";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts").wal();
            TableModel copyModel = new TableModel(configuration, tableCopyName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts").noWal();
            TableToken tableToken = createTable(model);
            createTable(copyModel);

            final int rowsToInsertTotal = 100;
            Rnd rnd = new Rnd();
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    WalWriter walWriter = engine.getWalWriter(tableToken);
                    TableWriter copyWriter = getWriter(tableCopyName)
            ) {
                for (int i = 0; i < rowsToInsertTotal; i++) {
                    if (rnd.nextBoolean()) {
                        walWriter.resetMatViewState(i, i, true, "Invalidating " + i, i, null, -1);
                    }
                    String symbol = rnd.nextInt(10) == 5 ? null : rnd.nextString(rnd.nextInt(9) + 1);
                    int v = rnd.nextInt(rowsToInsertTotal);

                    TableWriter.Row row = walWriter.newRow(0);
                    row.putInt(0, v);
                    row.putSym(1, symbol);
                    row.append();

                    TableWriter.Row rowc = copyWriter.newRow(0);
                    rowc.putInt(0, v);
                    rowc.putSym(1, symbol);
                    rowc.append();
                }

                copyWriter.commit();
                if (rnd.nextBoolean()) {
                    walWriter.commit();
                } else {
                    walWriter.commitMatView(0, 0, 0, 0, 0);
                }

                drainWalQueue();
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableCopyName, tableName, LOG);
            }
        });
    }

    @Test
    public void testLargeSegmentRollover() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken;
            // Schema with 8 columns, 8 bytes each = 64 bytes per row
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .timestamp("ts")
                    .col("a", ColumnType.LONG)
                    .wal();
            tableToken = createTable(model);

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                final RowInserter ins = new RowInserter() {
                    private long count;
                    private long ts = 1000000000L;

                    @Override
                    public long getCount() {
                        return count;
                    }

                    @Override
                    public void insertRow() {
                        TableWriter.Row row = walWriter.newRow(ts);
                        row.putLong(1, count);
                        row.append();
                        ts += 1000;
                        ++count;
                    }
                };

                ins.insertRow();
                walWriter.commit();
                assertEquals(1, ins.getCount());

                // Just one segment.
                assertWalExistence(true, tableName, 1);
                assertWalExistence(false, tableName, 2);
                assertSegmentExistence(true, tableName, 1, 0);
                assertSegmentExistence(false, tableName, 1, 1);

                while (ins.getCount() < configuration.getWalSegmentRolloverRowCount()) {
                    ins.insertRow();
                    if (ins.getCount() % 1000 == 0) {  // Committing occasionally to avoid too many log messages.
                        walWriter.commit();
                    }
                }
                walWriter.commit();

                // Still just one segment.
                assertWalExistence(true, tableName, 1);
                assertSegmentExistence(true, tableName, 1, 0);
                assertSegmentExistence(false, tableName, 1, 1);

                ins.insertRow();
                walWriter.commit();

                // Rolled over to second segment.
                assertWalExistence(true, tableName, 1);
                assertSegmentExistence(true, tableName, 1, 0);
                assertSegmentExistence(true, tableName, 1, 1);
                assertSegmentExistence(false, tableName, 1, 2);

                assertSegmentLockEngagement(false, tableName, 1, 0);

                drainWalQueue();
                drainPurgeJob();

                assertSegmentExistence(false, tableName, 1, 0);
            }
        });
    }

    @Test
    public void testLegacyMatViewMessages() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                walWriter.setLegacyMatViewFormat(true);

                TableWriter.Row row = walWriter.newRow(4);
                row.putByte(0, (byte) 1);
                row.putStr(1, "foobar");
                row.append();
                walWriter.commitMatView(1, 2, 3, 4, 5);

                walWriter.resetMatViewState(6, 7, true, "test", 8, null, -1);
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.MAT_VIEW_DATA, eventCursor.getType());

                final WalEventCursor.MatViewDataInfo mvDataInfo = eventCursor.getMatViewDataInfo();
                assertEquals(1, mvDataInfo.getLastRefreshBaseTableTxn());
                assertEquals(2, mvDataInfo.getLastRefreshTimestampUs());
                // last period value should be written along with replace range lo/hi timestamps
                assertEquals(3, mvDataInfo.getLastPeriodHi());
                assertEquals(4, mvDataInfo.getReplaceRangeTsLow());
                assertEquals(5, mvDataInfo.getReplaceRangeTsHi());

                assertTrue(eventCursor.hasNext());
                assertEquals(WalTxnType.MAT_VIEW_INVALIDATE, eventCursor.getType());

                final WalEventCursor.MatViewInvalidationInfo mvInfo = eventCursor.getMatViewInvalidationInfo();
                assertEquals(6, mvInfo.getLastRefreshBaseTableTxn());
                assertEquals(7, mvInfo.getLastRefreshTimestampUs());
                assertTrue(mvInfo.isInvalid());
                TestUtils.assertEquals("test", mvInfo.getInvalidationReason());
                // last period and cached txn intervals values should be ignored
                assertEquals(Numbers.LONG_NULL, mvInfo.getLastPeriodHi());
                assertNotNull(mvInfo.getRefreshIntervals());
                assertEquals(0, mvInfo.getRefreshIntervals().size());
                assertEquals(-1, mvInfo.getRefreshIntervalsBaseTxn());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testMaxLagTxnCount() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
        configOverrideWalMaxLagTxnCount();
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            execute("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T23:00:00.000000Z')");
            tickWalQueue(1);

            assertSql(
                    """
                            a\tb\tts
                            0\t\t2023-08-04T23:00:00.000000Z
                            """,
                    tableToken.getTableName()
            );

            execute("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T22:00:00.000000Z')");
            execute("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T21:00:00.000000Z')");
            execute("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T20:00:00.000000Z')");

            // Run WAL apply job two times:
            // Tick 1. Put row 2023-08-04T22 into the lag.
            // Tick 2. Instead of putting row 2023-08-04T21 into the lag, we force full commit.
            // Add memory pressure to switch to 1 by 1 txn commit
            var pressureControl = engine.getTableSequencerAPI().getTxnTracker(tableToken).getMemPressureControl();
            pressureControl.setMaxBlockRowCount(1);

            tickWalQueue(2);

            // We expect all, but the last row to be visible.
            assertSql(
                    """
                            a\tb\tts
                            0\t\t2023-08-04T21:00:00.000000Z
                            0\t\t2023-08-04T22:00:00.000000Z
                            0\t\t2023-08-04T23:00:00.000000Z
                            """,
                    tableToken.getTableName()
            );

            drainWalQueue();

            assertSql(
                    """
                            a\tb\tts
                            0\t\t2023-08-04T20:00:00.000000Z
                            0\t\t2023-08-04T21:00:00.000000Z
                            0\t\t2023-08-04T22:00:00.000000Z
                            0\t\t2023-08-04T23:00:00.000000Z
                            """,
                    tableToken.getTableName()
            );
        });
    }

    @Test
    public void testOverlappingStructureChangeCannotCreateFile() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.endsWithAscii(name, "0" + Files.SEPARATOR + "c.d")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
            }
        };

        assertMemoryLeak(
                ff, () -> {
                    TableToken tableToken = createTable(testName.getMethodName());

                    try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                        try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                            addColumn(walWriter1, "c", ColumnType.INT);
                            addColumn(walWriter2, "d", ColumnType.INT);
                            assertExceptionNoLeakCheck("Exception expected");
                        } catch (CairoException e) {
                            // this exception will be handled in ILP/PG/HTTP
                            TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                        }
                    }
                }
        );
    }

    @Test
    public void testOverlappingStructureChangeFails() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name.asAsciiCharSequence(), "_txnlog.meta.d")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        assertMemoryLeak(
                ff, () -> {
                    final TableToken tableToken = createTable(testName.getMethodName());

                    try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                        try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                            addColumn(walWriter1, "c", ColumnType.INT);
                            addColumn(walWriter2, "d", ColumnType.INT);
                            Assert.fail("Exception expected");
                        } catch (Exception e) {
                            // this exception will be handled in ILP/PG/HTTP
                            assertTrue(e.getMessage().contains("could not open"));
                        }
                    }
                }
        );
    }

    @Test
    public void testOverlappingStructureChangeMissing() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long readNonNegativeLong(long fd, long offset) {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[1].getClassName().endsWith("TableTransactionLog$TableMetadataChangeLogImpl") && stackTrace[1].getMethodName().equals("of")) {
                        return -1;
                    }
                }
                return Files.readNonNegativeLong(fd, offset);
            }
        };

        assertMemoryLeak(
                ff, () -> {
                    TableToken tableToken = createTable(testName.getMethodName());

                    try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                        try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                            addColumn(walWriter1, "c", ColumnType.INT);
                            addColumn(walWriter2, "d", ColumnType.INT);
                            assertExceptionNoLeakCheck("Exception expected");
                        } catch (Exception e) {
                            // this exception will be handled in ILP/PG/HTTP
                            assertEquals("[0] expected to read table structure changes but there is no saved in the sequencer [structureVersionLo=0]", e.getMessage());
                        }
                    }
                }
        );
    }

    @Test
    public void testReadAndWriteAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAllTypes";
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
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
                    .col("geoByte", ColumnType.GEOBYTE)
                    .col("geoInt", ColumnType.GEOINT)
                    .col("geoShort", ColumnType.GEOSHORT)
                    .col("geoLong", ColumnType.GEOLONG)
                    .col("bin", ColumnType.BINARY)
                    .col("bin2", ColumnType.BINARY)
                    .col("long256b", ColumnType.LONG256) // putLong256(int columnIndex, Long256 value)
                    .col("long256c", ColumnType.LONG256) // putLong256(int columnIndex, CharSequence hexString)
                    .col("long256d", ColumnType.LONG256) // putLong256(int columnIndex, @NotNull CharSequence hexString, int start, int end)
                    .col("timestampb", ColumnType.TIMESTAMP) // putTimestamp(int columnIndex, CharSequence value)
                    .col("stringb", ColumnType.STRING) // putStr(int columnIndex, char value)
                    .col("stringc", ColumnType.STRING) // putStr(int columnIndex, CharSequence value, int pos, int len)
                    .col("symbol", ColumnType.SYMBOL) // putSym(int columnIndex, CharSequence value)
                    .col("symbolb", ColumnType.SYMBOL) // putSym(int columnIndex, char value)
                    .col("symbol8", ColumnType.SYMBOL) // putSymUtf8(int columnIndex, DirectUtf8Sequence value)
                    .col("string8", ColumnType.STRING) // putStrUtf8(int columnIndex, DirectUtf8Sequence value)
                    .col("uuida", ColumnType.UUID) // putUUID(int columnIndex, long lo, long hi)
                    .col("uuidb", ColumnType.UUID) // putUUID(int columnIndex, CharSequence value)
                    .col("IPv4", ColumnType.IPv4)
                    .col("varchara", ColumnType.VARCHAR)
                    .col("varcharb", ColumnType.VARCHAR)
                    .col("array", ColumnType.encodeArrayType(ColumnType.DOUBLE, 1))
                    .col("decimal8", ColumnType.getDecimalType(2, 0))
                    .col("decimal16", ColumnType.getDecimalType(4, 0))
                    .col("decimal32", ColumnType.getDecimalType(6, 0))
                    .col("decimal64", ColumnType.getDecimalType(12, 0))
                    .col("decimal128", ColumnType.getDecimalType(25, 0))
                    .col("decimal256", ColumnType.getDecimalType(50, 0))
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            final int rowsToInsertTotal = 100;
            final long pointer = Unsafe.malloc(rowsToInsertTotal, MemoryTag.NATIVE_DEFAULT);
            final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ColumnType.TIMESTAMP);
            try {
                final long ts = Os.currentTimeMicros();
                final Long256Impl long256 = new Long256Impl();
                final StringSink stringSink = new StringSink();
                final DirectBinarySequence binSeq = new DirectBinarySequence();
                final Decimal128 decimal128 = new Decimal128();
                final Decimal256 decimal256 = new Decimal256();

                final String walName;
                final IntList walSymbolCounts = new IntList();
                try (WalWriter walWriter = engine.getWalWriter(tableToken);
                     DirectArray array = new DirectArray()) {

                    assertEquals(tableName, walWriter.getTableToken().getTableName());
                    walName = walWriter.getWalName();
                    for (int i = 0; i < rowsToInsertTotal; i++) {
                        stringSink.clear();
                        TableWriter.Row row = walWriter.newRow(ts);
                        row.putInt(0, i);
                        row.putByte(1, (byte) i);
                        row.putLong(2, i);
                        row.putLong256(3, i, i + 1, i + 2, i + 3);
                        row.putDouble(4, i + .5);
                        row.putFloat(5, i + .5f);
                        row.putShort(6, (short) i);
                        row.putTimestamp(7, i);
                        row.putChar(8, (char) i);
                        row.putBool(9, i % 2 == 0);
                        row.putDate(10, i);
                        row.putStr(11, String.valueOf(i));
                        row.putGeoHash(12, i); // geo byte
                        row.putGeoHash(13, i); // geo int
                        row.putGeoHash(14, i); // geo short
                        row.putGeoHash(15, i); // geo long

                        prepareBinPayload(pointer, i);
                        row.putBin(16, binSeq.of(pointer, i));
                        // putBin(address, length) treats length 0 the same as null.
                        // so let's start from 1 to avoid that edge-case
                        prepareBinPayload(pointer, i + 1);
                        row.putBin(17, pointer, i + 1);

                        long256.setAll(i, i + 1, i + 2, i + 3);
                        row.putLong256(18, long256);
                        long256.toSink(stringSink);
                        row.putLong256(19, stringSink);
                        int strLen = stringSink.length();
                        stringSink.put("some rubbish to be ignored");
                        row.putLong256(20, stringSink, 2, strLen);

                        row.putTimestamp(21, timestampDriver.implicitCast("2022-06-10T09:13:46." + (i + 1)));

                        row.putStr(22, (char) (65 + i % 26));
                        row.putStr(23, "abcdefghijklmnopqrstuvwxyz", 0, i % 26 + 1);

                        row.putSym(24, String.valueOf(i));
                        row.putSym(25, (char) (65 + i % 26));

                        TestUtils.putUtf8(row, (i % 2) == 0 ? "Щось" : "Таке-Сяке", 26, true);
                        TestUtils.putUtf8(row, (i % 2) == 0 ? "Щось" : "Таке-Сяке", 27, false);

                        row.putLong128(28, i, i + 1); // UUID
                        stringSink.clear();
                        Numbers.appendUuid(i, i + 1, stringSink);
                        row.putUuid(29, stringSink);
                        row.putInt(30, i);

                        row.putVarchar(31, new Utf8String(String.valueOf(i)));
                        row.putVarchar(32, null);

                        array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                        int arrLen = i % 10;
                        array.setDimLen(0, arrLen);
                        array.applyShape();
                        for (int j = 0; j < arrLen; j++) {
                            array.putDouble(j, i + j);
                        }
                        row.putArray(33, array);

                        decimal256.ofLong(i, 0);
                        row.putDecimal(34, decimal256);
                        row.putDecimal(35, decimal256);
                        row.putDecimal(36, decimal256);
                        row.putDecimal(37, decimal256);
                        row.putDecimal(38, decimal256);
                        row.putDecimal(39, decimal256);

                        row.append();
                    }

                    walSymbolCounts.add(walWriter.getSymbolMapReader(24).getSymbolCount());
                    walSymbolCounts.add(walWriter.getSymbolMapReader(25).getSymbolCount());
                    walSymbolCounts.add(walWriter.getSymbolMapReader(26).getSymbolCount());

                    assertEquals(rowsToInsertTotal, walWriter.getSegmentRowCount());
                    assertEquals("WalWriter{name=" + walName + ", table=" + tableName + "}", walWriter.toString());
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, rowsToInsertTotal)) {
                    assertEquals(41, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(rowsToInsertTotal, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    final StringSink testSink = new StringSink();
                    int i = 0;
                    while (cursor.hasNext()) {
                        assertEquals(i, record.getInt(0));
                        assertEquals(i, record.getByte(1));
                        assertEquals(i, record.getLong(2));
                        long256.setAll(i, i + 1, i + 2, i + 3);
                        assertEquals(long256, record.getLong256A(3));
                        assertEquals(long256, record.getLong256B(3));
                        assertEquals(i + 0.5, record.getDouble(4), 0.1);
                        assertEquals(i + 0.5, record.getFloat(5), 0.1);
                        assertEquals(i, record.getShort(6));
                        assertEquals(i, record.getTimestamp(7));
                        assertEquals(i, record.getChar(8));
                        assertEquals(i % 2 == 0, record.getBool(9));
                        assertEquals(i, record.getDate(10));
                        TestUtils.assertEquals(String.valueOf(i), record.getStrA(11));
                        TestUtils.assertEquals(record.getStrA(11), record.getStrB(11));
                        assertEquals(String.valueOf(i).length(), record.getStrLen(11));
                        assertEquals(i, record.getGeoByte(12));
                        assertEquals(i, record.getGeoInt(13));
                        assertEquals(i, record.getGeoShort(14));
                        assertEquals(i, record.getGeoLong(15));

                        prepareBinPayload(pointer, i);
                        assertBinSeqEquals(binSeq.of(pointer, i), record.getBin(16));
                        prepareBinPayload(pointer, i + 1);
                        assertBinSeqEquals(binSeq.of(pointer, i + 1), record.getBin(17));
                        assertEquals(i + 1, record.getBinLen(17));

                        testSink.clear();
                        long256.toSink(testSink);
                        stringSink.clear();
                        record.getLong256(18, stringSink);
                        assertEquals(testSink.toString(), stringSink.toString());
                        stringSink.clear();
                        record.getLong256(19, stringSink);
                        assertEquals(testSink.toString(), stringSink.toString());
                        stringSink.clear();
                        record.getLong256(20, stringSink);
                        assertEquals(testSink.toString(), stringSink.toString());

                        assertEquals(1654852426000000L + (i + 1) * (long) (Math.pow(10, 5 - (int) Math.log10(i + 1))), record.getTimestamp(21));

                        TestUtils.assertEquals(String.valueOf((char) (65 + i % 26)), record.getStrA(22));
                        TestUtils.assertEquals("abcdefghijklmnopqrstuvwxyz".substring(0, i % 26 + 1), record.getStrA(23));

                        assertEquals(String.valueOf(i), record.getSymA(24));
                        assertEquals(String.valueOf((char) (65 + i % 26)), record.getSymA(25));

                        TestUtils.assertEquals((i % 2) == 0 ? "Щось" : "Таке-Сяке", record.getSymA(26));
                        TestUtils.assertEquals((i % 2) == 0 ? "Щось" : "Таке-Сяке", record.getStrA(27));

                        assertEquals(i, record.getLong128Lo(28));
                        assertEquals(i + 1, record.getLong128Hi(28));

                        assertEquals(i, record.getLong128Lo(29));
                        assertEquals(i + 1, record.getLong128Hi(29));
                        assertEquals(i, record.getIPv4(30));

                        TestUtils.assertEquals(String.valueOf(i), record.getVarcharA(31));
                        TestUtils.assertEquals(record.getVarcharA(31), record.getVarcharB(31));
                        // the string is ascii, so length is same as size
                        assertEquals(String.valueOf(i).length(), record.getVarcharSize(31));

                        assertNull(record.getVarcharA(32));
                        assertNull(record.getVarcharB(32));
                        // the string is ascii, so length is same as size
                        assertEquals(-1, record.getVarcharSize(32));
                        ArrayView array = record.getArray(33, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
                        assertEquals(1, array.getDimCount());
                        assertEquals(i % 10, array.getCardinality());
                        assertEquals(i % 10, array.getDimLen(0));
                        for (int j = 0; j < array.getCardinality(); j++) {
                            assertEquals(i + j, array.getDouble(j), 0.0001);
                        }

                        assertEquals(i, record.getDecimal8(34));
                        assertEquals(i, record.getDecimal16(35));
                        assertEquals(i, record.getDecimal32(36));
                        assertEquals(i, record.getDecimal64(37));
                        record.getDecimal128(38, decimal128);
                        assertEquals(0, decimal128.getHigh());
                        assertEquals(i, decimal128.getLow());
                        record.getDecimal256(39, decimal256);
                        assertEquals(0, decimal256.getHh());
                        assertEquals(0, decimal256.getHl());
                        assertEquals(0, decimal256.getLh());
                        assertEquals(i, decimal256.getLl());

                        assertEquals(ts, record.getTimestamp(40));
                        assertEquals(i, record.getRowId());
                        testSink.clear();
                        ((Sinkable) record).toSink(testSink);
                        assertEquals("WalReaderRecord [recordIndex=" + i + "]", testSink.toString());
                        try {
                            cursor.getRecordB();
                            assertExceptionNoLeakCheck("UnsupportedOperationException expected");
                        } catch (UnsupportedOperationException e) {
                            // ignore, this is expected
                        }
                        try {
                            record.getUpdateRowId();
                            assertExceptionNoLeakCheck("UnsupportedOperationException expected");
                        } catch (UnsupportedOperationException e) {
                            // ignore, this is expected
                        }
                        i++;
                    }
                    assertEquals(i, cursor.size());
                    assertEquals(i, reader.size());
                }
            } finally {
                Unsafe.free(pointer, rowsToInsertTotal, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadMatViewStateInvalidFileFormat() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testReadMatViewStateInvalidFileFormat";
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path();
                 MemoryCMR txnMem = Vm.getCMRInstance();
                 BlockFileReader reader = new BlockFileReader(configuration);
                 WalEventReader walEventReader = new WalEventReader(configuration)
            ) {
                MatViewStateReader matViewStateReader = new MatViewStateReader();
                path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
                int tableLen = path.size();

                final long fd = TableUtils.openFileRWOrFail(ff, path.concat(SEQ_DIR).concat(TXNLOG_FILE_NAME).$(), configuration.getWriterFileOpenOpts());
                try {
                    engine.clear(); // release WalWriters and txnlog file, so we can truncate it
                    Assert.assertTrue(ff.truncate(fd, TableTransactionLogFile.HEADER_SIZE / 2));
                } finally {
                    ff.close(fd);
                }

                try {
                    WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "invalid transaction log file");
                }
            }
        });
    }

    @Test
    public void testReadMatViewStateUnknownFormat() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testReadMatViewStateUnknownFormat";
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            try (Path path = new Path();
                 MemoryCMR txnMem = Vm.getCMRInstance();
                 MemoryCMARW txnLogMem = Vm.getCMARWInstance();
                 BlockFileReader reader = new BlockFileReader(configuration);
                 WalEventReader walEventReader = new WalEventReader(configuration)
            ) {
                MatViewStateReader matViewStateReader = new MatViewStateReader();
                path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
                int tableLen = path.size();

                txnLogMem.smallFile(configuration.getFilesFacade(), path.concat(SEQ_DIR).concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_TX_LOG);
                txnLogMem.putInt(0, 3333);

                try {
                    WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
                    Assert.fail();
                } catch (UnsupportedOperationException e) {
                    TestUtils.assertContains(e.getMessage(), "Unsupported transaction log version: 3333");
                }
            }
        });
    }

    @Test
    public void testReadMatViewStateV1() throws Exception {
        assertMemoryLeak(() -> testReadMatViewState(0));
    }

    @Test
    public void testReadMatViewStateV2() throws Exception {
        assertMemoryLeak(() -> testReadMatViewState(2));
    }

    @Test
    public void testRemoveColumnRollsUncommittedRowsToNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    walName1 = walWriter1.getWalName();
                    walName2 = walWriter2.getWalName();

                    TableWriter.Row row = walWriter1.newRow(0);
                    row.putByte(0, (byte) 1);
                    row.append();
                    walWriter1.commit();

                    row = walWriter2.newRow(0);
                    row.putByte(0, (byte) 10);
                    row.append();
                    walWriter2.commit();

                    row = walWriter2.newRow(0);
                    row.putByte(0, (byte) 100);
                    row.append();

                    removeColumn(walWriter1, "b");

                    walWriter2.commit();

                    row = walWriter1.newRow(0);
                    row.putByte(0, (byte) 110);
                    row.append();
                    walWriter1.commit();
                }
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(10, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal();
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 1, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(2, reader.getRealColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(110, record.getByte(0));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 1, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(2, reader.getRealColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(100, record.getByte(0));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testRemovingColumnClosesSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();
                removeColumn(walWriter, "a");
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }

            try {
                engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 1, 0);
                assertExceptionNoLeakCheck("Segment 1 should not exist");
            } catch (CairoException e) {
                TestUtils.assertContains(
                        e.getFlyweightMessage(), "could not open, file does not exist: " + engine.getConfiguration().getDbRoot() +
                                File.separatorChar + tableName + TableUtils.SYSTEM_TABLE_NAME_SUFFIX + "1" +
                                File.separatorChar + walName +
                                File.separatorChar + "1" +
                                File.separatorChar + TableUtils.META_FILE_NAME + "]"
                );
            }
        });
    }

    @Test
    public void testRemovingNonExistentColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();

                try {
                    removeColumn(walWriter, "noColLikeThis");
                    assertExceptionNoLeakCheck("Should not be able to remove non existent column");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "cannot remove, column does not exist [table=testRemovingNonExistentColumn, column=noColLikeThis]");
                }
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 10);
                row.append();

                walWriter.commit();
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 2)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(2, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertTrue(cursor.hasNext());
                assertEquals(10, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(1, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(1, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(1, dataInfo.getStartRowID());
                assertEquals(2, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testRemovingSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(0);
                row.putInt(0, 12);
                row.putSym(1, "symb");
                row.putSym(2, "symc");
                row.append();
                walWriter.commit();

                removeColumn(walWriter, "b");

                row = walWriter.newRow(0);
                row.putInt(0, 133);
                try {
                    row.putSym(1, "anything");
                    assertExceptionNoLeakCheck("UnsupportedOperationException expected");
                } catch (UnsupportedOperationException ignore) {
                }
                try {
                    TestUtils.putUtf8(row, "Щось", 1, true);
                    assertExceptionNoLeakCheck("UnsupportedOperationException expected");
                } catch (UnsupportedOperationException ignore) {
                }

                TestUtils.putUtf8(row, "Таке-Сяке", 2, true);
                row.append();
                walWriter.commit();
            }

            model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .timestamp("ts");
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(12, record.getInt(0));
                assertEquals("symb", record.getSymA(1));
                assertEquals("symc", record.getSymA(2));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());

                SymbolMapDiff symbolMapDiff = dataInfo.nextSymbolMapDiff();
                assertEquals(1, symbolMapDiff.getColumnIndex());
                int expectedKey = 0;
                SymbolMapDiffEntry entry;
                while ((entry = symbolMapDiff.nextEntry()) != null) {
                    assertEquals(expectedKey, entry.getKey());
                    assertEquals("symb", entry.getSymbol().toString());
                    expectedKey++;
                }
                assertEquals(1, expectedKey);
                symbolMapDiff = dataInfo.nextSymbolMapDiff();
                assertEquals(2, symbolMapDiff.getColumnIndex());
                expectedKey = 0;
                while ((entry = symbolMapDiff.nextEntry()) != null) {
                    assertEquals(expectedKey, entry.getKey());
                    assertEquals("symc", entry.getSymbol().toString());
                    expectedKey++;
                }
                assertEquals(1, expectedKey);
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("c", ColumnType.SYMBOL)
                    .timestamp("ts");
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 1, 1)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(133, record.getInt(0));
                assertEquals("Таке-Сяке", record.getSymA(2));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                SymbolMapDiff symbolMapDiff = dataInfo.nextSymbolMapDiff();
                assertEquals(1, symbolMapDiff.getRecordCount());
                assertEquals(2, symbolMapDiff.getColumnIndex());
                assertEquals(0, symbolMapDiff.getCleanSymbolCount());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testRenameColumnRollsUncommittedRowsToNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    walName1 = walWriter1.getWalName();
                    walName2 = walWriter2.getWalName();

                    TableWriter.Row row = walWriter1.newRow();
                    row.putByte(0, (byte) 1);
                    row.append();
                    walWriter1.commit();

                    row = walWriter2.newRow();
                    row.putByte(0, (byte) 10);
                    row.append();
                    walWriter2.commit();

                    row = walWriter2.newRow();
                    row.putByte(0, (byte) 100);
                    row.append();

                    renameColumn(walWriter1);

                    walWriter2.commit();

                    row = walWriter1.newRow();
                    row.putByte(0, (byte) 110);
                    row.append();
                    walWriter1.commit();
                }
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 0, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(10, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)
                    .col("c", ColumnType.STRING)
                    .timestamp("ts")
                    .wal();
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 1, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName1, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(110, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName2, 1, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName2, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(100, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testRollToNextSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                assertEquals(0, walWriter.getSegmentRowCount());
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                assertEquals(1, walWriter.getSegmentRowCount());
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 11);
                row.append();
                assertEquals(2, walWriter.getSegmentRowCount());

                walWriter.commit();
                walWriter.rollSegment();
                assertEquals(0, walWriter.getSegmentRowCount());
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 112);
                row.append();
                walWriter.commit();
                assertEquals(1, walWriter.getSegmentRowCount());
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 2)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(2, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertTrue(cursor.hasNext());
                assertEquals(11, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(1, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(2, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 1, 1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(112, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(1, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());

                assertFalse(eventCursor.hasNext());
            }

            try (Path path = new Path().of(configuration.getDbRoot())) {
                assertWalFileExist(path, tableToken, walName, 0, "_meta");
                assertWalFileExist(path, tableToken, walName, 0, "_event");
                assertWalFileExist(path, tableToken, walName, 0, "a.d");
                assertWalFileExist(path, tableToken, walName, 0, "b.d");
                assertWalFileExist(path, tableToken, walName, 0, "b.i");
                assertWalFileExist(path, tableToken, walName, 1, "_meta");
                assertWalFileExist(path, tableToken, walName, 1, "_event");
                assertWalFileExist(path, tableToken, walName, 1, "a.d");
                assertWalFileExist(path, tableToken, walName, 1, "b.d");
                assertWalFileExist(path, tableToken, walName, 1, "b.i");
            }
        });
    }

    public void testRolloverSegmentSize(int colType, boolean colNeedsIndex, long bytesPerRow, long additionalBytesPerTxn, Consumer<TableWriter.Row> valueInserter) throws Exception {
        try {
            assertMemoryLeak(() -> {
                final String tableName = testName.getMethodName();
                final TableToken tableToken;
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                        .col("a", colType)
                        .timestamp("ts")
                        .wal();
                tableToken = createTable(model);

                final long rolloverSize = 1024;
                // 1 KiB
                node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_SIZE, rolloverSize);

                final long eventsBytesPerTxn = 50 + additionalBytesPerTxn;
                final long eventsHeader = 12;  // number of bytes in the events file header.
                final long txnCount = 3;  // number of `.commit()` calls in this test.

                // The maximum number of rows we can insert before we trigger the size-based rollover.
                // If the col needs an index, we need to remove this from the size count.
                final long nonBreachRowCount = (
                        rolloverSize
                                - (colNeedsIndex ? 8 : 0)
                                - eventsHeader
                                - (eventsBytesPerTxn * txnCount)
                ) / bytesPerRow;

                long timestamp = 1694590000000000L;

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    // Insert the one less than the maximum number of rows to cause a roll-over at the next row.
                    for (long rowIndex = 0; rowIndex < (nonBreachRowCount - 1); ++rowIndex, timestamp += 1000) {
                        final TableWriter.Row row = walWriter.newRow(timestamp);
                        valueInserter.accept(row);
                        row.append();
                    }
                    walWriter.commit();

                    assertWalExistence(true, tableName, 1);
                    assertSegmentExistence(true, tableName, 1, 0);
                    assertSegmentExistence(false, tableName, 1, 1);

                    // Inserting the next row is still within limit: Will not roll over.
                    {
                        final TableWriter.Row row = walWriter.newRow(timestamp);
                        valueInserter.accept(row);
                        row.append();
                        timestamp += 1000;
                    }
                    walWriter.commit();

                    assertSegmentExistence(false, tableName, 1, 1);

                    // We're now over the limit, but the logic is to roll over the row _after_ this one.
                    {
                        final TableWriter.Row row = walWriter.newRow(timestamp);
                        valueInserter.accept(row);
                        row.append();
                        timestamp += 1000;
                    }
                    walWriter.commit();

                    assertSegmentExistence(false, tableName, 1, 1);

                    // finally, a row rolled over.
                    {
                        final TableWriter.Row row = walWriter.newRow(timestamp);
                        valueInserter.accept(row);
                        row.append();
                    }
                    walWriter.commit();

                    assertSegmentExistence(true, tableName, 1, 1);
                }
            });
        } finally {
            node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_SIZE, 0);
        }
    }

    @Test
    public void testRolloverSegmentSizeInt() throws Exception {
        // Size of all columns per row: 4 bytes for INT, 16 bytes for timestamp (8 bytes index + 8 bytes timestamp).
        final long bytesPerRow = 4 + 16;
        final AtomicInteger value = new AtomicInteger();
        testRolloverSegmentSize(ColumnType.INT, false, bytesPerRow, 0, (row) -> row.putInt(0, value.getAndIncrement()));
    }

    @Test
    public void testRolloverSegmentSizeStr() throws Exception {
        // NB. All our test strings are unique 3 chars. This gives us fixed sizes per row.
        //
        // Size of all columns per row:
        //   * 8 bytes for the string index column (secondary column).
        //   * 10 bytes data column (primary column):
        //       * 6 bytes payload (because utf-16).
        //       * 4 bytes len prefix.
        //   * 16 bytes for timestamp (8 bytes index + 8 bytes timestamp).
        final long bytesPerRow = 8 + 10 + 16;
        final AtomicInteger value = new AtomicInteger();
        testRolloverSegmentSize(
                ColumnType.STRING, true, bytesPerRow, 0, (row) -> {
                    final String formatted = String.format("%03d", value.getAndIncrement());
                    row.putStr(0, formatted);
                }
        );
    }

    @Test
    public void testRolloverSegmentSizeSymbol() throws Exception {
        // NB. All our test strings are unique 3 chars. This gives us fixed sizes per row.
        //
        // Size of all columns per row:
        //   * 4 bytes for the symbol index column (primary column):
        //   * 14 bytes in the events file:
        //       * 4 bytes for symbol index value.
        //       * 4 bytes len prefix.
        //       * 6 bytes payload (because utf-16).
        final long bytesPerRow = 4 + 14 + 16;

        // Overhead to track symbols per txn (per symbol column, in actual fact - but we only have one).
        final long additionalBytesPerTxn = 17;
        final AtomicInteger value = new AtomicInteger();
        testRolloverSegmentSize(
                ColumnType.SYMBOL, false, bytesPerRow, additionalBytesPerTxn, (row) -> {
                    final String formatted = String.format("%03d", value.getAndIncrement());
                    row.putSym(0, formatted);
                }
        );
    }

    @Test
    public void testSameWalAfterEngineCleared() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken = createTable(testName.getMethodName());

            String wal1Name;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                wal1Name = walWriter.getWalName();
                for (int i = 0; i < 18; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                for (int i = 0; i < 6; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.commit(); //should not create new txn, this is noop
                walWriter.commit(); //should not create new txn, this is noop
                for (int i = 0; i < 20; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.rollSegment();
                for (int i = 0; i < 7; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
            }

            engine.clear();

            String wal2Name;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                wal2Name = walWriter.getWalName();
                for (int i = 0; i < 18; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                for (int i = 0; i < 6; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.commit(); //should not create new txn, this is noop
                walWriter.commit(); //should not create new txn, this is noop
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.rollSegment();
                for (int i = 0; i < 7; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
            }

            TableModel model = defaultModel(tableName);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, wal1Name, 0, 44)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(wal1Name, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(44, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i > 23 ? i - 24 : (i > 17 ? i - 18 : i), record.getByte(0));
                    assertNull(record.getStrA(1));
                    assertEquals(i, record.getRowId());
                    i++;
                }
                assertEquals(44, i);

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(18, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(1, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(18, dataInfo.getStartRowID());
                assertEquals(24, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(2, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(24, dataInfo.getStartRowID());
                assertEquals(44, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, wal1Name, 1, 7)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(wal1Name, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(7, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertNull(record.getStrA(1));
                    assertEquals(i, record.getRowId());
                    i++;
                }
                assertEquals(7, i);

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(7, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, wal2Name, 0, 34)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(wal2Name, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(34, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i > 23 ? i - 24 : (i > 17 ? i - 18 : i), record.getByte(0));
                    assertNull(record.getStrA(1));
                    assertEquals(i, record.getRowId());
                    i++;
                }
                assertEquals(34, i);

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(18, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(1, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(18, dataInfo.getStartRowID());
                assertEquals(24, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertTrue(eventCursor.hasNext());
                assertEquals(2, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                dataInfo = eventCursor.getDataInfo();
                assertEquals(24, dataInfo.getStartRowID());
                assertEquals(34, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, wal2Name, 1, 7)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(wal2Name, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(7, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertNull(record.getStrA(1));
                    assertEquals(i, record.getRowId());
                    i++;
                }
                assertEquals(7, i);

                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(7, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    @Test
    public void testSequencerFilesNotCached() throws Exception {
        AtomicInteger fdOpenCount = new AtomicInteger();
        AtomicInteger fdOpenNoCacheCount = new AtomicInteger();

        FilesFacade ff = new TestFilesFacadeImpl() {
            public long openRO(@NotNull LPSZ path) {
                long fd = super.openRO(path);
                if (Utf8s.containsAscii(path, WalUtils.WAL_NAME_BASE) || Utf8s.containsAscii(path, WalUtils.SEQ_DIR)) {
                    fdOpenCount.incrementAndGet();
                }

                return fd;
            }

            public long openRONoCache(@NotNull LPSZ path) {
                long fd = super.openRONoCache(path);
                if (Utf8s.containsAscii(path, WalUtils.WAL_NAME_BASE) || Utf8s.containsAscii(path, WalUtils.SEQ_DIR)) {
                    fdOpenNoCacheCount.incrementAndGet();
                }

                return fd;
            }
        };

        node1.setProperty(PropertyKey.CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE, 0);
        final String tableName = testName.getMethodName();

        assertMemoryLeak(ff, () -> {
            execute("create table " + tableName + " (ts timestamp) timestamp(ts) partition by day wal;");
            TableToken tt = engine.verifyTableName(tableName);

            engine.releaseInactive();

            // Keep _txnlog.meta.i open while replica is running to force it stying in fd cache.
            Path p = Path.getThreadLocal(root).concat(tt).concat(WalUtils.SEQ_DIR).concat(WalUtils.TXNLOG_FILE_NAME_META_INX);
            long fd = ff.openRO(p.$());
            Assert.assertTrue(fd > 0);
            Assert.assertEquals(1, fdOpenCount.get());

            for (int i = 0; i < 10; i++) {
                engine.execute("alter table " + tableName + " add column x" + i + " int;");
                engine.execute("insert into " + tableName + "(ts, x0) values ('2022-03-24', 1)");
            }

            drainWalQueue();

            assertSql("count\n10\n", "select count() from " + tableName);
            ff.close(fd);

            Assert.assertEquals(1, fdOpenCount.get());
            Assert.assertTrue(fdOpenNoCacheCount.get() > 0);
        });

    }

    @Test
    public void testSymbolWal() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testSymTable";
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .col("d", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            try (TableWriter tableWriter = getWriter(tableToken)) {
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = tableWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.putSym(1, "sym" + i);
                    row.putSym(2, "s" + i % 2);
                    row.putSym(3, "symbol" + i % 2);
                    row.append();
                }
                tableWriter.commit();
            }

            final String walName;
            final IntList walSymbolCounts = new IntList();
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.putSym(1, "sym" + i);
                    row.putSym(2, "s" + i % 2);
                    row.putSym(3, "symbol" + i % 3);
                    row.append();
                }

                assertNull(walWriter.getSymbolMapReader(0));
                walSymbolCounts.add(walWriter.getSymbolMapReader(1).getSymbolCount());
                walSymbolCounts.add(walWriter.getSymbolMapReader(2).getSymbolCount());
                walSymbolCounts.add(walWriter.getSymbolMapReader(3).getSymbolCount());

                assertNull(walWriter.getSymbolMapReader(1).valueOf(10));

                walWriter.commit();
            }

            try (
                    TableReader reader = engine.getReader(tableToken);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                assertEquals(5, reader.getMetadata().getColumnCount());
                assertEquals(5, reader.getTransientRowCount());
                Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertEquals(i, record.getInt(1));
                    assertEquals("sym" + i, record.getSymA(1));
                    assertEquals("sym" + i, reader.getSymbolMapReader(1).valueOf(i));
                    assertEquals(i % 2, record.getInt(2));
                    assertEquals("s" + i % 2, record.getSymA(2));
                    assertEquals("s" + i % 2, reader.getSymbolMapReader(2).valueOf(i % 2));
                    assertEquals(i % 2, record.getInt(3));
                    assertEquals("symbol" + i % 2, record.getSymA(3));
                    assertEquals(record.getSymB(3), record.getSymA(3));
                    assertEquals("symbol" + i % 2, reader.getSymbolMapReader(3).valueOf(i % 2));
                    i++;
                }
                assertEquals(i, reader.getTransientRowCount());
                assertNull(reader.getSymbolMapReader(0));
                assertNull(reader.getSymbolMapReader(1).valueOf(5));
                assertNull(reader.getSymbolMapReader(2).valueOf(2));
                assertNull(reader.getSymbolMapReader(3).valueOf(2));
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 10L)) {
                assertEquals(5, reader.getColumnCount());
                assertEquals(10, reader.size());
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertEquals(i, record.getInt(1));
                    assertEquals("sym" + i, record.getSymA(1));
                    assertEquals(i % 2, record.getInt(2));
                    assertEquals("s" + i % 2, record.getSymA(2));
                    assertEquals(i % 3, record.getInt(3));
                    assertEquals("symbol" + i % 3, record.getSymA(3));
                    assertEquals(record.getSymB(3), record.getSymA(3));
                    i++;
                }
                assertEquals(i, reader.size());
                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(10, dataInfo.getEndRowID());
                assertEquals(0, dataInfo.getMinTimestamp());
                assertEquals(0, dataInfo.getMaxTimestamp());
                assertFalse(dataInfo.isOutOfOrder());

                SymbolMapDiff symbolMapDiff = dataInfo.nextSymbolMapDiff();
                assertEquals(1, symbolMapDiff.getColumnIndex());
                int expectedKey = 5;
                SymbolMapDiffEntry entry;
                while ((entry = symbolMapDiff.nextEntry()) != null) {
                    assertEquals(expectedKey, entry.getKey());
                    assertEquals("sym" + expectedKey, entry.getSymbol().toString());
                    expectedKey++;
                }
                assertEquals(10, expectedKey);
                assertEmptySymbolDiff(dataInfo, 2);

                symbolMapDiff = dataInfo.nextSymbolMapDiff();
                assertEquals(3, symbolMapDiff.getColumnIndex());
                expectedKey = 2;
                while ((entry = symbolMapDiff.nextEntry()) != null) {
                    assertEquals(expectedKey, entry.getKey());
                    assertEquals("symbol" + expectedKey, entry.getSymbol().toString());
                    expectedKey++;
                }
                assertEquals(3, expectedKey);
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }

            try (Path path = new Path().of(configuration.getDbRoot())) {
                assertWalFileExist(path, tableToken, walName, 0, "_meta");
                assertWalFileExist(path, tableToken, walName, 0, "_event");
                assertWalFileExist(path, tableToken, walName, 0, "a.d");
                assertWalFileExist(path, tableToken, walName, 0, "b.d");
                assertWalFileExist(path, tableToken, walName, 0, "c.d");
                assertWalFileExist(path, tableToken, walName, 0, "d.d");
                assertWalFileExist(path, tableToken, walName, "b.c");
                assertWalFileExist(path, tableToken, walName, "b.k");
                assertWalFileExist(path, tableToken, walName, "b.o");
                assertWalFileExist(path, tableToken, walName, "b.v");
                assertWalFileExist(path, tableToken, walName, "c.c");
                assertWalFileExist(path, tableToken, walName, "c.k");
                assertWalFileExist(path, tableToken, walName, "c.o");
                assertWalFileExist(path, tableToken, walName, "c.v");
                assertWalFileExist(path, tableToken, walName, "d.c");
                assertWalFileExist(path, tableToken, walName, "d.k");
                assertWalFileExist(path, tableToken, walName, "d.o");
                assertWalFileExist(path, tableToken, walName, "d.v");
            }
        });
    }

    @Test
    public void testTableDropExceptionThrownIfSequencerCannotBeOpenTableIsDropped() throws Exception {
        assertMemoryLeak(() -> {
            createTable(testName.getMethodName());
            TableToken tableToken = engine.verifyTableName(testName.getMethodName());

            // Now that the table is really dropped
            engine.execute("drop table " + tableToken.getTableName());

            ff = new TestFilesFacadeImpl() {
                @Override
                public long openRW(LPSZ name, int opts) {
                    if (Utf8s.endsWithAscii(name, WAL_INDEX_FILE_NAME)) {
                        // Set errno to path does not exist
                        this.openRO(Path.getThreadLocal2("does-not-exist").$());
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                }
            };

            try {
                engine.getTableSequencerAPI().lastTxn(tableToken);
                Assert.fail("Exception expected");
            } catch (CairoException e) {
                // We should receive table is dropped error
                Assert.assertTrue(e.isTableDropped());
                TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped");
            }
        });
    }

    @Test
    public void testTruncateWithoutKeepingSymbolTablesThrows() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walWriter.truncate();
                Assert.fail();
            } catch (UnsupportedOperationException ex) {
                TestUtils.assertContains(ex.getMessage(), "cannot truncate symbol tables on WAL table");
            }
        });
    }

    @Test
    public void testWalEventReaderConcurrentReadWrite() throws Exception {
        AtomicReference<TestUtils.LeakProneCode> eventFileLengthCallBack = new AtomicReference<>();

        FilesFacade ff = new TestFilesFacadeImpl() {

            @Override
            public long length(long fd) {
                long len = super.length(fd);
                if (fd == this.fd && eventFileLengthCallBack.get() != null) {
                    TestUtils.unchecked(() -> {
                        eventFileLengthCallBack.get().run();
                        eventFileLengthCallBack.set(null);
                    });
                }
                return len;
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == this.fd) {
                    if (eventFileLengthCallBack.get() != null) {
                        TestUtils.unchecked(() -> {
                            eventFileLengthCallBack.get().run();
                            eventFileLengthCallBack.set(null);
                        });
                    }

                    // Windows does not allow to map beyond file length
                    // simulate it with asserts for all other platforms
                    Assert.assertTrue(offset + len <= super.length(fd));
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ path) {
                long fd = super.openRO(path);
                if (Utf8s.endsWithAscii(path, EVENT_FILE_NAME)) {
                    this.fd = fd;
                }
                return fd;
            }
        };

        assertMemoryLeak(
                ff, () -> {
                    final String tableName = testName.getMethodName();
                    final TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                            .col("a", ColumnType.INT)
                            .col("b", ColumnType.SYMBOL)
                            .timestamp("ts")
                            .wal();
                    final TableToken tableToken = createTable(model);

                    final WalWriter walWriter = engine.getWalWriter(tableToken);
                    final TableWriter.Row row = walWriter.newRow(0);
                    row.putInt(0, 1);
                    row.append();

                    walWriter.commit();

                    eventFileLengthCallBack.set(() -> {
                        // Close wal segments after the moment when _event file length is taken
                        // but before it's mapped to memory
                        walWriter.close();
                        engine.releaseInactive();
                    });

                    drainWalQueue();

                    assertSql(
                            """
                                    a\tb\tts
                                    1\t\t1970-01-01T00:00:00.000000Z
                                    """, tableName
                    );
                }
        );
    }

    @Test
    public void testWalEventReaderMaxTxnTooLarge() throws Exception {
        // This test simulates the scenario where data was written via mmap,
        // but not fully flushed to disk.
        // The specific case is that the `_event` file has a `maxTxn` outside the index
        // recorded in `_event.i`.
        // On Windows we tolerate the `_event.i` file being shorter than expected (see `try/catch` in impl),
        // On other platforms we require the file to be at least as long, but can tolerate
        // null index data.

        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            final TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal();
            final TableToken tableToken = createTable(model);

            final WalWriter walWriter = engine.getWalWriter(tableToken);
            final TableWriter.Row row = walWriter.newRow(0);
            row.putInt(0, 1);
            row.append();

            walWriter.commit();
            walWriter.close();
            engine.releaseInactive();

            final int newMaxTxn = 200000;
            try (
                    final Path walePath = new Path()
                            .of(configuration.getDbRoot())
                            .concat(tableToken)
                            .concat(WAL_NAME_BASE + 1)
                            .concat("0")
                            .concat(EVENT_FILE_NAME);
                    final MemoryMARW eventMem = Vm.getCMARWInstance()
            ) {
                Assert.assertTrue(Files.exists(walePath.$()));
                eventMem.of(
                        engine.getConfiguration().getFilesFacade(),
                        walePath.$(),
                        configuration.getWalEventAppendPageSize(),
                        WALE_HEADER_SIZE,
                        MemoryTag.MMAP_TABLE_WAL_WRITER,
                        CairoConfiguration.O_NONE,
                        Files.POSIX_MADV_RANDOM
                );

                // We hack the wale header's `maxTxn` so it's
                // well outside what's both the `_event` and `_event.i` files.
                eventMem.putInt(0, newMaxTxn);

                if (!Os.isWindows()) {
                    try (
                            final Path waleIndexPath = new Path()
                                    .of(configuration.getDbRoot())
                                    .concat(tableToken)
                                    .concat(WAL_NAME_BASE + 1)
                                    .concat("0")
                                    .concat(EVENT_INDEX_FILE_NAME)) {
                        try (
                                final MemoryMARW eventIndexMem = Vm.getCMARWInstance()
                        ) {
                            eventIndexMem.of(
                                    engine.getConfiguration().getFilesFacade(),
                                    waleIndexPath.$(),
                                    configuration.getWalEventAppendPageSize(),
                                    -1,
                                    MemoryTag.MMAP_TABLE_WAL_WRITER,
                                    CairoConfiguration.O_NONE,
                                    Files.POSIX_MADV_RANDOM
                            );

                            // Extend the file with 0 content to simulate unflushed pages.
                            eventIndexMem.putLong((newMaxTxn + 1) * Long.BYTES, 0);

                            // Don't truncate!
                            eventIndexMem.close(false);
                        }

                        final long newWaleIndexSize = engine.getConfiguration().getFilesFacade().length(waleIndexPath.$());
                        Assert.assertTrue(newWaleIndexSize >= (newMaxTxn + 2) * Long.BYTES);
                    }
                }
            }

            drainWalQueue();

            assertSql(
                    """
                            a\tb\tts
                            1\t\t1970-01-01T00:00:00.000000Z
                            """, tableName
            );
        });
    }

    @Test
    public void testWalSegmentInit() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testWalSegmentInit";
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            assertTableExistence(true, tableToken);

            WalDirectoryPolicy oldPolicy = engine.getWalDirectoryPolicy();
            try {
                engine.setWalDirectoryPolicy(new WalDirectoryPolicy() {
                    @Override
                    public void initDirectory(Path dirPath) {
                        final File segmentDirFile = new File(dirPath.toString());
                        final File customInitFile = new File(segmentDirFile, "customInitFile");
                        try {
                            //noinspection ResultOfMethodCallIgnored
                            customInitFile.createNewFile();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public boolean isInUse(Path path) {
                        return false;
                    }

                    @Override
                    public void rollbackDirectory(Path path) {
                        // do nothing
                    }

                    @Override
                    public boolean truncateFilesOnClose() {
                        return true;
                    }
                });

                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                    for (int i = 0; i < 10; i++) {
                        TableWriter.Row row = walWriter.newRow(0);
                        row.putByte(0, (byte) i);
                        row.append();
                    }

                    walWriter.commit();
                }

                assertWalExistence(true, tableToken, 1);
                File segmentDir = assertSegmentExistence(true, tableToken, 1, 0);

                final File customInitFile = new File(segmentDir, "customInitFile");
                assertTrue(customInitFile.exists());
            } finally {
                engine.setWalDirectoryPolicy(oldPolicy);
            }
        });
    }

    @Test
    public void testWalSegmentKeepsPendingOnClose() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testWalSegmentKeepsPendingOnClose";
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal();
            tableToken = createTable(model);

            assertTableExistence(true, tableToken);
            String pending = "custom.pending";
            WalDirectoryPolicy oldPolicy = engine.getWalDirectoryPolicy();
            try {
                engine.setWalDirectoryPolicy(new WalDirectoryPolicy() {
                    @Override
                    public void initDirectory(Path dirPath) {
                        final File segmentDirFile = new File(dirPath.toString());
                        final File customInitFile = new File(segmentDirFile, pending);
                        try {
                            //noinspection ResultOfMethodCallIgnored
                            customInitFile.createNewFile();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public boolean isInUse(Path path) {
                        return true;
                    }

                    @SuppressWarnings("ResultOfMethodCallIgnored")
                    @Override
                    public void rollbackDirectory(Path path) {
                        final File segmentDirFile = new File(path.toString());
                        final File customInitFile = new File(segmentDirFile, pending);
                        customInitFile.delete();
                    }

                    @Override
                    public boolean truncateFilesOnClose() {
                        return true;
                    }
                });

                try (WalWriter wal1 = engine.getWalWriter(tableToken);
                     WalWriter wal2 = engine.getWalWriter(tableToken)
                ) {
                    for (int i = 0; i < 10; i++) {
                        TableWriter.Row row = wal1.newRow(0);
                        row.putByte(0, (byte) i);
                        row.append();
                    }
                    wal1.commit();

                    // wal2 without commits
                    wal2.truncateSoft();
                }

                assertWalExistence(true, tableToken, 1);
                File segmentDir = assertSegmentExistence(true, tableToken, 1, 0);

                final File pendingFile = new File(segmentDir, pending);
                assertTrue(pendingFile.exists());

                assertWalExistence(true, tableToken, 2);
                File segmentDir2 = assertSegmentExistence(true, tableToken, 2, 0);

                final File pendingFile2 = new File(segmentDir2, pending);
                assertTrue(pendingFile2.exists());
            } finally {
                engine.setWalDirectoryPolicy(oldPolicy);
            }
        });
    }

    @Test
    public void testWalWritersUnknownTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken;
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts"); // not a WAL table
            tableToken = createTable(model);
            try (WalWriter ignored = engine.getWalWriter(tableToken)) {
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                TestUtils.assertContains(e.getFlyweightMessage(), tableName);
            }
        });
    }

    @SuppressWarnings("SameParameterValue")
    private static void checkWalEvents(TableToken tableToken, long refreshTxn, boolean newFormat) {
        try (Path path = new Path();
             WalEventReader walEventReader = new WalEventReader(configuration);
             TransactionLogCursor transactionLogCursor = engine.getTableSequencerAPI().getCursor(tableToken, 0)) {
            path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            int pathLen = path.size();
            while (transactionLogCursor.hasNext()) {
                final int walId = transactionLogCursor.getWalId();
                final int segmentId = transactionLogCursor.getSegmentId();
                final int segmentTxn = transactionLogCursor.getSegmentTxn();
                path.trimTo(pathLen).concat(WAL_NAME_BASE).put(walId).slash().put(segmentId);
                WalEventCursor walEventCursor = walEventReader.of(path, segmentTxn);
                if (walEventCursor.getType() == WalTxnType.MAT_VIEW_INVALIDATE) {
                    if (newFormat) {
                        WalEventCursor.MatViewInvalidationInfo info = walEventCursor.getMatViewInvalidationInfo();
                        assertTrue(info.isInvalid());
                        assertEquals("test_invalidate", info.getInvalidationReason().toString());
                    } else {
                        Assert.fail("Invalidation event should not be present in old format");
                    }
                }
                if (WalTxnType.isDataType(walEventCursor.getType())) {
                    WalEventCursor.DataInfo info = walEventCursor.getDataInfo();
                    assertEquals(segmentTxn, info.getStartRowID());
                    assertEquals(segmentTxn + 1, info.getEndRowID());
                }
                if (walEventCursor.getType() == WalTxnType.MAT_VIEW_DATA) {
                    if (newFormat) {
                        WalEventCursor.MatViewDataInfo info = walEventCursor.getMatViewDataInfo();
                        assertEquals(segmentTxn, info.getStartRowID());
                        assertEquals(segmentTxn + 1, info.getEndRowID());
                        assertEquals(refreshTxn + segmentTxn, info.getLastRefreshBaseTableTxn());
                        assertEquals(segmentTxn, info.getLastRefreshTimestampUs());
                    } else {
                        Assert.fail("MVData event should not be present in old format");
                    }
                }
            }
        }
    }

    private static Path constructPath(Path path, TableToken tableName, CharSequence walName, long segment, CharSequence fileName) {
        return segment < 0
                ? path.concat(tableName).slash().concat(walName).slash().concat(fileName)
                : path.concat(tableName).slash().concat(walName).slash().put(segment).slash().concat(fileName);
    }

    private static TableModel defaultModel(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.STRING)
                .timestamp("ts")
                .wal();
    }

    private static void testReadMatViewState(int chunkSize) {
        int chunkSizeOld = node1.getConfiguration().getDefaultSeqPartTxnCount();
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, chunkSize);
        assertEquals(node1.getConfiguration().getDefaultSeqPartTxnCount(), chunkSize);
        final String tableName = "testReadMatViewState" + (chunkSize > 0 ? "_v2" : "_v1");
        TableToken tableToken;
        TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.SYMBOL)
                .timestamp("ts")
                .wal();
        tableToken = createTable(model);

        FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                MemoryCMR txnMem = Vm.getCMRInstance();
                BlockFileReader reader = new BlockFileReader(configuration);
                WalEventReader walEventReader = new WalEventReader(configuration)
        ) {
            MatViewStateReader matViewStateReader = new MatViewStateReader();
            path.of(configuration.getDbRoot()).concat(tableToken.getDirName());
            int tableLen = path.size();
            boolean success = WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
            assertFalse(success); // no transactions

            long maxTxn = 3;
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < maxTxn; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.putSym(1, "sym" + i);
                    row.append();
                    walWriter.commit();
                }
            }

            success = WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
            assertFalse(success); // incomplete refresh, no commitMatView

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                for (int i = 0; i < maxTxn; i++) {
                    TableWriter.Row row = walWriter.newRow(0);
                    row.putByte(0, (byte) i);
                    row.putSym(1, "sym" + i);
                    row.append();
                    if (i == 1) {
                        walWriter.commitMatView(42, 42, 42, 0, 1);
                    } else {
                        walWriter.commit();
                    }
                }
            }

            success = WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
            assertTrue(success);
            assertEquals(42, matViewStateReader.getLastRefreshBaseTxn()); // refresh commit

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walWriter.resetMatViewState(45, 45, true, "test_invalidate", 45, null, -1);
            }

            success = WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
            assertTrue(success);
            assertTrue(matViewStateReader.isInvalid());
            assertEquals(45, matViewStateReader.getLastRefreshBaseTxn()); // invalidate commit

            final LongList intervals = new LongList();
            intervals.add(1L, 2L);
            intervals.add(3L, 4L);
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                // reset invalidation
                walWriter.resetMatViewState(43, 43, false, "test_invalidate", 43, intervals, 48);
            }

            drainWalQueue();
            engine.clear(); // release WAL writers
            path.trimTo(tableLen).concat(WAL_NAME_BASE).put(1).slash().put(0).concat(EVENT_FILE_NAME);
            ff.remove(path.$());
            Assert.assertFalse(ff.exists(path.$()));
            success = WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
            assertTrue(success);
            assertEquals(43, matViewStateReader.getLastRefreshBaseTxn()); // no _event file, state file
            TestUtils.assertEquals(intervals, matViewStateReader.getRefreshIntervals());
            assertEquals(48, matViewStateReader.getRefreshIntervalsBaseTxn());

            path.trimTo(tableLen).concat(MatViewState.MAT_VIEW_STATE_FILE_NAME);
            ff.remove(path.$());
            Assert.assertFalse(ff.exists(path.$()));
            success = WalUtils.readMatViewState(path.trimTo(tableLen), tableToken, configuration, txnMem, walEventReader, reader, matViewStateReader);
            assertFalse(success); // no _event file, no state file
        } finally {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, chunkSizeOld);
        }
    }

    private void assertColumnMetadata(TableModel expected, WalReader reader) {
        final int columnCount = expected.getColumnCount();
        assertEquals(columnCount, reader.getRealColumnCount());
        int skip = 0;
        for (int i = 0; i < reader.getColumnCount(); i++) {
            if (reader.getColumnType(i) < 0) {
                skip++;
                continue;
            }
            assertEquals(expected.getColumnName(i - skip), reader.getColumnName(i));
            assertEquals(expected.getColumnType(i - skip), reader.getColumnType(i));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void assertEmptySymbolDiff(WalEventCursor.DataInfo dataInfo, int columnIndex) {
        SymbolMapDiff symbolMapDiff = dataInfo.nextSymbolMapDiff();
        assertEquals(columnIndex, symbolMapDiff.getColumnIndex());
        assertEquals(0, symbolMapDiff.getRecordCount());
        assertNotNull(symbolMapDiff);
        assertNull(symbolMapDiff.nextEntry());
    }

    @SuppressWarnings("SameParameterValue")
    private void assertWalFileExist(Path path, TableToken tableName, String walName, String fileName) {
        assertWalFileExist(path, tableName, walName, -1, fileName);
    }

    private void assertWalFileExist(Path path, TableToken tableName, String walName, int segment, String fileName) {
        final int pathLen = path.size();
        try {
            path = constructPath(path, tableName, walName, segment, fileName);
            if (!Files.exists(path.$())) {
                throw new AssertionError("Path " + path + " does not exist!");
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private TableToken createPopulateTable(String tableName, long refreshTxn, boolean newFormat) {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.SYMBOL)
                .timestamp("ts")
                .wal();

        TableToken tableToken = createTable(model);
        try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
            for (int i = 0; i < 10; i++) {
                TableWriter.Row row = walWriter.newRow(i);
                row.putByte(0, (byte) i);
                row.putSym(1, "sym" + i);
                row.append();

                if (i % 2 == 0) {
                    walWriter.commit();
                } else {
                    if (newFormat) {
                        walWriter.commitMatView(refreshTxn + i, i, i, i, i + 1);
                    } else {
                        walWriter.commit();
                    }
                }
            }
            if (newFormat) {
                walWriter.resetMatViewState(1, 1, true, "test_invalidate", 1, null, -1);
            }
        }
        return tableToken;
    }

    private void generateRow(WalWriter writer, Rnd threadRnd, long timestamp) {
        var row = writer.newRow(timestamp);
        var meta = writer.getMetadata();
        for (int c = 0, n = meta.getColumnCount(); c < n; c++) {
            int type = meta.getColumnType(c);
            if (type > 0) {
                if (threadRnd.nextInt(10) > 0) {
                    switch (type) {
                        case ColumnType.SYMBOL:
                            row.putSym(c, threadRnd.nextChars(2));
                            break;
                        case ColumnType.DOUBLE:
                            if (threadRnd.nextInt(50) != 0) {
                                row.putDouble(c, threadRnd.nextDouble());
                            }
                            break;
                        case ColumnType.STRING:
                            row.putStr(c, threadRnd.nextChars(6));
                            break;
                        case ColumnType.LONG:
                            row.putLong(c, threadRnd.nextLong());
                            break;
                    }
                }
            }
        }
        row.append();
    }

    private void testApply1RowCommitManyWriters(long tsStep, int totalRows, int walWriterCount) throws Exception {
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 500_000);
        assertMemoryLeak(() -> {
            execute("create table sm (id int, ts timestamp, y long, s string, v varchar, m symbol) timestamp(ts) partition by DAY WAL");
            TableToken tableToken = engine.verifyTableName("sm");

            long ts = MicrosTimestampDriver.floor("2022-02-24");
            int symbolCount = 75;

            Utf8StringSink sink = new Utf8StringSink();
            StringSink stringSink = new StringSink();

            Rnd rnd = TestUtils.generateRandom(LOG);

            ObjList<WalWriter> writerObjList = new ObjList<>();
            for (int c = 0; c < walWriterCount; c++) {
                writerObjList.add(engine.getWalWriter(tableToken));
            }

            try {
                for (int i = 0; i < totalRows; i++) {
                    var writer = writerObjList.getQuick(rnd.nextInt(walWriterCount));

                    TableWriter.Row row = writer.newRow(ts);
                    row.putInt(0, i);
                    row.putLong(2, i + 1);
                    stringSink.clear();
                    stringSink.put(i);
                    row.putStr(3, stringSink);
                    sink.clear();
                    sink.put(i);
                    row.putVarchar(4, sink);
                    stringSink.clear();
                    stringSink.put(i % symbolCount);
                    row.putSym(5, stringSink);
                    row.append();
                    writer.commit();

                    ts += tsStep;
                }
            } finally {
                Misc.freeObjListIfCloseable(writerObjList);
            }

            WorkerPool sharedWorkerPool = null;
            try {
                sharedWorkerPool = new TestWorkerPool(4, node1.getMetrics());
                WorkerPoolUtils.setupWriterJobs(sharedWorkerPool, engine);
                sharedWorkerPool.start(LOG);

                long start = Os.currentTimeMicros();
                drainWalQueue();
                long end = Os.currentTimeMicros();

                LOG.info().$("Time to drain WAL queue: ").$((end - start) / 1_000_000.0).$("s").$();

            } finally {
                if (sharedWorkerPool != null) {
                    sharedWorkerPool.halt();
                }
            }

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql(
                    "count\tmin\tmax\n" +
                            totalRows + "\t2022-02-24T00:00:00.000000Z\t" + Micros.toUSecString(ts - tsStep) + "\n", "select count(*), min(ts), max(ts) from sm"
            );
            assertSqlCursors("sm", "select * from sm order by id");
            assertSql("id\tts\ty\ts\tv\tm\n", "select * from sm WHERE id <> cast(s as int)");
            assertSql("id\tts\ty\ts\tv\tm\n", "select * from sm WHERE id <> cast(v as int)");
            assertSql("id\tts\ty\ts\tv\tm\n", "select * from sm WHERE id % " + symbolCount + " <> cast(m as int)");

            Assert.assertTrue(engine.getTableSequencerAPI().getTxnTracker(tableToken).getMemPressureControl().getMaxBlockRowCount() > 1000);

        });
    }

    private void testDesignatedTimestampIncludesSegmentRowNumber(int[] timestampOffsets, boolean expectedOutOfOrder) throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            TableToken tableToken = createTable();

            final String walName;
            final long ts = Os.currentTimeMicros();
            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(ts);
                row.putByte(0, (byte) 1);
                row.append();
                row = walWriter.newRow(ts + timestampOffsets[0]);
                row.putByte(0, (byte) 17);
                row.append();
                row = walWriter.newRow(ts + timestampOffsets[1]);
                row.append();
                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 3)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(3, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(ts, record.getTimestamp(2));
                assertEquals(0, record.getRowId());
                assertEquals(0, ((WalDataRecord) record).getDesignatedTimestampRowId(2));
                assertTrue(cursor.hasNext());
                assertEquals(17, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(ts + timestampOffsets[0], record.getTimestamp(2));
                assertEquals(1, record.getRowId());
                assertEquals(1, ((WalDataRecord) record).getDesignatedTimestampRowId(2));
                assertTrue(cursor.hasNext());
                assertEquals(0, record.getByte(0));
                assertNull(record.getStrA(1));
                assertEquals(ts + timestampOffsets[1], record.getTimestamp(2));
                assertEquals(2, record.getRowId());
                assertEquals(2, ((WalDataRecord) record).getDesignatedTimestampRowId(2));
                assertFalse(cursor.hasNext());

                TableModel model = defaultModel(tableName);
                assertColumnMetadata(model, reader);

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(0, eventCursor.getTxn());
                assertEquals(WalTxnType.DATA, eventCursor.getType());

                final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                assertEquals(0, dataInfo.getStartRowID());
                assertEquals(3, dataInfo.getEndRowID());
                assertEquals(ts, dataInfo.getMinTimestamp());
                assertEquals(ts + Math.max(timestampOffsets[0], timestampOffsets[1]), dataInfo.getMaxTimestamp());
                assertEquals(expectedOutOfOrder, dataInfo.isOutOfOrder());
                assertNull(dataInfo.nextSymbolMapDiff());

                assertFalse(eventCursor.hasNext());
            }
        });
    }

    static void addColumn(WalWriter writer, String columnName, int columnType) {
        writer.addColumn(columnName, columnType);
    }

    static void assertBinSeqEquals(BinarySequence expected, BinarySequence actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.length(), actual.length());
        for (int i = 0; i < expected.length(); i++) {
            byte expectedByte = expected.byteAt(i);
            byte actualByte = actual.byteAt(i);
            assertEquals(
                    "Binary sequences not equals at offset " + i
                            + ". Expected byte: " + expectedByte + ", actual byte: " + actualByte + ".",
                    expectedByte, actualByte
            );
        }
    }

    static TableToken createTable(String tableName) {
        return createTable(defaultModel(tableName));
    }

    static TableToken createTable() {
        return createTable(defaultModel("testTable"));
    }

    static void prepareBinPayload(long pointer, int limit) {
        for (int offset = 0; offset < limit; offset++) {
            Unsafe.getUnsafe().putByte(pointer + offset, (byte) limit);
        }
    }

    static void removeColumn(TableWriterAPI writer, String columnName) {
        AlterOperationBuilder removeColumnOperation = new AlterOperationBuilder().ofDropColumn(0, writer.getTableToken(), 0);
        removeColumnOperation.ofDropColumn(columnName);
        writer.apply(removeColumnOperation.build(), true);
    }

    static void renameColumn(TableWriterAPI writer) {
        AlterOperationBuilder renameColumnC = new AlterOperationBuilder().ofRenameColumn(0, writer.getTableToken(), 0);
        renameColumnC.ofRenameColumn("b", "c");
        writer.apply(renameColumnC.build(), true);
    }


    interface RowInserter {
        long getCount();

        void insertRow();
    }
}
