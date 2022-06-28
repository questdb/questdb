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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.NullMapWriter;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.*;

public class WalWriterTest extends AbstractGriffinTest {

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
    public void testSymbolWal() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testSymTable";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .col("d", ColumnType.SYMBOL)
            ) {
                createTable(model);
            }

            try (TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), tableName, "symbolTest")) {
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = tableWriter.newRow();
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
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.putSym(1, "sym" + i);
                    row.putSym(2, "s" + i % 2);
                    row.putSym(3, "symbol" + i % 3);
                    row.append();
                }

                assertEquals(NullMapWriter.INSTANCE, walWriter.getSymbolMapWriter(0));
                walSymbolCounts.add(walWriter.getSymbolMapWriter(1).getSymbolCount());
                walSymbolCounts.add(walWriter.getSymbolMapWriter(2).getSymbolCount());
                walSymbolCounts.add(walWriter.getSymbolMapWriter(3).getSymbolCount());

                assertNull(walWriter.getSymbolMapWriter(1).valueOf(10));
                try {
                    walWriter.getSymbolMapWriter(1).valueBOf(0);
                    fail("UnsupportedOperationException expected");
                } catch (UnsupportedOperationException e) {
                    // ignore, this is expected
                }
            }

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(5, reader.getTransientRowCount());
                RecordCursor cursor = reader.getCursor();
                Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertEquals(i, record.getInt(1));
                    assertEquals("sym" + i, record.getSym(1));
                    assertEquals("sym" + i, reader.getSymbolMapReader(1).valueOf(i));
                    assertEquals(i % 2, record.getInt(2));
                    assertEquals("s" + i % 2, record.getSym(2));
                    assertEquals("s" + i % 2, reader.getSymbolMapReader(2).valueOf(i % 2));
                    assertEquals(i % 2, record.getInt(3));
                    assertEquals("symbol" + i % 2, record.getSym(3));
                    assertEquals(record.getSymB(3), record.getSym(3));
                    assertEquals("symbol" + i % 2, reader.getSymbolMapReader(3).valueOf(i % 2));
                    i++;
                }
                assertEquals(i, reader.getTransientRowCount());
                assertNull(reader.getSymbolMapReader(0));
                assertNull(reader.getSymbolMapReader(1).valueOf(5));
                assertNull(reader.getSymbolMapReader(2).valueOf(2));
                assertNull(reader.getSymbolMapReader(3).valueOf(2));
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, walSymbolCounts, 10L)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(10, reader.size());
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertEquals(i, record.getInt(1));
                    assertEquals("sym" + i, record.getSym(1));
                    assertEquals("sym" + i, cursor.getSymbolTable(1).valueOf(i));
                    assertEquals(i % 2, record.getInt(2));
                    assertEquals("s" + i % 2, record.getSym(2));
                    assertEquals("s" + i % 2, reader.getSymbolMapReader(2).valueOf(i % 2));
                    assertEquals(i % 3, record.getInt(3));
                    assertEquals("symbol" + i % 3, record.getSym(3));
                    assertEquals(record.getSymB(3), record.getSym(3));
                    assertEquals("symbol" + i % 3, cursor.newSymbolTable(3).valueOf(i % 3).toString());
                    i++;
                }
                assertEquals(i, reader.size());
                assertNull(reader.getSymbolMapReader(0));
                assertNull(reader.getSymbolMapReader(1).valueOf(10));
                assertNull(reader.getSymbolMapReader(2).valueOf(2));
                assertNull(reader.getSymbolMapReader(3).valueOf(3));

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(1, eventCursor.getTxn());
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
                SymbolMapDiff.Entry entry;
                while ((entry = symbolMapDiff.nextEntry()) != null) {
                    assertEquals(expectedKey, entry.getKey());
                    assertEquals("sym" + expectedKey, entry.getSymbol().toString());
                    expectedKey++;
                }
                assertEquals(10, expectedKey);
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

            try (Path path = new Path().of(configuration.getRoot())) {
                assertWalFileExist(path, tableName, walName, 0, "_meta");
                assertWalFileExist(path, tableName, walName, 0, "_event");
                assertWalFileExist(path, tableName, walName, 0, "a.d");
                assertWalFileExist(path, tableName, walName, 0, "b.d");
                assertWalFileExist(path, tableName, walName, 0, "c.d");
                assertWalFileExist(path, tableName, walName, 0, "d.d");
                assertWalFileExist(path, tableName, walName, "b.c");
                assertWalFileExist(path, tableName, walName, "b.k");
                assertWalFileExist(path, tableName, walName, "b.o");
                assertWalFileExist(path, tableName, walName, "b.v");
                assertWalFileExist(path, tableName, walName, "c.c");
                assertWalFileExist(path, tableName, walName, "c.k");
                assertWalFileExist(path, tableName, walName, "c.o");
                assertWalFileExist(path, tableName, walName, "c.v");
                assertWalFileExist(path, tableName, walName, "d.c");
                assertWalFileExist(path, tableName, walName, "d.k");
                assertWalFileExist(path, tableName, walName, "d.o");
                assertWalFileExist(path, tableName, walName, "d.v");
            }
        });
    }

    @Test
    public void testRollToNextSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                assertEquals(0, walWriter.size());
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                assertEquals(1, walWriter.size());
                row = walWriter.newRow();
                row.putByte(0, (byte) 11);
                row.append();
                assertEquals(2, walWriter.size());

                walWriter.rollSegment();
                assertEquals(0, walWriter.size());
                row = walWriter.newRow();
                row.putByte(0, (byte) 112);
                row.append();
                assertEquals(1, walWriter.size());
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 2)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(2, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(1, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertTrue(cursor.hasNext());
                    assertEquals(11, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(1, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(1, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(2, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());

                    assertFalse(eventCursor.hasNext());
                }
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(112, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(2, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(1, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());

                    assertFalse(eventCursor.hasNext());
                }
            }

            try (Path path = new Path().of(configuration.getRoot())) {
                assertWalFileExist(path, tableName, walName, 0, "_meta");
                assertWalFileExist(path, tableName, walName, 0, "_event");
                assertWalFileExist(path, tableName, walName, 0, "a.d");
                assertWalFileExist(path, tableName, walName, 0, "b.d");
                assertWalFileExist(path, tableName, walName, 0, "b.i");
                assertWalFileExist(path, tableName, walName, 1, "_meta");
                assertWalFileExist(path, tableName, walName, 1, "_event");
                assertWalFileExist(path, tableName, walName, 1, "a.d");
                assertWalFileExist(path, tableName, walName, 1, "b.d");
                assertWalFileExist(path, tableName, walName, 1, "b.i");
            }
        });
    }

    @Test
    public void testConcurrentWals() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
            ) {
                createTable(model);
            }

            final int numOfRows = 4000;
            final int maxRowCount = 500;
            final int numOfSegments = numOfRows / maxRowCount;
            final int numOfThreads = 10;
            final int numOfTxn = numOfThreads * numOfSegments;
            final SOCountDownLatch writeFinished = new SOCountDownLatch(numOfThreads);

            final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
            rollStrategy.setMaxRowCount(maxRowCount);

            for (int i = 0; i < numOfThreads; i++) {
                new Thread(() -> {
                    try {
                        TableWriter.Row row;
                        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                            walWriter.setRollStrategy(rollStrategy);
                            assertEquals(0, walWriter.size());
                            for (int n = 0; n < numOfRows; n++) {
                                row = walWriter.newRow();
                                row.putInt(0, n);
                                row.putSym(1, "test" + n);
                                row.append();
                                walWriter.rollSegmentIfLimitReached();
                            }
                        }
                    } catch (Exception e) {
                        Assert.fail("Write failed [e=" + e + "]");
                        throw new RuntimeException(e);
                    } finally {
                        writeFinished.countDown();
                    }
                }).start();
            }
            writeFinished.await();

            final LongHashSet txnSet = new LongHashSet(numOfTxn);
            final IntList symbolCounts = new IntList();
            symbolCounts.add(numOfRows);
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
            ) {
                for (int i = 0; i < numOfThreads; i++) {
                    final String walName = WalWriter.WAL_NAME_BASE + (i + 1);
                    for (int j = 0; j < numOfSegments; j++) {
                        try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, j, symbolCounts, maxRowCount)) {
                            assertEquals(3, reader.getColumnCount());
                            assertEquals(walName, reader.getWalName());
                            assertEquals(tableName, reader.getTableName());
                            assertEquals(maxRowCount, reader.size());

                            final RecordCursor cursor = reader.getDataCursor();
                            final Record record = cursor.getRecord();
                            int n = 0;
                            while(cursor.hasNext()) {
                                assertEquals(j * maxRowCount + n, record.getInt(0));
                                assertEquals("test" + (j * maxRowCount + n), record.getSym(1));
                                assertEquals(n, record.getRowId());
                                n++;
                            }
                            assertEquals(maxRowCount, n);

                            assertColumnMetadata(model, reader);

                            final WalEventCursor eventCursor = reader.getEventCursor();
                            assertTrue(eventCursor.hasNext());
                            assertEquals(WalTxnType.DATA, eventCursor.getType());
                            txnSet.add(eventCursor.getTxn());

                            final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                            assertEquals(0, dataInfo.getStartRowID());
                            assertEquals(maxRowCount, dataInfo.getEndRowID());
                            assertEquals(0, dataInfo.getMinTimestamp());
                            assertEquals(0, dataInfo.getMaxTimestamp());
                            assertFalse(dataInfo.isOutOfOrder());

                            final SymbolMapDiff symbolMapDiff = dataInfo.nextSymbolMapDiff();
                            assertEquals(1, symbolMapDiff.getColumnIndex());
                            int expectedKey = 0;
                            SymbolMapDiff.Entry entry;
                            while ((entry = symbolMapDiff.nextEntry()) != null) {
                                assertEquals(j * maxRowCount + expectedKey, entry.getKey());
                                assertEquals("test" + (j * maxRowCount + expectedKey), entry.getSymbol().toString());
                                expectedKey++;
                            }
                            assertEquals(maxRowCount, expectedKey);
                            assertNull(dataInfo.nextSymbolMapDiff());

                            assertFalse(eventCursor.hasNext());
                        }

                        try (Path path = new Path().of(configuration.getRoot())) {
                            assertWalFileExist(path, tableName, walName, j, "_meta");
                            assertWalFileExist(path, tableName, walName, j, "_event");
                            assertWalFileExist(path, tableName, walName, j, "a.d");
                            assertWalFileExist(path, tableName, walName, j, "b.d");
                            assertWalFileExist(path, tableName, walName, j, "ts.d");
                        }
                    }
                }
            }

            assertEquals(numOfTxn, txnSet.size());
            for (int i = 1; i <= numOfTxn; i++) {
                txnSet.remove(i);
            }
            assertEquals(0, txnSet.size());
        });
    }

    @Test
    public void testCancelRowStartsNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                assertEquals(0, walWriter.size());
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                assertEquals(1, walWriter.size());
                row = walWriter.newRow();
                row.putByte(0, (byte) 11);
                row.append();
                assertEquals(2, walWriter.size());
                row = walWriter.newRow();
                row.putByte(0, (byte) 111);
                assertEquals(2, walWriter.size());
                row.cancel(); // new segment
                assertEquals(0, walWriter.size());
                row = walWriter.newRow();
                row.putByte(0, (byte) 112);
                row.append();
                assertEquals(1, walWriter.size());
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 2)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(2, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(1, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertTrue(cursor.hasNext());
                    assertEquals(11, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(1, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);
                }
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(112, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);
                }
            }

            try (Path path = new Path().of(configuration.getRoot())) {
                assertWalFileExist(path, tableName, walName, 0, "_meta");
                assertWalFileExist(path, tableName, walName, 0, "_event");
                assertWalFileExist(path, tableName, walName, 0, "a.d");
                assertWalFileExist(path, tableName, walName, 0, "b.d");
                assertWalFileExist(path, tableName, walName, 0, "b.i");
                assertWalFileExist(path, tableName, walName, 1, "_meta");
                assertWalFileExist(path, tableName, walName, 1, "_event");
                assertWalFileExist(path, tableName, walName, 1, "a.d");
                assertWalFileExist(path, tableName, walName, 1, "b.d");
                assertWalFileExist(path, tableName, walName, 1, "b.i");
            }
        });
    }

    @Test
    public void testCommit() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableCommit";
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < 18; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                for (int i = 0; i < 6; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.commit(); //should not create new txn, this is noop
                walWriter.commit(); //should not create new txn, this is noop
                for (int i = 0; i < 20; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.rollSegment();
                for (int i = 0; i < 7; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 44)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(44, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    int i = 0;
                    while (cursor.hasNext()) {
                        assertEquals(i > 23 ? i - 24 : (i > 17 ? i - 18 : i), record.getByte(0));
                        assertNull(record.getStr(1));
                        assertEquals(i, record.getRowId());
                        i++;
                    }
                    assertEquals(44, i);

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));
                    assertNull(reader.getSymbolMapReader(1));

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(1, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(18, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());
                    assertNull(dataInfo.nextSymbolMapDiff());

                    assertTrue(eventCursor.hasNext());
                    assertEquals(2, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    dataInfo = eventCursor.getDataInfo();
                    assertEquals(18, dataInfo.getStartRowID());
                    assertEquals(24, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());
                    assertNull(dataInfo.nextSymbolMapDiff());

                    assertTrue(eventCursor.hasNext());
                    assertEquals(3, eventCursor.getTxn());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 7)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(7, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    int i = 0;
                    while (cursor.hasNext()) {
                        assertEquals(i, record.getByte(0));
                        assertNull(record.getStr(1));
                        assertEquals(i, record.getRowId());
                        i++;
                    }
                    assertEquals(7, i);

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));
                    assertNull(reader.getSymbolMapReader(1));

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(4, eventCursor.getTxn());
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
            }
        });
    }

    @Test
    public void testAddingColumnStartsNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAddCol";
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.addColumn("c", ColumnType.INT);
                row = walWriter.newRow();
                row.putByte(0, (byte) 10);
                row.append();
                walWriter.addColumn("d", ColumnType.SHORT);
                row = walWriter.newRow();
                row.putByte(0, (byte) 100);
                row.append();
                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(1, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));
                    assertNull(reader.getSymbolMapReader(1));

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(1, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(1, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());
                    assertNull(dataInfo.nextSymbolMapDiff());

                    assertTrue(eventCursor.hasNext());
                    assertEquals(2, eventCursor.getTxn());
                    assertEquals(WalTxnType.ADD_COLUMN, eventCursor.getType());

                    final WalEventCursor.AddColumnInfo addColumnInfo = eventCursor.getAddColumnInfo();
                    assertEquals(2, addColumnInfo.getColumnIndex());
                    assertEquals("c", addColumnInfo.getColumnName().toString());
                    assertEquals(ColumnType.INT, addColumnInfo.getColumnType());

                    assertFalse(eventCursor.hasNext());
                }
                model.col("c", ColumnType.INT);
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 1)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(10, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(Integer.MIN_VALUE, record.getInt(2));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));
                    assertNull(reader.getSymbolMapReader(1));
                    assertNull(reader.getSymbolMapReader(2));

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(3, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(1, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());
                    assertNull(dataInfo.nextSymbolMapDiff());

                    assertTrue(eventCursor.hasNext());
                    assertEquals(4, eventCursor.getTxn());
                    assertEquals(WalTxnType.ADD_COLUMN, eventCursor.getType());

                    final WalEventCursor.AddColumnInfo addColumnInfo = eventCursor.getAddColumnInfo();
                    assertEquals(3, addColumnInfo.getColumnIndex());
                    assertEquals("d", addColumnInfo.getColumnName().toString());
                    assertEquals(ColumnType.SHORT, addColumnInfo.getColumnType());

                    assertFalse(eventCursor.hasNext());
                }
                model.col("d", ColumnType.SHORT);
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 2, new IntList(), 1)) {
                    assertEquals(4, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(100, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(Integer.MIN_VALUE, record.getInt(2));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));
                    assertNull(reader.getSymbolMapReader(1));
                    assertNull(reader.getSymbolMapReader(2));
                    assertNull(reader.getSymbolMapReader(3));

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(5, eventCursor.getTxn());
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
            }
        });
    }

    @Test
    public void testRemovingColumnStartsNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableRemoveCol";
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.removeColumn("a");
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(1, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(1, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(1, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());
                    assertNull(dataInfo.nextSymbolMapDiff());

                    assertTrue(eventCursor.hasNext());
                    assertEquals(2, eventCursor.getTxn());
                    assertEquals(WalTxnType.REMOVE_COLUMN, eventCursor.getType());

                    final WalEventCursor.RemoveColumnInfo removeColumnInfo = eventCursor.getRemoveColumnInfo();
                    assertEquals(0, removeColumnInfo.getColumnIndex());

                    assertFalse(eventCursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testAddingSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAddSymbolCol";
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 125);
                row.append();
                walWriter.addColumn("c", ColumnType.SYMBOL);
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(125, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));
                    assertNull(reader.getSymbolMapReader(1));

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(1, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(1, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());
                    assertNull(dataInfo.nextSymbolMapDiff());

                    assertTrue(eventCursor.hasNext());
                    assertEquals(2, eventCursor.getTxn());
                    assertEquals(WalTxnType.ADD_COLUMN, eventCursor.getType());

                    final WalEventCursor.AddColumnInfo addColumnInfo = eventCursor.getAddColumnInfo();
                    assertEquals(2, addColumnInfo.getColumnIndex());
                    assertEquals("c", addColumnInfo.getColumnName().toString());
                    assertEquals(ColumnType.SYMBOL, addColumnInfo.getColumnType());

                    assertFalse(eventCursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testRemovingSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableRemoveSymCol";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
            ) {
                createTable(model);
            }

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putInt(0, 12);
                row.putSym(1, "sym");
                row.append();
                walWriter.removeColumn("b");
                row = walWriter.newRow();
                row.putInt(0, 133);
                row.append();
            }

            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
            ) {
                final IntList walSymbolCounts = new IntList();
                walSymbolCounts.add(1);
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, walSymbolCounts, 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(12, record.getInt(0));
                    assertEquals("sym", record.getSym(1));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    assertNull(reader.getSymbolMapReader(0));
                    assertEquals(1, reader.getSymbolMapReader(1).getSymbolCount());

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(1, eventCursor.getTxn());
                    assertEquals(WalTxnType.DATA, eventCursor.getType());

                    final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                    assertEquals(0, dataInfo.getStartRowID());
                    assertEquals(1, dataInfo.getEndRowID());
                    assertEquals(0, dataInfo.getMinTimestamp());
                    assertEquals(0, dataInfo.getMaxTimestamp());
                    assertFalse(dataInfo.isOutOfOrder());

                    final SymbolMapDiff symbolMapDiff = dataInfo.nextSymbolMapDiff();
                    assertEquals(1, symbolMapDiff.getColumnIndex());
                    int expectedKey = 0;
                    SymbolMapDiff.Entry entry;
                    while ((entry = symbolMapDiff.nextEntry()) != null) {
                        assertEquals(expectedKey, entry.getKey());
                        assertEquals("sym", entry.getSymbol().toString());
                        expectedKey++;
                    }
                    assertEquals(1, expectedKey);
                    assertNull(dataInfo.nextSymbolMapDiff());

                    assertTrue(eventCursor.hasNext());
                    assertEquals(2, eventCursor.getTxn());
                    assertEquals(WalTxnType.REMOVE_COLUMN, eventCursor.getType());

                    final WalEventCursor.RemoveColumnInfo removeColumnInfo = eventCursor.getRemoveColumnInfo();
                    assertEquals(1, removeColumnInfo.getColumnIndex());

                    assertFalse(eventCursor.hasNext());
                }
            }
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

    private void testDesignatedTimestampIncludesSegmentRowNumber(int[] timestampOffsets, boolean expectedOutOfOrder) throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            createTable(tableName, true);

            final String walName;
            final long ts = Os.currentTimeMicros();
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow(ts);
                row.putByte(0, (byte) 1);
                row.append();
                row = walWriter.newRow(ts + timestampOffsets[0]);
                row.putByte(0, (byte) 17);
                row.append();
                row = walWriter.newRow(ts + timestampOffsets[1]);
                row.append();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 3)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(3, reader.size());

                final RecordCursor cursor = reader.getDataCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(ts, record.getTimestamp(2));
                assertEquals(0, record.getRowId());
                assertEquals(0, ((WalDataRecord) record).getDesignatedTimestampRowId(2));
                assertTrue(cursor.hasNext());
                assertEquals(17, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(ts + timestampOffsets[0], record.getTimestamp(2));
                assertEquals(1, record.getRowId());
                assertEquals(1, ((WalDataRecord) record).getDesignatedTimestampRowId(2));
                assertTrue(cursor.hasNext());
                assertEquals(0, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(ts + timestampOffsets[1], record.getTimestamp(2));
                assertEquals(2, record.getRowId());
                assertEquals(2, ((WalDataRecord) record).getDesignatedTimestampRowId(2));
                assertFalse(cursor.hasNext());

                try (TableModel model = defaultModel(tableName, true)) {
                    assertColumnMetadata(model, reader);
                }

                final WalEventCursor eventCursor = reader.getEventCursor();
                assertTrue(eventCursor.hasNext());
                assertEquals(1, eventCursor.getTxn());
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

    @Test
    public void testReadAndWriteAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAllTypes";
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
                    .timestamp("ts")
            ) {
                createTable(model);
            }

            final int rowsToInsertTotal = 100;
            final long pointer = Unsafe.getUnsafe().allocateMemory(rowsToInsertTotal);
            try {
                final long ts = Os.currentTimeMicros();
                final Long256Impl long256 = new Long256Impl();
                final StringSink stringSink = new StringSink();
                final DirectBinarySequence binSeq = new DirectBinarySequence();

                final String walName;
                final IntList walSymbolCounts = new IntList();
                try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    assertEquals(tableName, walWriter.getTableName());
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

                        row.putTimestamp(21, "2022-06-10T09:13:46." + (i + 1));

                        row.putStr(22, (char) (65 + i % 26));
                        row.putStr(23, "abcdefghijklmnopqrstuvwxyz", 0, i % 26 + 1);

                        row.putSym(24, String.valueOf(i));
                        row.putSym(25, (char) (65 + i % 26));

                        row.append();
                    }

                    walSymbolCounts.add(walWriter.getSymbolMapWriter(24).getSymbolCount());
                    walSymbolCounts.add(walWriter.getSymbolMapWriter(25).getSymbolCount());

                    assertEquals(rowsToInsertTotal, walWriter.size());
                    assertEquals("WalWriter{name=" + tableName + "}", walWriter.toString());
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, walSymbolCounts, rowsToInsertTotal)) {
                    assertEquals(27, reader.getColumnCount());
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
                        assertEquals(String.valueOf(i), record.getStr(11).toString());
                        assertEquals(record.getStr(11).toString(), record.getStrB(11).toString());
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

                        assertEquals(String.valueOf((char) (65 + i % 26)), record.getStr(22).toString());
                        assertEquals("abcdefghijklmnopqrstuvwxyz".substring(0, i % 26 + 1), record.getStr(23).toString());

                        assertEquals(String.valueOf(i), record.getSym(24));
                        assertEquals(String.valueOf((char) (65 + i % 26)), record.getSym(25));

                        assertEquals(ts, record.getTimestamp(26));
                        assertEquals(i, record.getRowId());
                        testSink.clear();
                        ((Sinkable) record).toSink(testSink);
                        assertEquals("WalReaderRecord [recordIndex=" + i + "]", testSink.toString());
                        try {
                            cursor.getRecordB();
                            fail("UnsupportedOperationException expected");
                        } catch (UnsupportedOperationException e) {
                            // ignore, this is expected
                        }
                        try {
                            record.getUpdateRowId();
                            fail("UnsupportedOperationException expected");
                        } catch (UnsupportedOperationException e) {
                            // ignore, this is expected
                        }
                        i++;
                    }
                    assertEquals(i, cursor.size());
                    assertEquals(i, reader.size());
                }
            } finally {
                Unsafe.getUnsafe().freeMemory(pointer);
            }
        });
    }

    @Test
    public void testRollSegmentByStorageSize() throws Exception {
        assertMemoryLeak(() -> {
            final int rowsToInsertTotal = 50;
            final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
            rollStrategy.setMaxSegmentSize(2048L);
            final LongList expectedRowCounts = new LongList();
            expectedRowCounts.add(14L);
            expectedRowCounts.add(13L);
            expectedRowCounts.add(12L);
            expectedRowCounts.add(11L);

            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts);
        });
    }

    @Test
    public void testRollSegmentByRowCount() throws Exception {
        assertMemoryLeak(() -> {
            final int rowsToInsertTotal = 55;
            final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
            rollStrategy.setMaxRowCount(15L);
            final LongList expectedRowCounts = new LongList();
            expectedRowCounts.add(15L);
            expectedRowCounts.add(15L);
            expectedRowCounts.add(15L);
            expectedRowCounts.add(10L);

            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts);
        });
    }

    @Test
    public void testRollSegmentByStorageSizeThenByRowCount() throws Exception {
        assertMemoryLeak(() -> {
            final int rowsToInsertTotal = 50;
            final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
            rollStrategy.setMaxSegmentSize(2048L);
            final LongList expectedRowCounts = new LongList();
            expectedRowCounts.add(14L);
            expectedRowCounts.add(13L);
            expectedRowCounts.add(10L);
            expectedRowCounts.add(10L);
            expectedRowCounts.add(3L);

            final IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates = new IntObjHashMap<>();
            rollStrategyUpdates.put(30, strategy -> strategy.setMaxRowCount(10L));
            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
        });
    }

    @Test
    public void testRollSegmentByRowCountThenByStorageSize() throws Exception {
        assertMemoryLeak(() -> {
            final int rowsToInsertTotal = 70;
            final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
            rollStrategy.setMaxRowCount(8L);
            final LongList expectedRowCounts = new LongList();
            expectedRowCounts.add(8L);
            expectedRowCounts.add(8L);
            expectedRowCounts.add(8L);
            expectedRowCounts.add(8L);
            expectedRowCounts.add(22L);
            expectedRowCounts.add(16L);

            final IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates = new IntObjHashMap<>();
            rollStrategyUpdates.put(23, strategy -> strategy.setMaxSegmentSize(4096L));
            rollStrategyUpdates.put(33, strategy -> strategy.setMaxRowCount(100L));
            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
        });
    }

    @Test
    public void testRollSegmentByInterval() throws Exception {
        assertMemoryLeak(() -> {
            final int rowsToInsertTotal = 57;
            final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
            rollStrategy.setRollInterval(20000L);
            final LongList expectedRowCounts = new LongList();
            expectedRowCounts.add(18L);
            expectedRowCounts.add(15L);
            expectedRowCounts.add(20L);
            expectedRowCounts.add(4L);

            final IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates = new IntObjHashMap<>();
            rollStrategyUpdates.put(10, strategy -> currentMillis += 2000L);
            rollStrategyUpdates.put(22, strategy -> currentMillis += 5000L);
            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
        });
    }

    @Test
    public void testRollSegmentByRowCountThenByStorageSizeThenByInterval() throws Exception {
        assertMemoryLeak(() -> {
            final int rowsToInsertTotal = 70;
            final WalWriterRollStrategy rollStrategy = new WalWriterRollStrategyImpl();
            rollStrategy.setMaxRowCount(8L);
            rollStrategy.setRollInterval(30000L);
            final LongList expectedRowCounts = new LongList();
            expectedRowCounts.add(8L);
            expectedRowCounts.add(8L);
            expectedRowCounts.add(24L);
            expectedRowCounts.add(11L);
            expectedRowCounts.add(19L);

            final IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates = new IntObjHashMap<>();
            rollStrategyUpdates.put(7, strategy -> strategy.setMaxSegmentSize(4096L));
            rollStrategyUpdates.put(17, strategy -> strategy.setMaxRowCount(100L));
            rollStrategyUpdates.put(50, strategy -> currentMillis += 20000L);
            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
        });
    }

    private void testRollSegment(int rowsToInsertTotal, WalWriterRollStrategy rollStrategy, LongList expectedRowCounts) {
        final IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates = new IntObjHashMap<>();
        testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
    }

    private void testRollSegment(int rowsToInsertTotal, WalWriterRollStrategy rollStrategy, LongList expectedRowCounts,
                                 IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates) {
        final String tableName = "testTableRollSegment";
        createRollSegmentTestTable(tableName);

        final long pointer = Unsafe.getUnsafe().allocateMemory(rowsToInsertTotal);
        try {
            final String walName;
            final long ts = Os.currentTimeMicros();
            final DirectBinarySequence binSeq = new DirectBinarySequence();
            final LongList rowCounts = new LongList();
            final ObjList<IntList> walSymbolCounts = new ObjList<>();

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walWriter.setRollStrategy(rollStrategy);
                assertEquals(tableName, walWriter.getTableName());
                walName = walWriter.getWalName();
                for (int i = 0; i < rowsToInsertTotal; i++) {
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
                    row.putSym(17, String.valueOf(i));
                    row.append();

                    currentMillis += 1000L;
                    final Consumer<WalWriterRollStrategy> rollStrategyUpdate = rollStrategyUpdates.get(i);
                    if (rollStrategyUpdate != null) {
                        rollStrategyUpdate.accept(rollStrategy);
                    }

                    final long rolledRowCount = walWriter.rollSegmentIfLimitReached();
                    if (rolledRowCount != 0 || i == rowsToInsertTotal - 1) {
                        rowCounts.add(rolledRowCount == 0 ? walWriter.size() : rolledRowCount);
                        final IntList symbolCounts = new IntList();
                        walSymbolCounts.add(symbolCounts);
                        symbolCounts.add(walWriter.getSymbolMapWriter(17).getSymbolCount());
                    }
                }

                assertEquals("WalWriter{name=" + tableName + "}", walWriter.toString());
            }

            assertEquals(expectedRowCounts, rowCounts);

            int i = 0;
            for (int segmentId = 0; segmentId < rowCounts.size(); segmentId++) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName,
                        segmentId, walSymbolCounts.get(segmentId), rowCounts.get(segmentId))) {
                    assertEquals(19, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(rowCounts.get(segmentId), reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    final Long256Impl long256 = new Long256Impl();
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
                        assertEquals(String.valueOf(i), record.getStr(11).toString());
                        assertEquals(record.getStr(11).toString(), record.getStrB(11).toString());
                        assertEquals(String.valueOf(i).length(), record.getStrLen(11));
                        assertEquals(i, record.getGeoByte(12));
                        assertEquals(i, record.getGeoInt(13));
                        assertEquals(i, record.getGeoShort(14));
                        assertEquals(i, record.getGeoLong(15));
                        prepareBinPayload(pointer, i);
                        assertBinSeqEquals(binSeq.of(pointer, i), record.getBin(16));
                        assertEquals(String.valueOf(i), record.getSym(17));
                        assertEquals(ts, record.getTimestamp(18));

                        long rowId = i;
                        for (int x = 0; x < segmentId; x++) {
                            rowId -= rowCounts.getQuick(x);
                        }
                        assertEquals(rowId, record.getRowId());
                        sink.clear();
                        ((Sinkable) record).toSink(sink);
                        assertEquals("WalReaderRecord [recordIndex=" + rowId + "]", sink.toString());
                        i++;
                    }
                    assertEquals(rowCounts.get(segmentId), cursor.size());
                    assertEquals(rowCounts.get(segmentId), reader.size());
                }
            }
        } finally {
            Unsafe.getUnsafe().freeMemory(pointer);
        }
    }

    private void createTable(String tableName) {
        createTable(tableName, false);
    }

    private void createTable(String tableName, boolean withTimestamp) {
        try (TableModel model = defaultModel(tableName, withTimestamp)) {
            createTable(model);
        }
    }

    private void createTable(TableModel model) {
        TableUtils.createTable(
                configuration,
                model.getMem(),
                model.getPath(),
                model,
                1
        );
    }

    private TableModel defaultModel(String tableName) {
        return defaultModel(tableName, false);
    }

    private TableModel defaultModel(String tableName, boolean withTimestamp) {
        return withTimestamp
                ? new TableModel(configuration, tableName, PartitionBy.HOUR)
                        .col("a", ColumnType.BYTE)
                        .col("b", ColumnType.STRING)
                        .timestamp("ts")
                : new TableModel(configuration, tableName, PartitionBy.NONE)
                        .col("a", ColumnType.BYTE)
                        .col("b", ColumnType.STRING);
    }

    private void createRollSegmentTestTable(String tableName) {
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
                .col("geoByte", ColumnType.GEOBYTE)
                .col("geoInt", ColumnType.GEOINT)
                .col("geoShort", ColumnType.GEOSHORT)
                .col("geoLong", ColumnType.GEOLONG)
                .col("bin", ColumnType.BINARY)
                .col("symbol", ColumnType.SYMBOL)
                .timestamp("ts")
        ) {
            createTable(model);
        }
    }

    private static void prepareBinPayload(long pointer, int limit) {
        for (int offset = 0; offset < limit; offset++) {
            Unsafe.getUnsafe().putByte(pointer + offset, (byte) limit);
        }
    }

    private static void assertBinSeqEquals(BinarySequence expected, BinarySequence actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.length(), actual.length());
        for (int i = 0; i < expected.length(); i++) {
            byte expectedByte = expected.byteAt(i);
            byte actualByte = actual.byteAt(i);
            assertEquals("Binary sequences not equals at offset " + i
                    + ". Expected byte: " + expectedByte + ", actual byte: " + actualByte +".",
                    expectedByte, actualByte);
        }
    }

    private void assertColumnMetadata(TableModel expected, WalReader reader) {
        final int columnCount = expected.getColumnCount();
        assertEquals(columnCount, reader.getColumnCount());
        for (int i = 0; i < columnCount; i++) {
            assertEquals(expected.getColumnName(i), reader.getColumnName(i));
            assertEquals(expected.getColumnType(i), reader.getColumnType(i));
        }
    }

    private void assertWalFileExist(Path path, String tableName, String walName, String fileName) {
        assertWalFileExist(path, tableName, walName, -1, fileName);
    }

    private void assertWalFileExist(Path path, String tableName, String walName, int segment, String fileName) {
        final int pathLen = path.length();
        try {
            path = constructPath(path, tableName, walName, segment, fileName);
            if (!Files.exists(path)) {
                throw new AssertionError("Path " + path + " does not exists!");
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    private static Path constructPath(Path path, CharSequence tableName, CharSequence walName, long segment, CharSequence fileName) {
        return segment < 0
                ? path.concat(tableName).slash().concat(walName).slash().concat(fileName).$()
                : path.concat(tableName).slash().concat(walName).slash().put(segment).slash().concat(fileName).$();
    }
}