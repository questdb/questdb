/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.wal.*;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.str.*;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.questdb.cairo.wal.WalUtils.*;
import static org.junit.Assert.*;

public class WalWriterTest extends AbstractCairoTest {

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

            try (TableModel model = defaultModel(tableToken.getTableName())) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName1, reader.getWalName());
                    assertEquals(tableToken.getTableName(), reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(1, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(0, record.getRowId());
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertTrue(eventCursor.hasNext());
                    assertEquals(0, eventCursor.getTxn());
                    Assert.assertEquals(WalTxnType.DATA, eventCursor.getType());

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
                    assertNull(record.getStr(1));
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
                    assertNull(record.getStr(1));
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
                    assertNull(record.getStr(1));
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
            }
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

            try (TableModel model = defaultModel(tableToken.getTableName())) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableToken.getTableName(), reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(1, record.getByte(0));
                    assertNull(record.getStr(1));
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
                    assertNull(record.getStr(1));
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
                    assertNull(record.getStr(1));
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

            try (TableModel model = defaultModel(tableToken.getTableName())) {
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

            try (TableModel model = defaultModel(tableName)) {
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
                    assertNull(record.getStr(1));
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
                    assertException("Should not be able to add duplicate column");
                } catch (CairoException e) {
                    assertEquals("[-1] duplicate column name: c", e.getMessage());
                }

                row = walWriter.newRow(0);
                row.putByte(0, (byte) 100);
                row.append();
                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
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
                    assertNull(record.getStr(1));
                    assertEquals(Integer.MIN_VALUE, record.getInt(3));
                    assertEquals(0, record.getRowId());
                    assertTrue(cursor.hasNext());
                    assertEquals(100, record.getByte(0));
                    assertNull(record.getStr(1));
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
                    assertException("Segment 2 should not exist");
                } catch (CairoException e) {
                    assertTrue(e.getMessage().endsWith("could not open read-only [file=" + engine.getConfiguration().getRoot() +
                            File.separatorChar + tableName + TableUtils.SYSTEM_TABLE_NAME_SUFFIX + "1" +
                            File.separatorChar + walName +
                            File.separatorChar + "2" +
                            File.separatorChar + TableUtils.META_FILE_NAME + "]"));
                }
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

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
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
            }
        });
    }

    @Test
    public void testAlterAddChangeLag() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());
            ddl("alter table " + tableToken.getTableName() + " SET PARAM o3MaxLag = 20s");
            ddl("alter table " + tableToken.getTableName() + " add i2 int");
            insert("insert into " + tableToken.getTableName() + "(ts, i2) values ('2022-02-24', 2)");

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql("a\tb\tts\ti2\n" +
                    "0\t\t2022-02-24T00:00:00.000000Z\t2\n", tableToken.getTableName());
        });
    }

    @Test
    public void testAlterAddChangeMaxUncommitted() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());
            ddl("alter table " + tableToken.getTableName() + " set PARAM maxUncommittedRows = 20000");
            ddl("alter table " + tableToken.getTableName() + " add i2 int");
            insert("insert into " + tableToken.getTableName() + "(ts, i2) values ('2022-02-24', 2)");

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql("a\tb\tts\ti2\n" +
                    "0\t\t2022-02-24T00:00:00.000000Z\t2\n", tableToken.getTableName());
        });
    }

    @Test
    public void testAlterAddDropIndex() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());
            compile("alter table " + tableToken.getTableName() + " add sym2 symbol");
            compile("alter table " + tableToken.getTableName() + " alter column sym2 add index");
            compile("alter table " + tableToken.getTableName() + " alter column sym2 drop index");
            compile("alter table " + tableToken.getTableName() + " add i2 int");
            insert("insert into " + tableToken.getTableName() + "(ts, i2) values ('2022-02-24', 2)");

            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            assertSql("a\tb\tts\tsym2\ti2\n" +
                    "0\t\t2022-02-24T00:00:00.000000Z\t\t2\n", tableToken.getTableName());
        });
    }

    @Test
    public void testAlterTableRejectedIfTransactionPending() throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
                TableWriter.Row row = walWriter.newRow(0);
                row.putByte(0, (byte) 1);
                row.append();
                // no commit intentional
                addColumn(walWriter, "c", ColumnType.INT);
                assertException("Exception expected");
            } catch (Exception e) {
                // this exception will be handled in ILP/PG/HTTP
                assertTrue(e.getMessage().endsWith("cannot alter table with uncommitted inserts [table=testAlterTableRejectedIfTransactionPending]"));
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

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 3)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(3, reader.size());

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

                    assertTrue(cursor.hasNext());
                    assertEquals(112, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(2, record.getRowId());

                    assertFalse(cursor.hasNext());
                    assertColumnMetadata(model, reader);
                }
            }

            try (Path path = new Path().of(configuration.getRoot())) {
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

            try (TableModel model = defaultModel(tableName)) {
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
                        assertNull(record.getStr(1));
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
                        assertNull(record.getStr(1));
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
            }
        });
    }

    @Test
    public void testConcurrentAddRemoveColumn_DifferentColNamePerThread() throws Exception {
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
                    th.printStackTrace();
                }
                Assert.fail("Write failed");
            }

            final LongHashSet txnSet = new LongHashSet(numOfThreads);
            final IntList symbolCounts = new IntList();
            symbolCounts.add(numOfRows);
            try (TableModel model = defaultModel(tableName)) {
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
                                assertEquals("test" + n, record.getStr(1).toString());
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

                        try (Path path = new Path().of(configuration.getRoot())) {
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
            }

            assertEquals(numOfThreads, txnSet.size());
        });
    }

    @Test
    public void testConcurrentInsert() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            TableToken tableToken;
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
                tableToken = createTable(model);
            }

            final int numOfRows = 4000;
            final int maxRowCount = 500;
            configOverrideWalSegmentRolloverRowCount(maxRowCount);
            Assert.assertEquals(configuration.getWalSegmentRolloverRowCount(), maxRowCount);
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
                    th.printStackTrace();
                }
                Assert.fail("Write failed");
            }

            final LongHashSet txnSet = new LongHashSet(numOfTxn);
            final IntList symbolCounts = new IntList();
            symbolCounts.add(numOfRows);
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
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
                                assertEquals("test" + ((segmentId % numOfSegments) * maxRowCount + n), record.getSym(1));
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

                        try (Path path = new Path().of(configuration.getRoot())) {
                            assertWalFileExist(path, tableToken, walName, segmentId, "_meta");
                            assertWalFileExist(path, tableToken, walName, segmentId, "_event");
                            assertWalFileExist(path, tableToken, walName, segmentId, "a.d");
                            assertWalFileExist(path, tableToken, walName, segmentId, "b.d");
                            assertWalFileExist(path, tableToken, walName, segmentId, "ts.d");
                        }
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
    public void testExceptionThrownIfSequencerCannotBeCreated() throws Exception {
        assertMemoryLeak(() -> {
            ff = new TestFilesFacadeImpl() {
                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, WAL_INDEX_FILE_NAME)) {
                        // Set errno to path does not exist
                        this.openRO(Path.getThreadLocal2("does-not-exist").$());
                        return -1;
                    }
                    return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
                }
            };

            try {
                createTable(testName.getMethodName());
                assertException("Exception expected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped");
            }
        });
    }

    @Test
    public void testExceptionThrownIfSequencerCannotBeOpened() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long getPageSize() {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[4].getClassName().endsWith("TableSequencerImpl") && stackTrace[4].getMethodName().equals("open")) {
                        throw e;
                    }
                }
                return Files.PAGE_SIZE;
            }
        };

        assertMemoryLeak(ff, () -> {
            try {
                createTable(testName.getMethodName());
                assertException("Exception expected");
            } catch (Exception e) {
                // this exception will be handled in ILP/PG/HTTP
                assertEquals("Test failure", e.getMessage());
            }
        });
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

        assertMemoryLeak(ff, () -> {
            try {
                createTable(testName.getMethodName());
                assertException("Exception expected");
            } catch (Exception e) {
                // this exception will be handled in ILP/PG/HTTP
                assertTrue(e.getMessage().startsWith("[999] Cannot create sequencer directory:"));
            }
        });
    }

    @Test
    public void testLargeSegmentRollover() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            TableToken tableToken;
            // Schema with 8 columns, 8 bytes each = 64 bytes per row
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .timestamp("ts")
                    .col("a", ColumnType.LONG)
                    .wal()
            ) {
                tableToken = createTable(model);
            }

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
                Assert.assertEquals(ins.getCount(), 1);

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
                runWalPurgeJob();

                assertSegmentExistence(false, tableName, 1, 0);
            }
        });
    }

    @Test
    public void testMaxLagTxnCount() throws Exception {
        configOverrideWalApplyTableTimeQuota(0);
        configOverrideWalMaxLagTxnCount();
        assertMemoryLeak(() -> {
            TableToken tableToken = createTable(testName.getMethodName());

            insert("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T23:00:00.000000Z')");
            tickWalQueue(1);

            assertSql(
                    "a\tb\tts\n" +
                            "0\t\t2023-08-04T23:00:00.000000Z\n",
                    tableToken.getTableName()
            );

            insert("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T22:00:00.000000Z')");
            insert("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T21:00:00.000000Z')");
            insert("insert into " + tableToken.getTableName() + "(ts) values ('2023-08-04T20:00:00.000000Z')");

            // Run WAL apply job two times:
            // Tick 1. Put row 2023-08-04T22 into the lag.
            // Tick 2. Instead of putting row 2023-08-04T21 into the lag, we force full commit.
            tickWalQueue(2);

            // We expect all, but the last row to be visible.
            assertSql(
                    "a\tb\tts\n" +
                            "0\t\t2023-08-04T21:00:00.000000Z\n" +
                            "0\t\t2023-08-04T22:00:00.000000Z\n" +
                            "0\t\t2023-08-04T23:00:00.000000Z\n",
                    tableToken.getTableName()
            );

            drainWalQueue();

            assertSql(
                    "a\tb\tts\n" +
                            "0\t\t2023-08-04T20:00:00.000000Z\n" +
                            "0\t\t2023-08-04T21:00:00.000000Z\n" +
                            "0\t\t2023-08-04T22:00:00.000000Z\n" +
                            "0\t\t2023-08-04T23:00:00.000000Z\n",
                    tableToken.getTableName()
            );
        });
    }

    @Test
    public void testOverlappingStructureChangeCannotCreateFile() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "0" + Files.SEPARATOR + "c.d")) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            TableToken tableToken = createTable(testName.getMethodName());

            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                    assertException("Exception expected");
                } catch (CairoException e) {
                    // this exception will be handled in ILP/PG/HTTP
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-write");
                }
            }
        });
    }

    @Test
    public void testOverlappingStructureChangeFails() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[2].getClassName().endsWith("TableTransactionLog") && stackTrace[2].getMethodName().equals("openFileRO")) {
                        return -1;
                    }
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            TableToken tableToken = createTable(testName.getMethodName());

            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                    assertException("Exception expected");
                } catch (Exception e) {
                    // this exception will be handled in ILP/PG/HTTP
                    assertTrue(e.getMessage().contains("could not open read-only"));
                }
            }
        });
    }

    @Test
    public void testOverlappingStructureChangeMissing() throws Exception {
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long readNonNegativeLong(int fd, long offset) {
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

        assertMemoryLeak(ff, () -> {
            TableToken tableToken = createTable(testName.getMethodName());

            try (WalWriter walWriter1 = engine.getWalWriter(tableToken)) {
                try (WalWriter walWriter2 = engine.getWalWriter(tableToken)) {
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                    assertException("Exception expected");
                } catch (Exception e) {
                    // this exception will be handled in ILP/PG/HTTP
                    assertEquals("[0] expected to read table structure changes but there is no saved in the sequencer [structureVersionLo=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testReadAndWriteAllTypes() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableAllTypes";
            TableToken tableToken;
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
                    .col("symbol8", ColumnType.SYMBOL) // putSymUtf8(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars)
                    .col("string8", ColumnType.STRING) // putStrUtf8AsUtf16(int columnIndex, DirectByteCharSequence value, boolean hasNonAsciiChars)
                    .col("uuida", ColumnType.UUID) // putUUID(int columnIndex, long lo, long hi)
                    .col("uuidb", ColumnType.UUID) // putUUID(int columnIndex, CharSequence value)
                    .col("IPv4", ColumnType.IPv4)
                    .timestamp("ts")
                    .wal()
            ) {
                tableToken = createTable(model);
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
                try (WalWriter walWriter = engine.getWalWriter(tableToken)) {
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

                        row.putTimestamp(21, SqlUtil.implicitCastStrAsTimestamp("2022-06-10T09:13:46." + (i + 1)));

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
                    assertEquals(32, reader.getColumnCount());
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

                        assertEquals((i % 2) == 0 ? "Щось" : "Таке-Сяке", record.getSym(26).toString());
                        assertEquals((i % 2) == 0 ? "Щось" : "Таке-Сяке", record.getStr(27).toString());

                        assertEquals(i, record.getLong128Lo(28));
                        assertEquals(i + 1, record.getLong128Hi(28));

                        assertEquals(i, record.getLong128Lo(29));
                        assertEquals(i + 1, record.getLong128Hi(29));
                        assertEquals(i, record.getIPv4(30));

                        assertEquals(ts, record.getTimestamp(31));
                        assertEquals(i, record.getRowId());
                        testSink.clear();
                        ((Sinkable) record).toSink(testSink);
                        assertEquals("WalReaderRecord [recordIndex=" + i + "]", testSink.toString());
                        try {
                            cursor.getRecordB();
                            assertException("UnsupportedOperationException expected");
                        } catch (UnsupportedOperationException e) {
                            // ignore, this is expected
                        }
                        try {
                            record.getUpdateRowId();
                            assertException("UnsupportedOperationException expected");
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

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName1, reader.getWalName());
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
                    assertNull(record.getStr(1));
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
            }
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            ) {
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

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
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
                    assertException("Segment 1 should not exist");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open read-only [file=" + engine.getConfiguration().getRoot() +
                            File.separatorChar + tableName + TableUtils.SYSTEM_TABLE_NAME_SUFFIX + "1" +
                            File.separatorChar + walName +
                            File.separatorChar + "1" +
                            File.separatorChar + TableUtils.META_FILE_NAME + "]");
                }
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
                    assertException("Should not be able to remove non existent column");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "cannot remove column, column does not exist [table=testRemovingNonExistentColumn, column=noColLikeThis]");
                }
                row = walWriter.newRow(0);
                row.putByte(0, (byte) 10);
                row.append();

                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 2)) {
                    assertEquals(3, reader.getColumnCount());
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
                    assertEquals(10, record.getByte(0));
                    assertNull(record.getStr(1));
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
            }
        });
    }

    @Test
    public void testRemovingSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken;
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
                tableToken = createTable(model);
            }

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
                    assertException("UnsupportedOperationException expected");
                } catch (UnsupportedOperationException ignore) {
                }
                try {
                    TestUtils.putUtf8(row, "Щось", 1, true);
                    assertException("UnsupportedOperationException expected");
                } catch (UnsupportedOperationException ignore) {
                }

                TestUtils.putUtf8(row, "Таке-Сяке", 2, true);
                row.append();
                walWriter.commit();
            }

            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .timestamp("ts")
            ) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 1)) {
                    assertEquals(4, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(12, record.getInt(0));
                    assertEquals("symb", record.getSym(1));
                    assertEquals("symc", record.getSym(2));
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
            }
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("c", ColumnType.SYMBOL)
                    .timestamp("ts")
            ) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 1, 1)) {
                    assertEquals(4, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(133, record.getInt(0));
                    assertEquals("Таке-Сяке", record.getSym(2));
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
                    assertEquals(1, symbolMapDiff.getSize());
                    assertEquals(2, symbolMapDiff.getColumnIndex());
                    assertEquals(0, symbolMapDiff.getCleanSymbolCount());

                    assertFalse(eventCursor.hasNext());
                }
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

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName1, reader.getWalName());
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
                    assertNull(record.getStr(1));
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
            }
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)
                    .col("c", ColumnType.STRING)
                    .timestamp("ts")
                    .wal()
            ) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName1, 1, 1)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName1, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(110, record.getByte(0));
                    assertNull(record.getStr(1));
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
                    assertNull(record.getStr(1));
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

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 2)) {
                    assertEquals(3, reader.getColumnCount());
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
                    assertNull(record.getStr(1));
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
            }

            try (Path path = new Path().of(configuration.getRoot())) {
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
                try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                        .col("a", colType)
                        .timestamp("ts")
                        .wal()
                ) {
                    tableToken = createTable(model);
                }

                final long rolloverSize = 1024;
                configOverrideWalSegmentRolloverSize(rolloverSize);  // 1 KiB

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
            configOverrideWalSegmentRolloverSize(0);
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
        testRolloverSegmentSize(ColumnType.STRING, true, bytesPerRow, 0, (row) -> {
            final String formatted = String.format("%03d", value.getAndIncrement());
            row.putStr(0, formatted);
        });
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
        testRolloverSegmentSize(ColumnType.SYMBOL, false, bytesPerRow, additionalBytesPerTxn, (row) -> {
            final String formatted = String.format("%03d", value.getAndIncrement());
            row.putSym(0, formatted);
        });
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

            try (TableModel model = defaultModel(tableName)) {
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
                        assertNull(record.getStr(1));
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
                        assertNull(record.getStr(1));
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
                        assertNull(record.getStr(1));
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
                        assertNull(record.getStr(1));
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
            }
        });
    }

    @Test
    public void testSymbolWal() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testSymTable";
            TableToken tableToken;
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .col("d", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
                tableToken = createTable(model);
            }

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

            try (TableReader reader = engine.getReader(tableToken)) {
                assertEquals(5, reader.getMetadata().getColumnCount());
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

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getSecurityContext(), tableToken, walName, 0, 10L)) {
                assertEquals(5, reader.getColumnCount());
                assertEquals(10, reader.size());
                RecordCursor cursor = reader.getDataCursor();
                Record record = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    assertEquals(i, record.getByte(0));
                    assertEquals(i, record.getInt(1));
                    assertEquals("sym" + i, record.getSym(1));
                    assertEquals(i % 2, record.getInt(2));
                    assertEquals("s" + i % 2, record.getSym(2));
                    assertEquals(i % 3, record.getInt(3));
                    assertEquals("symbol" + i % 3, record.getSym(3));
                    assertEquals(record.getSymB(3), record.getSym(3));
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

            try (Path path = new Path().of(configuration.getRoot())) {
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
    public void testWalEvenReaderConcurrentReadWrite() throws Exception {
        AtomicReference<TestUtils.LeakProneCode> evenFileLengthCallBack = new AtomicReference<>();

        FilesFacade ff = new TestFilesFacadeImpl() {

            @Override
            public long length(int fd) {
                long len = super.length(fd);
                if (fd == this.fd && evenFileLengthCallBack.get() != null) {
                    TestUtils.unchecked(() -> {
                        evenFileLengthCallBack.get().run();
                        evenFileLengthCallBack.set(null);
                    });
                }
                return len;
            }

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (fd == this.fd) {
                    if (evenFileLengthCallBack.get() != null) {
                        TestUtils.unchecked(() -> {
                            evenFileLengthCallBack.get().run();
                            evenFileLengthCallBack.set(null);
                        });
                    }

                    // Windows does not allow to map beyond file length
                    // simulate it with asserts for all other platforms
                    Assert.assertTrue(offset + len <= super.length(fd));
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public int openRO(LPSZ path) {
                int fd = super.openRO(path);
                if (Utf8s.endsWithAscii(path, EVENT_FILE_NAME)) {
                    this.fd = fd;
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken;
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
                tableToken = createTable(model);
            }

            WalWriter walWriter = engine.getWalWriter(tableToken);
            TableWriter.Row row = walWriter.newRow(0);
            row.putInt(0, 1);
            row.append();

            walWriter.commit();

            evenFileLengthCallBack.set(() -> {
                // Close wal segments after the moment when _even file length is taken
                // but before it's mapped to memory
                walWriter.close();
                engine.releaseInactive();
            });

            drainWalQueue();

            assertSql("a\tb\tts\n" +
                    "1\t\t1970-01-01T00:00:00.000000Z\n", tableName);
        });
    }

    @Test
    public void testWalSegmentInit() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testWalSegmentInit";
            TableToken tableToken;
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.YEAR)
                    .col("a", ColumnType.BYTE)
                    .timestamp("ts")
                    .wal()
            ) {
                tableToken = createTable(model);
            }

            assertTableExistence(true, tableToken);

            engine.setWalDirectoryPolicy(new WalDirectoryPolicy() {
                @Override
                public void initDirectory(Path dirPath) {
                    final File segmentDirFile = new File(dirPath.toString());
                    final File customInitFile = new File(segmentDirFile, "customInitFile");
                    try {
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
        });
    }

    @Test
    public void testWalWritersUnknownTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            TableToken tableToken;
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts") // not a WAL table
            ) {
                tableToken = createTable(model);
            }
            try (WalWriter ignored = engine.getWalWriter(tableToken)) {
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "table is dropped");
                TestUtils.assertContains(e.getFlyweightMessage(), tableName);
            }
        });
    }

    private static Path constructPath(Path path, TableToken tableName, CharSequence walName, long segment, CharSequence fileName) {
        return segment < 0
                ? path.concat(tableName).slash().concat(walName).slash().concat(fileName).$()
                : path.concat(tableName).slash().concat(walName).slash().put(segment).slash().concat(fileName).$();
    }

    @SuppressWarnings("resource")
    private static TableModel defaultModel(String tableName) {
        return new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.STRING)
                .timestamp("ts")
                .wal();
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
        assertEquals(0, symbolMapDiff.getSize());
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
            if (!Files.exists(path)) {
                throw new AssertionError("Path " + path + " does not exists!");
            }
        } finally {
            path.trimTo(pathLen);
        }
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

                try (TableModel model = defaultModel(tableName)) {
                    assertColumnMetadata(model, reader);
                }

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
            assertEquals("Binary sequences not equals at offset " + i
                            + ". Expected byte: " + expectedByte + ", actual byte: " + actualByte + ".",
                    expectedByte, actualByte
            );
        }
    }

    static TableToken createTable(String tableName) {
        try (TableModel model = defaultModel(tableName)) {
            return createTable(model);
        }
    }

    static TableToken createTable() {
        try (TableModel model = defaultModel("testTable")) {
            return createTable(model);
        }
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
