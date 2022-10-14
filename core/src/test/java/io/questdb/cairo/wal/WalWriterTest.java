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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class WalWriterTest extends AbstractGriffinTest {

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
    public void tesWalWritersOpenPoolClose() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
                createTable(model);
            }

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                Assert.assertTrue(walWriter.isOpen());
            }

            engine.getTableRegistry().close();

            try (WalWriter ignored = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                Assert.fail();
            } catch (PoolClosedException ignored) {
            }
            engine.getTableRegistry().reopen();
        });
    }

    @Test
    public void tesWalWritersReturnToClosedPool() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
                createTable(model);
            }

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                Assert.assertTrue(walWriter.isOpen());
                engine.getTableRegistry().close();
            }

            engine.getTableRegistry().reopen();
        });
    }

    @Test
    public void tesWalWritersReturnToClosedPool2() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
            ) {
                createTable(model);
            }

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                Assert.assertTrue(walWriter.isOpen());
            } // return it to pool

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                Assert.assertTrue(walWriter.isOpen());
                engine.getTableRegistry().close();
            } // close it as the pool is closed too

            engine.getTableRegistry().reopen();
        });
    }

    @Test
    public void tesWalWritersUnknownTable() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "NotExist";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts") // not a WAL table
            ) {
                createTable(model);
            }
            try (WalWriter ignored = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                Assert.fail();
            } catch (CairoError e) {
                MatcherAssert.assertThat(e.getMessage(), containsString("Unknown table [name=`" +
                        engine.getSystemTableName(tableName) + "`]"));
            }
        });
    }

    @Test
    public void testAddingColumnOverlapping() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    walName1 = walWriter1.getWalName();
                    walName2 = walWriter2.getWalName();
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                }
            }

            try (TableModel model = defaultModel(tableName)) {
                model.col("c", ColumnType.INT);
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 0, 0)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName1, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(0, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    assertFalse(cursor.hasNext());

                    assertColumnMetadata(model, reader);

                    final WalEventCursor eventCursor = reader.getEventCursor();
                    assertFalse(eventCursor.hasNext());
                }

                model.col("d", ColumnType.INT);
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 0, 0)) {
                    assertEquals(4, reader.getColumnCount());
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
    public void testAddingColumnOverlappingAndAddRow() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 0, 1)) {
                    assertEquals(4, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 0, 0)) {
                    assertEquals(4, reader.getColumnCount());
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
    public void testOverlappingStructureChangeMissing() throws Exception {
        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long readULong(long fd, long offset) {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[1].getClassName().endsWith("TxnCatalog$SequencerStructureChangeCursorImpl") && stackTrace[1].getMethodName().equals("of")) {
                        return -1;
                    }
                }
                return Files.readULong(fd, offset);
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                    fail("Exception expected");
                } catch(Exception e) {
                    // this exception will be handled in ILP/PG/HTTP
                    assertEquals("[0] expected to read table structure changes but there is no saved in the sequencer [fromStructureVersion=0]", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testOverlappingStructureChangeCannotCreateFile() throws Exception {
        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, long opts) {
                if (Chars.endsWith(name, "0" + Files.SEPARATOR + "c.d")) {
                    return -1;
                }
                return Files.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                    fail("Exception expected");
                } catch(Exception e) {
                    // this exception will be handled in ILP/PG/HTTP
                    assertTrue(e.getMessage().startsWith("[0] could not apply table definition changes to the current transaction. could not add column [error=could not open read-write"));
                }
            }
        });
    }

    @Test
    public void testOverlappingStructureChangeFails() throws Exception {
        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[1].getClassName().endsWith("TxnCatalog") && stackTrace[1].getMethodName().equals("openFileRO")) {
                        return -1;
                    }
                }
                return Files.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    addColumn(walWriter1, "c", ColumnType.INT);
                    addColumn(walWriter2, "d", ColumnType.INT);
                    fail("Exception expected");
                } catch(Exception e) {
                    // this exception will be handled in ILP/PG/HTTP
                    assertTrue(e.getMessage().contains("could not open read-only"));
                }
            }
        });
    }

    @Test
    public void testAddColumnRollsUncommittedRowsToNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
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

                    addColumn(walWriter1, "c", ColumnType.INT);

                    walWriter2.commit();

                    row = walWriter1.newRow();
                    row.putByte(0, (byte) 110);
                    row.append();
                    walWriter1.commit();
                }
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 0, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 0, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
                model.col("c", ColumnType.INT);
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 1, 1)) {
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 1, 1)) {
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
    public void testAlterTableRejectedIfTransactionPending() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                // no commit intentional
                addColumn(walWriter, "c", ColumnType.INT);
                fail("Exception expected");
            } catch(Exception e) {
                // this exception will be handled in ILP/PG/HTTP
                MatcherAssert.assertThat(e.getMessage(), CoreMatchers.endsWith("cannot alter table with uncommitted inserts [table=testAlterTableRejectedIfTransactionPending]"));
            }
        });
    }

    @Test
    public void testAddingColumnClosesSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();

                addColumn(walWriter, "c", ColumnType.INT);
                row = walWriter.newRow();
                row.putByte(0, (byte) 10);
                row.append();
                walWriter.commit();

                addColumn(walWriter, "d", ColumnType.SHORT);
                row = walWriter.newRow();
                row.putByte(0, (byte) 100);
                row.putShort(3, (short) 1000);
                row.append();
                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 1)) {
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, 1)) {
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 2, 1)) {
                    assertEquals(4, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(100, record.getByte(0));
                    assertEquals(1000, record.getShort(3));
                    assertNull(record.getStr(1));
                    assertEquals(Integer.MIN_VALUE, record.getInt(2));
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
    public void testAddingDuplicateColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();

                addColumn(walWriter, "c", ColumnType.INT);
                row = walWriter.newRow();
                row.putByte(0, (byte) 10);
                row.append();
                walWriter.commit();

                try {
                    addColumn(walWriter, "c", ColumnType.SHORT);
                    fail("Should not be able to add duplicate column");
                } catch (CairoException e) {
                    assertEquals("[-1] could not add column [error=duplicate column name: c, errno=0]", e.getMessage());
                }

                row = walWriter.newRow();
                row.putByte(0, (byte) 100);
                row.append();
                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 1)) {
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, 2)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(2, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(10, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(Integer.MIN_VALUE, record.getInt(2));
                    assertEquals(0, record.getRowId());
                    assertTrue(cursor.hasNext());
                    assertEquals(100, record.getByte(0));
                    assertNull(record.getStr(1));
                    assertEquals(Integer.MIN_VALUE, record.getInt(2));
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
                    engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 2, 1);
                    fail("Segment 2 should not exist");
                } catch (CairoException e) {
                    CharSequence systemTableName = engine.getSystemTableName(tableName);
                    assertTrue(e.getMessage().endsWith("could not open read-only [file=" + engine.getConfiguration().getRoot() +
                            File.separatorChar + systemTableName +
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
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 125);
                row.append();
                walWriter.commit();
                addColumn(walWriter, "c", ColumnType.SYMBOL);
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 1)) {
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
    public void testCancelRowDoesNotStartsNewSegment() throws Exception {
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
                row.cancel();

                assertEquals(2, walWriter.size());
                row = walWriter.newRow();
                row.putByte(0, (byte) 112);
                row.append();
                assertEquals(3, walWriter.size());
                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 3)) {
                    assertEquals(2, reader.getColumnCount());
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
                assertWalFileExist(path, tableName, walName, 0, "_meta");
                assertWalFileExist(path, tableName, walName, 0, "_event");
                assertWalFileExist(path, tableName, walName, 0, "a.d");
                assertWalFileExist(path, tableName, walName, 0, "b.d");
                assertWalFileExist(path, tableName, walName, 0, "b.i");
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
                walWriter.commit();
                walWriter.rollSegment();
                for (int i = 0; i < 7; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }

                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 44)) {
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, 7)) {
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
    public void testConcurrentInsert() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTable";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .timestamp("ts")
                    .wal()
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

            // map<walId, numOfThreadsUsedThisWalWriter>
            final ConcurrentMap<Integer, AtomicInteger> counters = new ConcurrentHashMap<>(numOfThreads);
            for (int i = 0; i < numOfThreads; i++) {
                new Thread(() -> {
                    TableWriter.Row row;
                    try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                        final Integer walId = walWriter.getWalId();
                        final AtomicInteger counter = counters.computeIfAbsent(walId, name -> new AtomicInteger());
                        counter.incrementAndGet();
                        walWriter.setRollStrategy(rollStrategy);
                        assertEquals(0, walWriter.size());
                        for (int n = 0; n < numOfRows; n++) {
                            row = walWriter.newRow();
                            row.putInt(0, n);
                            row.putSym(1, "test" + n);
                            row.append();
                            walWriter.rollSegmentIfLimitReached();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assert.fail("Write failed [e=" + e + "]");
                        throw new RuntimeException(e);
                    } finally {
                        Path.clearThreadLocals();
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
                    .wal()
            ) {
                for (Map.Entry<Integer, AtomicInteger> counterEntry: counters.entrySet()) {
                    final int walId = counterEntry.getKey();
                    final int count = counterEntry.getValue().get();
                    final String walName = WAL_NAME_BASE + walId;

                    for (int segmentId = 0; segmentId < count * numOfSegments; segmentId++) {
                        try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, segmentId, maxRowCount)) {
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
                            assertWalFileExist(path, tableName, walName, segmentId, "_meta");
                            assertWalFileExist(path, tableName, walName, segmentId, "_event");
                            assertWalFileExist(path, tableName, walName, segmentId, "a.d");
                            assertWalFileExist(path, tableName, walName, segmentId, "b.d");
                            assertWalFileExist(path, tableName, walName, segmentId, "ts.d");
                        }
                    }
                }
            }

            assertEquals(numOfTxn, txnSet.size());
            for (Map.Entry<Integer, AtomicInteger> counterEntry: counters.entrySet()) {
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
    public void testConcurrentAddRemoveColumn_DifferentColNamePerThread() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final int numOfRows = 11;
            final int numOfThreads = 10;
            final int numOfTxn = numOfThreads;
            final CountDownLatch alterFinished = new CountDownLatch(numOfThreads);
            final SOCountDownLatch writeFinished = new SOCountDownLatch(numOfThreads);
            final AtomicInteger columnNumber = new AtomicInteger();

            for (int i = 0; i < numOfThreads; i++) {
                new Thread(() -> {
                    final String colName = "col" + columnNumber.incrementAndGet();
                    TableWriter.Row row;
                    boolean countedDown = false;
                    try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                        addColumn(walWriter, colName, ColumnType.LONG);
                        removeColumn(walWriter, colName);
                        addColumn(walWriter, colName, ColumnType.STRING);
                        removeColumn(walWriter, colName);
                        addColumn(walWriter, colName, ColumnType.BYTE);
                        removeColumn(walWriter, colName);

                        alterFinished.countDown();
                        countedDown = true;

                        alterFinished.await();
                        assertEquals(0, walWriter.size());
                        for (int n = 0; n < numOfRows; n++) {
                            row = walWriter.newRow();
                            row.putByte(0, (byte) 1);
                            row.putStr(1, "test" + n);
                            row.append();
                        }
                        walWriter.commit();
                        walWriter.rollSegment();
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assert.fail("Alter failed [e=" + e + "]");
                        throw new RuntimeException(e);
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

            final LongHashSet txnSet = new LongHashSet(numOfTxn);
            final IntList symbolCounts = new IntList();
            symbolCounts.add(numOfRows);
            try (TableModel model = defaultModel(tableName)) {
                for (int i = 0; i < numOfThreads; i++) {
                    final String walName = WAL_NAME_BASE + (i + 1);
                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, numOfRows)) {
                        assertEquals(2, reader.getRealColumnCount());
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
                        txnSet.add(Numbers.encodeLowHighInts(i, (int) eventCursor.getTxn()));

                        final WalEventCursor.DataInfo dataInfo = eventCursor.getDataInfo();
                        assertEquals(0, dataInfo.getStartRowID());
                        assertEquals(numOfRows, dataInfo.getEndRowID());
                        assertEquals(0, dataInfo.getMinTimestamp());
                        assertEquals(0, dataInfo.getMaxTimestamp());
                        assertFalse(dataInfo.isOutOfOrder());

                        assertNull(dataInfo.nextSymbolMapDiff());

                        assertFalse(eventCursor.hasNext());
                    }

                    try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, 0)) {
                        assertEquals(2, reader.getRealColumnCount());
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
                        assertWalFileExist(path, tableName, walName, 0, "_meta");
                        assertWalFileExist(path, tableName, walName, 0, "_event");
                        assertWalFileExist(path, tableName, walName, 0, "a.d");
                        assertWalFileExist(path, tableName, walName, 0, "b.d");
                        assertWalFileExist(path, tableName, walName, 1, "_meta");
                        assertWalFileExist(path, tableName, walName, 1, "_event");
                        assertWalFileExist(path, tableName, walName, 1, "a.d");
                        assertWalFileExist(path, tableName, walName, 1, "b.d");
                    }
                }
            }

            assertEquals(numOfTxn, txnSet.size());
            for (int i = 0; i < numOfThreads; i++) {
                txnSet.remove(Numbers.encodeLowHighInts(i, 0));
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
                    .wal()
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

                        row.putTimestamp(21, SqlUtil.implicitCastStrAsTimestamp("2022-06-10T09:13:46." + (i + 1)));

                        row.putStr(22, (char) (65 + i % 26));
                        row.putStr(23, "abcdefghijklmnopqrstuvwxyz", 0, i % 26 + 1);

                        row.putSym(24, String.valueOf(i));
                        row.putSym(25, (char) (65 + i % 26));

                        row.append();
                    }

                    walSymbolCounts.add(walWriter.getSymbolMapReader(24).getSymbolCount());
                    walSymbolCounts.add(walWriter.getSymbolMapReader(25).getSymbolCount());

                    assertEquals(rowsToInsertTotal, walWriter.size());
                    assertEquals("WalWriter{name=" + walName + ", table=" + tableName + "}", walWriter.toString());
                    walWriter.commit();
                }

                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, rowsToInsertTotal)) {
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
    public void testExceptionThrownIfSequencerCannotBeCreated() throws Exception {
        assertMemoryLeak(() -> {
            stackFailureClass = "SequencerImpl";
            stackFailureMethod = "<init>";

            final String tableName = testName.getMethodName();
            try {
                createTable(tableName);
                fail("Exception expected");
            } catch(Exception e) {
                // this exception will be handled in ILP/PG/HTTP
                assertEquals("Test failure", e.getMessage());
            }

            stackFailureClass = null;
            stackFailureMethod = null;
        });
    }

    @Test
    public void testExceptionThrownIfSequencerCannotBeOpened() throws Exception {
        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long getPageSize() {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[4].getClassName().endsWith("SequencerImpl") && stackTrace[4].getMethodName().equals("open")) {
                        throw e;
                    }
                }
                return Files.PAGE_SIZE;
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            try {
                createTable(tableName);
                fail("Exception expected");
            } catch(Exception e) {
                // this exception will be handled in ILP/PG/HTTP
                assertEquals("Test failure", e.getMessage());
            }
        });
    }

    @Test
    public void testExceptionThrownIfSequencerCannotCreateDir() throws Exception {
        final FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                try {
                    throw new RuntimeException("Test failure");
                } catch (Exception e) {
                    final StackTraceElement[] stackTrace = e.getStackTrace();
                    if (stackTrace[1].getClassName().endsWith("SequencerImpl") && stackTrace[1].getMethodName().equals("createSequencerDir")) {
                        return 1;
                    }
                }
                return Files.mkdirs(path, mode);
            }

            @Override
            public int errno() {
                return 999;
            }
        };

        assertMemoryLeak(ff, () -> {
            final String tableName = testName.getMethodName();
            try {
                createTable(tableName);
                fail("Exception expected");
            } catch(Exception e) {
                // this exception will be handled in ILP/PG/HTTP
                assertTrue(e.getMessage().startsWith("[999] Cannot create sequencer directory:"));
            }
        });
    }

    @Test
    public void testRenameColumnRollsUncommittedRowsToNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
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

                    renameColumn(walWriter1, "b", "c");

                    walWriter2.commit();

                    row = walWriter1.newRow();
                    row.putByte(0, (byte) 110);
                    row.append();
                    walWriter1.commit();
                }
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 0, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 0, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
                    .wal()
            ) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 1, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 1, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
    public void testRemoveColumnRollsUncommittedRowsToNewSegment() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = testName.getMethodName();
            createTable(tableName);

            final String walName1, walName2;
            try (WalWriter walWriter1 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                try (WalWriter walWriter2 = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
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

                    removeColumn(walWriter1, "b");

                    walWriter2.commit();

                    row = walWriter1.newRow();
                    row.putByte(0, (byte) 110);
                    row.append();
                    walWriter1.commit();
                }
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 0, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 0, 1)) {
                    assertEquals(2, reader.getColumnCount());
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
                    .wal()
            ) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName1, 1, 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(1, reader.getRealColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName2, 1, 1)) {
                    assertEquals(2, reader.getColumnCount());
                    assertEquals(1, reader.getRealColumnCount());
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
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();
                removeColumn(walWriter, "a");
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 1)) {
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
                    engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, 0);
                    fail("Segment 1 should not exist");
                } catch (CairoException e) {
                    CharSequence systemTableName = engine.getSystemTableName(tableName);
                    assertTrue(e.getMessage().endsWith("could not open read-only [file=" + engine.getConfiguration().getRoot() +
                            File.separatorChar + systemTableName +
                            File.separatorChar + walName +
                            File.separatorChar + "1" +
                            File.separatorChar + TableUtils.META_FILE_NAME + "]"));
                }
            }
        });
    }

    @Test
    public void testRemovingNonExistentColumn() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableRemoveNonExistentCol";
            createTable(tableName);

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.commit();

                try {
                    removeColumn(walWriter, "noColLikeThis");
                    fail("Should not be able to remove non existent column");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getMessage(), "Invalid column: noColLikeThis");
                }
                row = walWriter.newRow();
                row.putByte(0, (byte) 10);
                row.append();

                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 2)) {
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
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .wal()
            ) {
                createTable(model);
            }

            final String walName;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                TableWriter.Row row = walWriter.newRow();
                row.putInt(0, 12);
                row.putSym(1, "symb");
                row.putSym(2, "symc");
                row.append();
                walWriter.commit();

                removeColumn(walWriter, "b");

                row = walWriter.newRow();
                row.putInt(0, 133);
                try {
                    row.putSym(1, "anything");
                    fail("UnsupportedOperationException expected");
                } catch (UnsupportedOperationException e) {
                    // ignore, this is expected
                }
                row.putSym(2, "symc");
                row.append();
                walWriter.commit();
            }

            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
            ) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 1)) {
                    assertEquals(3, reader.getColumnCount());
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
            ) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, 1)) {
                    assertEquals(3, reader.getColumnCount());
                    assertEquals(walName, reader.getWalName());
                    assertEquals(tableName, reader.getTableName());
                    assertEquals(1, reader.size());

                    final RecordCursor cursor = reader.getDataCursor();
                    final Record record = cursor.getRecord();
                    assertTrue(cursor.hasNext());
                    assertEquals(133, record.getInt(0));
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
                    assertEquals(1, symbolMapDiff.getSize());
                    assertEquals(2, symbolMapDiff.getColumnIndex());
                    assertEquals(0, symbolMapDiff.getCleanSymbolCount());

                    assertFalse(eventCursor.hasNext());
                }
            }
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
            rollStrategyUpdates.put(10, strategy -> currentMicros += 2000_000L);
            rollStrategyUpdates.put(22, strategy -> currentMicros += 5000_000L);
            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
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
            rollStrategyUpdates.put(50, strategy -> currentMicros += 20_000_000L);
            testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
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

                walWriter.commit();
                walWriter.rollSegment();
                assertEquals(0, walWriter.size());
                row = walWriter.newRow();
                row.putByte(0, (byte) 112);
                row.append();
                walWriter.commit();
                assertEquals(1, walWriter.size());
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 2)) {
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, 1)) {
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
    public void testSameWalAfterEngineCleared() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "testTableCommit";
            createTable(tableName);

            String wal1Name;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                wal1Name = walWriter.getWalName();
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
                walWriter.commit();
                walWriter.rollSegment();
                for (int i = 0; i < 7; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
            }

            engine.clear();

            String wal2Name;
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                wal2Name = walWriter.getWalName();
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
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
                walWriter.rollSegment();
                for (int i = 0; i < 7; i++) {
                    TableWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.append();
                }
                walWriter.commit();
            }

            try (TableModel model = defaultModel(tableName)) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, wal1Name, 0, 44)) {
                    assertEquals(2, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, wal1Name, 1, 7)) {
                    assertEquals(2, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, wal2Name, 0, 34)) {
                    assertEquals(2, reader.getColumnCount());
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
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, wal2Name, 1, 7)) {
                    assertEquals(2, reader.getColumnCount());
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
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SYMBOL)
                    .col("c", ColumnType.SYMBOL)
                    .col("d", ColumnType.SYMBOL)
                    .wal()
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

                assertNull(walWriter.getSymbolMapReader(0));
                walSymbolCounts.add(walWriter.getSymbolMapReader(1).getSymbolCount());
                walSymbolCounts.add(walWriter.getSymbolMapReader(2).getSymbolCount());
                walSymbolCounts.add(walWriter.getSymbolMapReader(3).getSymbolCount());

                assertNull(walWriter.getSymbolMapReader(1).valueOf(10));

                walWriter.commit();
            }

            try (TableReader reader = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                assertEquals(4, reader.getMetadata().getColumnCount());
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

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 10L)) {
                assertEquals(4, reader.getColumnCount());
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

    static void addColumn(TableWriterFrontend writer, String columnName, int columnType) throws SqlException {
        AlterOperationBuilder addColumnC = new AlterOperationBuilder().ofAddColumn(0, Chars.toString(writer.getTableName()), 0);
        addColumnC.ofAddColumn(columnName, columnType, 0, false, false, 0);
        writer.applyAlter(addColumnC.build(), true);
    }

    static void removeColumn(TableWriterFrontend writer, String columnName) throws SqlException {
        AlterOperationBuilder removeColumnOperation = new AlterOperationBuilder().ofDropColumn(0, Chars.toString(writer.getTableName()), 0);
        removeColumnOperation.ofDropColumn(columnName);
        writer.applyAlter(removeColumnOperation.build(), true);
    }

    static void renameColumn(TableWriterFrontend writer, String columnName, String newColumnName) throws SqlException {
        AlterOperationBuilder renameColumnC = new AlterOperationBuilder().ofRenameColumn(0, Chars.toString(writer.getTableName()), 0);
        renameColumnC.ofRenameColumn(columnName, newColumnName);
        writer.applyAlter(renameColumnC.build(), true);
    }

    static void prepareBinPayload(long pointer, int limit) {
        for (int offset = 0; offset < limit; offset++) {
            Unsafe.getUnsafe().putByte(pointer + offset, (byte) limit);
        }
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
                    expectedByte, actualByte);
        }
    }

    private static Path constructPath(Path path, CharSequence tableName, CharSequence walName, long segment, CharSequence fileName) {
        CharSequence systemTableName = engine.getSystemTableName(tableName);
        return segment < 0
                ? path.concat(systemTableName).slash().concat(walName).slash().concat(fileName).$()
                : path.concat(systemTableName).slash().concat(walName).slash().put(segment).slash().concat(fileName).$();
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

    @SuppressWarnings("SameParameterValue")
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
                .wal()
                .timestamp("ts")
        ) {
            createTable(model);
        }
    }

    static void createTable(String tableName) {
        try (TableModel model = defaultModel(tableName)) {
            createTable(model);
        }
    }

    static void createTable(String tableName, boolean withTimestamp) {
        try (TableModel model = defaultModel(tableName, withTimestamp)) {
            createTable(model);
        }
    }

    static void createTable(TableModel model) {
        engine.createTable(
                AllowAllCairoSecurityContext.INSTANCE,
                model.getMem(),
                model.getPath(),
                model,
                false
        );
    }

    private static TableModel defaultModel(String tableName) {
        return defaultModel(tableName, false);
    }

    @SuppressWarnings("resource")
    private static TableModel defaultModel(String tableName, boolean withTimestamp) {
        return withTimestamp
                ? new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.STRING)
                .timestamp("ts")
                .wal()
                : new TableModel(configuration, tableName, PartitionBy.NONE)
                .col("a", ColumnType.BYTE)
                .col("b", ColumnType.STRING)
                .wal();
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
                walWriter.commit();
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, 3)) {
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

    private void testRollSegment(int rowsToInsertTotal, WalWriterRollStrategy rollStrategy, LongList expectedRowCounts) {
        final IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates = new IntObjHashMap<>();
        testRollSegment(rowsToInsertTotal, rollStrategy, expectedRowCounts, rollStrategyUpdates);
    }

    private void testRollSegment(int rowsToInsertTotal, WalWriterRollStrategy rollStrategy, LongList expectedRowCounts,
                                 IntObjHashMap<Consumer<WalWriterRollStrategy>> rollStrategyUpdates) {
        final String tableName = testName.getMethodName();
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

                    currentMicros += 1000_000L;
                    final Consumer<WalWriterRollStrategy> rollStrategyUpdate = rollStrategyUpdates.get(i);
                    if (rollStrategyUpdate != null) {
                        rollStrategyUpdate.accept(rollStrategy);
                    }

                    final long rolledRowCount = walWriter.rollSegmentIfLimitReached();
                    if (rolledRowCount != 0 || i == rowsToInsertTotal - 1) {
                        rowCounts.add(rolledRowCount == 0 ? walWriter.size() : rolledRowCount);
                        final IntList symbolCounts = new IntList();
                        walSymbolCounts.add(symbolCounts);
                        symbolCounts.add(walWriter.getSymbolMapReader(17).getSymbolCount());
                    }
                }

                assertEquals("WalWriter{name=" + walName + ", table=" + tableName + "}", walWriter.toString());
                walWriter.commit();
            }

            assertEquals(expectedRowCounts, rowCounts);

            int i = 0;
            for (int segmentId = 0; segmentId < rowCounts.size(); segmentId++) {
                try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName,
                        segmentId, rowCounts.get(segmentId))) {
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
}