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
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.junit.Test;

import static org.junit.Assert.*;

public class WalWriterTest extends AbstractGriffinTest {

    @Test
    public void testSymbolWal() {
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
                WalWriter.Row row = walWriter.newRow();
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
            } catch(UnsupportedOperationException e) {
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

        try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, walSymbolCounts, 10L, -1)) {
            assertEquals(4, reader.getColumnCount());
            assertEquals(10, reader.size());
            RecordCursor cursor = reader.getCursor();
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

            assertNull(reader.getSymbolMapDiff(0));
            final SymbolMapDiff symbolMapDiff1 = reader.getSymbolMapDiff(1);
            assertEquals(5, symbolMapDiff1.size());
            for (int k = 0; k < symbolMapDiff1.size(); k++) {
                final int expectedKey = k + 5;
                assertEquals(expectedKey, symbolMapDiff1.getKey(k));
                assertEquals("sym" + expectedKey, symbolMapDiff1.getSymbol(k));
            }
            assertNull(reader.getSymbolMapDiff(2));
            final SymbolMapDiff symbolMapDiff3 = reader.getSymbolMapDiff(3);
            assertEquals(1, symbolMapDiff3.size());
            for (int k = 0; k < symbolMapDiff3.size(); k++) {
                final int expectedKey = k + 2;
                assertEquals(expectedKey, symbolMapDiff3.getKey(k));
                assertEquals("symbol" + expectedKey, symbolMapDiff3.getSymbol(k));
            }
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertWalFileExist(path, tableName, walName, 0, "_meta");
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
    }

    @Test
    public void testRollToNextSegment() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            assertEquals(0, walWriter.size());
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
            assertEquals(1, walWriter.size());
            row = walWriter.newRow();
            row.putByte(0, (byte) 11);
            row.append();
            assertEquals(2, walWriter.size());
            walWriter.rollToNextSegment();
            assertEquals(0, walWriter.size());
            row = walWriter.newRow();
            row.putByte(0, (byte) 112);
            row.append();
            assertEquals(1, walWriter.size());
        }

        try (TableModel model = defaultModel(tableName)) {
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 2, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(2, reader.size());

                final RecordCursor cursor = reader.getCursor();
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

                assertColumnData(model, reader);
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 1, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(112, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);
            }
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertWalFileExist(path, tableName, walName, 0, "_meta");
            assertWalFileExist(path, tableName, walName, 0, "a.d");
            assertWalFileExist(path, tableName, walName, 0, "b.d");
            assertWalFileExist(path, tableName, walName, 0, "b.i");
            assertWalFileExist(path, tableName, walName, 1, "_meta");
            assertWalFileExist(path, tableName, walName, 1, "a.d");
            assertWalFileExist(path, tableName, walName, 1, "b.d");
            assertWalFileExist(path, tableName, walName, 1, "b.i");
        }
    }

    @Test
    public void testCancelRowStartsNewSegment() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            assertEquals(0, walWriter.size());
            WalWriter.Row row = walWriter.newRow();
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
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 2, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(2, reader.size());

                final RecordCursor cursor = reader.getCursor();
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

                assertColumnData(model, reader);
            }
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 1, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(112, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);
            }
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertWalFileExist(path, tableName, walName, 0, "_meta");
            assertWalFileExist(path, tableName, walName, 0, "a.d");
            assertWalFileExist(path, tableName, walName, 0, "b.d");
            assertWalFileExist(path, tableName, walName, 0, "b.i");
            assertWalFileExist(path, tableName, walName, 1, "_meta");
            assertWalFileExist(path, tableName, walName, 1, "a.d");
            assertWalFileExist(path, tableName, walName, 1, "b.d");
            assertWalFileExist(path, tableName, walName, 1, "b.i");
        }
    }

    @Test
    public void testAddingColumnStartsNewSegment() {
        final String tableName = "testTableAddCol";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
            walWriter.addColumn("c", ColumnType.INT);
            row = walWriter.newRow();
            row.putByte(0, (byte) 10);
            row.append();
            walWriter.addColumn("d", ColumnType.SHORT);
        }

        try (TableModel model = defaultModel(tableName)) {
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 1, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);
            }
            model.col("c", ColumnType.INT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 1, -1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(10, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(Integer.MIN_VALUE, record.getInt(2));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);
            }
            model.col("d", ColumnType.SHORT);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 2, new IntList(), 0, -1)) {
                assertEquals(4, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(0, reader.size());

                final RecordCursor cursor = reader.getCursor();
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);
            }
        }
    }

    @Test
    public void testRemovingColumnStartsNewSegment() {
        final String tableName = "testTableRemoveCol";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
            walWriter.removeColumn("a");
        }

        try (TableModel model = defaultModel(tableName)) {
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 1, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(1, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);
            }
        }
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                .col("b", ColumnType.STRING)) {
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 0, -1)) {
                assertEquals(1, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(0, reader.size());

                final RecordCursor cursor = reader.getCursor();
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);
            }
        }
    }

    @Test
    public void testAddingSymbolColumn() {
        final String tableName = "testTableAddSymbolCol";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 125);
            row.append();
            walWriter.addColumn("c", ColumnType.SYMBOL);
        }

        try (TableModel model = defaultModel(tableName)) {
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 1, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(125, record.getByte(0));
                assertNull(record.getStr(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);

                assertNull(reader.getSymbolMapReader(0));
                assertNull(reader.getSymbolMapDiff(0));
                assertNull(reader.getSymbolMapReader(1));
                assertNull(reader.getSymbolMapDiff(1));
            }
            model.col("c", ColumnType.SYMBOL);
            final IntList walSymbolCounts = new IntList();
            walSymbolCounts.add(0);
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, walSymbolCounts, 0, -1)) {
                assertEquals(3, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(0, reader.size());

                final RecordCursor cursor = reader.getCursor();
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);

                assertNull(reader.getSymbolMapReader(0));
                assertNull(reader.getSymbolMapDiff(0));
                assertNull(reader.getSymbolMapReader(1));
                assertNull(reader.getSymbolMapDiff(1));
                assertEquals(0, reader.getSymbolMapReader(2).getSymbolCount());
                assertNull(reader.getSymbolMapDiff(2));
            }
        }
    }

    @Test
    public void testRemovingSymbolColumn() {
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
            WalWriter.Row row = walWriter.newRow();
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
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, walSymbolCounts, 1, -1)) {
                assertEquals(2, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(12, record.getInt(0));
                assertEquals("sym", record.getSym(1));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);

                assertNull(reader.getSymbolMapReader(0));
                assertEquals(1, reader.getSymbolMapReader(1).getSymbolCount());
                assertNull(reader.getSymbolMapDiff(0));
                final SymbolMapDiff symbolMapDiff1 = reader.getSymbolMapDiff(1);
                assertEquals(1, symbolMapDiff1.size());
                assertEquals(0, symbolMapDiff1.getKey(0));
                assertEquals("sym", symbolMapDiff1.getSymbol(0));
            }
        }
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                .col("a", ColumnType.INT)) {
            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 1, new IntList(), 1, -1)) {
                assertEquals(1, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(1, reader.size());

                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTrue(cursor.hasNext());
                assertEquals(133, record.getInt(0));
                assertEquals(0, record.getRowId());
                assertFalse(cursor.hasNext());

                assertColumnData(model, reader);

                assertNull(reader.getSymbolMapReader(0));
                assertNull(reader.getSymbolMapDiff(0));
            }
        }
    }

    @Test
    public void testDesignatedTimestampIncludesSegmentRowNumber() {
        final String tableName = "testTable";
        createTable(tableName, true);

        final String walName;
        final long ts = Os.currentTimeMicros();
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow(ts);
            row.putByte(0, (byte) 1);
            row.append();
            row = walWriter.newRow(ts + 500);
            row.putByte(0, (byte) 17);
            row.append();
            row = walWriter.newRow(ts + 1200);
            row.append();
        }

        try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), 3, 2)) {
            assertEquals(3, reader.getColumnCount());
            assertEquals(walName, reader.getWalName());
            assertEquals(tableName, reader.getTableName());
            assertEquals(3, reader.size());

            final RecordCursor cursor = reader.getCursor();
            final Record record = cursor.getRecord();
            assertTrue(cursor.hasNext());
            assertEquals(1, record.getByte(0));
            assertNull(record.getStr(1));
            assertEquals(ts, record.getTimestamp(2));
            assertEquals(0, record.getRowId());
            assertEquals(0, ((WalReaderRecord) record).getDesignatedTimestampRowId(2));
            assertTrue(cursor.hasNext());
            assertEquals(17, record.getByte(0));
            assertNull(record.getStr(1));
            assertEquals(ts + 500, record.getTimestamp(2));
            assertEquals(1, record.getRowId());
            assertEquals(1, ((WalReaderRecord) record).getDesignatedTimestampRowId(2));
            assertTrue(cursor.hasNext());
            assertEquals(0, record.getByte(0));
            assertNull(record.getStr(1));
            assertEquals(ts + 1200, record.getTimestamp(2));
            assertEquals(2, record.getRowId());
            assertEquals(2, ((WalReaderRecord) record).getDesignatedTimestampRowId(2));
            assertFalse(cursor.hasNext());

            try (TableModel model = defaultModel(tableName, true)) {
                assertColumnData(model, reader);
            }
        }
    }

    @Test
    public void testReadAndWriteAllTypes() {
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
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                assertEquals(tableName, walWriter.getTableName());
                walName = walWriter.getWalName();
                for (int i = 0; i < rowsToInsertTotal; i++) {
                    stringSink.clear();
                    WalWriter.Row row = walWriter.newRow(ts);
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

                    row.append();
                }
                assertEquals(rowsToInsertTotal, walWriter.size());
                assertEquals("WalWriter{name=" + tableName + "}", walWriter.toString());
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), rowsToInsertTotal, 21)) {
                assertEquals(22, reader.getColumnCount());
                assertEquals(walName, reader.getWalName());
                assertEquals(tableName, reader.getTableName());
                assertEquals(rowsToInsertTotal, reader.size());

                final RecordCursor cursor = reader.getCursor();
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

                    assertEquals(ts, record.getTimestamp(21));
                    assertEquals(i, record.getRowId());
                    testSink.clear();
                    ((Sinkable) record).toSink(testSink);
                    assertEquals("WalReaderRecord [recordIndex=" + i + "]", testSink.toString());
                    try {
                        cursor.getRecordB();
                        fail("UnsupportedOperationException expected");
                    } catch(UnsupportedOperationException e) {
                        // ignore, this is expected
                    }
                    try {
                        record.getUpdateRowId();
                        fail("UnsupportedOperationException expected");
                    } catch(UnsupportedOperationException e) {
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

    private void assertColumnData(TableModel expected, WalReader reader) {
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