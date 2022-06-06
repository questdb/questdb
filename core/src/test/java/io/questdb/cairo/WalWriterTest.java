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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class WalWriterTest extends AbstractGriffinTest {

    @Test
    public void testBootstrapWal() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            for (int i = 0; i < 100; i++) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) i);
                row.putStr(1, String.valueOf(i));
                row.append();
            }
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertWalFileExist(tableName, walName, "a", 0, path);
            assertWalFileExist(tableName, walName, "b", 0, path);
        }
    }

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
            walWriter.commit();
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
            assertNull(reader.getSymbolMapReader(1).valueOf(5));
            assertNull(reader.getSymbolMapReader(2).valueOf(2));
            assertNull(reader.getSymbolMapReader(3).valueOf(2));
        }

        try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, walSymbolCounts, 10L)) {
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
            assertWalFileExist(tableName, walName, "a", 0, path);
            assertWalFileExist(tableName, walName, "b", 0, path);
            assertWalSymbolFileExist(tableName, walName, "b", ".c", path);
            assertWalSymbolFileExist(tableName, walName, "b", ".k", path);
            assertWalSymbolFileExist(tableName, walName, "b", ".o", path);
            assertWalSymbolFileExist(tableName, walName, "b", ".v", path);
        }
    }

    @Test
    public void testRowCount_simple() {
        final String tableName = "testTable";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
                     .col("c", ColumnType.INT)
        ) {
            createTable(model);
        }

        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            for (int i = 0; i < 100; i++) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) i);
                row.putStr(1, String.valueOf(i));
                row.putInt(2, 42);
                row.append();
            }
            assertEquals(100, walWriter.getCurrentWalDSegmentRowCount());
            walWriter.newRow().cancel(); // force a new WAL-D segment
            assertEquals(0, walWriter.getCurrentWalDSegmentRowCount());
            for (int i = 0; i < 50; i++) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) i);
                row.putStr(1, String.valueOf(i));
                row.putInt(2, 42);
                row.append();
            }
            assertEquals(50, walWriter.getCurrentWalDSegmentRowCount());
        }
    }

    @Test
    public void cancelRowStartsANewSegment_for_now() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.cancel();

            row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertWalFileExist(tableName, walName, "a", 0, path);
            assertWalFileExist(tableName, walName, "a", 1, path);
        }
    }

    @Test
    public void testDdlMetadataReadable() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
            walWriter.newRow().cancel(); // new segment
            row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
        }

        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = defaultModel(tableName, false)) {
            assertValidMetadataFileCreated(model, walName, 0, path);
            assertValidMetadataFileCreated(model, walName, 1, path);
        }
    }

    @Test
    public void testDdlMetadataCreated() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertMetadataFileExist(tableName, walName, 0, path);
        }
    }

    @Test
    public void testDdlMetadataForNewSegmentReadable() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertMetadataFileExist(tableName, walName, 0, path);
        }
    }

    @Test
    public void testDdlMetadataCreatedForNewSegment() {
        final String tableName = "testTable";
        createTable(tableName);

        final String walName;
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
            walWriter.newRow().cancel(); // force new segment
            row = walWriter.newRow();
            row.putByte(0, (byte) 1);
            row.append();
        }

        try (Path path = new Path().of(configuration.getRoot())) {
            assertMetadataFileExist(tableName, walName, 0, path);
            assertMetadataFileExist(tableName, walName, 1, path);
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
        }

        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = defaultModel(tableName, false)
        ) {
            assertValidMetadataFileCreated(model, walName, 0, path);
            assertValidMetadataFileCreated(model.col("c", ColumnType.INT), walName, 1, path);
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
            walWriter.removeColumn("b");
        }

        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = defaultModel(tableName, false)
        ) {
            assertValidMetadataFileCreated(model, walName, 0, path);
        }
        try (Path path = new Path().of(configuration.getRoot());
             TableModel updatedModel = new TableModel(configuration, tableName, PartitionBy.NONE)
                .col("a", ColumnType.BYTE)) {
            assertValidMetadataFileCreated(updatedModel, walName, 1, path);
        }
    }

    @Test
    public void testDesignatedTimestampsIncludesSegmentRowNumber() {
        final String tableName = "testTable";
        createTable(tableName, true);

        final String walName;
        final long ts = Os.currentTimeMicros();
        try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
            walName = walWriter.getWalName();
            WalWriter.Row row = walWriter.newRow(ts);
            row.putByte(0, (byte) 1);
            row.append();
        }

        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = defaultModel(tableName, true)
        ) {
            assertValidMetadataFileCreated(model, walName, 0, path);
        }
        try (Path path = new Path().of(configuration.getRoot());
             WalSegmentDataReader segmentReader = new WalSegmentDataReader(configuration.getFilesFacade(), toWalPath(tableName, walName, 0, path))) {
            int tsIndex = segmentReader.getColumnIndex("ts");
            assertDesignatedTimestamp(segmentReader, tsIndex, 0, ts);
        }
    }

    @Test
    public void testReadAndWriteAllTypes() {
        // todo: geohash
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
                walWriter.commit();
                assertEquals(rowsToInsertTotal, walWriter.size());
                assertEquals("WalWriter{name=" + tableName + "}", walWriter.toString());
            }

            try (WalReader reader = engine.getWalReader(sqlExecutionContext.getCairoSecurityContext(), tableName, walName, 0, new IntList(), rowsToInsertTotal)) {
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

    private static void assertDesignatedTimestamp(WalSegmentDataReader segmentReader, int tsIndex, int expectedRowNumber, long expectedTimestamp) {
        assertEquals(expectedTimestamp, segmentReader.nextLong(tsIndex));
        assertEquals(expectedRowNumber, segmentReader.nextLong(tsIndex));
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

    private static void assertCharsEquals(CharSequence expected, CharSequence actual) {
        if (!Chars.equals(expected, actual)) {
            throw new AssertionError("CharSequence not equal. Expected: '" + expected + "', actual: '" + " actual '");
        }
    }

    private void assertValidMetadataFileCreated(TableModel model, String walName, long segment, Path path) {
        final int pathLen = path.length();
        try {
            toMetadataPath(model.getTableName(), walName, segment, path);
            WalMetadataReader walMetadataReader = new WalMetadataReader(configuration.getFilesFacade(), path);
            assertMetadataMatchesModel(model, walMetadataReader);
        } finally {
            path.trimTo(pathLen);
        }
    }

    private void assertWalFileExist(String tableName, String walName, String columnName, int segment, Path path) {
        final int pathLen = path.length();
        try {
            path.concat(tableName).slash().concat(walName)
                    .slash().put(segment)
                    .slash().concat(columnName + TableUtils.FILE_SUFFIX_D).$();
            assertPathExists(path);
        } finally {
            path.trimTo(pathLen);
        }
    }

    private void assertWalSymbolFileExist(String tableName, String walName, String columnName, String extension, Path path) {
        final int pathLen = path.length();
        try {
            path.concat(tableName).slash().concat(walName).slash().concat(columnName + extension).$();
            assertPathExists(path);
        } finally {
            path.trimTo(pathLen);
        }
    }

    private void assertMetadataFileExist(String tableName, CharSequence walName, int segment, Path path) {
        final int pathLen = path.length();
        try {
            assertPathExists(toMetadataPath(tableName, walName, segment, path));
        } finally {
            path.trimTo(pathLen);
        }
    }

    private static Path toMetadataPath(CharSequence tableName, CharSequence walName, long segment, Path path) {
        return toWalPath(tableName, walName, segment, path)
                .concat("_meta").$();
    }

    private static Path toWalPath(CharSequence tableName, CharSequence walName, long segment, Path path) {
        return path.concat(tableName).slash()
                .concat(walName).slash()
                .put(segment).slash();
    }

    private static void assertPathExists(Path path) {
        if (!Files.exists(path)) {
            throw new AssertionError("Path " + path + " does not exists!");
        }
    }

    private static Long256Acceptor assertingAcceptor(long expectedL0, long expectedL1, long expectedL2, long expectedL3) {
        return (l0, l1, l2, l3) -> {
            assertEquals(expectedL0, l0);
            assertEquals(expectedL1, l1);
            assertEquals(expectedL2, l2);
            assertEquals(expectedL3, l3);
        };
    }

    private static void assertMetadataMatchesModel(TableModel model, WalMetadataReader metadata) {
        int columnCount = model.getColumnCount();
        assertEquals(columnCount, metadata.getColumnCount());
        for (int i = 0; i < columnCount; i++) {
            assertEquals(model.getColumnType(i), metadata.getColumnType(i));
            assertEquals(model.getColumnName(i), metadata.getColumnName(i));
        }
    }

    public static class WalSegmentDataReader implements Closeable {
        private final FilesFacade ff;
        private final List<MemoryMR> primaryColumns;
        private final LongList offsets;
        private final ObjIntHashMap<String> name2columns;

        public WalSegmentDataReader(FilesFacade ff, Path path) {
            this.ff = ff;
            this.primaryColumns = new ArrayList<>();
            this.offsets = new LongList();
            this.name2columns = new ObjIntHashMap<>();
            of(path);
        }

        @Override
        public void close() {
            for (MemoryMR mem : primaryColumns) {
                mem.close();
            }
            primaryColumns.clear();
            offsets.clear();
            name2columns.clear();
        }

        public WalSegmentDataReader of(Path path) {
            close();
            int plen = path.length();
            try (WalMetadataReader walMetadataReader = new WalMetadataReader(ff, path.concat("_meta").$())) {
                int columnCount = walMetadataReader.getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    String name = walMetadataReader.getColumnName(i);
                    MemoryMR primaryMem = Vm.getMRInstance();
                    primaryMem.wholeFile(ff, TableUtils.dFile(path.trimTo(plen), name), MemoryTag.MMAP_DEFAULT);
                    primaryColumns.add(primaryMem);
                    name2columns.put(name, i);
                }
                offsets.setAll(columnCount, 0);
                return this;
            } finally {
                path.trimTo(plen);
            }
        }

        public int getColumnCount() {
            return name2columns.size();
        }

        public int getColumnIndex(String columnName) {
            return name2columns.get(columnName);
        }

        public int nextInt(int columnIndex) {
            long offset = offsets.get(columnIndex);
            int result = primaryColumns.get(columnIndex).getInt(offset);
            offset += Integer.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public byte nextByte(int columnIndex) {
            long offset = offsets.get(columnIndex);
            byte result = primaryColumns.get(columnIndex).getByte(offset);
            offset += Byte.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public CharSequence nextString(int columnIndex) {
            long offset = offsets.get(columnIndex);
            CharSequence result = primaryColumns.get(columnIndex).getStr(offset);
            offset += Vm.getStorageLength(result.length());
            offsets.set(columnIndex, offset);
            return result;
        }

        public long nextLong(int columnIndex) {
            long offset = offsets.get(columnIndex);
            long result = primaryColumns.get(columnIndex).getLong(offset);
            offset += Long.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public double nextDouble(int columnIndex) {
            long offset = offsets.get(columnIndex);
            double result = primaryColumns.get(columnIndex).getDouble(offset);
            offset += Double.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public float nextFloat(int columnIndex) {
            long offset = offsets.get(columnIndex);
            float result = primaryColumns.get(columnIndex).getFloat(offset);
            offset += Float.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public short nextShort(int columnIndex) {
            long offset = offsets.get(columnIndex);
            short result = primaryColumns.get(columnIndex).getShort(offset);
            offset += Short.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public long nextTimestamp(int columnIndex) {
            long offset = offsets.get(columnIndex);
            long result = primaryColumns.get(columnIndex).getLong(offset);
            offset += Long.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public char nextChar(int columnIndex) {
            long offset = offsets.get(columnIndex);
            char result = primaryColumns.get(columnIndex).getChar(offset);
            offset += Character.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public byte nextGeoByte(int columnIndex) {
            long offset = offsets.get(columnIndex);
            byte result = primaryColumns.get(columnIndex).getByte(offset);
            offset += Byte.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public int nextGeoInt(int columnIndex) {
            long offset = offsets.get(columnIndex);
            int result = primaryColumns.get(columnIndex).getInt(offset);
            offset += Integer.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public short nextGeoShort(int columnIndex) {
            long offset = offsets.get(columnIndex);
            short result = primaryColumns.get(columnIndex).getShort(offset);
            offset += Short.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public long nextGeoLong(int columnIndex) {
            long offset = offsets.get(columnIndex);
            long result = primaryColumns.get(columnIndex).getLong(offset);
            offset += Long.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public BinarySequence nextBin(int columnIndex) {
            long offset = offsets.get(columnIndex);
            BinarySequence result = primaryColumns.get(columnIndex).getBin(offset);
            offset += result.length() + Long.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public boolean nextBool(int columnIndex) {
            long offset = offsets.get(columnIndex);
            boolean result = primaryColumns.get(columnIndex).getBool(offset);
            offset += 1;
            offsets.set(columnIndex, offset);
            return result;
        }

        public long nextDate(int columnIndex) {
            long offset = offsets.get(columnIndex);
            long result = primaryColumns.get(columnIndex).getLong(offset);
            offset += Long.BYTES;
            offsets.set(columnIndex, offset);
            return result;
        }

        public void nextLong256(int columnIndex, Long256Acceptor acceptor) {
            long offset = offsets.get(columnIndex);
            primaryColumns.get(columnIndex).getLong256(offset, acceptor);
            offset += Long256.BYTES;
            offsets.set(columnIndex, offset);
        }
    }

    public static class WalMetadataReader implements Closeable {
        private final FilesFacade ff;
        private final MemoryMR metaMem;
        private int columnCount;
        private final IntList columnTypes;
        private final List<String> columnNames;

        public WalMetadataReader(FilesFacade ff, Path path) {
            this.ff = ff;
            this.metaMem = Vm.getMRInstance();
            this.columnTypes = new IntList();
            this.columnNames = new ArrayList<>();
            of(path, WalWriter.WAL_FORMAT_VERSION);
        }

        @Override
        public void close() {
            Misc.free(metaMem);
        }

        public WalMetadataReader of(Path path, int expectedVersion) {
            columnNames.clear();
            columnTypes.clear();
            try {
                this.metaMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
                int offset = 0;
                int version = metaMem.getInt(offset);
                offset += Integer.BYTES;
                if (version != expectedVersion) {
                    throw CairoException.instance(CairoException.METADATA_VALIDATION)
                            .put("Invalid metadata at fd=")
                            .put(metaMem.getFd())
                            .put(". ");
                }
                this.columnCount = metaMem.getInt(offset);
                offset += Integer.BYTES;
                for (int i = 0; i < columnCount; i++) {
                    int columnType = metaMem.getInt(offset);
                    offset += Integer.BYTES;
                    CharSequence name = metaMem.getStr(offset);
                    offset += Vm.getStorageLength(name);
                    columnTypes.add(columnType);
                    columnNames.add(Chars.toString(name));
                }
            } catch (Throwable e) {
                close();
                throw e;
            }
            return this;
        }

        public int getColumnCount() {
            return columnCount;
        }

        public int getColumnType(int columnIndex) {
            return columnTypes.get(columnIndex);
        }

        public String getColumnName(int columnIndex) {
            return columnNames.get(columnIndex);
        }
    }
}