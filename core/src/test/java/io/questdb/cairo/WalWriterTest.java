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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Acceptor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class WalWriterTest extends AbstractGriffinTest {

    @Test
    public void bootstrapWal() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < 100; i++) {
                    WalWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.putStr(1, String.valueOf(i));
                    row.append();
                }
            }
            assertWalFileExist(tableName, walName, "a", 0, path);
            assertWalFileExist(tableName, walName, "b", 0, path);
        }
    }

    @Test
    public void symbolWal() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.SYMBOL)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < 50; i++) {
                    WalWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.putSym(1, "sym" + i);
                    row.append();
                }
            }
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
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
                     .col("c", ColumnType.INT)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
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
    }

    @Test
    public void cancelRowStartsANewSegment_for_now() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.cancel();

                row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertWalFileExist(tableName, walName, "a", 0, path);
            assertWalFileExist(tableName, walName, "a", 1, path);
        }
    }

    @Test
    public void testDddlMetadataReadable() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
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
            assertValidMetadataFileCreated(model, walName, 0, path);
            assertValidMetadataFileCreated(model, walName, 1, path);
        }
    }

    @Test
    public void testddlMetadataCreated() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertMetadataFileExist(tableName, walName, 0, path);
        }
    }

    @Test
    public void testDdlMetadataForNewSegmentReadable() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertMetadataFileExist(tableName, walName, 0, path);
        }
    }

    @Test
    public void ddlMetadataCreatedForNewSegment() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
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
            assertMetadataFileExist(tableName, walName, 0, path);
            assertMetadataFileExist(tableName, walName, 1, path);
        }
    }


    @Test
    public void testAddingColumnStartsNewSegment() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.addColumn("c", ColumnType.INT);
            }
            assertValidMetadataFileCreated(model, walName, 0, path);
            assertValidMetadataFileCreated(model.col("c", ColumnType.INT), walName, 1, path);
        }
    }

    @Test
    public void testRemovingColumnStartsNewSegment() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.removeColumn("b");
            }
            assertValidMetadataFileCreated(model, walName, 0, path);
            try (TableModel updatedModel = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)) {
                assertValidMetadataFileCreated(updatedModel, walName, 1, path);
            }
        }
    }

    @Test
    public void testDesignatedTimestampsIncludesSegmentRowNumber() {
        String tableName = "testtable";
        String walName;
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
                     .timestamp("ts")
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            long ts = Os.currentTimeMicros();
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                WalWriter.Row row = walWriter.newRow(ts);
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertValidMetadataFileCreated(model, walName, 0, path);
            // todo: tests content of the ts column
        }
    }

    @Test
    public void testReadAndWriteSimpleFixedLengthTypes() {
        String tableName = "testtable";
        String walName;
        final int rowsToInsertTotal = 100;

        try (Path path = new Path().of(configuration.getRoot());
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
                     .timestamp("ts")
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            long ts = Os.currentTimeMicros();
            try (WalWriter walWriter = engine.getWalWriter(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                walName = walWriter.getWalName();
                for (int i = 0; i < rowsToInsertTotal; i++) {
                    WalWriter.Row row = walWriter.newRow(ts);
                    row.putInt(0, i);
                    row.putByte(1, (byte) i);
                    row.putLong(2, i);
                    row.putLong256(3, i, i + 1 , i + 2, i + 3);
                    row.putDouble(4, i + .5);
                    row.putFloat(5, i + .5f);
                    row.putShort(6, (short) i);
                    row.putTimestamp(7, i);
                    row.putChar(8, (char) i);
                    row.putBool(9, i % 2 == 0);
                    row.putDate(10, i);
                    row.putStr(11, String.valueOf(i));
                    row.append();
                }
            }

            try (WalSegmentDataReader segmentReader = new WalSegmentDataReader(configuration.getFilesFacade(), toWalPath(tableName, walName, 0, path))) {
                int intIndex = segmentReader.getColumnIndex("int");
                int byteIndex = segmentReader.getColumnIndex("byte");
                int longIndex = segmentReader.getColumnIndex("long");
                int long256Index = segmentReader.getColumnIndex("long256");
                int tsIndex = segmentReader.getColumnIndex("ts");
                int doubleIndex = segmentReader.getColumnIndex("double");
                int floatIndex = segmentReader.getColumnIndex("float");
                int shortIndex = segmentReader.getColumnIndex("short");
                int timestampIndex = segmentReader.getColumnIndex("timestamp");
                int charIndex = segmentReader.getColumnIndex("char");
                int booleanIndex = segmentReader.getColumnIndex("boolean");
                int dateIndex = segmentReader.getColumnIndex("date");
                int stringIndex = segmentReader.getColumnIndex("string");
                for (int i = 0; i < rowsToInsertTotal; i++) {
                    assertEquals(i, segmentReader.nextInt(intIndex));
                    assertEquals(i, segmentReader.nextByte(byteIndex));
                    assertEquals(i, segmentReader.nextLong(longIndex));
                    segmentReader.nextLong256(long256Index, assertingAcceptor(i, i + 1, i + 2, i + 3));
                    assertEquals(ts, segmentReader.nextLong(tsIndex));
                    assertEquals(i, segmentReader.nextLong(tsIndex));
                    assertEquals(i + 0.5, segmentReader.nextDouble(doubleIndex), 0.1);
                    assertEquals(i + 0.5, segmentReader.nextFloat(floatIndex), 0.1);
                    assertEquals(i, segmentReader.nextShort(shortIndex));
                    assertEquals(i, segmentReader.nextTimestamp(timestampIndex));
                    assertEquals(i, segmentReader.nextChar(charIndex));
                    assertEquals(i % 2 == 0, segmentReader.nextBool(booleanIndex));
                    assertEquals(i, segmentReader.nextDate(dateIndex));
                    assertCharsEquals(String.valueOf(i), segmentReader.nextString(stringIndex));
                }
            }
        }
    }

    private static void assertCharsEquals(CharSequence expected, CharSequence actual) {
        if (!Chars.equals(expected, actual)) {
            throw new AssertionError("CharSequence not equal. Expected: '" + expected + "', actual: '" + " actual '");
        }
    }

    private void assertValidMetadataFileCreated(TableModel model, String walName, long segment, Path path) {
        int plen = path.length();
        try {
            toMetadataPath(model.getTableName(), walName, segment, path);
            WalMetadataReader walMetadataReader = new WalMetadataReader(configuration.getFilesFacade(), path);
            assertMetadataMatchesModel(model, walMetadataReader);
        } finally {
            path.trimTo(plen);
        }
    }

    private void assertWalFileExist(String tableName, String walName, String columnName, int segment, Path path) {
        int plen = path.length();
        try {
            path.concat(tableName).slash().concat(walName)
                    .slash().put(segment)
                    .slash().concat(columnName + ".wald").$();
            assertPathExists(path);
        } finally {
            path.trimTo(plen);
        }
    }

    private void assertWalSymbolFileExist(String tableName, String walName, String columnName, String extension, Path path) {
        int plen = path.length();
        try {
            path.concat(tableName).slash().concat(walName).slash().concat(columnName + extension).$();
            assertPathExists(path);
        } finally {
            path.trimTo(plen);
        }
    }

    private void assertMetadataFileExist(String tableName, CharSequence walName, int segment, Path path) {
        int plen = path.length();
        try {
            assertPathExists(toMetadataPath(tableName, walName, segment, path));
        } finally {
            path.trimTo(plen);
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
                    primaryMem.wholeFile(ff, TableUtils.walDFile(path.trimTo(plen), name), MemoryTag.MMAP_DEFAULT);
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