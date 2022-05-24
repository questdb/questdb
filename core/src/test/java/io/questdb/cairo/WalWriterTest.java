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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
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
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                for (int i = 0; i < 100; i++) {
                    WalWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.putStr(1, String.valueOf(i));
                    row.append();
                }
            }
            assertWalFileExist(tableName, "a", 0, path);
            assertWalFileExist(tableName, "b", 0, path);
        }
    }

    @Test
    public void symbolWal() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.SYMBOL)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                for (int i = 0; i < 5; i++) {
                    WalWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.putSym(1, "sym" + i);
                    row.append();
                }
            }
            assertWalFileExist(tableName, "a", 0, path);
            assertWalFileExist(tableName, "b", 0, path);
            assertWalSymbolFileExist(tableName, "b", ".c", path);
            assertWalSymbolFileExist(tableName, "b", ".k", path);
            assertWalSymbolFileExist(tableName, "b", ".o", path);
            assertWalSymbolFileExist(tableName, "b", ".v", path);
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
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
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
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.cancel();

                row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertWalFileExist(tableName, "a", 0, path);
            assertWalFileExist(tableName, "a", 1, path);
        }
    }

    @Test
    public void testDddlMetadataReadable() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.newRow().cancel(); // new segment
                row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertValidMetadataFileCreated(model, 0, path);
            assertValidMetadataFileCreated(model, 1, path);
        }
    }

    @Test
    public void testddlMetadataCreated() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertMetadataFileExist(tableName, 0, path);
        }
    }

    @Test
    public void testDdlMetadataForNewSegmentReadable() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertMetadataFileExist(tableName, 0, path);
        }
    }

    @Test
    public void ddlMetadataCreatedForNewSegment() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.newRow().cancel(); // force new segment
                row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertMetadataFileExist(tableName, 0, path);
            assertMetadataFileExist(tableName, 1, path);
        }
    }


    @Test
    public void testAddingColumnStartsNewSegment() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.addColumn("c", ColumnType.INT);
            }
            assertValidMetadataFileCreated(model, 0, path);
            assertValidMetadataFileCreated(model.col("c", ColumnType.INT), 1, path);
        }
    }

    @Test
    public void testRemovingColumnStartsNewSegment() {
        String tableName = "testtable";
        try (Path path = new Path().of(configuration.getRoot());
             TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)
                     .col("a", ColumnType.BYTE)
                     .col("b", ColumnType.STRING)
        ) {
            int plen = path.length();
            TableUtils.createTable(configuration, Vm.getMARWInstance(), path, model, 0);
            path.trimTo(plen);
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow();
                row.putByte(0, (byte) 1);
                row.append();
                walWriter.removeColumn("b");
            }
            assertValidMetadataFileCreated(model, 0, path);
            try (TableModel updatedModel = new TableModel(configuration, tableName, PartitionBy.NONE)
                    .col("a", ColumnType.BYTE)) {
                assertValidMetadataFileCreated(updatedModel, 1, path);
            }
        }
    }

    @Test
    public void testDesignatedTimestampsIncludesSegmentRowNumber() {
        String tableName = "testtable";
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
            try (WalWriter walWriter = new WalWriter(configuration, tableName, metrics)) {
                WalWriter.Row row = walWriter.newRow(ts);
                row.putByte(0, (byte) 1);
                row.append();
            }
            assertValidMetadataFileCreated(model, 0, path);
            // todo: tests content of the ts column
        }
    }

    private void assertValidMetadataFileCreated(TableModel model, long segment, Path path) {
        int plen = path.length();
        try {
            toMetadataPath(model.getTableName(), segment, path);
            WalMetadataReader walMetadataReader = new WalMetadataReader(configuration.getFilesFacade(), path);
            assertMetadataMatchesModel(model, walMetadataReader);
        } finally {
            path.trimTo(plen);
        }
    }

    private void assertWalFileExist(String tableName, String columnName, int segment, Path path) {
        int plen = path.length();
        try {
            path.concat(tableName).slash().concat("wal")
                    .slash().put(segment)
                    .slash().concat(columnName + ".wald").$();
            assertPathExists(path);
        } finally {
            path.trimTo(plen);
        }
    }

    private void assertWalSymbolFileExist(String tableName, String columnName, String extension, Path path) {
        int plen = path.length();
        try {
            path.concat(tableName).slash().concat("wal").slash().concat(columnName + extension).$();
            assertPathExists(path);
        } finally {
            path.trimTo(plen);
        }
    }

    private void assertMetadataFileExist(String tableName, int segment, Path path) {
        int plen = path.length();
        try {
            toMetadataPath(tableName, segment, path);
            assertPathExists(path);
        } finally {
            path.trimTo(plen);
        }
    }

    private void toMetadataPath(CharSequence tableName, long segment, Path path) {
        path.concat(tableName).slash()
                .concat("wal").slash()
                .put(segment).slash()
                .concat("_meta").$();
    }


    private static void assertPathExists(Path path) {
        if (!Files.exists(path)) {
            throw new AssertionError("Path " + path + " does not exists!");
        }
    }

    private static void assertMetadataMatchesModel(TableModel model, WalMetadataReader metadata) {
        int columnCount = model.getColumnCount();
        assertEquals(columnCount, metadata.getColumnCount());
        for (int i = 0; i < columnCount; i++) {
            assertEquals(model.getColumnType(i), metadata.getColumnType(i));
            assertEquals(model.getColumnName(i), metadata.getColumnName(i));
        }
    }

//    public static class WalSegmentDataReader {
//        private final FilesFacade ff;
//
//
//        public WalSegmentDataReader(FilesFacade ff, Path path) {
//            this.ff = ff;
//        }
//
//        public WalSegmentDataReader of(Path path, int expectedVersion) {
//            WalMetadataReader walMetadataReader = new WalMetadataReader(ff, path);
//        }
//    }

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