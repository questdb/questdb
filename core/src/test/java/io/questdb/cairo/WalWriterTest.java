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
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
            assertMetadataFileCreated(model, 0, path);
            assertMetadataFileCreated(model, 1, path);
        }
    }

    private void assertMetadataFileCreated(TableModel model, long segment, Path path) {
        int plen = path.length();
        try {
            toMetadataPath(model.getTableName(), segment, path);
            TableReaderMetadata readerMetadata = new TableReaderMetadata(configuration.getFilesFacade(), path);
            assertMetadataMatchesModel(model, readerMetadata);
        } finally {
            path.trimTo(plen);
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

    private static void assertMetadataMatchesModel(TableModel model, TableReaderMetadata metadata) {
        // todo: assert on more properties - symbol column configuration, etc
        int columnCount = model.getColumnCount();
        assertEquals(columnCount, metadata.getColumnCount());
        assertEquals(model.getPartitionBy(), metadata.getPartitionBy());
        for (int i = 0; i < columnCount; i++) {
            assertEquals(model.getColumnType(i), metadata.getColumnType(i));
            assertEquals(model.getColumnName(i), metadata.getColumnName(i));
        }
    }
}