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
                assertEquals(100, walWriter.getCurrentWalDPartitionRowCount());
                walWriter.newRow().cancel(); // force a new WAL-D partition
                assertEquals(0, walWriter.getCurrentWalDPartitionRowCount());
                for (int i = 0; i < 50; i++) {
                    WalWriter.Row row = walWriter.newRow();
                    row.putByte(0, (byte) i);
                    row.putStr(1, String.valueOf(i));
                    row.putInt(2, 42);
                    row.append();
                }
                assertEquals(50, walWriter.getCurrentWalDPartitionRowCount());
            }
        }
    }

    @Test
    public void cancelRowStartsANewPartition_for_now() {
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
    public void ddlMetadataCreated() {
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

    private void assertWalFileExist(String tableName, String columnName, int partition, Path path) {
        int plen = path.length();
        try {
            path.concat(tableName).slash().concat("wal").slash().concat(String.valueOf(partition)).slash().concat(columnName + ".wald").$();
            assertPathExists(path);
        } finally {
            path.trimTo(plen);
        }
    }

    private void assertMetadataFileExist(String tableName, int partition, Path path) {
        int plen = path.length();
        try {
            path.concat(tableName).slash().concat("wal").slash().concat(String.valueOf(partition)).slash().concat("_meta").$();
            assertPathExists(path);
        } finally {
            path.trimTo(plen);
        }
    }


    private static void assertPathExists(Path path) {
        if (!Files.exists(path)) {
            throw new AssertionError("Path " + path + " does not exists!");
        }
    }
}