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

import io.questdb.Metrics;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;

public class CairoTestUtils {

    public static void create(TableModel model) {
        TableUtils.createTable(
                model.getConfiguration(),
                model.getMem(),
                model.getPath(),
                model,
                1
        );
    }

    public static void createTableWithVersion(TableModel model, int version) {
        TableUtils.createTable(
                model.getConfiguration(),
                model.getMem(),
                model.getPath(),
                model,
                version,
                1
        );
    }

    public static void createAllTable(CairoConfiguration configuration, int partitionBy) {
        try (TableModel model = getAllTypesModel(configuration, partitionBy)) {
            createTableWithVersionAndId(model, ColumnType.VERSION, 1);
        }
    }

    public static void createAllTableWithNewTypes(CairoConfiguration configuration, int partitionBy) {
        try (TableModel model = getAllTypesModelWithNewTypes(configuration, partitionBy)) {
            create(model);
        }
    }

    public static void createAllTableWithTimestamp(CairoConfiguration configuration, int partitionBy) {
        try (TableModel model = getAllTypesModel(configuration, partitionBy).col("ts", ColumnType.TIMESTAMP).timestamp()) {
            createTableWithVersionAndId(model, ColumnType.VERSION, 1);
        }
    }

    public static void createTable(TableModel model) {
        createTable(model, ColumnType.VERSION);
    }

    public static void createTable(TableModel model, int version) {
        TableUtils.createTable(
                model.getConfiguration(),
                model.getMem(),
                model.getPath(),
                model,
                version,
                1
        );
    }

    public static void createTableWithVersionAndId(TableModel model, int version, int tableId) {
        TableUtils.createTable(
                model.getConfiguration(),
                model.getMem(),
                model.getPath(),
                model,
                version,
                tableId
        );
    }

    public static void createTestTable(int n, Rnd rnd, TestRecord.ArrayBinarySequence binarySequence) {
        createTestTable(AbstractCairoTest.configuration, n, rnd, binarySequence);
    }

    public static void createTestTable(CairoConfiguration configuration, int n, Rnd rnd, TestRecord.ArrayBinarySequence binarySequence) {
        createTestTable(configuration, "x", n, rnd, binarySequence);
    }

    public static void createTestTable(CairoConfiguration configuration, String tableName, int n, Rnd rnd, TestRecord.ArrayBinarySequence binarySequence) {
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
            model
                    .col("a", ColumnType.BYTE)
                    .col("b", ColumnType.SHORT)
                    .col("c", ColumnType.INT)
                    .col("d", ColumnType.LONG)
                    .col("e", ColumnType.DATE)
                    .col("f", ColumnType.TIMESTAMP)
                    .col("g", ColumnType.FLOAT)
                    .col("h", ColumnType.DOUBLE)
                    .col("i", ColumnType.STRING)
                    .col("j", ColumnType.SYMBOL)
                    .col("k", ColumnType.BOOLEAN)
                    .col("l", ColumnType.BINARY);
            create(model);
        }

        try (TableWriter writer = new TableWriter(configuration, tableName, Metrics.disabled())) {
            for (int i = 0; i < n; i++) {
                TableWriter.Row row = writer.newRow();
                row.putByte(0, rnd.nextByte());
                row.putShort(1, rnd.nextShort());

                if (rnd.nextInt() % 4 == 0) {
                    row.putInt(2, Numbers.INT_NaN);
                } else {
                    row.putInt(2, rnd.nextInt());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(3, Numbers.LONG_NaN);
                } else {
                    row.putLong(3, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(4, Numbers.LONG_NaN);
                } else {
                    row.putDate(4, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(5, Numbers.LONG_NaN);
                } else {
                    row.putTimestamp(5, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putFloat(6, Float.NaN);
                } else {
                    row.putFloat(6, rnd.nextFloat());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putDouble(7, Double.NaN);
                } else {
                    row.putDouble(7, rnd.nextDouble());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putStr(8, null);
                } else {
                    row.putStr(8, rnd.nextChars(5));
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putSym(9, null);
                } else {
                    row.putSym(9, rnd.nextChars(3));
                }

                row.putBool(10, rnd.nextBoolean());

                if (rnd.nextInt() % 4 == 0) {
                    row.putBin(11, null);
                } else {
                    binarySequence.of(rnd.nextBytes(25));
                    row.putBin(11, binarySequence);
                }
                row.append();
            }
            writer.commit();
        }
    }

    public static TableModel getAllTypesModel(CairoConfiguration configuration, int partitionBy) {
        return new TableModel(configuration, "all", partitionBy)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL).symbolCapacity(64)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE);
    }

    public static TableModel getAllTypesModelWithNewTypes(CairoConfiguration configuration, int partitionBy) {
        return new TableModel(configuration, "all2", partitionBy)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL).symbolCapacity(64)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE)
                .col("long256", ColumnType.LONG256)
                .col("chr", ColumnType.CHAR)
                .timestamp()
                ;

    }

    public static TableModel getGeoHashTypesModelWithNewTypes(CairoConfiguration configuration, int partitionBy) {
        return new TableModel(configuration, "allgeo", partitionBy)
                .col("hb", ColumnType.getGeoHashTypeWithBits(6))
                .col("hs", ColumnType.getGeoHashTypeWithBits(12))
                .col("hi", ColumnType.getGeoHashTypeWithBits(27))
                .col("hl", ColumnType.getGeoHashTypeWithBits(44))
                .timestamp();
    }
}
