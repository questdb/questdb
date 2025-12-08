/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestRecord;
import io.questdb.test.tools.TestUtils;

public class CreateTableTestUtils {

    public static void createAllTable(CairoEngine engine, int partitionBy, int timestampType) {
        TableModel model = getAllTypesModel(engine.getConfiguration(), partitionBy).timestamp(timestampType);
        TestUtils.createTable(engine, model);
    }

    public static void createAllTableWithNewTypes(CairoEngine engine, int partitionBy, int timestampType) {
        TableModel model = getAllTypesModelWithNewTypes(engine.getConfiguration(), partitionBy, timestampType);
        TestUtils.createTable(engine, model);
    }

    public static void createDecimalsTable(CairoEngine engine, int partitionBy, int timestampType) {
        TableModel model = getDecimalTypesModel(engine.getConfiguration(), partitionBy, timestampType);
        TestUtils.createTable(engine, model);
    }

    public static void createTableWithVersionAndId(TableModel model, CairoEngine engine, int version, int tableId) {
        TableToken tableToken = engine.lockTableName(model.getTableName(), tableId, false, false);
        if (tableToken == null) {
            throw CairoException.critical(0).put("table already exists: ").put(model.getTableName());
        }
        TestUtils.createTable(model, model.getConfiguration(), version, tableId, tableToken);
        engine.registerTableToken(tableToken);
    }

    public static void createTestTable(int n, Rnd rnd, TestRecord.ArrayBinarySequence binarySequence) {
        createTestTable(AbstractCairoTest.engine, n, rnd, binarySequence);
    }

    public static void createTestTable(CairoEngine engine, int n, Rnd rnd, TestRecord.ArrayBinarySequence binarySequence) {
        try {
            TableModel model = new TableModel(engine.getConfiguration(), "x", PartitionBy.NONE);
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
                    .col("l", ColumnType.BINARY)
                    .col("m", ColumnType.UUID)
                    .col("n", ColumnType.VARCHAR)
                    .col("o", ColumnType.getDecimalType(2, 0)) // DECIMAL8
                    .col("p", ColumnType.getDecimalType(4, 0)) // DECIMAL16
                    .col("q", ColumnType.getDecimalType(9, 0)) // DECIMAL32
                    .col("r", ColumnType.getDecimalType(18, 0)) // DECIMAL64
                    .col("s", ColumnType.getDecimalType(38, 0)) // DECIMAL128
                    .col("t", ColumnType.getDecimalType(76, 0)); // DECIMAL256
            TestUtils.createTable(engine, model);
        } catch (RuntimeException e) {
            if ("table already exists: x".equals(e.getMessage())) {
                try (TableWriter writer = TestUtils.newOffPoolWriter(engine.getConfiguration(), engine.verifyTableName("x"), engine)) {
                    writer.truncate();
                }
            } else {
                throw e;
            }
        }

        Utf8StringSink utf8Sink = new Utf8StringSink();
        try (TableWriter writer = TestUtils.newOffPoolWriter(engine.getConfiguration(), engine.verifyTableName("x"), engine)) {
            for (int i = 0; i < n; i++) {
                TableWriter.Row row = writer.newRow();
                row.putByte(0, rnd.nextByte());
                row.putShort(1, rnd.nextShort());

                if (rnd.nextInt() % 4 == 0) {
                    row.putInt(2, Numbers.INT_NULL);
                } else {
                    row.putInt(2, rnd.nextInt());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(3, Numbers.LONG_NULL);
                } else {
                    row.putLong(3, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(4, Numbers.LONG_NULL);
                } else {
                    row.putDate(4, rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(5, Numbers.LONG_NULL);
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

                // UUID
                if (rnd.nextInt() % 4 == 0) {
                    row.putLong128(12, Numbers.LONG_NULL, Numbers.LONG_NULL);
                } else {
                    row.putLong128(12, rnd.nextLong(), rnd.nextLong());
                }

                if (rnd.nextInt() % 4 == 0) {
                    row.putVarchar(13, null);
                } else {
                    utf8Sink.clear();
                    if (rnd.nextInt() % 4 == 0) {
                        rnd.nextUtf8AsciiStr(5, utf8Sink);
                        row.putVarchar(13, utf8Sink);
                    } else {
                        rnd.nextUtf8Str(5, utf8Sink);
                        row.putVarchar(13, utf8Sink);
                    }
                }

                // DECIMAL8
                if (rnd.nextInt() % 4 == 0) {
                    row.putByte(14, Decimals.DECIMAL8_NULL);
                } else {
                    row.putByte(14, rnd.nextByte((byte) 100));
                }

                // DECIMAL16
                if (rnd.nextInt() % 4 == 0) {
                    row.putShort(15, Decimals.DECIMAL16_NULL);
                } else {
                    row.putShort(15, rnd.nextShort((short) 10000));
                }

                // DECIMAL32
                if (rnd.nextInt() % 4 == 0) {
                    row.putInt(16, Decimals.DECIMAL32_NULL);
                } else {
                    row.putInt(16, rnd.nextInt(1000000000));
                }

                // DECIMAL64
                if (rnd.nextInt() % 4 == 0) {
                    row.putLong(17, Decimals.DECIMAL64_NULL);
                } else {
                    row.putLong(17, rnd.nextLong(1000000000000000000L));
                }

                // DECIMAL128
                if (rnd.nextInt() % 4 == 0) {
                    row.putDecimal128(18, Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL);
                } else {
                    row.putDecimal128(18, rnd.nextLong(Decimal128.MAX_VALUE.getHigh()), rnd.nextLong());
                }

                // DECIMAL256
                if (rnd.nextInt() % 4 == 0) {
                    row.putDecimal256(19, Decimals.DECIMAL256_HH_NULL, Decimals.DECIMAL256_HL_NULL,
                            Decimals.DECIMAL256_LH_NULL, Decimals.DECIMAL256_LL_NULL);
                } else {
                    row.putDecimal256(19, rnd.nextLong(Decimal256.MAX_VALUE.getHh()), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                }

                row.append();
            }
            writer.commit();
        }
    }

    public static TableModel getAllTypesModel(CairoConfiguration configuration, int partitionBy) {
        return new TableModel(configuration, "all", partitionBy)
                .col("int", ColumnType.INT) // 0
                .col("short", ColumnType.SHORT) // 1
                .col("byte", ColumnType.BYTE) // 2
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL).symbolCapacity(64)
                .col("bool", ColumnType.BOOLEAN)
                .col("bin", ColumnType.BINARY)
                .col("date", ColumnType.DATE) // 10
                .col("varchar", ColumnType.VARCHAR); // 11
    }

    public static TableModel getAllTypesModelWithNewTypes(CairoConfiguration configuration, int partitionBy, int timestampType) {
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
                .col("uuid", ColumnType.UUID)
                .col("ipv4", ColumnType.IPv4)
                .col("varchar", ColumnType.VARCHAR)
                .timestamp(timestampType);
    }

    public static TableModel getDecimalTypesModel(CairoConfiguration configuration, int partitionBy, int timestampType) {
        return new TableModel(configuration, "decimals", partitionBy)
                .col("dec8", ColumnType.getDecimalType(2, 1))
                .col("dec16", ColumnType.getDecimalType(4, 2))
                .col("dec32", ColumnType.getDecimalType(9, 3))
                .col("dec64", ColumnType.getDecimalType(18, 4))
                .col("dec128", ColumnType.getDecimalType(38, 5))
                .col("dec256", ColumnType.getDecimalType(76, 6))
                .timestamp(timestampType);
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
