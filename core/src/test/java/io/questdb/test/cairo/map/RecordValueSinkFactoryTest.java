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

package io.questdb.test.cairo.map;

import io.questdb.cairo.*;
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.TableModel;
import org.junit.Assert;
import org.junit.Test;

public class RecordValueSinkFactoryTest extends AbstractCairoTest {

    @Test
    public void testAllSupportedTypes() {
        SingleColumnType keyTypes = new SingleColumnType(ColumnType.INT);
        try (TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("sym", ColumnType.SYMBOL).symbolCapacity(64)
                .col("bool", ColumnType.BOOLEAN)
                .col("date", ColumnType.DATE)
                .col("ts", ColumnType.TIMESTAMP)
                .col("ipv4", ColumnType.IPv4)
        ) {
            CreateTableTestUtils.create(model);
        }

        final int N = 1024;
        final Rnd rnd = new Rnd();
        try (TableWriter writer = newTableWriter(configuration, "all", metrics)) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, rnd.nextInt());
                row.putShort(1, rnd.nextShort());
                row.putByte(2, rnd.nextByte());
                row.putDouble(3, rnd.nextDouble());
                row.putFloat(4, rnd.nextFloat());
                row.putLong(5, rnd.nextLong());
                row.putSym(6, rnd.nextChars(10));
                row.putBool(7, rnd.nextBoolean());
                row.putDate(8, rnd.nextLong());
                row.putTimestamp(9, rnd.nextLong());
                row.putInt(10, rnd.nextInt());
                row.append();
            }
            writer.commit();
        }

        try (TableReader reader = newTableReader(configuration, "all")) {
            final SymbolAsIntTypes valueTypes = new SymbolAsIntTypes().of(reader.getMetadata());
            try (final Map map = new FastMap(Numbers.SIZE_1MB, keyTypes, valueTypes, N, 0.5, 100)) {
                EntityColumnFilter columnFilter = new EntityColumnFilter();
                columnFilter.of(reader.getMetadata().getColumnCount());
                RecordValueSink sink = RecordValueSinkFactory.getInstance(new BytecodeAssembler(), reader.getMetadata(), columnFilter);
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();

                int index = 0;
                while (cursor.hasNext()) {
                    MapKey key = map.withKey();
                    key.putInt(index++);
                    MapValue value = key.createValue();
                    sink.copy(record, value);
                }

                Assert.assertEquals(N, index);

                rnd.reset();

                StaticSymbolTable symbolTable = reader.getSymbolMapReader(6);

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);
                    MapValue value = key.findValue();

                    Assert.assertNotNull(value);
                    Assert.assertEquals(rnd.nextInt(), value.getInt(0));
                    Assert.assertEquals(rnd.nextShort(), value.getShort(1));
                    Assert.assertEquals(rnd.nextByte(), value.getByte(2));
                    Assert.assertEquals(rnd.nextDouble(), value.getDouble(3), 0.000001);
                    Assert.assertEquals(rnd.nextFloat(), value.getFloat(4), 0.000001f);
                    Assert.assertEquals(rnd.nextLong(), value.getLong(5));
                    Assert.assertEquals(symbolTable.keyOf(rnd.nextChars(10)), value.getInt(6));
                    Assert.assertEquals(rnd.nextBoolean(), value.getBool(7));
                    Assert.assertEquals(rnd.nextLong(), value.getDate(8));
                    Assert.assertEquals(rnd.nextLong(), value.getTimestamp(9));
                    Assert.assertEquals(rnd.nextInt(), value.getIPv4(10));
                }
            }
        }
    }

    @Test
    public void testSubset() {
        SingleColumnType keyTypes = new SingleColumnType(ColumnType.INT);
        try (TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT)
                .col("short", ColumnType.SHORT)
                .col("byte", ColumnType.BYTE)
                .col("double", ColumnType.DOUBLE)
                .col("float", ColumnType.FLOAT)
                .col("long", ColumnType.LONG)
                .col("sym", ColumnType.SYMBOL).symbolCapacity(64)
                .col("bool", ColumnType.BOOLEAN)
                .col("date", ColumnType.DATE)
                .col("ts", ColumnType.TIMESTAMP)
                .col("IPv4", ColumnType.IPv4)
        ) {
            CreateTableTestUtils.create(model);
        }

        final int N = 1024;
        final Rnd rnd = new Rnd();
        try (TableWriter writer = newTableWriter(configuration, "all", metrics)) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, rnd.nextInt());
                row.putShort(1, rnd.nextShort());
                row.putByte(2, rnd.nextByte());
                row.putDouble(3, rnd.nextDouble());
                row.putFloat(4, rnd.nextFloat());
                row.putLong(5, rnd.nextLong());
                row.putSym(6, rnd.nextChars(10));
                row.putBool(7, rnd.nextBoolean());
                row.putDate(8, rnd.nextLong());
                row.putTimestamp(9, rnd.nextLong());
                row.putInt(10, rnd.nextInt());
                row.append();
            }
            writer.commit();
        }

        try (TableReader reader = newTableReader(configuration, "all")) {
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.BOOLEAN);
            valueTypes.add(ColumnType.TIMESTAMP);
            valueTypes.add(ColumnType.INT);
            try (final Map map = new FastMap(Numbers.SIZE_1MB, keyTypes, valueTypes, N, 0.5, 100)) {
                ListColumnFilter columnFilter = new ListColumnFilter();
                columnFilter.add(8);
                columnFilter.add(10);
                columnFilter.add(7);

                RecordValueSink sink = RecordValueSinkFactory.getInstance(new BytecodeAssembler(), reader.getMetadata(), columnFilter);
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();

                int index = 0;
                while (cursor.hasNext()) {
                    MapKey key = map.withKey();
                    key.putInt(index++);
                    MapValue value = key.createValue();
                    sink.copy(record, value);
                }

                Assert.assertEquals(N, index);

                rnd.reset();

                StaticSymbolTable symbolTable = reader.getSymbolMapReader(6);

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);
                    MapValue value = key.findValue();

                    Assert.assertNotNull(value);
                    rnd.nextInt(); // 0
                    rnd.nextShort(); // 1
                    rnd.nextByte(); // 2
                    rnd.nextDouble(); // 3
                    rnd.nextFloat(); // 4
                    rnd.nextLong(); // 5
                    Assert.assertEquals(symbolTable.keyOf(rnd.nextChars(10)), value.getInt(2)); // 6
                    Assert.assertEquals(rnd.nextBoolean(), value.getBool(0)); // 7
                    rnd.nextLong(); // 8
                    Assert.assertEquals(rnd.nextLong(), value.getTimestamp(1)); // 9
                    rnd.nextInt(); //10
                }
            }
        }
    }
}