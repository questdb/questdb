/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo.map;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.SymbolTable;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.Numbers;
import com.questdb.std.Rnd;
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
        ) {
            CairoTestUtils.create(model);
        }

        final int N = 1024;
        final Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(configuration, "all")) {

            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, rnd.nextInt());
                row.putShort(1, rnd.nextShort());
                row.putByte(2, rnd.nextByte());
                row.putDouble(3, rnd.nextDouble2());
                row.putFloat(4, rnd.nextFloat2());
                row.putLong(5, rnd.nextLong());
                row.putSym(6, rnd.nextChars(10));
                row.putBool(7, rnd.nextBoolean());
                row.putDate(8, rnd.nextLong());
                row.putTimestamp(9, rnd.nextLong());
                row.append();
            }
            writer.commit();
        }

        try (TableReader reader = new TableReader(configuration, "all")) {
            final SymbolAsIntTypes valueTypes = new SymbolAsIntTypes().of(reader.getMetadata());
            try (final Map map = new FastMap(Numbers.SIZE_1MB, keyTypes, valueTypes, N, 0.5)) {

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

                SymbolTable symbolTable = reader.getSymbolMapReader(6);

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);
                    MapValue value = key.findValue();

                    Assert.assertNotNull(value);
                    Assert.assertEquals(rnd.nextInt(), value.getInt(0));
                    Assert.assertEquals(rnd.nextShort(), value.getShort(1));
                    Assert.assertEquals(rnd.nextByte(), value.getByte(2));
                    Assert.assertEquals(rnd.nextDouble2(), value.getDouble(3), 0.000001);
                    Assert.assertEquals(rnd.nextFloat2(), value.getFloat(4), 0.000001f);
                    Assert.assertEquals(rnd.nextLong(), value.getLong(5));
                    Assert.assertEquals(symbolTable.getQuick(rnd.nextChars(10)), value.getInt(6));
                    Assert.assertEquals(rnd.nextBoolean(), value.getBool(7));
                    Assert.assertEquals(rnd.nextLong(), value.getDate(8));
                    Assert.assertEquals(rnd.nextLong(), value.getTimestamp(9));
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
        ) {
            CairoTestUtils.create(model);
        }

        final int N = 1024;
        final Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(configuration, "all")) {

            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow();
                row.putInt(0, rnd.nextInt());
                row.putShort(1, rnd.nextShort());
                row.putByte(2, rnd.nextByte());
                row.putDouble(3, rnd.nextDouble2());
                row.putFloat(4, rnd.nextFloat2());
                row.putLong(5, rnd.nextLong());
                row.putSym(6, rnd.nextChars(10));
                row.putBool(7, rnd.nextBoolean());
                row.putDate(8, rnd.nextLong());
                row.putTimestamp(9, rnd.nextLong());
                row.append();
            }
            writer.commit();
        }

        try (TableReader reader = new TableReader(configuration, "all")) {
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.BOOLEAN);
            valueTypes.add(ColumnType.TIMESTAMP);
            valueTypes.add(ColumnType.INT);
            try (final Map map = new FastMap(Numbers.SIZE_1MB, keyTypes, valueTypes, N, 0.5)) {

                ListColumnFilter columnFilter = new ListColumnFilter();
                columnFilter.add(7);
                columnFilter.add(9);
                columnFilter.add(6);


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

                SymbolTable symbolTable = reader.getSymbolMapReader(6);

                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    key.putInt(i);
                    MapValue value = key.findValue();

                    Assert.assertNotNull(value);
                    rnd.nextInt(); // 0
                    rnd.nextShort(); // 1
                    rnd.nextByte(); // 2
                    rnd.nextDouble2(); // 3
                    rnd.nextFloat2(); // 4
                    rnd.nextLong(); // 5
                    Assert.assertEquals(symbolTable.getQuick(rnd.nextChars(10)), value.getInt(2)); // 6
                    Assert.assertEquals(rnd.nextBoolean(), value.getBool(0)); // 7
                    rnd.nextLong(); // 8
                    Assert.assertEquals(rnd.nextLong(), value.getTimestamp(1)); // 9
                }
            }
        }
    }
}