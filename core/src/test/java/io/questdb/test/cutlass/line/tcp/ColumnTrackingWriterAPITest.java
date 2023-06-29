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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cutlass.line.tcp.ColumnTrackingWriterAPI;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiConsumer;

public class ColumnTrackingWriterAPITest extends AbstractCairoTest {

    @Test
    public void testColumnTracking() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int strLen = 3;
            final long strPtr = Unsafe.malloc(strLen, MemoryTag.NATIVE_DEFAULT);
            for (int i = 0; i < strLen; i++) {
                Unsafe.getUnsafe().putByte(strPtr + i, (byte) 'a');
            }
            final DirectByteCharSequence dbcs = new DirectByteCharSequence();
            dbcs.of(strPtr, strPtr + strLen);
            final DirectBinarySequence dbs = new DirectBinarySequence();
            dbs.of(strPtr, strLen);

            final Long256Impl long256 = new Long256Impl();
            long256.setAll(42, 42, 42, 42);

            class Column {
                final String name;
                final short type;
                final BiConsumer<TableWriter.Row, Integer> writeFn;

                Column(String name, short type, BiConsumer<TableWriter.Row, Integer> writeFn) {
                    this.name = name;
                    this.type = type;
                    this.writeFn = writeFn;
                }
            }
            final Column[] columns = new Column[]{
                    new Column("a_boolean", ColumnType.BOOLEAN, (r, i) -> r.putBool(i, true)),
                    new Column("a_long", ColumnType.LONG, (r, i) -> r.putLong(i, 42)),
                    new Column("an_int", ColumnType.INT, (r, i) -> r.putInt(i, 42)),
                    new Column("a_short", ColumnType.SHORT, (r, i) -> r.putShort(i, (short) 42)),
                    new Column("a_byte", ColumnType.BYTE, (r, i) -> r.putByte(i, (byte) 42)),
                    new Column("a_timestamp", ColumnType.TIMESTAMP, (r, i) -> r.putTimestamp(i, 42)),
                    new Column("a_date", ColumnType.DATE, (r, i) -> r.putDate(i, 42)),
                    new Column("a_double", ColumnType.DOUBLE, (r, i) -> r.putDouble(i, 42)),
                    new Column("a_float", ColumnType.FLOAT, (r, i) -> r.putFloat(i, 42)),
                    new Column("a_str_1", ColumnType.STRING, (r, i) -> r.putStrUtf8AsUtf16(i, dbcs, false)),
                    new Column("a_str_2", ColumnType.STRING, (r, i) -> r.putStr(i, "foobar")),
                    new Column("a_str_3", ColumnType.STRING, (r, i) -> r.putStr(i, "foobar", 0, 3)),
                    new Column("a_str_4", ColumnType.STRING, (r, i) -> r.putStr(i, 'a')),
                    new Column("a_bin_1", ColumnType.BINARY, (r, i) -> r.putBin(i, strPtr, strLen)),
                    new Column("a_bin_2", ColumnType.BINARY, (r, i) -> r.putBin(i, dbs)),
                    new Column("a_sym_1", ColumnType.SYMBOL, (r, i) -> r.putSymUtf8(i, dbcs, false)),
                    new Column("a_sym_2", ColumnType.SYMBOL, (r, i) -> r.putSym(i, "barbaz")),
                    new Column("a_sym_3", ColumnType.SYMBOL, (r, i) -> r.putSym(i, 'a')),
                    new Column("a_sym_4", ColumnType.SYMBOL, (r, i) -> r.putSymIndex(i, SymbolMapReader.VALUE_IS_NULL)),
                    new Column("a_char", ColumnType.CHAR, (r, i) -> r.putChar(i, 'a')),
                    new Column("a_uuid", ColumnType.UUID, (r, i) -> r.putUuid(i, "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")),
                    new Column("a_geo_hash_1", ColumnType.GEOLONG, (r, i) -> r.putGeoHash(i, 42)),
                    new Column("a_geo_hash_2", ColumnType.GEOLONG, (r, i) -> r.putGeoHashDeg(i, 42, 42)),
                    new Column("a_geo_hash_3", ColumnType.GEOLONG, (r, i) -> r.putGeoStr(i, "qeustdb")),
                    new Column("a_long256_1", ColumnType.LONG256, (r, i) -> r.putLong256(i, long256.toString())),
                    new Column("a_long256_2", ColumnType.LONG256, (r, i) -> r.putLong256(i, long256.toString(), 2, long256.toString().length())),
                    new Column("a_long256_3", ColumnType.LONG256, (r, i) -> r.putLong256(i, long256)),
                    new Column("a_long256_4", ColumnType.LONG256, (r, i) -> r.putLong256(i, 42, 42, 42, 42)),
                    new Column("a_long128", ColumnType.LONG256, (r, i) -> r.putLong128(i, 42, 42)),
            };

            final String tableName = "x";
            final TableToken tableToken;
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)) {
                for (Column column : columns) {
                    model.col(column.name, column.type);
                }
                model.timestamp().wal();
                tableToken = CreateTableTestUtils.create(model);
            }

            try (WalWriter _w = engine.getWalWriter(tableToken)) {
                final ColumnTrackingWriterAPI w = new ColumnTrackingWriterAPI(_w);
                Assert.assertEquals(0, w.getWrittenColumnNames().size());

                final ObjList<CharSequence> expected = new ObjList<>();

                TableWriter.Row r = w.newRow();
                for (int i = 0; i < columns.length; i++) {
                    columns[i].writeFn.accept(r, i);
                    assertExpectedColumns(w, expected, columns[i].name);
                }
                r.append();

                w.commit();
                Assert.assertEquals(0, w.getWrittenColumnNames().size());
            } finally {
                Unsafe.free(strPtr, strLen, MemoryTag.NATIVE_DEFAULT);
                engine.releaseInactive();
            }
        });
    }

    @Test
    public void testResetsTrackedColumnsOnCommitAndRollback() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String tableName = "x";
            final TableToken tableToken;
            try (
                    TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                            .col("an_int", ColumnType.INT)
                            .timestamp()
                            .wal()
            ) {
                tableToken = CreateTableTestUtils.create(model);
            }

            try (WalWriter _w = engine.getWalWriter(tableToken)) {
                final ColumnTrackingWriterAPI w = new ColumnTrackingWriterAPI(_w);
                Assert.assertEquals(0, w.getWrittenColumnNames().size());

                // ic()
                TableWriter.Row r = w.newRow();
                r.putInt(0, 42);
                r.append();
                Assert.assertEquals(1, w.getWrittenColumnNames().size());

                w.ic();
                Assert.assertEquals(0, w.getWrittenColumnNames().size());

                // ic(o3MaxLag)
                r = w.newRow();
                r.putInt(0, 42);
                r.append();
                Assert.assertEquals(1, w.getWrittenColumnNames().size());

                w.ic(100);
                Assert.assertEquals(0, w.getWrittenColumnNames().size());

                // commit()
                r = w.newRow();
                r.putInt(0, 42);
                r.append();
                Assert.assertEquals(1, w.getWrittenColumnNames().size());

                w.commit();
                Assert.assertEquals(0, w.getWrittenColumnNames().size());

                // rollback()
                r = w.newRow();
                r.putInt(0, 42);
                r.append();
                Assert.assertEquals(1, w.getWrittenColumnNames().size());

                w.rollback();
                Assert.assertEquals(0, w.getWrittenColumnNames().size());
            } finally {
                engine.releaseInactive();
            }
        });
    }

    private void assertExpectedColumns(ColumnTrackingWriterAPI wrapped, ObjList<CharSequence> expected, CharSequence justWritten) {
        expected.add(justWritten);
        TestUtils.assertEquals(expected, wrapped.getWrittenColumnNames());
    }
}
