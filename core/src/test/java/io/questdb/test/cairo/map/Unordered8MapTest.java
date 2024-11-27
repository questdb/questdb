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

package io.questdb.test.cairo.map;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongLongHeap;
import io.questdb.std.DirectLongLongMinHeap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Unordered8MapTest extends AbstractCairoTest {

    @Test
    public void testFuzz() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            HashMap<Long, Long> oracle = new HashMap<>();
            try (Unordered8Map map = new Unordered8Map(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE)) {
                final int N = 100000;
                for (int i = 0; i < N; i++) {
                    MapKey key = map.withKey();
                    long l = rnd.nextLong();
                    key.putLong(l);

                    MapValue value = key.createValue();
                    value.putLong(0, l);

                    oracle.put(l, l);
                }

                Assert.assertEquals(oracle.size(), map.size());

                // assert map contents
                for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                    MapKey key = map.withKey();
                    key.putLong(e.getKey());

                    MapValue value = key.findValue();
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals((long) e.getKey(), value.getLong(0));
                    Assert.assertEquals((long) e.getValue(), value.getLong(0));
                }
            }
        });
    }

    @Test
    public void testPutBinUnsupported() throws Exception {
        assertUnsupported(key -> key.putBin(null));
    }

    @Test
    public void testPutLong128Unsupported() throws Exception {
        assertUnsupported(key -> key.putLong128(0, 0));
    }

    @Test
    public void testPutLong256ObjectUnsupported() throws Exception {
        assertUnsupported(key -> key.putLong256(null));
    }

    @Test
    public void testPutLong256ValuesUnsupported() throws Exception {
        assertUnsupported(key -> key.putLong256(0, 0, 0, 0));
    }

    @Test
    public void testPutStrRangeUnsupported() throws Exception {
        assertUnsupported(key -> key.putStr(null, 0, 0));
    }

    @Test
    public void testPutStrUnsupported() throws Exception {
        assertUnsupported(key -> key.putStr(null));
    }

    @Test
    public void testPutVarcharUnsupported() throws Exception {
        assertUnsupported(key -> key.putVarchar((Utf8Sequence) null));
    }

    @Test
    public void testSingleZeroKey() {
        try (Unordered8Map map = new Unordered8Map(new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
            MapKey key = map.withKey();
            key.putLong(0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 42);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(42, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(42, record.getLong(0));
            }
        }
    }

    @Test
    public void testTopK() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int heapCapacity = 5;
            SingleColumnType keyTypes = new SingleColumnType(ColumnType.LONG);
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    Unordered8Map map = new Unordered8Map(keyTypes, valueTypes, 64, 0.8, Integer.MAX_VALUE);
                    DirectLongLongHeap heap = new DirectLongLongMinHeap(heapCapacity, MemoryTag.NATIVE_DEFAULT)
            ) {
                for (int i = 0; i < 100; i++) {
                    MapKey key = map.withKey();
                    key.putLong(i);

                    MapValue value = key.createValue();
                    value.putLong(0, i);
                }

                MapRecordCursor mapCursor = map.getCursor();
                mapCursor.longTopK(heap, new LongColumn(0));

                Assert.assertEquals(heapCapacity, heap.size());

                MapRecord mapRecord = mapCursor.getRecord();
                DirectLongLongHeap.Cursor heapCursor = heap.getCursor();
                for (int i = 0; i < heapCapacity; i++) {
                    Assert.assertTrue(heapCursor.hasNext());
                    mapCursor.recordAt(mapRecord, heapCursor.index());
                    Assert.assertEquals(heapCursor.value(), mapRecord.getLong(0));
                }
            }
        });
    }

    @Test
    public void testTwoKeysIncludingZero() {
        try (Unordered8Map map = new Unordered8Map(new SingleColumnType(ColumnType.LONG), new SingleColumnType(ColumnType.LONG), 16, 0.8, 24)) {
            MapKey key = map.withKey();
            key.putLong(0);
            MapValue value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 0);

            key = map.withKey();
            key.putLong(1);
            value = key.createValue();
            Assert.assertTrue(value.isNew());
            value.putLong(0, 1);

            try (RecordCursor cursor = map.getCursor()) {
                final Record record = cursor.getRecord();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong(1));
                Assert.assertEquals(1, record.getLong(0));
                // Zero is always last when iterating.
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(0, record.getLong(0));

                // Validate that we get the same sequence after toTop.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(1, record.getLong(1));
                Assert.assertEquals(1, record.getLong(0));
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(0, record.getLong(1));
                Assert.assertEquals(0, record.getLong(0));
            }
        }
    }

    @Test
    public void testUnsupportedKeyTypes() throws Exception {
        short[] columnTypes = new short[]{
                ColumnType.BINARY,
                ColumnType.STRING,
                ColumnType.VARCHAR,
                ColumnType.LONG128,
                ColumnType.UUID,
                ColumnType.LONG256,
        };
        for (short columnType : columnTypes) {
            TestUtils.assertMemoryLeak(() -> {
                try (Unordered8Map ignore = new Unordered8Map(new SingleColumnType(columnType), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
                    Assert.fail();
                } catch (CairoException e) {
                    Assert.assertTrue(Chars.contains(e.getMessage(), "unexpected key size"));
                }
            });
        }
    }

    private static void assertUnsupported(Consumer<? super MapKey> putKeyFn) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered8Map map = new Unordered8Map(new SingleColumnType(ColumnType.BOOLEAN), new SingleColumnType(ColumnType.LONG), 64, 0.5, 1)) {
                MapKey key = map.withKey();
                try {
                    putKeyFn.accept(key);
                    Assert.fail();
                } catch (UnsupportedOperationException e) {
                    Assert.assertTrue(true);
                }
            }
        });
    }
}
