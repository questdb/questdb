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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.std.BitSet;
import io.questdb.std.Chars;
import io.questdb.std.DirectLongLongHeap;
import io.questdb.std.DirectLongLongMinHeap;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestDirectUtf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

public class UnorderedVarcharMapTest extends AbstractCairoTest {

    @Test
    public void testBlankKey() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (
                DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
                DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
                UnorderedVarcharMap map = newDefaultMap(valueType)
        ) {
            Assert.assertNull(findValue("", map));
            putStable("", 42, map, sinkA, true);
            Assert.assertEquals(42, get("", map));
            Assert.assertEquals(1, map.size());

            putStable("", 43, map, sinkB, false);
            Assert.assertEquals(43, get("", map));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testClear() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (
                DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
                UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB)
        ) {
            putStable("foo", 42, map, sinkA, true);
            putUnstable("foo", 42, map, false);
            Assert.assertEquals(42, get("foo", map));
            Assert.assertEquals(1, map.size());
            map.clear();
            Assert.assertNull(findValue("foo", map));
            Assert.assertEquals(0, map.size());
        }
    }

    @Test
    public void testClearFreeHeapMemory() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE, 1024, 4 * Numbers.SIZE_1GB)) {
            long memUsedBefore = Unsafe.getMemUsed();
            for (int i = 0; i < 10_000; i++) {
                putUnstable("foo" + i, 42, map, true);
            }
            long memUsedAfterInsert = Unsafe.getMemUsed();
            Assert.assertTrue(memUsedAfterInsert > memUsedBefore);

            map.clear();
            map.restoreInitialCapacity();
            long memUsedAfterClear = Unsafe.getMemUsed();
            Assert.assertEquals(memUsedAfterClear, memUsedBefore);
        }
    }

    @Test
    public void testCursor() throws Exception {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (
                DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
                UnorderedVarcharMap map = newDefaultMap(valueType)
        ) {
            int keyCount = 100_000;
            for (int i = 0; i < keyCount; i++) {
                putStable(String.valueOf(i), i, map, sinkA, true);
            }
            putStable("", -1, map, sinkA, true);
            putStable(null, -2, map, sinkA, true);

            MapRecordCursor danglingCursor;
            try (MapRecordCursor cursor = map.getCursor()) {
                assertCursor(cursor, keyCount);
                Assert.assertFalse(cursor.hasNext());

                cursor.toTop();
                assertCursor(cursor, keyCount);
                Assert.assertFalse(cursor.hasNext());
                danglingCursor = cursor;
            }
            // double-close must be noop
            danglingCursor.close();
        }
    }

    @Test
    public void testHashPacking() {
        long hash = Integer.MAX_VALUE;
        long packed = UnorderedVarcharMap.packHashSizeFlags(hash, 0, (byte) 0);
        Assert.assertEquals(hash & 0xffffffffL, UnorderedVarcharMap.unpackHash(packed));

        hash = Integer.MIN_VALUE;
        packed = UnorderedVarcharMap.packHashSizeFlags(hash, 0, (byte) 0);
        Assert.assertEquals(hash & 0xffffffffL, UnorderedVarcharMap.unpackHash(packed));

        for (int i = 0; i < 1000; i++) {
            hash = ThreadLocalRandom.current().nextLong();
            packed = UnorderedVarcharMap.packHashSizeFlags(hash, 0, (byte) 0);
            Assert.assertEquals(hash & 0xffffffffL, UnorderedVarcharMap.unpackHash(packed));
        }
    }

    @Test
    public void testKeyCopyFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);
            try (
                    DirectUtf8Sink sinkA = new DirectUtf8Sink(10 * 1024 * 1024);
                    UnorderedVarcharMap mapA = newDefaultMap(valueTypes);
                    UnorderedVarcharMap mapB = newDefaultMap(valueTypes)
            ) {
                final int N = 100_000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = putStable("foo" + i, i + 1, mapA, sinkA, true);

                    MapKey keyB = mapB.withKey();
                    keyB.copyFrom(keyA);
                    MapValue valueB = keyB.createValue();
                    Assert.assertTrue(valueB.isNew());
                    valueB.putInt(0, i + 1);
                }

                Assert.assertEquals(mapA.size(), mapB.size());

                // assert that all map A keys can be found in map B
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i + 1, get("foo" + i, mapA));
                    Assert.assertEquals(i + 1, get("foo" + i, mapB));
                }
            }
        });
    }

    @Test
    public void testKeyHashCode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
            try (
                    Map map = newDefaultMap(valueType);
                    DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024)
            ) {
                final int N = 100000;
                final LongList keyHashCodes = new LongList(N);
                long lo = sinkA.hi();
                TestDirectUtf8String directUtf8 = new TestDirectUtf8String(true);
                for (int i = 0; i < N; i++) {
                    MapKey mapKey = map.withKey();
                    sinkA.put("foo").put(i);
                    long hi = sinkA.hi();
                    directUtf8.of(lo, hi, true);
                    lo = hi;
                    mapKey.putVarchar(directUtf8);
                    mapKey.commit();
                    long hashCode = mapKey.hash();
                    keyHashCodes.add(hashCode);

                    MapValue value = mapKey.createValue(hashCode);
                    Assert.assertTrue(value.isNew());
                    value.putInt(0, i + 2);
                }

                final LongList recordHashCodes = new LongList(N);
                RecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                while (cursor.hasNext()) {
                    recordHashCodes.add(record.keyHashCode());
                }

                keyHashCodes.sort();
                recordHashCodes.sort();
                TestUtils.assertEquals(keyHashCodes, recordHashCodes);
            }
        });
    }

    @Test
    public void testLongKeyRecordHashAndSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
            int keyLength = 20_000_000;
            try (
                    Map map = newDefaultMap(valueType);
                    DirectUtf8Sink sinkA = new DirectUtf8Sink(keyLength)
            ) {
                for (int i = 0; i < keyLength; i++) {
                    sinkA.put('k');
                }
                MapKey mapKey = map.withKey();
                TestDirectUtf8String directUtf8 = new TestDirectUtf8String(true);
                directUtf8.of(sinkA.lo(), sinkA.hi(), true);
                mapKey.putVarchar(directUtf8);
                mapKey.commit();
                long hashCode1 = mapKey.hash();

                MapValue value = mapKey.createValue();
                Assert.assertTrue(value.isNew());

                RecordCursor cursor = map.getCursor();
                MapRecord record = map.getRecord();
                Assert.assertTrue(cursor.hasNext());
                int size = record.getVarcharSize(1);
                Assert.assertEquals(keyLength, size);

                long hashcode2 = record.keyHashCode();
                Assert.assertEquals(hashCode1, hashcode2);

                Assert.assertFalse(cursor.hasNext());
            }
        });
    }

    @Test
    public void testMerge() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (
                DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
                DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
                UnorderedVarcharMap mapA = newDefaultMap(valueType);
                UnorderedVarcharMap mapB = newDefaultMap(valueType)
        ) {
            int keyCountA = 100;
            int keyCountB = 200;
            for (int i = 0; i < keyCountA; i++) {
                putStable("foo" + i, i, mapA, sinkA, true);
            }

            for (int i = 0; i < keyCountB; i++) {
                putStable("foo" + i, i, mapB, sinkB, true);
            }

            mapA.merge(mapB, (dstValue, srcValue) -> dstValue.putInt(0, dstValue.getInt(0) + srcValue.getInt(0)));

            for (int i = 0; i < keyCountA; i++) {
                Assert.assertEquals(i * 2, get("foo" + i, mapA));
            }
            for (int i = keyCountA; i < keyCountB; i++) {
                Assert.assertEquals(i, get("foo" + i, mapA));
            }
        }
    }

    @Test
    public void testMergeUnstable() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (UnorderedVarcharMap mapA = newDefaultMap(valueType)) {
            int keyCountA = 100;
            int keyCountB = 200;
            try (UnorderedVarcharMap mapB = newDefaultMap(valueType)) {
                for (int i = 0; i < keyCountA; i++) {
                    putUnstable("foo" + i, i, mapA, true);
                }
                for (int i = 0; i < keyCountB; i++) {
                    putUnstable("foo" + i, i, mapB, true);
                }
                mapA.merge(mapB, (dstValue, srcValue) -> dstValue.putInt(0, dstValue.getInt(0) + srcValue.getInt(0)));
            }

            for (int i = 0; i < keyCountA; i++) {
                Assert.assertEquals(i * 2, get("foo" + i, mapA));
            }
            for (int i = keyCountA; i < keyCountB; i++) {
                Assert.assertEquals(i, get("foo" + i, mapA));
            }
        }
    }

    @Test
    public void testNullKey() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (
                DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
                DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
                UnorderedVarcharMap map = newDefaultMap(valueType)
        ) {
            Assert.assertNull(findValue(null, map));
            putStable(null, 42, map, sinkA, true);
            Assert.assertEquals(42, get(null, map));
            Assert.assertEquals(1, map.size());

            putStable(null, 43, map, sinkB, false);
            Assert.assertEquals(43, get(null, map));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testRehashing() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (
                DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
                DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
                UnorderedVarcharMap map = newDefaultMap(valueType)
        ) {
            int keyCount = 1_000;
            for (int i = 0; i < keyCount; i++) {
                putStable("foo" + i, i, map, sinkA, true);
            }

            for (int i = 0; i < keyCount; i++) {
                Assert.assertEquals(i, get("foo" + i, map));
            }

            sinkB.clear();
            for (int i = 0; i < keyCount; i++) {
                putStable("foo" + i, -i, map, sinkB, false);
            }
            for (int i = 0; i < keyCount; i++) {
                Assert.assertEquals(-i, get("foo" + i, map));
            }
        }
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
            UnorderedVarcharMap danglingMap;
            try (
                    DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
                    DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
                    UnorderedVarcharMap map = newDefaultMap(valueType)
            ) {
                danglingMap = map;

                putStable("foo", 42, map, sinkA, true);
                Assert.assertEquals(42, get("foo", map));

                Assert.assertNull(findValue("bar", map));
                sinkB.clear();
                Assert.assertEquals(42, get("foo", map));

                putStable("foo", 43, map, sinkB, false);
                Assert.assertEquals(43, get("foo", map));

                map.clear();
                sinkA.clear();
                int keyCount = 1_000;
                for (int i = 0; i < keyCount; i++) {
                    putStable("foo" + i, i, map, sinkA, true);
                }
                for (int i = 0; i < keyCount; i++) {
                    Assert.assertEquals(i, get("foo" + i, map));
                }
                for (int i = 0; i < keyCount; i++) {
                    putUnstable("foo" + i, i, map, false);
                }

                map.clear();
                for (int i = 0; i < keyCount; i++) {
                    putUnstable("foo" + i, i, map, true);
                }
                for (int i = 0; i < keyCount; i++) {
                    putStable("foo" + i, i, map, sinkA, false);
                }

                for (int i = 0; i < keyCount; i++) {
                    MapValue value = findValue("foo" + i, map);
                    Assert.assertNotNull(value);
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(i, value.getInt(0));
                }
            }
            danglingMap.close();
        });
    }

    @Test
    public void testTopK() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int heapCapacity = 7;
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (
                    UnorderedVarcharMap map = newDefaultMap(valueTypes);
                    DirectLongLongHeap heap = new DirectLongLongMinHeap(heapCapacity, MemoryTag.NATIVE_DEFAULT)
            ) {
                for (int i = 0; i < 100; i++) {
                    MapKey key = map.withKey();
                    key.putVarchar(String.valueOf(i));

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

    private static void assertCursor(MapRecordCursor cursor, int keyCount) throws NumericException {
        BitSet keys = new BitSet();
        boolean nullObserved = false;
        boolean emptyObserved = false;
        MapRecord record = cursor.getRecord();
        while (cursor.hasNext()) {
            Utf8Sequence varcharA = record.getVarcharA(1);
            int varcharSize = record.getVarcharSize(1);
            int value = record.getInt(0);
            if (value >= 0) {
                int n = Numbers.parseInt(varcharA);
                Assert.assertEquals(varcharA.size(), varcharSize);
                Assert.assertEquals(value, n);
                Assert.assertFalse(keys.getAndSet(n));
            } else if (value == -1) {
                TestUtils.assertEquals("", varcharA);
                Assert.assertEquals(0, varcharSize);
                Assert.assertFalse(emptyObserved);
                emptyObserved = true;
            } else if (value == -2) {
                Assert.assertEquals(-1, varcharSize);
                Assert.assertNull(varcharA);
                Assert.assertFalse(nullObserved);
                nullObserved = true;
            }
        }
        Assert.assertTrue(nullObserved);
        Assert.assertTrue(emptyObserved);
        for (int i = 0; i < keyCount; i++) {
            Assert.assertTrue(keys.get(i));
        }
    }

    private static MapValue findValue(String stringKey, UnorderedVarcharMap map) {
        MapKey mapKey = map.withKey();
        if (stringKey == null) {
            mapKey.putVarchar((Utf8Sequence) null);
            return mapKey.findValue();
        }
        try (DirectUtf8Sink sink = new DirectUtf8Sink(stringKey.length() * 4L)) {
            sink.put(stringKey);
            TestDirectUtf8String key = new TestDirectUtf8String(false);
            key.of(sink.lo(), sink.hi(), sink.isAscii());
            mapKey.putVarchar(key);
            return mapKey.findValue();
        }
    }

    private static int get(String stringKey, UnorderedVarcharMap map) {
        MapValue value = findValue(stringKey, map);
        Assert.assertNotNull(value);
        Assert.assertFalse(value.isNew());
        return value.getInt(0);
    }

    private static UnorderedVarcharMap newDefaultMap(ColumnTypes valueTypes) {
        return new UnorderedVarcharMap(valueTypes, 16, 0.6, Integer.MAX_VALUE, 128 * 1024, 4 * Numbers.SIZE_1GB);
    }

    private static MapKey putStable(String stringKey, int intValue, UnorderedVarcharMap map, DirectUtf8Sink sink, boolean isNew) {
        MapKey mapKey = map.withKey();
        if (stringKey == null) {
            mapKey.putVarchar((Utf8Sequence) null);
        } else {
            long lo = sink.hi();
            sink.put(stringKey);
            TestDirectUtf8String key = new TestDirectUtf8String(true);
            key.of(lo, sink.hi(), Chars.isAscii(stringKey));
            mapKey.putVarchar(key);
        }
        MapValue value = mapKey.createValue();
        Assert.assertNotNull(value);
        Assert.assertEquals(isNew, value.isNew());
        value.putInt(0, intValue);

        return mapKey;
    }

    private static void putUnstable(String stringKey, int intValue, UnorderedVarcharMap map, boolean isNew) {
        MapKey mapKey = map.withKey();
        if (stringKey == null) {
            mapKey.putVarchar((Utf8Sequence) null);
        } else {
            mapKey.putVarchar(new Utf8String(stringKey));
        }
        MapValue value = mapKey.createValue();
        Assert.assertNotNull(value);
        Assert.assertEquals(isNew, value.isNew());
        value.putInt(0, intValue);
    }
}
