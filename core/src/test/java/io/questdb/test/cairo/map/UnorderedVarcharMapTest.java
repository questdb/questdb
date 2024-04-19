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
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.std.Chars;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UnorderedVarcharMapTest extends AbstractCairoTest {

    @Test
    public void testClear() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
             UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE)
        ) {
            put("foo", 42, map, sinkA, true);
            Assert.assertEquals(42, get("foo", map));
            Assert.assertEquals(1, map.size());
            map.clear();
            Assert.assertNull(findValue("foo", map));
            Assert.assertEquals(0, map.size());
        }
    }

    @Test
    public void testCursor() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
             UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE)
        ) {
            int keyCount = 1_000;
            for (int i = 0; i < keyCount; i++) {
                put("foo" + i, i + 1, map, sinkA, true);
            }
            put("", 0, map, sinkA, true);
            put(null, -1, map, sinkA, true);

            try (MapRecordCursor cursor = map.getCursor()) {

            }
        }
    }

    @Test
    public void testEmptyKey() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
             DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
             UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE)
        ) {
            Assert.assertNull(findValue("", map));
            put("", 42, map, sinkA, true);
            Assert.assertEquals(42, get("", map));
            Assert.assertEquals(1, map.size());

            put("", 43, map, sinkB, false);
            Assert.assertEquals(43, get("", map));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testKeyCopyFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SingleColumnType valueTypes = new SingleColumnType(ColumnType.INT);
            try (
                    DirectUtf8Sink sinkA = new DirectUtf8Sink(10 * 1024 * 1024);
                    UnorderedVarcharMap mapA = new UnorderedVarcharMap(valueTypes, 16, 0.6, Integer.MAX_VALUE);
                    UnorderedVarcharMap mapB = new UnorderedVarcharMap(valueTypes, 16, 0.6, Integer.MAX_VALUE)
            ) {
                final int N = 100_000;
                for (int i = 0; i < N; i++) {
                    MapKey keyA = put("foo" + i, i + 1, mapA, sinkA, true);

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
    public void testMerge() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
             DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
             UnorderedVarcharMap mapA = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE);
             UnorderedVarcharMap mapB = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE)
        ) {
            int keyCountA = 100;
            int keyCountB = 200;
            for (int i = 0; i < keyCountA; i++) {
                put("foo" + i, i, mapA, sinkA, true);
            }

            for (int i = 0; i < keyCountB; i++) {
                put("foo" + i, i, mapB, sinkB, true);
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
    public void testNullKey() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
             DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
             UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE)
        ) {
            Assert.assertNull(findValue(null, map));
            put(null, 42, map, sinkA, true);
            Assert.assertEquals(42, get(null, map));
            Assert.assertEquals(1, map.size());

            put(null, 43, map, sinkB, false);
            Assert.assertEquals(43, get(null, map));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testRehashing() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
             DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
             UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE)
        ) {
            int keyCount = 1_000;
            for (int i = 0; i < keyCount; i++) {
                put("foo" + i, i, map, sinkA, true);
            }

            for (int i = 0; i < keyCount; i++) {
                Assert.assertEquals(i, get("foo" + i, map));
            }

            sinkB.clear();
            for (int i = 0; i < keyCount; i++) {
                put("foo" + i, -i, map, sinkB, false);
            }
            for (int i = 0; i < keyCount; i++) {
                Assert.assertEquals(-i, get("foo" + i, map));
            }
        }
    }

    @Test
    public void testSmoke() {
        SingleColumnType valueType = new SingleColumnType(ColumnType.INT);
        try (DirectUtf8Sink sinkA = new DirectUtf8Sink(1024 * 1024);
             DirectUtf8Sink sinkB = new DirectUtf8Sink(1024 * 1024);
             UnorderedVarcharMap map = new UnorderedVarcharMap(valueType, 16, 0.6, Integer.MAX_VALUE)
        ) {
            put("foo", 42, map, sinkA, true);
            Assert.assertEquals(42, get("foo", map));

            Assert.assertNull(findValue("bar", map));
            sinkB.clear();
            Assert.assertEquals(42, get("foo", map));

            put("foo", 43, map, sinkB, false);
            Assert.assertEquals(43, get("foo", map));

            map.clear();
            sinkA.clear();
            int keyCount = 1_000;
            for (int i = 0; i < keyCount; i++) {
                put("foo" + i, i, map, sinkA, true);
            }

            for (int i = 0; i < keyCount; i++) {
                Assert.assertEquals(i, get("foo" + i, map));
            }
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
            DirectUtf8String key = new DirectUtf8String(true);
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

    private static MapKey put(String stringKey, int intValue, UnorderedVarcharMap map, DirectUtf8Sink sink, boolean isNew) {
        MapKey mapKey = map.withKey();
        if (stringKey == null) {
            mapKey.putVarchar((Utf8Sequence) null);
        } else {
            long lo = sink.hi();
            sink.put(stringKey);
            DirectUtf8String key = new DirectUtf8String(true);
            key.of(lo, sink.hi(), Chars.isAscii(stringKey));
            mapKey.putVarchar(key);
        }
        MapValue value = mapKey.createValue();
        Assert.assertNotNull(value);
        Assert.assertEquals(isNew, value.isNew());
        value.putInt(0, intValue);

        return mapKey;
    }
}
