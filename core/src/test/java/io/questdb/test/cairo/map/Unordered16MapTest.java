/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.Unordered16Map;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Objects;

public class Unordered16MapTest extends AbstractCairoTest {

    @Test
    public void testFactoryRequiresExplicitCompactKeyOptIn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Map defaultMap = MapFactory.createUnorderedMap(configuration, keyTypes(), null)) {
                Assert.assertFalse(defaultMap instanceof Unordered16Map);
            }
            try (Map compactMap = MapFactory.createUnorderedMap(
                    configuration,
                    keyTypes(),
                    null,
                    configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlSmallMapPageSize(),
                    false,
                    true,
                    true
            )) {
                Assert.assertTrue(compactMap instanceof Unordered16Map);
            }
        });
    }

    @Test
    public void testFuzzInsertFindAndCursor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final HashMap<Pair, Long> oracle = new HashMap<>();
            final SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);

            try (Unordered16Map map = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE)) {
                final int n = 50_000;
                for (int i = 0; i < n; i++) {
                    final int keyLo = i == 0 ? 0 : rnd.nextInt();
                    final long keyHi = i == 0 ? 0 : rnd.nextLong();
                    final Pair pair = new Pair(keyLo, keyHi);
                    final Long expected = oracle.get(pair);

                    final MapKey key = map.withKey();
                    key.putInt(keyLo);
                    key.putLong(keyHi);
                    final MapValue value = key.createValue();
                    Assert.assertEquals(expected == null, value.isNew());
                    if (expected == null) {
                        value.putLong(0, i);
                        oracle.put(pair, (long) i);
                    } else {
                        Assert.assertEquals(expected.longValue(), value.getLong(0));
                    }
                }

                Assert.assertEquals(oracle.size(), map.size());
                for (Entry<Pair, Long> entry : oracle.entrySet()) {
                    final MapKey key = map.withKey();
                    key.putInt(entry.getKey().lo);
                    key.putLong(entry.getKey().hi);
                    final MapValue value = key.findValue();
                    Assert.assertNotNull(value);
                    Assert.assertFalse(value.isNew());
                    Assert.assertEquals(entry.getValue().longValue(), value.getLong(0));
                }

                final HashMap<Pair, Long> cursorContents = new HashMap<>();
                try (MapRecordCursor cursor = map.getCursor()) {
                    final MapRecord record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        cursorContents.put(new Pair(record.getInt(1), record.getLong(2)), record.getLong(0));
                    }
                    Assert.assertEquals(oracle, cursorContents);

                    cursor.toTop();
                    Assert.assertTrue(cursor.hasNext());
                    final long rowId = record.getRowId();
                    final Pair pair = new Pair(record.getInt(1), record.getLong(2));
                    cursor.recordAt(cursor.getRecordB(), rowId);
                    Assert.assertEquals(pair.lo, cursor.getRecordB().getInt(1));
                    Assert.assertEquals(pair.hi, cursor.getRecordB().getLong(2));
                }
            }
        });
    }

    @Test
    public void testLazyOpenClearAndReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered16Map map = new Unordered16Map(
                    keyTypes(),
                    new SingleColumnType(ColumnType.LONG),
                    16,
                    0.7,
                    24,
                    false
            )) {
                Assert.assertFalse(map.isOpen());
                map.clear();
                Assert.assertFalse(map.isOpen());

                map.reopen();
                put(map, 1, 2, 3);
                Assert.assertEquals(1, map.size());

                // clear() only resets compact control bytes. Stale entry/value bytes must
                // neither appear in the cursor nor leak into a newly inserted value.
                map.clear();
                Assert.assertEquals(0, map.size());
                Assert.assertFalse(map.getCursor().hasNext());
                final MapKey reusedKey = map.withKey();
                reusedKey.putInt(1);
                reusedKey.putLong(2);
                final MapValue reusedValue = reusedKey.createValue();
                Assert.assertTrue(reusedValue.isNew());
                Assert.assertEquals(0, reusedValue.getLong(0));
                reusedValue.putLong(0, 7);
                Assert.assertEquals(7, find(map, 1, 2));

                map.close();
                Assert.assertFalse(map.isOpen());
                map.reopen();
                Assert.assertEquals(0, map.size());
                put(map, 4, 5, 6);
                Assert.assertEquals(1, map.size());
            }
        });
    }

    @Test
    public void testMergeAndCopyToKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
            try (
                    Unordered16Map mapA = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    Unordered16Map mapB = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    Unordered16Map copy = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE)
            ) {
                for (int i = 0; i < 1_000; i++) {
                    put(mapA, i, i * 17L, 1);
                }
                for (int i = 500; i < 1_500; i++) {
                    put(mapB, i, i * 17L, 2);
                }

                mapA.merge(mapB, (destValue, srcValue) -> destValue.addLong(0, srcValue.getLong(0)));
                Assert.assertEquals(1_500, mapA.size());
                for (int i = 0; i < 1_500; i++) {
                    Assert.assertEquals(i < 500 ? 1 : i < 1_000 ? 3 : 2, find(mapA, i, i * 17L));
                }

                try (MapRecordCursor cursor = mapA.getCursor()) {
                    final MapRecord record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        final MapKey copyKey = copy.withKey();
                        record.copyToKey(copyKey);
                        final MapValue copyValue = copyKey.createValue(record.keyHashCode());
                        Assert.assertTrue(copyValue.isNew());
                        copyValue.putLong(0, record.getLong(0));
                    }
                }

                Assert.assertEquals(mapA.size(), copy.size());
                for (int i = 0; i < 1_500; i++) {
                    Assert.assertEquals(i < 500 ? 1 : i < 1_000 ? 3 : 2, find(copy, i, i * 17L));
                }
            }
        });
    }

    @Test
    public void testMergeReportsOnlyNewEntries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
            try (
                    Unordered16Map dest = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    Unordered16Map src = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    Unordered16Map zeroDest = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    Unordered16Map zeroSrc = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE)
            ) {
                for (int i = 0; i < 1_000; i++) {
                    put(dest, i, i * 17L, 1);
                }
                for (int i = 500; i < 1_500; i++) {
                    put(src, i, i * 17L, 2);
                }

                final HashMap<Pair, Long> admitted = new HashMap<>();
                dest.merge(
                        src,
                        (destValue, srcValue) -> destValue.addLong(0, srcValue.getLong(0)),
                        record -> admitted.put(new Pair(record.getInt(1), record.getLong(2)), record.getLong(0))
                );
                Assert.assertEquals(500, admitted.size());
                for (int i = 1_000; i < 1_500; i++) {
                    Assert.assertEquals(Long.valueOf(2), admitted.get(new Pair(i, i * 17L)));
                }

                put(zeroSrc, 0, 0, 7);
                final int[] zeroCallbackCount = {0};
                zeroDest.merge(zeroSrc, (destValue, srcValue) -> {
                }, record -> {
                    zeroCallbackCount[0]++;
                    Assert.assertEquals(0, record.getInt(1));
                    Assert.assertEquals(0, record.getLong(2));
                    Assert.assertEquals(7, record.getLong(0));
                });
                Assert.assertEquals(1, zeroCallbackCount[0]);
            }
        });
    }

    @Test
    public void testMergeZeroKey() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final SingleColumnType valueTypes = new SingleColumnType(ColumnType.LONG);
            try (
                    Unordered16Map dest = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    Unordered16Map src = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE);
                    Unordered16Map emptyDest = new Unordered16Map(keyTypes(), valueTypes, 16, 0.7, Integer.MAX_VALUE)
            ) {
                put(dest, 0, 0, 1);
                put(src, 0, 0, 2);

                dest.merge(src, (destValue, srcValue) -> destValue.addLong(0, srcValue.getLong(0)));
                Assert.assertEquals(1, dest.size());
                Assert.assertEquals(3, find(dest, 0, 0));

                emptyDest.merge(src, (destValue, srcValue) -> destValue.addLong(0, srcValue.getLong(0)));
                Assert.assertEquals(1, emptyDest.size());
                Assert.assertEquals(2, find(emptyDest, 0, 0));
            }
        });
    }

    @Test
    public void testZeroAndPartiallyZeroKeysAreDistinct() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Unordered16Map map = new Unordered16Map(
                    keyTypes(),
                    new SingleColumnType(ColumnType.LONG),
                    16,
                    0.7,
                    24
            )) {
                put(map, 0, 0, 10);
                put(map, 0, 42, 20);
                put(map, 7, 0, 30);
                put(map, -1, -1, 40);

                Assert.assertEquals(4, map.size());
                Assert.assertEquals(10, find(map, 0, 0));
                Assert.assertEquals(20, find(map, 0, 42));
                Assert.assertEquals(30, find(map, 7, 0));
                Assert.assertEquals(40, find(map, -1, -1));

                final MapKey missing = map.withKey();
                missing.putInt(7);
                missing.putLong(42);
                Assert.assertNull(missing.findValue());
            }
        });
    }

    private static long find(Unordered16Map map, int keyLo, long keyHi) {
        final MapKey key = map.withKey();
        key.putInt(keyLo);
        key.putLong(keyHi);
        final MapValue value = key.findValue();
        Assert.assertNotNull(value);
        return value.getLong(0);
    }

    private static ArrayColumnTypes keyTypes() {
        return new ArrayColumnTypes().add(ColumnType.INT).add(ColumnType.LONG);
    }

    private static void put(Unordered16Map map, int keyLo, long keyHi, long value) {
        final MapKey key = map.withKey();
        key.putInt(keyLo);
        key.putLong(keyHi);
        final MapValue mapValue = key.createValue();
        Assert.assertTrue(mapValue.isNew());
        mapValue.putLong(0, value);
    }

    private static final class Pair {
        private final long hi;
        private final int lo;

        private Pair(int lo, long hi) {
            this.lo = lo;
            this.hi = hi;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Pair)) {
                return false;
            }
            final Pair that = (Pair) obj;
            return lo == that.lo && hi == that.hi;
        }

        @Override
        public int hashCode() {
            return Objects.hash(lo, hi);
        }
    }

}
