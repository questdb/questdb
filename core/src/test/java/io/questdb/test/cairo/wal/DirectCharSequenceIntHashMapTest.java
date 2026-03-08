/*******************************************************************************
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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.wal.DirectCharSequenceIntHashMap;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DirectCharSequenceIntHashMapTest {

    @Test
    public void testClearResetsState() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap()) {
            map.put("first", 5);
            map.put("second", 6);
            Assert.assertEquals(2, map.size());

            map.clear();
            Assert.assertEquals(0, map.size());
            Assert.assertEquals(DirectCharSequenceIntHashMap.NO_ENTRY_VALUE, map.get("first"));
            Assert.assertEquals(-1, map.nextOffset());
        }
    }

    @Test
    public void testCopyToFiltersMinimumValue() {
        try (
                DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap();
                MemoryARW mem = new MemoryCARWImpl(64, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
        ) {
            map.put("low", 1);
            map.put("mid", 5);
            map.put("high", 10);

            int copied = map.copyTo(mem, 5);
            Assert.assertEquals(2, copied);
            List<Map.Entry<String, Integer>> entries = readEntries(mem, copied);
            Assert.assertEquals(2, entries.size());
            Assert.assertEquals("mid", entries.get(0).getKey());
            Assert.assertEquals(Integer.valueOf(5), entries.get(0).getValue());
            Assert.assertEquals("high", entries.get(1).getKey());
            Assert.assertEquals(Integer.valueOf(10), entries.get(1).getValue());
        }
    }

    @Test
    public void testCopyToFuzz() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap(64, 0.5, DirectCharSequenceIntHashMap.NO_ENTRY_VALUE)) {
            Map<String, Integer> values = new HashMap<>();
            List<String> order = new ArrayList<>();
            Rnd rnd = TestUtils.generateRandom(null);

            for (int i = 0; i < 2_000; i++) {
                int op = rnd.nextPositiveInt() & 7;
                if (op == 0) {
                    map.clear();
                    values.clear();
                    order.clear();
                } else {
                    String key = rnd.nextChars((rnd.nextPositiveInt() & 15) + 1).toString();
                    int value = rnd.nextInt();
                    boolean isNew = !values.containsKey(key);
                    map.put(key, value);
                    values.put(key, value);
                    if (isNew) {
                        order.add(key);
                    }
                }

                int minValue = rnd.nextInt();
                try (MemoryARW mem = new MemoryCARWImpl(64, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    int copied = map.copyTo(mem, minValue);
                    List<Map.Entry<String, Integer>> actual = readEntries(mem, copied);
                    List<Map.Entry<String, Integer>> expected = expectedEntries(order, values, minValue);
                    Assert.assertEquals(expected.size(), copied);
                    Assert.assertEquals(expected, actual);
                }
            }
        }
    }

    @Test
    public void testEmptyStringKey() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap()) {
            map.put("", 5);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(5, map.get(""));

            map.put("", 42);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(42, map.get(""));

            int offset = map.nextOffset();
            Assert.assertNotEquals(-1, offset);
            Assert.assertEquals(42, map.get(offset));
            Assert.assertEquals(-1, map.nextOffset(offset));
        }
    }

    @Test
    public void testInvalidLoadFactor() {
        expectIllegalArgument(() -> new DirectCharSequenceIntHashMap(16, 0, 0));
        expectIllegalArgument(() -> new DirectCharSequenceIntHashMap(16, 1, 0));
    }

    @Test
    public void testKeyIndexDetectsExistingEntry() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap()) {
            map.put("existing", 7);
            int idx = map.keyIndex("existing");
            Assert.assertTrue(idx < 0);
            Assert.assertEquals(7, map.get("existing"));
        }
    }

    @Test
    public void testKeyValueBufferReallocation() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap(8, 0.7, DirectCharSequenceIntHashMap.NO_ENTRY_VALUE, 1)) {
            for (int i = 0; i < 100; i++) {
                map.put("key" + i, i);
                Assert.assertEquals(i + 1, map.size());
                Assert.assertEquals(i, map.get("key" + i));
            }
        }
    }

    @Test
    public void testOffsetsIterateInInsertionOrder() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap()) {
            map.put("a", 10);
            map.put("bc", 20);
            map.put("def", 30);

            List<Integer> values = new ArrayList<>();
            int offset = map.nextOffset();
            while (offset != -1) {
                int value = map.get(offset);
                values.add(value);
                offset = map.nextOffset(offset);
            }

            Assert.assertEquals(Arrays.asList(10, 20, 30), values);
        }
    }

    @Test
    public void testOverwriteValueKeepsSize() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap()) {
            map.put("dup", 1);
            Assert.assertEquals(1, map.size());
            map.put("dup", 42);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(42, map.get("dup"));
        }
    }

    @Test
    public void testPutAndGet() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap(32, 0.5, DirectCharSequenceIntHashMap.NO_ENTRY_VALUE)) {
            map.put("alpha", 1);
            map.put("beta", 2);
            map.put("gamma", 3);

            Assert.assertEquals(1, map.get("alpha"));
            Assert.assertEquals(2, map.get("beta"));
            Assert.assertEquals(3, map.get("gamma"));
            Assert.assertEquals(DirectCharSequenceIntHashMap.NO_ENTRY_VALUE, map.get("delta"));
            Assert.assertEquals(3, map.size());
        }
    }

    @Test
    public void testPutAtUsesRawIndex() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap()) {
            CharSequence key = "manual";
            int hashCode = Chars.hashCode(key);
            int index = map.keyIndex(key, hashCode);
            Assert.assertTrue(index >= 0);

            map.putAt(index, key, 123, hashCode);
            Assert.assertEquals(123, map.get(key));
        }
    }

    @Test
    public void testRandomisedInsertUpdateAndDeleteFuzz() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap(64, 0.5, DirectCharSequenceIntHashMap.NO_ENTRY_VALUE)) {
            Map<String, Integer> expected = new HashMap<>();
            Rnd rnd = TestUtils.generateRandom(null);

            for (int i = 0; i < 5_000; i++) {
                int action = rnd.nextPositiveInt() & 7;
                if (action == 0) {
                    map.clear();
                    expected.clear();
                } else {
                    String key = rnd.nextChars((rnd.nextPositiveInt() & 15) + 1).toString();
                    int value = rnd.nextInt();
                    map.put(key, value);
                    expected.put(key, value);
                }

                assertState(map, expected);
            }
        }
    }

    @Test
    public void testRehashPreservesEntries() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap(16, 0.5, DirectCharSequenceIntHashMap.NO_ENTRY_VALUE)) {
            int count = 64;
            for (int i = 0; i < count; i++) {
                map.put("k" + i, i);
            }

            Assert.assertEquals(count, map.size());

            for (int i = 0; i < count; i++) {
                Assert.assertEquals(i, map.get("k" + i));
            }
        }
    }

    @Test
    public void testValueAtMatchesStoredValues() {
        try (DirectCharSequenceIntHashMap map = new DirectCharSequenceIntHashMap()) {
            map.put("alpha", 10);
            map.put("beta", 20);
            map.put("gamma", 30);

            int idx = map.keyIndex("beta");
            Assert.assertTrue(idx < 0);
            Assert.assertEquals(20, map.valueAt(idx));
        }
    }

    private static void assertState(DirectCharSequenceIntHashMap map, Map<String, Integer> expected) {
        Assert.assertEquals(expected.size(), map.size());

        Set<String> iterated = new HashSet<>();
        int offset = map.nextOffset();
        while (offset != -1) {
            CharSequence seq = map.getKey(offset);
            String key = seq.toString();
            Assert.assertTrue("unexpected key: " + key, expected.containsKey(key));
            Assert.assertEquals(expected.get(key).intValue(), map.get(offset));

            int idx = map.keyIndex(key);
            Assert.assertTrue(idx < 0);
            Assert.assertEquals(expected.get(key).intValue(), map.valueAt(idx));

            iterated.add(key);
            offset = map.nextOffset(offset);
        }

        Assert.assertEquals(expected.keySet(), iterated);

        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            Assert.assertEquals(entry.getValue().intValue(), map.get(entry.getKey()));
        }
    }

    private static void expectIllegalArgument(Runnable r) {
        try {
            r.run();
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ignore) {
        }
    }

    private static List<Map.Entry<String, Integer>> expectedEntries(List<String> order, Map<String, Integer> values, int minValue) {
        List<Map.Entry<String, Integer>> result = new ArrayList<>();
        for (String key : order) {
            Integer value = values.get(key);
            if (value != null && value >= minValue) {
                result.add(new AbstractMap.SimpleEntry<>(key, value));
            }
        }
        return result;
    }

    private static List<Map.Entry<String, Integer>> readEntries(MemoryARW mem, int copied) {
        List<Map.Entry<String, Integer>> result = new ArrayList<>(copied);
        long offset = 0;
        for (int i = 0; i < copied; i++) {
            int value = mem.getInt(offset);
            offset += Integer.BYTES;
            String key = mem.getStrA(offset).toString();
            offset += ((long) key.length() << 1) + Integer.BYTES;
            result.add(new AbstractMap.SimpleEntry<>(key, value));
        }
        return result;
    }
}
