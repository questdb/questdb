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

package io.questdb.test.std;

import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Utf8SequenceLongHashMap;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import org.junit.Assert;
import org.junit.Test;

public class Utf8SequenceLongHashMapTest {

    @Test
    public void testAll() {
        Rnd rnd = new Rnd();
        Utf8SequenceLongHashMap map = new Utf8SequenceLongHashMap();
        final int N = 1000;

        // populate map
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            boolean b = map.put(utf8, rnd.nextLong());
            Assert.assertTrue(b);
        }
        Assert.assertEquals(N, map.size());

        rnd.reset();

        // assert that map contains the values we just added
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            Assert.assertFalse(map.excludes(utf8));
            Assert.assertEquals(rnd.nextLong(), map.get(utf8));
        }

        Rnd rnd2 = new Rnd();
        rnd.reset();

        // remove some keys and assert that the size() complies
        int removed = 0;
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            rnd.nextLong();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.remove(utf8) > -1);
                removed++;
                Assert.assertEquals(N - removed, map.size());
            }
        }

        // if we didn't remove anything test has no value
        Assert.assertTrue(removed > 0);

        rnd2.reset();
        rnd.reset();

        Rnd rnd3 = new Rnd();

        // assert that keys we didn't remove are still there and
        // keys we removed are not
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            long value = rnd.nextLong();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.excludes(utf8));
            } else {
                Assert.assertFalse(map.excludes(utf8));
                int index = map.keyIndex(utf8);
                Assert.assertEquals(value, map.valueAt(index));
                // update value
                map.putAt(index, utf8, rnd3.nextLong());
            }
        }

        rnd.reset();
        rnd2.reset();
        rnd3.reset();

        // increment values
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            rnd.nextLong();
            if (rnd2.nextPositiveInt() % 16 != 0) {
                map.inc(utf8);
                int index = map.keyIndex(utf8);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(rnd3.nextLong() + 1, map.valueAt(index));
            } else {
                map.inc(utf8);
                int index = map.keyIndex(utf8);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(1, map.valueAt(index));
            }
        }

        // put all values into another map
        Utf8SequenceLongHashMap map2 = new Utf8SequenceLongHashMap();
        map2.putAll(map);

        // assert values
        rnd.reset();
        rnd2.reset();
        rnd3.reset();
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            rnd.nextLong();
            if (rnd2.nextPositiveInt() % 16 != 0) {
                int index = map2.keyIndex(utf8);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(rnd3.nextLong() + 1, map2.valueAt(index));
            } else {
                int index = map2.keyIndex(utf8);
                Assert.assertTrue(index < 0);
                Assert.assertEquals(1, map2.valueAt(index));
            }
        }

        // re-populate map
        rnd.reset();
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            map.put(utf8, rnd.nextLong());
        }

        // remove some keys again
        rnd.reset();
        rnd2.reset();
        removed = 0;
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            rnd.nextLong();
            if (rnd2.nextPositiveInt() % 4 == 0) {
                map.remove(utf8);
                removed++;
                Assert.assertEquals(N - removed, map.size());
            }
        }

        // assert putIfAbsent behavior
        rnd.reset();
        rnd2.reset();
        rnd3.reset();

        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            rnd.nextLong();
            // putIfAbsent is not available in Utf8SequenceLongHashMap, so we simulate it
            if (map.excludes(utf8)) {
                map.put(utf8, rnd3.nextLong());
            } else {
                rnd3.nextLong(); // consume the value
            }
        }

        // assert final values
        rnd.reset();
        rnd2.reset();
        rnd3.reset();
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(15));
            long value1 = rnd.nextLong();
            long value2 = rnd3.nextLong();
            if (rnd2.nextPositiveInt() % 4 == 0) {
                Assert.assertEquals(value2, map.get(utf8));
            } else {
                Assert.assertEquals(value1, map.get(utf8));
            }
        }
    }

    @Test
    public void testClear() {
        Utf8SequenceLongHashMap map = new Utf8SequenceLongHashMap();
        Rnd rnd = new Rnd();
        final int N = 100;

        // populate map
        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(10));
            map.put(utf8, rnd.nextLong());
        }

        Assert.assertEquals(N, map.size());

        // clear and verify
        map.clear();
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(0, map.keys().size());

        // verify we can add again after clear
        Utf8String testKey = new Utf8String("test");
        map.put(testKey, 123L);
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(123L, map.get(testKey));
    }

    @Test
    public void testContains() {
        Utf8SequenceLongHashMap map = new Utf8SequenceLongHashMap();
        Rnd rnd = new Rnd();
        final int N = 1000;

        for (int i = 0; i < N; i++) {
            Utf8String utf8 = new Utf8String(rnd.nextString(10));
            map.put(utf8, i);
        }

        final ObjList<Utf8String> keys = map.keys();
        for (int i = 0; i < keys.size(); i++) {
            Assert.assertTrue(map.contains(keys.get(i)));
        }
    }

    @Test
    public void testIncrement() {
        Utf8SequenceLongHashMap map = new Utf8SequenceLongHashMap();
        Utf8String key1 = new Utf8String("test1");
        Utf8String key2 = new Utf8String("test2");

        // Test incrementing non-existent key
        map.inc(key1);
        Assert.assertEquals(1, map.get(key1));

        // Test incrementing existing key
        map.inc(key1);
        Assert.assertEquals(2, map.get(key1));

        // Test incrementing another key
        map.inc(key2);
        Assert.assertEquals(1, map.get(key2));
        Assert.assertEquals(2, map.get(key1)); // first key should be unchanged
    }

    @Test
    public void testNoEntryValue() {
        Utf8SequenceLongHashMap map = new Utf8SequenceLongHashMap();
        Utf8String nonExistentKey = new Utf8String("nonexistent");

        Assert.assertEquals(Utf8SequenceLongHashMap.NO_ENTRY_VALUE, map.get(nonExistentKey));

        int index = map.keyIndex(nonExistentKey);
        Assert.assertTrue(index >= 0); // positive index means key not found
        Assert.assertEquals(Utf8SequenceLongHashMap.NO_ENTRY_VALUE, map.valueAt(index));
    }

    @Test
    public void testUtf8StringSink() {
        Utf8SequenceLongHashMap map = new Utf8SequenceLongHashMap();
        Utf8StringSink sink = new Utf8StringSink();

        // Test with Utf8StringSink
        sink.put("hello").put("world");
        map.put(sink, 42L);

        // Verify we can retrieve using different but equal Utf8String
        Utf8String key = new Utf8String("helloworld");
        Assert.assertEquals(42L, map.get(key));

        sink.clear();
        sink.put("test");
        map.put(sink, 100L);
        Assert.assertEquals(100L, map.get(new Utf8String("test")));
    }
}