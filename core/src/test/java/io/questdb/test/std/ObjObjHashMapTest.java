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

package io.questdb.test.std;

import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.ref.WeakReference;

public class ObjObjHashMapTest {
    @Test
    public void testAll() {

        Rnd rnd = new Rnd();
        // populate map
        ObjObjHashMap<Integer, CharSequence> map = new ObjObjHashMap<>();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            map.put(rnd.nextInt(), cs.toString());
        }
        Assert.assertEquals(N, map.size());

        rnd.reset();

        // assert that map contains the values we just added
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            int value = rnd.nextInt();
            Assert.assertFalse(map.excludes(value));
            Assert.assertEquals(cs, map.get(value));
        }

        Rnd rnd2 = new Rnd();

        rnd.reset();

        // remove some keys and assert that the size() complies
        int removed = 0;
        for (int i = 0; i < N; i++) {
            rnd.nextChars(15);
            int value = rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.remove(value) > -1);
                Assert.assertEquals(-1, map.remove((value * 872) ^ value));
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
            CharSequence cs = rnd.nextChars(15);
            int value = rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.excludes(value));
            } else {
                Assert.assertFalse(map.excludes(value));

                int index = map.keyIndex(value);
                TestUtils.assertEquals(cs, map.valueAt(index));

                // update value
                map.putAt(index, value, rnd3.nextChars(5));
            }
        }

        // assert that update is visible correctly

        rnd3.reset();
        rnd2.reset();
        rnd.reset();

        // assert that keys we didn't remove are still there and
        // keys we removed are not
        for (int i = 0; i < N; i++) {
            rnd.nextChars(15);
            int value = rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.excludes(value));
            } else {
                Assert.assertFalse(map.excludes(value));
                TestUtils.assertEquals(rnd3.nextChars(3), map.get(value));
            }
        }
    }

    @Test
    public void testClearReleasesValues() {
        final ObjObjHashMap<Integer, Object> map = new ObjObjHashMap<>();
        // more values than the initial capacity, so that the map rehashes
        final ObjList<WeakReference<Object>> values = putValues(map, 100);
        map.clear();
        Assert.assertEquals(0, map.size());
        int retainedCount = values.size();
        for (int attempt = 0; attempt < 10 && retainedCount > 0; attempt++) {
            System.gc();
            retainedCount = 0;
            for (int i = 0, n = values.size(); i < n; i++) {
                if (values.getQuick(i).get() != null) {
                    retainedCount++;
                }
            }
        }
        Assert.assertEquals("values the cleared map keeps", 0, retainedCount);

        // the cleared map works as a new one
        Assert.assertTrue(map.excludes(1));
        Assert.assertNull(map.get(1));
        map.put(1, "one");
        Assert.assertEquals(1, map.size());
        Assert.assertEquals("one", map.get(1));
        Assert.assertNull(map.get(2));
    }

    // Puts count values into the map, and returns weak references to them, so that only the map keeps them.
    private static ObjList<WeakReference<Object>> putValues(ObjObjHashMap<Integer, Object> map, int count) {
        final ObjList<WeakReference<Object>> values = new ObjList<>();
        for (int i = 0; i < count; i++) {
            final Object value = new Object();
            map.put(i, value);
            values.add(new WeakReference<>(value));
        }
        Assert.assertEquals(count, map.size());
        return values;
    }
}