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

import io.questdb.std.DirectIntLongHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class DirectIntLongHashMapTest {

    @Test
    public void testAll() {
        Rnd rnd = new Rnd();
        // populate map
        try (DirectIntLongHashMap map = new DirectIntLongHashMap(4, 0.5, Integer.MIN_VALUE, Integer.MIN_VALUE, MemoryTag.NATIVE_DEFAULT)) {
            map.put(Integer.MAX_VALUE, Long.MAX_VALUE);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(Long.MAX_VALUE, map.get(Integer.MAX_VALUE));
            map.clear();

            final int N = 1000;
            for (int i = 0; i < N; i++) {
                int value = i + 1;
                map.put(i, value);
            }
            Assert.assertEquals(N, map.size());

            rnd.reset();

            // assert that map contains the values we just added
            for (int i = 0; i < N; i++) {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals(i + 1, map.get(i));
            }

            Rnd rnd2 = new Rnd();

            rnd.reset();

            rnd2.reset();
            rnd.reset();

            Rnd rnd3 = new Rnd();

            // assert that keys we didn't remove are still there and
            // keys we removed are not
            for (int i = 0; i < N; i++) {
                int value = rnd.nextInt();
                Assert.assertFalse(map.excludes(i));

                long index = map.keyIndex(i);
                Assert.assertEquals(i + 1, map.valueAt(index));

                // update value
                map.putAt(index, value, rnd3.nextInt());
            }

            // assert that update is visible correctly
            rnd3.reset();
            rnd2.reset();
            rnd.reset();

            // assert that keys we didn't remove are still there and
            // keys we removed are not
            for (int i = 0; i < N; i++) {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals(rnd3.nextInt(), map.get(i));
            }

            map.restoreInitialCapacity();
            Assert.assertEquals(0, map.size());
            Assert.assertEquals(8, map.capacity());

            // test putIfAbsent
            for (int i = 0; i < N; i++) {
                int value = i + 1;
                Assert.assertTrue(map.putIfAbsent(i, value));
                Assert.assertFalse(map.putIfAbsent(i, value));
            }
            Assert.assertEquals(N, map.size());

            // assert that map contains the values we just added
            for (int i = 0; i < N; i++) {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals(i + 1, map.get(i));
            }
        }
    }
}
