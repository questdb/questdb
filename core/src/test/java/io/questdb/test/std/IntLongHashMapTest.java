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

import io.questdb.std.IntLongHashMap;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class IntLongHashMapTest {

    @Test
    public void testAll() {
        Rnd rnd = new Rnd();
        // populate map
        IntLongHashMap map = new IntLongHashMap();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            long value = i + 1;
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

        // remove some keys and assert that the size() complies
        int removed = 0;
        for (int i = 0; i < N; i++) {
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.remove(i) > -1);
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
            int value = rnd.nextInt();
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.excludes(i));
            } else {
                Assert.assertFalse(map.excludes(i));

                int index = map.keyIndex(i);
                Assert.assertEquals(i + 1, map.valueAt(index));

                // update value
                map.putAt(index, value, rnd3.nextLong());
            }
        }

        // assert that update is visible correctly
        rnd3.reset();
        rnd2.reset();
        rnd.reset();

        // assert that keys we didn't remove are still there and
        // keys we removed are not
        for (int i = 0; i < N; i++) {
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(map.excludes(i));
            } else {
                Assert.assertFalse(map.excludes(i));
                Assert.assertEquals(rnd3.nextLong(), map.get(i));
            }
        }
    }
}
