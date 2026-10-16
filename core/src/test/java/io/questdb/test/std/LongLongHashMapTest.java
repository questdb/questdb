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

import io.questdb.std.LongLongHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class LongLongHashMapTest {
    private static final long NO_ENTRY_VALUE = -42;

    @Test
    public void testGrowthKeepsEveryEntry() {
        final LongLongHashMap map = newMap(4);
        final Rnd rnd = new Rnd();
        final int n = 10_000;
        for (int i = 0; i < n; i++) {
            map.put(rnd.nextLong(), i);
        }
        Assert.assertEquals(n, map.size());

        rnd.reset();
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(i, map.get(rnd.nextLong()));
        }
    }

    @Test
    public void testPutUpdatesAndMissReturnsNoEntryValue() {
        final LongLongHashMap map = newMap(4);
        map.put(10, 100);
        map.put(20, 200);
        map.put(10, 101);

        Assert.assertEquals(2, map.size());
        Assert.assertEquals(101, map.get(10));
        Assert.assertEquals(200, map.get(20));
        Assert.assertEquals(NO_ENTRY_VALUE, map.get(30));
        Assert.assertTrue(map.excludes(30));
    }

    @Test
    public void testRemoveAtKeepsRemainingKeysReachable() {
        final LongLongHashMap map = newMap(16);
        final int n = 2_000;
        for (int i = 0; i < n; i++) {
            map.put(key(i), i);
        }
        // Removing every other key frees slots in the middle of probe chains; the keys the removal
        // shifts back must stay reachable with their own values.
        for (int i = 0; i < n; i += 2) {
            final int index = map.keyIndex(key(i));
            Assert.assertTrue(index < 0);
            map.removeAt(index);
        }

        Assert.assertEquals(n / 2, map.size());
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(i % 2 == 0 ? NO_ENTRY_VALUE : i, map.get(key(i)));
        }
    }

    @Test
    public void testRestoreInitialCapacityAfterGrowth() {
        final LongLongHashMap map = newMap(4);
        for (int i = 0; i < 1_000; i++) {
            map.put(key(i), i);
        }

        map.restoreInitialCapacity();

        Assert.assertEquals(0, map.size());
        Assert.assertEquals(NO_ENTRY_VALUE, map.get(key(1)));
        for (int i = 0; i < 100; i++) {
            map.put(key(i), -i);
        }
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(-i, map.get(key(i)));
        }
    }

    @Test
    public void testZeroAndMinusOneAreKeysWhenNullMarksEmptySlot() {
        final LongLongHashMap map = newMap(4);
        map.put(0, 1);
        map.put(-1, 2);

        Assert.assertEquals(2, map.size());
        Assert.assertEquals(1, map.get(0));
        Assert.assertEquals(2, map.get(-1));
        Assert.assertEquals(NO_ENTRY_VALUE, map.get(1));
    }

    private static long key(int i) {
        // Spread the keys like partition timestamps, including negative ones.
        return (i - 500L) * 3_600_000_000L;
    }

    private static LongLongHashMap newMap(int capacity) {
        return new LongLongHashMap(capacity, 0.5, Numbers.LONG_NULL, NO_ENTRY_VALUE);
    }
}
