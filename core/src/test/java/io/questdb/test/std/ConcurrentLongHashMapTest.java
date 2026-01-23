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

import io.questdb.std.ConcurrentLongHashMap;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentLongHashMapTest {

    @Test
    public void testCompute() {
        ConcurrentLongHashMap<Long> map = identityMap();
        // add
        Assert.assertEquals(42, (long) map.compute(42, (k, v) -> 42L));
        // ignore
        map.compute(24, (k, v) -> null);
        Assert.assertFalse(map.containsKey(24));
        // replace
        Assert.assertEquals(42, (long) map.compute(1, (k, v) -> 42L));
        // remove
        map.compute(2, (k, v) -> null);
        Assert.assertFalse(map.containsKey(2));
    }

    @Test
    public void testComputeIfAbsent() {
        ConcurrentLongHashMap<Long> map = identityMap();

        map.putIfAbsent(42, 42L);
        Assert.assertTrue(map.containsKey(42));

        Assert.assertEquals(1, (long) map.computeIfAbsent(1, k -> 42L));

        map.computeIfAbsent(142, k -> null);
        Assert.assertFalse(map.containsKey(142));
    }

    @Test
    public void testComputeIfPresent() {
        ConcurrentLongHashMap<Long> map = identityMap();

        map.computeIfPresent(42, (k, v) -> 42L);
        Assert.assertFalse(map.containsKey(42));

        Assert.assertEquals(42, (long) map.computeIfPresent(1, (k, v) -> 42L));
    }

    @Test
    public void testNegativeKey() {
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>();
        Assert.assertNull(map.get(-1));
        Assert.assertThrows(IllegalArgumentException.class, () -> map.put(-2, "a"));
        Assert.assertThrows(IllegalArgumentException.class, () -> map.putIfAbsent(-3, "b"));
        Assert.assertThrows(IllegalArgumentException.class, () -> map.compute(-4, (val1, val2) -> val2));
        Assert.assertThrows(IllegalArgumentException.class, () -> map.computeIfAbsent(-5, (val) -> "c"));
        Assert.assertThrows(IllegalArgumentException.class, () -> map.computeIfPresent(-5, (val1, val2) -> val2));
    }

    @Test
    public void testSmoke() {
        ConcurrentLongHashMap<String> map = new ConcurrentLongHashMap<>(4);
        map.put(1, "1");
        map.put(2, "2");
        map.put(3, "3");
        map.put(4, "4");
        map.put(5, "5");
        map.putIfAbsent(5, "Hello");
        Assert.assertEquals(5, map.size());
        Assert.assertEquals(map.get(5), "5");
        Assert.assertNull(map.get(42));

        ConcurrentLongHashMap.KeySetView<Boolean> ks = ConcurrentLongHashMap.newKeySet(4);
        ks.add(1);
        ks.add(2);
        ks.add(3);
        ks.add(4);
        ks.add(5);
        Assert.assertEquals(5, ks.size());
    }

    private static ConcurrentLongHashMap<Long> identityMap() {
        ConcurrentLongHashMap<Long> identity = new ConcurrentLongHashMap<>(3, 0.9f);
        Assert.assertTrue(identity.isEmpty());
        identity.put(1, 1L);
        identity.put(2, 2L);
        identity.put(3, 3L);
        Assert.assertFalse(identity.isEmpty());
        Assert.assertEquals(3, identity.size());
        return identity;
    }
}
