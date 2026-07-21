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

import io.questdb.std.ConcurrentHashMap;
import org.junit.Test;

import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.*;

public class ConcurrentHashMapTest {

    @Test
    public void testCaseKey() {
        ConcurrentHashMap<String> map = new ConcurrentHashMap<>(4, false);
        map.put("Table", "1");
        map.put("tAble", "2");
        map.put("TaBle", "3");
        map.put("TABle", "4");
        map.put("TaBLE", "5");
        map.putIfAbsent("TaBlE", "Hello");
        assertEquals(1, map.size());
        assertEquals(map.get("TABLE"), "5");
        assertEquals(((Map<CharSequence, String>) map).get("TABLE"), "5");
        assertNull(((Map<CharSequence, String>) map).get(42));

        ConcurrentHashMap<String> cs = new ConcurrentHashMap<>(5, 0.58F);
        cs.put("Table", "1");
        cs.put("tAble", "2");
        cs.put("TaBle", "3");
        cs.put("TABle", "4");
        cs.put("TaBLE", "5");

        ConcurrentHashMap<String> ccs = new ConcurrentHashMap<>(cs);
        assertEquals(ccs.size(), cs.size());
        assertEquals(ccs.get("TaBLE"), "5");
        assertNull(ccs.get("TABLE"));

        ConcurrentHashMap<String> cci = new ConcurrentHashMap<>(cs, false);
        assertEquals(1, cci.size());
        assertNotNull(cci.get("TaBLE"));

        ConcurrentHashMap<String> ci = new ConcurrentHashMap<>(5, 0.58F, false);
        ci.put("Table", "1");
        ci.put("tAble", "2");
        ci.put("TaBle", "3");
        ci.put("TABle", "4");
        ci.put("TaBLE", "5");
        assertEquals(1, ci.size());

        ConcurrentHashMap.KeySetView<Boolean> ks0 = ConcurrentHashMap.newKeySet(4);
        ks0.add("Table");
        ks0.add("tAble");
        ks0.add("TaBle");
        ks0.add("TABle");
        ks0.add("TaBLE");
        assertEquals(5, ks0.size());
        ConcurrentHashMap.KeySetView<Boolean> ks1 = ConcurrentHashMap.newKeySet(4, false);
        ks1.add("Table");
        ks1.add("tAble");
        ks1.add("TaBle");
        ks1.add("TABle");
        ks1.add("TaBLE");
        assertEquals(1, ks1.size());
    }

    @Test
    public void testCompute() {
        ConcurrentHashMap<String> map = identityMap();
        // add
        assertEquals("X", map.compute("X", (k, v) -> "X"));
        // ignore
        map.compute("Y", (k, v) -> null);
        assertFalse(map.containsKey("Y"));
        // replace
        assertEquals("X", map.compute("A", (k, v) -> "X"));
        // remove
        map.compute("B", (k, v) -> null);
        assertFalse(map.containsKey("B"));

        try {
            map.compute(null, (k, v) -> null);
            fail("Null key");
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testComputeIfAbsent() {
        ConcurrentHashMap<String> map = identityMap();

        map.putIfAbsent("X", "X");
        assertTrue(map.containsKey("X"));

        assertEquals("A", map.computeIfAbsent("A", k -> "X"));

        map.computeIfAbsent("Y", k -> null);
        assertFalse(map.containsKey("Y"));

        try {
            map.computeIfAbsent(null, k -> null);
            fail("Null key");
        } catch (NullPointerException ignored) {
        }
    }

    @Test
    public void testComputeIfAbsentReturnsExistingWithoutInvokingMapping() {
        // Exercises the lock-free first-node fast path: when the key is already present,
        // computeIfAbsent must return the stored value and must NOT invoke the mapping function.
        ConcurrentHashMap<String> map = new ConcurrentHashMap<>(4);
        final int n = 256;
        for (int i = 0; i < n; i++) {
            map.put("k" + i, "v" + i);
        }
        assertEquals(n, map.size());

        final Function<CharSequence, String> failing = k -> {
            throw new AssertionError("mapping function must not run for present key: " + k);
        };
        // Every present key (whether bin head or further down a chain/tree) resolves without the mapping.
        // A fresh String instance forces the keyEquals comparison rather than reference equality.
        for (int i = 0; i < n; i++) {
            assertEquals("v" + i, map.computeIfAbsent(new StringBuilder("k").append(i).toString(), failing));
        }
        assertEquals(n, map.size());

        // The token (BiFunction) overload makes the same guarantee for a present key.
        assertEquals("v0", map.computeIfAbsent("k0", new Object(), (k, token) -> {
            throw new AssertionError("mapping function must not run for present key");
        }));
        assertEquals(n, map.size());

        // An absent key still falls through to the locked path and inserts.
        assertEquals("inserted", map.computeIfAbsent("absent", k -> "inserted"));
        assertTrue(map.containsKey("absent"));
        assertEquals(n + 1, map.size());
    }

    @Test
    public void testComputeIfPresent() {
        ConcurrentHashMap<String> map = identityMap();

        map.computeIfPresent("X", (k, v) -> "X");
        assertFalse(map.containsKey("X"));

        assertEquals("X", map.computeIfPresent("A", (k, v) -> "X"));

        try {
            map.computeIfPresent(null, (k, v) -> null);
            fail("Null key");
        } catch (NullPointerException ignored) {
        }
    }

    private static ConcurrentHashMap<String> identityMap() {
        ConcurrentHashMap<String> identity = new ConcurrentHashMap<>(3);
        assertTrue(identity.isEmpty());
        identity.put("A", "A");
        identity.put("B", "B");
        identity.put("C", "C");
        assertFalse(identity.isEmpty());
        assertEquals(3, identity.size());
        return identity;
    }
}
