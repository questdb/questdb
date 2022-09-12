/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConcurrentHashMapTest {
    @Test
    public void testComputeIfAbsent() {
        ConcurrentHashMap<String> map = identityMap();

        map.computeIfAbsent("X", k -> "X");
        assertTrue(map.containsKey("X"));

        assertEquals("A", map.computeIfAbsent("A", k -> "X"));

        map.computeIfAbsent("Y", k -> null);
        assertFalse(map.containsKey("Y"));

        try {
            map.computeIfAbsent(null, k -> null);
            fail("Null key");
        } catch (NullPointerException ignored) {}
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
        } catch (NullPointerException ignored) {}
    }

    @Test
    public void testCompute() {
        ConcurrentHashMap<String> map = identityMap();
        //add
        assertEquals("X", map.compute("X", (k, v) -> "X"));
        //ignore
        map.compute("Y", (k, v) -> null);
        assertFalse(map.containsKey("Y"));
        //replace
        assertEquals("X", map.compute("A", (k, v) -> "X"));
        //remove
        map.compute("B", (k, v) -> null);
        assertFalse(map.containsKey("B"));

        try {
            map.compute(null, (k, v) -> null);
            fail("Null key");
        } catch (NullPointerException ignored) {}
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