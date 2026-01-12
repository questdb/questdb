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

import io.questdb.std.IntShortHashMap;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IntShortHashMapTest extends AbstractTest {
    private static final short NO_ENTRY_VALUE = -1;
    private IntShortHashMap map;

    @Before
    public void setUp() {
        map = new IntShortHashMap();
    }

    @Test
    public void testBasicOperations() {
        map.put(1, (short) 100);
        Assert.assertEquals("Size should be 1 after single put", 1, map.size());
        Assert.assertEquals("Should retrieve correct value", 100, map.get(1));

        map.put(2, (short) 200);
        Assert.assertEquals("Size should be 2 after second put", 2, map.size());
        Assert.assertEquals("Should retrieve correct value for second key", 200, map.get(2));
    }

    @Test
    public void testClear() {
        map.put(1, (short) 100);
        map.put(2, (short) 200);
        map.clear();

        Assert.assertEquals("Size should be 0 after clear", 0, map.size());
        Assert.assertEquals("Cleared key should return NO_ENTRY_VALUE", NO_ENTRY_VALUE, map.get(1));
        Assert.assertEquals("Cleared key should return NO_ENTRY_VALUE", NO_ENTRY_VALUE, map.get(2));
    }

    @Test
    public void testCollisionHandling() {
        IntShortHashMap smallMap = new IntShortHashMap(2);

        for (int i = 0; i < 5; i++) {
            smallMap.put(i * 16, (short) (i * 100));
        }

        for (int i = 0; i < 5; i++) {
            Assert.assertEquals("Value should be retrievable after collision",
                    (short) (i * 100),
                    smallMap.get(i * 16));
        }
    }

    @Test
    public void testEdgeCases() {
        // Test with Integer.MAX_VALUE and MIN_VALUE
        map.put(Integer.MAX_VALUE, (short) 100);
        map.put(Integer.MIN_VALUE, (short) 200);

        Assert.assertEquals("Should handle MAX_VALUE", 100, map.get(Integer.MAX_VALUE));
        Assert.assertEquals("Should handle MIN_VALUE", 200, map.get(Integer.MIN_VALUE));

        map.put(0, (short) 300);
        Assert.assertEquals("Should handle 0", 300, map.get(0));
    }

    @Test
    public void testInitialState() {
        Assert.assertEquals("New map should be empty", 0, map.size());
        Assert.assertEquals("Non-existent key should return NO_ENTRY_VALUE", NO_ENTRY_VALUE, map.get(1));
    }

    @Test
    public void testOverwrite() {
        map.put(1, (short) 100);
        map.put(1, (short) 150);
        Assert.assertEquals("Size should remain 1 after overwrite", 1, map.size());
        Assert.assertEquals("Should retrieve updated value", 150, map.get(1));
    }

    @Test
    public void testPutAll() {
        IntShortHashMap source = new IntShortHashMap();
        source.put(1, (short) 100);
        source.put(2, (short) 200);

        map.putAll(source);
        Assert.assertEquals("Size should match source map", 2, map.size());
        Assert.assertEquals("Value should match source map", 100, map.get(1));
        Assert.assertEquals("Value should match source map", 200, map.get(2));
    }

    @Test
    public void testRehash() {
        // Fill the map to trigger rehash
        for (int i = 0; i < 20; i++) {
            map.put(i, (short) (i * 100));
        }

        // Verify all entries are still accessible
        for (int i = 0; i < 20; i++) {
            Assert.assertEquals("Value should be preserved after rehash", (short) (i * 100), map.get(i));
        }
    }

    @Test
    public void testRemove() {
        map.put(1, (short) 100);
        map.put(2, (short) 200);

        int index = map.remove(1);
        Assert.assertTrue("Remove should return valid index", index >= 0);
        Assert.assertEquals("Size should decrease after remove", 1, map.size());
        Assert.assertEquals("Removed key should return NO_ENTRY_VALUE", NO_ENTRY_VALUE, map.get(1));
        Assert.assertEquals("Remaining key should still be accessible", 200, map.get(2));
    }

    @Test
    public void testRemoveNonExistent() {
        int index = map.remove(999);
        Assert.assertEquals("Removing non-existent key should return -1", -1, index);
        Assert.assertEquals("Size should remain 0", 0, map.size());
    }
}