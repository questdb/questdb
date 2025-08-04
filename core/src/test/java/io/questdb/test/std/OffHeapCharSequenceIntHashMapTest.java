/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.OffHeapCharSequenceIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OffHeapCharSequenceIntHashMapTest {

    @Test
    public void testBasicOperations() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            // Test empty map
            Assert.assertTrue(map.isEmpty());
            Assert.assertEquals(0, map.size());
            Assert.assertEquals(-1, map.get("nonexistent"));

            // Test put and get
            map.put("key1", 100);
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(100, map.get("key1"));

            // Test update existing key
            map.put("key1", 200);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(200, map.get("key1"));

            // Test multiple keys
            map.put("key2", 300);
            map.put("key3", 400);
            Assert.assertEquals(3, map.size());
            Assert.assertEquals(200, map.get("key1"));
            Assert.assertEquals(300, map.get("key2"));
            Assert.assertEquals(400, map.get("key3"));
        }
    }

    @Test
    public void testClear() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            map.put("key1", 100);
            map.put("key2", 200);
            Assert.assertEquals(2, map.size());

            map.clear();
            Assert.assertTrue(map.isEmpty());
            Assert.assertEquals(0, map.size());
            Assert.assertEquals(-1, map.get("key1"));
            Assert.assertEquals(-1, map.get("key2"));

            // Test that we can still add after clear
            map.put("key3", 300);
            Assert.assertEquals(1, map.size());
            Assert.assertEquals(300, map.get("key3"));
        }
    }

    @Test
    public void testCustomNoEntryValue() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap(8, 0.4, -999)) {
            Assert.assertEquals(-999, map.get("nonexistent"));

            map.put("key1", 0); // Test with value 0
            Assert.assertEquals(0, map.get("key1"));
            Assert.assertEquals(-999, map.get("nonexistent"));
        }
    }

    @Test
    public void testEmptyStringKey() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            map.put("", 42);
            Assert.assertEquals(42, map.get(""));
            Assert.assertEquals(1, map.size());
        }
    }

    @Test
    public void testKeyCollisions() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap(4, 0.5, -1)) {
            // Add keys that are likely to cause hash collisions
            map.put("a", 1);
            map.put("b", 2);
            map.put("c", 3);
            map.put("aa", 4);
            map.put("bb", 5);
            map.put("cc", 6);

            // Verify all keys are still accessible despite collisions
            Assert.assertEquals(1, map.get("a"));
            Assert.assertEquals(2, map.get("b"));
            Assert.assertEquals(3, map.get("c"));
            Assert.assertEquals(4, map.get("aa"));
            Assert.assertEquals(5, map.get("bb"));
            Assert.assertEquals(6, map.get("cc"));
            Assert.assertEquals(6, map.size());
        }
    }

    @Test
    public void testKeyIndex() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            // Test keyIndex with non-existent key
            int index1 = map.keyIndex("key1");
            Assert.assertTrue("keyIndex should be positive for non-existent key", index1 >= 0);

            // Add key using putAt
            map.putAt(index1, "key1", 100);
            Assert.assertEquals(100, map.get("key1"));

            // Test keyIndex with existing key
            int index2 = map.keyIndex("key1");
            Assert.assertTrue("keyIndex should be negative for existing key", index2 < 0);

            // Test valueAt with existing key
            Assert.assertEquals(100, map.valueAt(index2));
        }
    }

    @Test
    public void testKeys() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            map.put("key1", 100);
            map.put("key2", 200);
            map.put("key3", 300);

            ObjList<CharSequence> keys = map.keys();
            Assert.assertEquals(3, keys.size());

            // Check that all keys are present (order may vary due to hashing)
            boolean foundKey1 = false, foundKey2 = false, foundKey3 = false;
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i).toString();
                if ("key1".equals(key)) foundKey1 = true;
                else if ("key2".equals(key)) foundKey2 = true;
                else if ("key3".equals(key)) foundKey3 = true;
            }
            Assert.assertTrue("key1 not found", foundKey1);
            Assert.assertTrue("key2 not found", foundKey2);
            Assert.assertTrue("key3 not found", foundKey3);
        }
    }

    @Test
    public void testLargeNumberOfKeys() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            final int numKeys = 1000;

            // Add many keys
            for (int i = 0; i < numKeys; i++) {
                map.put("key" + i, i);
            }

            Assert.assertEquals(numKeys, map.size());

            // Verify all keys are accessible
            for (int i = 0; i < numKeys; i++) {
                Assert.assertEquals(i, map.get("key" + i));
            }

            // Test keys() method with many keys
            ObjList<CharSequence> keys = map.keys();
            Assert.assertEquals(numKeys, keys.size());
        }
    }

    @Test
    public void testLongKeys() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            // Test with very long keys
            StringBuilder longKey = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                longKey.append("abcdefghij");
            }
            String longKeyStr = longKey.toString();

            map.put(longKeyStr, 12345);
            Assert.assertEquals(12345, map.get(longKeyStr));
        }
    }

    @Test
    public void testMemoryManagement() {
        // Test that closing and reopening doesn't leak memory
        for (int i = 0; i < 10; i++) {
            try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
                for (int j = 0; j < 100; j++) {
                    map.put("key" + j, j);
                }
                Assert.assertEquals(100, map.size());
            } // map should be properly closed and memory freed
        }
    }

    @Test
    public void testPutIfAbsent() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            // Test putIfAbsent with new key
            map.putIfAbsent("key1", 100);
            Assert.assertEquals(100, map.get("key1"));

            // Test putIfAbsent with existing key (should not update)
            map.putIfAbsent("key1", 200);
            Assert.assertEquals(100, map.get("key1")); // Should still be 100
        }
    }

    @Test
    public void testRehashing() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap(4, 0.5, -1)) {
            // Add enough elements to trigger rehashing
            for (int i = 0; i < 10; i++) {
                map.put("key" + i, i * 10);
            }

            // Verify all elements are still accessible after rehashing
            for (int i = 0; i < 10; i++) {
                Assert.assertEquals(i * 10, map.get("key" + i));
            }
            Assert.assertEquals(10, map.size());
        }
    }

    @Test
    public void testUnicodeKeys() {
        try (OffHeapCharSequenceIntHashMap map = new OffHeapCharSequenceIntHashMap()) {
            // Test Unicode characters
            String unicodeKey1 = "café";
            String unicodeKey2 = "测试";
            String unicodeKey3 = "🚀";

            map.put(unicodeKey1, 100);
            map.put(unicodeKey2, 200);
            map.put(unicodeKey3, 300);

            Assert.assertEquals(100, map.get(unicodeKey1));
            Assert.assertEquals(200, map.get(unicodeKey2));
            Assert.assertEquals(300, map.get(unicodeKey3));
            Assert.assertEquals(3, map.size());
        }
    }
}