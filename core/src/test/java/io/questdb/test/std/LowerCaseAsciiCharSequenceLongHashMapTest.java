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

import io.questdb.std.LowerCaseAsciiCharSequenceLongHashMap;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LowerCaseAsciiCharSequenceLongHashMapTest {
    @Test
    public void testInvalidLoadFactor() {
        try {
            new LowerCaseAsciiCharSequenceLongHashMap(8, 0.0, -1L);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("0 < loadFactor < 1", e.getMessage());
        }
        new LowerCaseAsciiCharSequenceLongHashMap(8, 0.0001, -1L);
        new LowerCaseAsciiCharSequenceLongHashMap(8, 0.9999, -1L);
        try {
            new LowerCaseAsciiCharSequenceLongHashMap(8, 1.0, -1L);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("0 < loadFactor < 1", e.getMessage());
        }
    }

    @Test
    public void testPutIfAbsent() {
        final LowerCaseAsciiCharSequenceLongHashMap lowerCaseMap = new LowerCaseAsciiCharSequenceLongHashMap();
        final CharSequence key = "testKey";
        lowerCaseMap.putIfAbsent(key, 100L);
        Assert.assertEquals(100L, lowerCaseMap.get(key));
        lowerCaseMap.putIfAbsent(key, 102L);
        Assert.assertEquals(100L, lowerCaseMap.get(key));
        lowerCaseMap.put(key, 102L);
        Assert.assertEquals(102L, lowerCaseMap.get(key));
    }

    @Test
    public void testSaturation() {

        final int N = 10_000;
        final Rnd rnd = new Rnd();
        final LowerCaseAsciiCharSequenceLongHashMap lowerCaseMap = new LowerCaseAsciiCharSequenceLongHashMap();
        final HashMap<String, Long> referenceMap = new HashMap<>();
        for (int i = 0; i < N; i++) {
            String str = rnd.nextString(4);
            long value = rnd.nextLong();

            int keyIndex = lowerCaseMap.keyIndex(str, 0, str.length());
            if (lowerCaseMap.put(str, value)) {
                Assert.assertNull("at " + i, referenceMap.put(str.toLowerCase(), value));
                Assert.assertTrue(keyIndex > -1);
            } else {
                Assert.assertTrue(keyIndex < 0);
                // this should fail to put
                lowerCaseMap.putIfAbsent(str, value);
                lowerCaseMap.putAt(keyIndex, str, referenceMap.get(str.toLowerCase()));
            }
        }

        // verify
        for (Map.Entry<String, Long> e : referenceMap.entrySet()) {
            Assert.assertTrue(lowerCaseMap.contains(e.getKey()));
            Assert.assertFalse(lowerCaseMap.excludes(e.getKey()));
            Assert.assertFalse(lowerCaseMap.excludes(e.getKey(), 0, e.getKey().length()));

            int keyIndex = lowerCaseMap.keyIndex(e.getKey());
            Assert.assertTrue(keyIndex < 0);
            Assert.assertEquals(e.getKey(), (long) e.getValue(), lowerCaseMap.valueAt(keyIndex));

            keyIndex = lowerCaseMap.keyIndex(e.getKey(), 0, e.getKey().length());
            Assert.assertEquals((long) e.getValue(), lowerCaseMap.valueAt(keyIndex));
        }

        for (int i = 0, n = lowerCaseMap.getKeyCount(); i < n; i++) {
            CharSequence v = lowerCaseMap.getKey(i);
            if (v != null) {
                Assert.assertTrue(referenceMap.containsKey(v.toString()));
            }
        }

        // remove every forth key
        // make sure rnd generates the same keys again
        rnd.reset();

        for (int i = 0; i < N; i++) {
            String s = rnd.nextString(4);
            if (i % 4 == 0) {
                lowerCaseMap.remove(s);
                referenceMap.remove(s.toLowerCase());
            }
        }

        // verify
        for (Map.Entry<String, Long> e : referenceMap.entrySet()) {
            Assert.assertTrue(e.getKey(), lowerCaseMap.contains(e.getKey()));
        }

        for (int i = 0, n = lowerCaseMap.getKeyCount(); i < n; i++) {
            CharSequence v = lowerCaseMap.getKey(i);
            if (v != null) {
                Assert.assertTrue(referenceMap.containsKey(v.toString()));
            }
        }
    }
}
