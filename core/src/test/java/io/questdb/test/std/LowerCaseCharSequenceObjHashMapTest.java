/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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


import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LowerCaseCharSequenceObjHashMapTest {

    @Test
    public void testEqualsAndHashCode() {
        final int items = 1000;

        final LowerCaseCharSequenceObjHashMap<Integer> mapA = new LowerCaseCharSequenceObjHashMap<>();
        final LowerCaseCharSequenceObjHashMap<Integer> mapB = new LowerCaseCharSequenceObjHashMap<>();

        Assert.assertEquals(mapA, mapB);
        Assert.assertEquals(mapA.hashCode(), mapB.hashCode());

        for (int i = 0; i < items; i++) {
            mapA.put(Integer.toString(i), i);
        }

        Assert.assertNotEquals(mapA, mapB);

        // Reverse the addition order, so that the elements of the underlying arrays aren't 1-to-1 between the maps.
        for (int i = items - 1; i > -1; i--) {
            mapB.put(Integer.toString(i), i);
        }

        Assert.assertEquals(mapA, mapB);
        Assert.assertEquals(mapA.hashCode(), mapB.hashCode());

        mapA.clear();
        mapB.clear();

        Assert.assertEquals(mapA, mapB);
        Assert.assertEquals(mapA.hashCode(), mapB.hashCode());
    }

    @Test
    public void testPutIfAbsent() {
        final LowerCaseCharSequenceObjHashMap<Integer> lowerCaseMap = new LowerCaseCharSequenceObjHashMap<>();
        lowerCaseMap.put("a", 1);

        lowerCaseMap.putIfAbsent("a", 1);
        Assert.assertEquals(1, lowerCaseMap.keys().size());

        lowerCaseMap.putIfAbsent("b", 2);
        Assert.assertEquals(2, lowerCaseMap.keys().size());
    }

    @Test
    public void testSaturation() {

        final int N = 10_000;
        final Rnd rnd = new Rnd();
        final LowerCaseCharSequenceObjHashMap<Integer> lowerCaseMap = new LowerCaseCharSequenceObjHashMap<>();
        final HashMap<String, Integer> referenceMap = new HashMap<>();
        for (int i = 0; i < N; i++) {
            String str = rnd.nextString(4);
            int value = rnd.nextInt();

            int keyIndex = lowerCaseMap.keyIndex(str, 0, str.length());
            if (lowerCaseMap.put(str, value)) {
                Assert.assertNull("at " + i, referenceMap.put(str, value));
                Assert.assertTrue(keyIndex > -1);
            } else {
                Assert.assertTrue(keyIndex < 0);
                // this should fail to put
                lowerCaseMap.putIfAbsent(str, value);
                lowerCaseMap.putAt(keyIndex, str, referenceMap.get(str));
            }
        }

        // verify
        for (Map.Entry<String, Integer> e : referenceMap.entrySet()) {
            Assert.assertTrue(lowerCaseMap.contains(e.getKey()));
            Assert.assertFalse(lowerCaseMap.excludes(e.getKey()));
            Assert.assertFalse(lowerCaseMap.excludes(e.getKey(), 0, e.getKey().length()));

            int keyIndex = lowerCaseMap.keyIndex(e.getKey());
            Assert.assertTrue(keyIndex < 0);
            Assert.assertEquals(e.getKey(), e.getValue(), lowerCaseMap.valueAt(keyIndex));

            keyIndex = lowerCaseMap.keyIndex(e.getKey(), 0, e.getKey().length());
            Assert.assertEquals(e.getValue(), lowerCaseMap.valueAt(keyIndex));
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
                referenceMap.remove(s);
            }
        }

        // verify
        for (Map.Entry<String, Integer> e : referenceMap.entrySet()) {
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