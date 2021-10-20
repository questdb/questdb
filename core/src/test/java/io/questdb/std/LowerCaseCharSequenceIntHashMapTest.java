/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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


import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LowerCaseCharSequenceIntHashMapTest {

    @Test
    public void testPutMutableCharSequence() {
        final LowerCaseCharSequenceIntHashMap lowerCaseMap = new LowerCaseCharSequenceIntHashMap();

        StringSink ss = new StringSink();
        ss.put("a");

        lowerCaseMap.putIfAbsent(ss, 1);

        ss.clear();
        ss.put("Bb");

        lowerCaseMap.putIfAbsent(ss, 2);

        Assert.assertEquals(1, lowerCaseMap.get("A"));
        Assert.assertEquals(2, lowerCaseMap.get("bb"));

        ObjList<CharSequence>  keys =  lowerCaseMap.keys();
        Assert.assertEquals(2, keys.size());
        Assert.assertEquals("a", keys.get(0));
        Assert.assertEquals("Bb", keys.get(1));
    }

    @Test
    public void testSaturation() {

        final int N = 10_000;
        final Rnd rnd = new Rnd();
        final LowerCaseCharSequenceIntHashMap lowerCaseMap = new LowerCaseCharSequenceIntHashMap();
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
                Assert.assertFalse(lowerCaseMap.putIfAbsent(str, value));
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
            Assert.assertEquals(e.getKey(), (int) e.getValue(), lowerCaseMap.valueAt(keyIndex));

            keyIndex = lowerCaseMap.keyIndex(e.getKey(), 0, e.getKey().length());
            Assert.assertEquals((int) e.getValue(), lowerCaseMap.valueAt(keyIndex));
        }

        for (int i = 0, n = lowerCaseMap.keys.length; i < n; i++) {
            CharSequence v = lowerCaseMap.keys[i];
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

        for (int i = 0, n = lowerCaseMap.keys.length; i < n; i++) {
            CharSequence v = lowerCaseMap.keys[i];
            if (v != null) {
                Assert.assertTrue(referenceMap.containsKey(v.toString()));
            }
        }

    }
}