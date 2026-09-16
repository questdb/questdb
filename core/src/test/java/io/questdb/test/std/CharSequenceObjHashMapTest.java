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

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class CharSequenceObjHashMapTest {

    @Test
    public void testContains() {
        CharSequenceObjHashMap<CharSequence> map = new CharSequenceObjHashMap<>();
        map.put("EGG", "CHICKEN");
        Assert.assertTrue(map.contains("EGG"));
        Assert.assertFalse(map.contains("CHICKEN"));
    }

    @Test
    public void testRemove() {
        // these are specific keys that have the same hash code value when map capacity is 8
        String[] collisionKeys = {
                "SHRUE",
                "JCTIZ",
                "FLSVI",
                "CJBEV",
                "XFSUW",
                "JIGFI",
                "RZUPV",
                "BHLNE",
                "WRSLB",
                "NVZHC",
                "KUNRD",
                "QELQD",
                "CVBNE",
                "ZMFYL",
                "XVBHB"
        };

        Rnd rnd = new Rnd();
        for (int i = 0; i < 10_000; i++) {
            final CharSequenceObjHashMap<Object> map = new CharSequenceObjHashMap<>(8);
            final ReadOnlyObjList<CharSequence> keys = map.keys();
            for (int k = 0; k < collisionKeys.length; k++) {
                // all four of these keys collide give the size of the hash map
                map.put(collisionKeys[k], new Object());
            }

            CharSequence v = collisionKeys[rnd.nextInt(collisionKeys.length)];
            map.remove(v);
            assertKeys(map, keys);
            map.put(v, new Object());
            assertKeys(map, keys);
        }
    }

    @Test
    public void testRemoveAtQuick() {
        final CharSequenceObjHashMap<Integer> map = new CharSequenceObjHashMap<>();
        final String first = "first";
        final String middle = "middle";
        final String last = "last";
        map.put(first, 1);
        map.put(middle, 2);
        map.put(last, 3);

        map.removeAtQuick(map.keyIndex(middle), 1);

        Assert.assertEquals(2, map.size());
        Assert.assertEquals(-1, map.remove(middle));
        Assert.assertSame(first, map.keys().getQuick(0));
        Assert.assertSame(last, map.keys().getQuick(1));
        Assert.assertEquals(Integer.valueOf(1), map.get(first));
        Assert.assertEquals(Integer.valueOf(3), map.get(last));
        Assert.assertTrue(map.keys().hasOnlyNullsBeyondSizeForTesting());
    }

    private void assertKeys(CharSequenceObjHashMap<Object> map, ReadOnlyObjList<CharSequence> keys) {
        for (int j = 0, n = keys.size(); j < n; j++) {
            Assert.assertNotNull(map.get(keys.getQuick(j)));
        }
    }
}
