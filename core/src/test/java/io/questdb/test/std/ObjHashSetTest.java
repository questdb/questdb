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

import io.questdb.std.ObjHashSet;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ObjHashSetTest {

    @Test
    public void testBasicOperations() {
        Rnd rnd = new Rnd();
        ObjHashSet<CharSequence> set = new ObjHashSet<>();
        final int N = 1000;

        for (int i = 0; i < N; i++) {
            set.add(rnd.nextChars(25).toString());
        }

        Assert.assertEquals(N, set.size());

        rnd.reset();

        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.keyIndex(rnd.nextChars(25)) < 0);
        }

        rnd.reset();

        for (int i = 0; i < N; i++) {
            TestUtils.assertEquals(rnd.nextChars(25), set.get(i));
        }

        rnd.reset();

        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.contains(rnd.nextChars(25)));
        }
    }

    @Test
    public void testNull() {
        ObjHashSet<String> set = new ObjHashSet<>();
        set.add("X");
        set.add(null);
        set.add("Y");
        Assert.assertEquals(3, set.size());

        Assert.assertEquals("X", set.get(0));
        Assert.assertNull(set.get(1));
        Assert.assertEquals("Y", set.get(2));

        int n = set.size();
        Assert.assertEquals(3, n);
        int nullCount = 0;
        for (int i = 0; i < n; i++) {
            if (set.get(i) == null) {
                nullCount++;
            }
        }

        Assert.assertEquals(1, nullCount);
    }

    @Test
    public void testRemove() {

        Rnd rnd = new Rnd();
        ObjHashSet<CharSequence> set = new ObjHashSet<>();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            Assert.assertTrue(set.add(cs.toString()));
        }
        Assert.assertEquals(N, set.size());

        rnd.reset();

        // assert that set contains the values we just added
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            Assert.assertTrue(set.contains(cs.toString()));
        }

        Rnd rnd2 = new Rnd();

        rnd.reset();

        // remove some keys and assert that the size() complies
        int removed = 0;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertTrue(set.remove(cs.toString()));
                removed++;
                Assert.assertEquals(N - removed, set.size());
            }
        }

        // if we didn't remove anything test has no value
        Assert.assertTrue(removed > 0);

        rnd2.reset();
        rnd.reset();

        // assert that keys we didn't remove are still there and
        // keys we removed are not
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextChars(15);
            if (rnd2.nextPositiveInt() % 16 == 0) {
                Assert.assertFalse(set.contains(cs.toString()));
            } else {
                Assert.assertTrue(set.contains(cs.toString()));
            }
        }

        Assert.assertFalse(set.add("ZTXZCXTOLXDJDKV")); // this should already exist
        Assert.assertFalse(set.remove("hello"));
    }
}
