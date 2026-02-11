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

import io.questdb.std.LongHashSet;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class LongHashSetTest {

    @Test
    public void testBasicOperations() {
        Rnd rnd = new Rnd();
        LongHashSet set = new LongHashSet();
        final int N = 1000;

        for (int i = 0; i < N; i++) {
            set.add(rnd.nextPositiveLong());
        }

        Assert.assertEquals(N, set.size());

        rnd.reset();

        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.keyIndex(rnd.nextPositiveLong()) < 0);
        }

        rnd.reset();

        for (int i = 0; i < N; i++) {
            Assert.assertEquals(rnd.nextPositiveLong(), set.get(i));
        }

        rnd.reset();

        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.contains(rnd.nextPositiveLong()));
        }
    }

    @Test
    public void testRemove() {
        Rnd rnd = new Rnd();
        LongHashSet set = new LongHashSet();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.add(rnd.nextPositiveLong()));
        }
        Assert.assertEquals(N, set.size());

        rnd.reset();

        // assert that set contains the values we just added
        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.contains(rnd.nextPositiveLong()));
        }

        Rnd rnd2 = new Rnd();

        rnd.reset();

        // remove some keys and assert that the size() complies
        int removed = 0;
        for (int i = 0; i < N; i++) {
            long n = rnd.nextPositiveLong();
            if (rnd2.nextPositiveLong() % 16 == 0) {
                Assert.assertTrue(set.remove(n) > -1);
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
            long n = rnd.nextPositiveLong();
            if (rnd2.nextPositiveLong() % 16 == 0) {
                Assert.assertFalse(set.contains(n));
            } else {
                Assert.assertTrue(set.contains(n));
            }
        }
    }
}
