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

import io.questdb.std.BitSet;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class BitSetTest {

    @Test
    public void testClearBelow() {
        BitSet set = new BitSet();
        for (int i = 0; i < 200; i++) {
            set.set(i);
        }
        final long capacity = set.capacity();
        set.clearBelow(0);
        Assert.assertTrue(set.get(0));
        set.clearBelow(70);
        for (int i = 0; i < 200; i++) {
            Assert.assertEquals(i >= 70, set.get(i));
        }
        set.clearBelow(128);
        for (int i = 0; i < 200; i++) {
            Assert.assertEquals(i >= 128, set.get(i));
        }
        set.clearBelow(10_000);
        for (int i = 0; i < 200; i++) {
            Assert.assertFalse(set.get(i));
        }
        Assert.assertEquals(capacity, set.capacity());
    }

    @Test
    public void testCopyBelow() {
        BitSet source = new BitSet();
        for (int i = 0; i < 200; i += 3) {
            source.set(i);
        }
        BitSet target = new BitSet(64);
        for (int i = 0; i < 200; i += 2) {
            target.set(i);
        }
        target.copyBelow(source, 0);
        for (int i = 0; i < 200; i++) {
            Assert.assertEquals(i % 2 == 0, target.get(i));
        }
        target.copyBelow(source, 70);
        for (int i = 0; i < 200; i++) {
            Assert.assertEquals(i < 70 ? i % 3 == 0 : i % 2 == 0, target.get(i));
        }
        target.copyBelow(source, 128);
        for (int i = 0; i < 200; i++) {
            Assert.assertEquals(i < 128 ? i % 3 == 0 : i % 2 == 0, target.get(i));
        }
        BitSet empty = new BitSet(64);
        empty.copyBelow(source, 130);
        for (int i = 0; i < 200; i++) {
            Assert.assertEquals(i < 130 && i % 3 == 0, empty.get(i));
        }
    }

    @Test
    public void testGetAndSet() {
        final int N = 1000;
        final Rnd rnd = new Rnd();
        BitSet set = new BitSet();

        Assert.assertTrue(set.capacity() > 0);

        for (int i = 0; i < N; i++) {
            Assert.assertFalse(set.get(i));
        }
        Assert.assertTrue(set.capacity() >= N);

        rnd.reset();
        for (int i = 0; i < N; i++) {
            Assert.assertFalse(set.getAndSet(i));
            Assert.assertTrue(set.get(i));
        }

        rnd.reset();
        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.getAndSet(i));
            Assert.assertTrue(set.get(i));
        }
    }

    @Test
    public void testResetCapacity() {
        final int N = 1000;
        final int max = 1_000_000;
        final Rnd rnd = new Rnd();
        BitSet set = new BitSet();

        final long initialCapacity = set.capacity();
        Assert.assertTrue(set.capacity() > 0);

        for (int i = 0; i < N; i++) {
            int idx = rnd.nextInt(max);
            Assert.assertFalse(set.get(idx));
            set.set(idx);
        }
        Assert.assertTrue(set.capacity() >= N);

        set.resetCapacity();
        Assert.assertEquals(initialCapacity, set.capacity());

        rnd.reset();
        for (int i = 0; i < N; i++) {
            Assert.assertFalse(set.get(rnd.nextInt(max)));
        }
    }

    @Test
    public void testSmoke() {
        final int N = 1000;
        final int max = 1_000_000;
        final Rnd rnd = new Rnd();
        BitSet set = new BitSet();

        Assert.assertTrue(set.capacity() > 0);

        for (int i = 0; i < N; i++) {
            Assert.assertFalse(set.get(rnd.nextInt(max)));
        }
        Assert.assertTrue(set.capacity() >= N);

        rnd.reset();
        for (int i = 0; i < N; i++) {
            set.set(rnd.nextInt(max));
        }

        rnd.reset();
        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.get(rnd.nextInt(max)));
        }

        set.clear();

        rnd.reset();
        for (int i = 0; i < N; i++) {
            Assert.assertFalse(set.get(rnd.nextInt(max)));
        }
    }
}
