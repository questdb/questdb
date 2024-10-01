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

import io.questdb.std.DirectBitSet;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class DirectBitSetTest {

    @Test
    public void testGetAndSet() {
        final int N = 1000;
        final Rnd rnd = new Rnd();
        try (DirectBitSet set = new DirectBitSet()) {
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
    }

    @Test
    public void testReopen() {
        final int N = 1000;
        final int max = 1_000_000;
        final Rnd rnd = new Rnd();
        try (DirectBitSet set = new DirectBitSet()) {
            int initialCapacity = set.capacity();
            Assert.assertTrue(set.capacity() > 0);

            for (int i = 0; i < N; i++) {
                int bitIdx = rnd.nextInt(max);
                Assert.assertFalse(set.get(bitIdx));
                set.set(bitIdx);
                Assert.assertTrue(set.get(bitIdx));
            }
            Assert.assertTrue(set.capacity() >= N);

            set.close();
            set.reopen();

            rnd.reset();
            for (int i = 0; i < N; i++) {
                int bitIdx = rnd.nextInt(max);
                Assert.assertFalse(set.get(bitIdx));
            }
            Assert.assertEquals(initialCapacity, set.capacity());
        }
    }

    @Test
    public void testResetCapacity() {
        final int N = 1000;
        final int max = 1_000_000;
        final Rnd rnd = new Rnd();
        try (DirectBitSet set = new DirectBitSet()) {
            int initialCapacity = set.capacity();
            Assert.assertTrue(set.capacity() > 0);

            for (int i = 0; i < N; i++) {
                int bitIdx = rnd.nextInt(max);
                Assert.assertFalse(set.get(bitIdx));
                set.set(bitIdx);
                Assert.assertTrue(set.get(bitIdx));
            }
            Assert.assertTrue(set.capacity() >= N);

            set.resetCapacity();

            rnd.reset();
            for (int i = 0; i < N; i++) {
                int bitIdx = rnd.nextInt(max);
                Assert.assertFalse(set.get(bitIdx));
            }
            Assert.assertEquals(initialCapacity, set.capacity());
        }
    }

    @Test
    public void testSmoke() {
        final int N = 1000;
        final int max = 1_000_000;
        final Rnd rnd = new Rnd();
        try (DirectBitSet set = new DirectBitSet()) {
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
}
