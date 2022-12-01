/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class LongLongHashSetTest {
    private final static Rnd rnd = new Rnd();

    @Test
    public void testInsertDupsKeys() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE);
        Set<TwoLongs> jdkSet = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            long key1 = rnd.nextLong(50);
            long key2 = rnd.nextLong(50);
            assertEquals(jdkSet.add(new TwoLongs(key1, key2)), longSet.add(key1, key2));
            maybeAssertContainsSameKeysAndSize(longSet, jdkSet);
        }
        assertContainsSameKeysAndSize(longSet, jdkSet);
    }

    @Test
    public void testInsertRandomKeys() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE);
        Set<TwoLongs> jdkSet = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            long key1 = rnd.nextLong();
            long key2 = rnd.nextLong();
            assertEquals(jdkSet.add(new TwoLongs(key1, key2)), longSet.add(key1, key2));
            maybeAssertContainsSameKeysAndSize(longSet, jdkSet);
        }
        assertContainsSameKeysAndSize(longSet, jdkSet);
    }

    @Test
    public void testInsertingNoEntrySentinel() {
        LongLongHashSet set = new LongLongHashSet(16, 0.5, 0);

        // when both keys are the no_entry_sentinel, we expect add to fail
        try {
            set.add(0, 0);
            fail("adding NO_ENTRY_VALUE sentinel should fail");
        } catch (IllegalArgumentException e) {
            TestUtils.assertContains(e.getMessage(), "NO_ENTRY_KEY");
        }

        // when only one key is the no_entry_sentinel, we expect add to succeed
        assertFalse(set.contains(0, 1));
        assertTrue(set.add(0, 1));
        assertFalse(set.add(0, 1));
        assertTrue(set.contains(0, 1));
        assertFalse(set.contains(1, 0));
        assertTrue(set.add(1, 0));
        assertFalse(set.add(1, 0));
        assertTrue(set.contains(1, 0));

        assertEquals(2, set.size());
        set.clear();
        assertEquals(0, set.size());

        // try again, after clearing
        assertFalse(set.contains(0, 1));
        assertTrue(set.add(0, 1));
        assertFalse(set.add(0, 1));
        assertTrue(set.contains(0, 1));
        assertFalse(set.contains(1, 0));
        assertTrue(set.add(1, 0));
        assertFalse(set.add(1, 0));
        assertTrue(set.contains(1, 0));
    }

    private static void assertContainsSameKeysAndSize(LongLongHashSet longSet, Set<TwoLongs> jdkSet) {
        assertEquals(jdkSet.size(), longSet.size());
        for (TwoLongs twoLongs : jdkSet) {
            long key1 = twoLongs.a;
            long key2 = twoLongs.b;
            String errorMsg = "JDK set contains keys  " + key1 + " and " + key2 + ", but this key is not present in " + LongLongHashSet.class.getSimpleName();
            assertTrue(errorMsg, longSet.contains(twoLongs.a, twoLongs.b));
        }
    }

    private static void maybeAssertContainsSameKeysAndSize(LongLongHashSet longSet, Set<TwoLongs> jdkSet) {
        if (rnd.nextInt(10) == 0) {
            assertContainsSameKeysAndSize(longSet, jdkSet);
        }
    }

    private static class TwoLongs {
        private final long a;
        private final long b;

        public TwoLongs(long a, long b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TwoLongs) {
                TwoLongs that = (TwoLongs) obj;
                return this.a == that.a && this.b == that.b;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (int) (a ^ b);
        }
    }
}
