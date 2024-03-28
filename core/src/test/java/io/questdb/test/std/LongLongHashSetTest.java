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

import io.questdb.std.LongLongHashSet;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

public class LongLongHashSetTest {
    private final static Rnd rnd = new Rnd();

    @Test
    public void testInsertDuplicateKeys() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
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
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
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
    public void testInsertRandomKeysViaAddAt() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
        Set<TwoLongs> jdkSet = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            long key1 = rnd.nextLong();
            long key2 = rnd.nextLong();
            final int index = longSet.keyIndex(key1, key2);
            if (index < 0) {
                continue;
            }
            longSet.addAt(index, key1, key2);
            jdkSet.add(new TwoLongs(key1, key2));
            maybeAssertContainsSameKeysAndSize(longSet, jdkSet);
        }
        assertContainsSameKeysAndSize(longSet, jdkSet);
    }

    @Test
    public void testInsertingNoEntrySentinel() {
        LongLongHashSet set = new LongLongHashSet(16, 0.5, 0, LongLongHashSet.LONG_LONG_STRATEGY);

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

    @Test
    public void testInvalidLoadFactor() {
        try {
            new LongLongHashSet(10, 0.0, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
            fail();
        } catch (IllegalArgumentException e) {
            TestUtils.assertContains(e.getMessage(), "load factor");
        }

        try {
            new LongLongHashSet(10, 1.0, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
            fail();
        } catch (IllegalArgumentException e) {
            TestUtils.assertContains(e.getMessage(), "load factor");
        }
    }

    @Test
    public void testOnlyFirstMatching() {
        LongLongHashSet set = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
        set.add(1, 1);
        assertFalse(set.contains(1, 2));
    }

    @Test
    public void testOnlySecondMatching() {
        LongLongHashSet set = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
        set.add(1, 1);
        assertFalse(set.contains(2, 1));
    }

    @Test
    public void testSinkable_EntriesWithMinLong_uuidStrategy() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.UUID_STRATEGY);
        longSet.add(Long.MIN_VALUE, 1);
        StringSink sink = new StringSink();
        longSet.toSink(sink);
        String expected = "['" + new UUID(1, Long.MIN_VALUE) + "']";
        assertEquals(expected, sink.toString());
    }

    @Test
    public void testSinkable_EntryWithMinLong_longLongStrategy() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
        longSet.add(Long.MIN_VALUE, 1);
        StringSink sink = new StringSink();
        longSet.toSink(sink);
        assertEquals("[[-9223372036854775808,1]]", sink.toString());
    }

    @Test
    public void testSinkable_singleEntry_longLongStrategy() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
        longSet.add(1, 1);
        StringSink sink = new StringSink();
        longSet.toSink(sink);
        assertEquals("[[1,1]]", sink.toString());
    }

    @Test
    public void testSinkable_singleEntry_uuidStrategy() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.UUID_STRATEGY);
        longSet.add(1, 1);
        StringSink sink = new StringSink();
        longSet.toSink(sink);
        String expected = "['" + new UUID(1, 1) + "']";
        assertEquals(expected, sink.toString());
    }

    @Test
    public void testSinkable_twoEntries_longLongStrategy() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.LONG_LONG_STRATEGY);
        longSet.add(1, 1);
        longSet.add(2, 1);
        StringSink sink = new StringSink();
        longSet.toSink(sink);
        assertEquals("[[1,1],[2,1]]", sink.toString());
    }

    @Test
    public void testSinkable_twoEntries_uuidStrategy() {
        LongLongHashSet longSet = new LongLongHashSet(10, 0.5f, Long.MIN_VALUE, LongLongHashSet.UUID_STRATEGY);
        longSet.add(1, 1);
        longSet.add(2, 1);
        StringSink sink = new StringSink();
        longSet.toSink(sink);
        String expected = "['" + new UUID(1, 1) + "','" + new UUID(1, 2) + "']";
        assertEquals(expected, sink.toString());
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
