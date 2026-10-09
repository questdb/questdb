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


package io.questdb.test.cutlass.qwp;

import io.questdb.cutlass.qwp.codec.QwpSymbolKeyMap;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

public class QwpSymbolKeyMapTest {

    @Test
    public void testClearKeepsArrayWithinQuery() {
        final QwpSymbolKeyMap map = new QwpSymbolKeyMap(1 << 18, new IntList());
        map.put(262_000, 0);
        Assert.assertEquals(1 << 18, map.getDenseCapacity());
        // a dictionary reset within the query keeps the array, forgets the ids
        map.clear();
        Assert.assertEquals(1 << 18, map.getDenseCapacity());
        Assert.assertEquals(QwpSymbolKeyMap.NO_ID, map.get(262_000));
    }

    @Test
    public void testKeyRecordedBeforeIdIsWritten() {
        final ThrowingIntList stored = new ThrowingIntList();
        final QwpSymbolKeyMap map = new QwpSymbolKeyMap(1024, stored);
        map.put(3, 0);
        stored.throwOnNextAdd = true;
        try {
            map.put(5, 7);
            Assert.fail("expected the out-of-memory error");
        } catch (OutOfMemoryError expected) {
            // the key could not be recorded
        }
        // the id was not written, so no later query can read it after the reset
        Assert.assertEquals(QwpSymbolKeyMap.NO_ID, map.get(5));
        map.resetForNewQuery();
        Assert.assertEquals(QwpSymbolKeyMap.NO_ID, map.get(3));
        Assert.assertEquals(QwpSymbolKeyMap.NO_ID, map.get(5));
        map.put(5, 1);
        Assert.assertEquals(1, map.get(5));
    }

    @Test
    public void testLargeArrayDroppedBeforeNextQuery() {
        final QwpSymbolKeyMap map = new QwpSymbolKeyMap(1 << 18, new IntList());
        // one row whose key is near the cap grows the array to the cap
        map.put(262_000, 0);
        Assert.assertEquals(1 << 18, map.getDenseCapacity());
        map.resetForNewQuery();
        Assert.assertEquals(0, map.getDenseCapacity());
        Assert.assertEquals(QwpSymbolKeyMap.NO_ID, map.get(262_000));
        // an empty query after it keeps nothing either
        map.resetForNewQuery();
        Assert.assertEquals(0, map.getDenseCapacity());
        // the next query grows the array again, as it needs
        map.put(10, 3);
        Assert.assertEquals(3, map.get(10));
        Assert.assertEquals(64, map.getDenseCapacity());
    }

    @Test
    public void testOverflowKeysForgottenAcrossQueries() {
        final QwpSymbolKeyMap map = new QwpSymbolKeyMap(100, new IntList());
        for (int k = 0; k < 5_000; k++) {
            map.put(100 + k, k);
        }
        Assert.assertEquals(4_999, map.get(5_099));
        map.resetForNewQuery();
        for (int k = 0; k < 5_000; k++) {
            Assert.assertEquals(QwpSymbolKeyMap.NO_ID, map.get(100 + k));
        }
        map.put(150, 9);
        Assert.assertEquals(9, map.get(150));
    }

    @Test
    public void testReleaseDropsAnyArray() {
        final QwpSymbolKeyMap map = new QwpSymbolKeyMap(1 << 18, new IntList());
        map.put(100, 0);
        map.resetForNewQuery();
        // small arrays are kept from one query to the next
        Assert.assertEquals(128, map.getDenseCapacity());
        map.put(100, 1);
        Assert.assertEquals(1, map.get(100));
        // the connection ends: nothing is kept for the next connection
        map.release();
        Assert.assertEquals(0, map.getDenseCapacity());
        Assert.assertEquals(QwpSymbolKeyMap.NO_ID, map.get(100));
        map.put(100, 2);
        Assert.assertEquals(2, map.get(100));
    }

    @Test
    public void testRetainedArrayIsBounded() {
        final QwpSymbolKeyMap map = new QwpSymbolKeyMap(1 << 18, new IntList());
        map.put(QwpSymbolKeyMap.RETAINED_DENSE_CAPACITY - 1, 0);
        map.resetForNewQuery();
        Assert.assertEquals(QwpSymbolKeyMap.RETAINED_DENSE_CAPACITY, map.getDenseCapacity());
        map.put(QwpSymbolKeyMap.RETAINED_DENSE_CAPACITY, 0);
        map.resetForNewQuery();
        Assert.assertEquals(0, map.getDenseCapacity());
    }

    private static class ThrowingIntList extends IntList {
        boolean throwOnNextAdd;

        @Override
        public void add(int value) {
            if (throwOnNextAdd) {
                throwOnNextAdd = false;
                throw new OutOfMemoryError("test");
            }
            super.add(value);
        }
    }
}
