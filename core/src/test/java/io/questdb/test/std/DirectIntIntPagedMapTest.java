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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntIntPagedMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.test.tools.LimitedMemoryTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DirectIntIntPagedMapTest {
    // 16 entries at load factor 0.5 take 32 slots of 8 bytes
    private static final long INITIAL_HASH_MAP_SIZE = 256;
    private static final int INITIAL_PAGE_TABLE_CAPACITY = 32;
    // 32 page table slots of 8 bytes
    private static final long INITIAL_PAGE_TABLE_SIZE = 256;
    private static final Log LOG = LogFactory.getLog(DirectIntIntPagedMapTest.class);
    private static final int NO_ENTRY_KEY = SymbolTable.VALUE_IS_NULL;
    private static final int NO_ENTRY_VALUE = -1;
    private static final int PAGE_SLOTS = 256;
    // 256 slots of 4 bytes
    private static final long PAGE_SIZE = 1024;

    @Test
    public void testClearKeepsPages() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                for (int i = 0; i < 600; i++) {
                    map.put(i, i + 1);
                }
                map.put(-7, 7);
                Assert.assertEquals(3, map.pageCount());
                final long used = tracker.getUsed();
                map.clear();
                Assert.assertEquals(0, map.size());
                Assert.assertEquals(0, map.hashMapSize());
                Assert.assertEquals(3, map.pageCount());
                Assert.assertEquals(used, tracker.getUsed());
                for (int i = 0; i < 600; i++) {
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(i));
                }
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(-7));
                map.put(5, 6);
                map.put(-7, 8);
                Assert.assertEquals(2, map.size());
                Assert.assertEquals(6, map.get(5));
                Assert.assertEquals(8, map.get(-7));
            }
        });
    }

    @Test
    public void testCloseThenReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0)) {
                final DirectIntIntPagedMap map = newMap(tracker);
                try {
                    for (int i = 0; i < 1_000; i++) {
                        map.put(i, i * 2);
                    }
                    map.put(-5, 11);
                    map.close();
                    Assert.assertFalse(map.isOpen());
                    Assert.assertEquals(0, map.size());
                    Assert.assertEquals(0, map.pageCount());
                    Assert.assertEquals(0, tracker.getUsed());
                    // a closed map reads as empty and refuses writes
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(3));
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(-5));
                    try {
                        map.put(3, 4);
                        Assert.fail("expected a closed map to refuse the write");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "closed");
                    }
                    Assert.assertEquals(0, tracker.getUsed());

                    // close() drops the tracker, so the caller binds it again before reopen()
                    map.setMemoryTracker(tracker);
                    map.reopen();
                    Assert.assertTrue(map.isOpen());
                    Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE, tracker.getUsed());
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(3));
                    map.put(3, 4);
                    map.put(-5, 12);
                    Assert.assertEquals(4, map.get(3));
                    Assert.assertEquals(12, map.get(-5));
                    Assert.assertEquals(2, map.size());
                    // reopen() leaves an open map as it is
                    map.reopen();
                    Assert.assertEquals(2, map.size());
                    Assert.assertEquals(4, map.get(3));
                } finally {
                    map.close();
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testDenseBlocksFarApart() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                // long-lived symbols with small keys, then recently created ones with large keys
                for (int i = 0; i < 1_000; i++) {
                    map.put(i, i);
                }
                for (int i = 0; i < 10_000; i++) {
                    map.put(1_000_000 + i, -i - 5);
                }
                Assert.assertEquals(11_000, map.size());
                // Pages 0 to 3 hold the small keys. The large keys span pages 3,906 to 3,945, and at
                // least 36 of those get a page. The first large keys came before the entries justified
                // a page table that covers them, and stay in the hash map.
                Assert.assertTrue(map.pageCount() >= 40);
                Assert.assertTrue(map.hashMapSize() > 0);
                Assert.assertTrue(map.hashMapSize() < 1_000);
                for (int pass = 0; pass < 2; pass++) {
                    // the first pass copies the hash map entries that have a page into it
                    for (int i = 0; i < 1_000; i++) {
                        Assert.assertEquals(i, map.get(i));
                    }
                    for (int i = 0; i < 10_000; i++) {
                        Assert.assertEquals(-i - 5, map.get(1_000_000 + i));
                    }
                }
                Assert.assertTrue(map.pageEntryCount() > 10_000);
                Assert.assertEquals(11_000, map.size());
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(1_000));
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(999_999));
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(1_010_000));
            }
        });
    }

    @Test
    public void testDenseKeys() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE, tracker.getUsed());
                for (int i = 0; i < 1_000; i++) {
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(i));
                    map.put(i, 1_000 - i);
                }
                Assert.assertEquals(1_000, map.size());
                // keys 0 to 999 take 4 pages, and the hash map stays closed
                Assert.assertEquals(4, map.pageCount());
                Assert.assertEquals(0, map.hashMapSize());
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + 4 * PAGE_SIZE, tracker.getUsed());
                for (int i = 0; i < 1_000; i++) {
                    Assert.assertEquals(1_000 - i, map.get(i));
                }
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(1_000));
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(1_023));
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(-3));
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(Integer.MAX_VALUE));
                // put() replaces the value of a key without counting it twice
                map.put(10, 42);
                Assert.assertEquals(42, map.get(10));
                Assert.assertEquals(1_000, map.size());
            }
        });
    }

    @Test
    public void testKeyInHashMapBeforeItsPageExists() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                for (int i = 0; i < 10; i++) {
                    map.put(i, i + 100);
                }
                // Page 0 holds 10 keys, less than a quarter of its slots, so keys 1,000 and 1,001
                // get no page and go to the hash map.
                map.put(1_000, 7);
                map.put(1_001, 9);
                Assert.assertEquals(1, map.pageCount());
                Assert.assertEquals(2, map.hashMapSize());
                for (int i = 10; i < 200; i++) {
                    map.put(i, i + 100);
                }
                // Page 0 holds 200 keys now, so key 1,002 gets the page of keys 768 to 1,023.
                map.put(1_002, 11);
                Assert.assertEquals(2, map.pageCount());
                Assert.assertEquals(2, map.hashMapSize());
                Assert.assertEquals(203, map.size());
                final long used = tracker.getUsed();

                // The lookup finds key 1,000 in the hash map, and copies it into the page without
                // counting it twice or allocating. The second lookup reads the copy.
                Assert.assertEquals(201, map.pageEntryCount());
                Assert.assertEquals(7, map.get(1_000));
                Assert.assertEquals(202, map.pageEntryCount());
                Assert.assertEquals(7, map.get(1_000));
                Assert.assertEquals(202, map.pageEntryCount());
                Assert.assertEquals(203, map.size());
                Assert.assertEquals(used, tracker.getUsed());

                // put() updates both copies of a copied key, and the hash map copy of an uncopied one
                map.put(1_000, 8);
                map.put(1_001, 10);
                Assert.assertEquals(203, map.size());
                Assert.assertEquals(2, map.hashMapSize());
                Assert.assertEquals(8, map.get(1_000));
                Assert.assertEquals(10, map.get(1_001));
                Assert.assertEquals(11, map.get(1_002));
                for (int i = 0; i < 200; i++) {
                    Assert.assertEquals(i + 100, map.get(i));
                }
            }
        });
    }

    @Test
    public void testMemoryTrackerCharging() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker firstTracker = new LimitedMemoryTracker(0);
                    LimitedMemoryTracker secondTracker = new LimitedMemoryTracker(0)
            ) {
                final DirectIntIntPagedMap map = new DirectIntIntPagedMap(
                        INITIAL_PAGE_TABLE_CAPACITY,
                        16,
                        0.5,
                        NO_ENTRY_KEY,
                        NO_ENTRY_VALUE,
                        MemoryTag.NATIVE_JOIN_MAP
                );
                try {
                    // created closed
                    Assert.assertFalse(map.isOpen());
                    Assert.assertEquals(0, map.size());
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(0));

                    map.setMemoryTracker(firstTracker);
                    map.reopen();
                    Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE, firstTracker.getUsed());

                    // a page
                    map.put(0, 1);
                    Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE, firstTracker.getUsed());

                    // the hash map opens and rehashes under the tracker
                    for (int i = 1; i <= 16; i++) {
                        map.put(-i, i);
                    }
                    Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE + 2 * INITIAL_HASH_MAP_SIZE, firstTracker.getUsed());

                    // the page table doubles to cover keys up to 16,383, and the keys take 36 pages
                    for (int i = 0; i < 9_000; i++) {
                        map.put(i, i + 1);
                    }
                    Assert.assertEquals(36, map.pageCount());
                    Assert.assertEquals(2 * INITIAL_PAGE_TABLE_SIZE + 36 * PAGE_SIZE + 2 * INITIAL_HASH_MAP_SIZE, firstTracker.getUsed());

                    // restoreInitialCapacity() frees the pages and the hash map, and shrinks the page table
                    map.restoreInitialCapacity();
                    Assert.assertEquals(0, map.size());
                    Assert.assertEquals(0, map.pageCount());
                    Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE, firstTracker.getUsed());
                    map.put(-1, 1);
                    Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + INITIAL_HASH_MAP_SIZE, firstTracker.getUsed());

                    // rebinding frees the blocks under the tracker that charged them
                    map.setMemoryTracker(secondTracker);
                    Assert.assertFalse(map.isOpen());
                    Assert.assertEquals(0, firstTracker.getUsed());
                    map.reopen();
                    map.put(-1, 1);
                    map.put(1, 1);
                    Assert.assertEquals(0, firstTracker.getUsed());
                    Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + INITIAL_HASH_MAP_SIZE + PAGE_SIZE, secondTracker.getUsed());
                } finally {
                    map.close();
                }
                Assert.assertEquals(0, firstTracker.getUsed());
                Assert.assertEquals(0, secondTracker.getUsed());
            }
        });
    }

    @Test
    public void testRandomFirstSightings() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                final Rnd rnd = TestUtils.generateRandom(LOG);
                final int n = 10_000;
                final int[] keys = new int[n];
                for (int round = 0; round < 10; round++) {
                    // keys 0 to 9,999 in random order
                    for (int i = 0; i < n; i++) {
                        keys[i] = i;
                    }
                    for (int i = n - 1; i > 0; i--) {
                        final int j = rnd.nextInt(i + 1);
                        final int t = keys[i];
                        keys[i] = keys[j];
                        keys[j] = t;
                    }
                    map.restoreInitialCapacity();
                    for (int i = 0; i < n; i++) {
                        map.put(keys[i], keys[i] * 3);
                    }
                    Assert.assertEquals(n, map.size());
                    // Every page ends up allocated, though the keys that came before their page
                    // stay in the hash map.
                    Assert.assertEquals(n / PAGE_SLOTS + 1, map.pageCount());
                    for (int pass = 0; pass < 2; pass++) {
                        for (int i = 0; i < n; i++) {
                            Assert.assertEquals(i * 3, map.get(i));
                        }
                    }
                    Assert.assertEquals(n, map.size());
                }
            }
        });
    }

    @Test
    public void testRestoreInitialCapacity() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                for (int i = 0; i < 20_000; i++) {
                    map.put(i, i + 1);
                }
                for (int i = 1; i <= 100; i++) {
                    map.put(-i, i);
                }
                Assert.assertEquals(20_100, map.size());
                map.restoreInitialCapacity();
                Assert.assertTrue(map.isOpen());
                Assert.assertEquals(0, map.size());
                Assert.assertEquals(0, map.hashMapSize());
                Assert.assertEquals(0, map.pageCount());
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE, tracker.getUsed());
                for (int i = 0; i < 20_000; i++) {
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(i));
                }
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(-1));
                // the map works again after the shrink
                for (int i = 0; i < 100; i++) {
                    map.put(i, i + 2);
                }
                map.put(-1, 3);
                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(i + 2, map.get(i));
                }
                Assert.assertEquals(3, map.get(-1));
                // restoreInitialCapacity() keeps a page table at initial capacity
                map.restoreInitialCapacity();
                map.restoreInitialCapacity();
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSentinelValuesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            final int[] values = {
                    SymbolTable.VALUE_IS_NULL,
                    SymbolTable.VALUE_NOT_FOUND,
                    Integer.MAX_VALUE,
                    Integer.MIN_VALUE + 1,
                    -3,
                    0,
                    1
            };
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                // keys in a page, in the hash map, and one that waits in the hash map for its page
                final int[] keys = {0, 1, 255, -2, -1_000, Integer.MAX_VALUE, 5_000_000, 700};
                for (int k = 0; k < keys.length; k++) {
                    map.put(keys[k], values[k % values.length]);
                }
                Assert.assertEquals(1, map.pageCount());
                for (int i = 256; i < 700; i++) {
                    map.put(i, i);
                }
                // keys 512 to 767 have a page now
                Assert.assertEquals(3, map.pageCount());
                for (int pass = 0; pass < 2; pass++) {
                    // the second pass reads key 700 from the copy that the first pass made
                    for (int k = 0; k < keys.length; k++) {
                        Assert.assertEquals("key " + keys[k], values[k % values.length], map.get(keys[k]));
                    }
                }
                // every value round-trips through a page slot and through the hash map
                for (int value : values) {
                    map.put(10, value);
                    map.put(-10, value);
                    Assert.assertEquals(value, map.get(10));
                    Assert.assertEquals(value, map.get(-10));
                }
            }
        });
    }

    @Test
    public void testSparseKeysStayInHashMap() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(0);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                // A lone large key does not grow the page table to 3,907 slots.
                map.put(999_999, 1);
                Assert.assertEquals(0, map.pageCount());
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + INITIAL_HASH_MAP_SIZE, tracker.getUsed());

                // Keys 100 apart take 3 of the 256 slots of a page, so only the first page, which
                // the fill bound exempts, gets allocated.
                for (int i = 0; i < 2_000; i++) {
                    map.put(i * 100, i);
                }
                Assert.assertEquals(2_001, map.size());
                Assert.assertEquals(1, map.pageCount());
                // keys 0, 100 and 200 fit the first page
                Assert.assertEquals(2_001 - 3, map.hashMapSize());
                for (int i = 0; i < 2_000; i++) {
                    Assert.assertEquals(i, map.get(i * 100));
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(i * 100 + 1));
                }
                Assert.assertEquals(1, map.get(999_999));
            }
        });
    }

    @Test
    public void testTrackerLimitBreachOnHashMapOpen() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                map.put(1, 2);
                try {
                    map.put(-1, 3);
                    Assert.fail("expected a breach on hash map open");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                }
                Assert.assertEquals(1, map.size());
                Assert.assertEquals(0, map.hashMapSize());
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE, tracker.getUsed());
                Assert.assertEquals(2, map.get(1));
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(-1));
                tracker.setLimit(0);
                map.put(-1, 3);
                Assert.assertEquals(3, map.get(-1));
                Assert.assertEquals(2, map.size());
            }
        });
    }

    @Test
    public void testTrackerLimitBreachOnHashMapRehash() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(INITIAL_PAGE_TABLE_SIZE + INITIAL_HASH_MAP_SIZE);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                // 15 entries fit the initial hash map, and the 16th rehashes it
                for (int i = 1; i < 16; i++) {
                    map.put(-i, i);
                }
                try {
                    map.put(-16, 16);
                    Assert.fail("expected a breach on hash map rehash");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                }
                // DirectIntIntHashMap stores the entry before the rehash that fails
                Assert.assertEquals(16, map.size());
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + INITIAL_HASH_MAP_SIZE, tracker.getUsed());
                for (int i = 1; i <= 16; i++) {
                    Assert.assertEquals(i, map.get(-i));
                }
            }
        });
    }

    @Test
    public void testTrackerLimitBreachOnOpen() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(INITIAL_PAGE_TABLE_SIZE - 1)) {
                final DirectIntIntPagedMap map = new DirectIntIntPagedMap(
                        INITIAL_PAGE_TABLE_CAPACITY,
                        16,
                        0.5,
                        NO_ENTRY_KEY,
                        NO_ENTRY_VALUE,
                        MemoryTag.NATIVE_JOIN_MAP
                );
                try {
                    map.setMemoryTracker(tracker);
                    try {
                        map.reopen();
                        Assert.fail("expected a breach on open");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                    }
                    Assert.assertFalse(map.isOpen());
                    Assert.assertEquals(0, map.size());
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(1));
                    Assert.assertEquals(0, tracker.getUsed());
                } finally {
                    map.close();
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testTrackerLimitBreachOnPageAllocation() throws Exception {
        assertMemoryLeak(() -> {
            // admits the initial page table and one page
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                for (int i = 0; i < PAGE_SLOTS; i++) {
                    map.put(i, i + 1);
                }
                try {
                    map.put(PAGE_SLOTS, 1);
                    Assert.fail("expected a breach on page allocation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                // the failed allocation leaves the map as it was
                Assert.assertEquals(PAGE_SLOTS, map.size());
                Assert.assertEquals(1, map.pageCount());
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE, tracker.getUsed());
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(PAGE_SLOTS));
                for (int i = 0; i < PAGE_SLOTS; i++) {
                    Assert.assertEquals(i + 1, map.get(i));
                }
                // and usable once the limit allows the allocation
                tracker.setLimit(0);
                map.put(PAGE_SLOTS, 1);
                Assert.assertEquals(1, map.get(PAGE_SLOTS));
                Assert.assertEquals(PAGE_SLOTS + 1, map.size());
                Assert.assertEquals(2, map.pageCount());
            }
        });
    }

    @Test
    public void testTrackerLimitBreachOnPageTableGrowth() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    LimitedMemoryTracker tracker = new LimitedMemoryTracker(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE);
                    DirectIntIntPagedMap map = newMap(tracker)
            ) {
                for (int i = 0; i < 100; i++) {
                    map.put(i, i + 1);
                }
                // key 8,192 lies past the 32 pages that the initial page table covers
                try {
                    map.put(8_192, 1);
                    Assert.fail("expected a breach on page table growth");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                }
                Assert.assertEquals(100, map.size());
                Assert.assertEquals(INITIAL_PAGE_TABLE_SIZE + PAGE_SIZE, tracker.getUsed());
                Assert.assertEquals(NO_ENTRY_VALUE, map.get(8_192));
                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(i + 1, map.get(i));
                }
                tracker.setLimit(0);
                map.put(8_192, 1);
                Assert.assertEquals(1, map.get(8_192));
                Assert.assertEquals(101, map.size());
            }
        });
    }

    @Test
    public void testTrackerLimitBreachOnShrink() throws Exception {
        assertMemoryLeak(() -> {
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(0)) {
                final DirectIntIntPagedMap map = newMap(tracker);
                try {
                    for (int i = 0; i < 9_000; i++) {
                        map.put(i, i);
                    }
                    // The shrink frees the pages and the grown page table, then allocates the
                    // initial page table. With the limit below its size, the allocation fails and
                    // leaves the map closed.
                    tracker.setLimit(INITIAL_PAGE_TABLE_SIZE - 1);
                    try {
                        map.restoreInitialCapacity();
                        Assert.fail("expected a breach on shrink");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                    }
                    Assert.assertFalse(map.isOpen());
                    Assert.assertEquals(0, map.size());
                    Assert.assertEquals(0, map.pageCount());
                    Assert.assertEquals(0, tracker.getUsed());
                    Assert.assertEquals(NO_ENTRY_VALUE, map.get(5));
                } finally {
                    map.close();
                }
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    private static DirectIntIntPagedMap newMap(LimitedMemoryTracker tracker) {
        final DirectIntIntPagedMap map = new DirectIntIntPagedMap(
                INITIAL_PAGE_TABLE_CAPACITY,
                16,
                0.5,
                NO_ENTRY_KEY,
                NO_ENTRY_VALUE,
                MemoryTag.NATIVE_JOIN_MAP
        );
        map.setMemoryTracker(tracker);
        map.reopen();
        return map;
    }
}
