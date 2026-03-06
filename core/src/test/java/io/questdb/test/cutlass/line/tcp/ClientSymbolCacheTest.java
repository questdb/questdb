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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cutlass.line.tcp.ClientSymbolCache;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link ClientSymbolCache}.
 */
public class ClientSymbolCacheTest {

    @Test
    public void testGetMiss_returnsNoEntry() {
        ClientSymbolCache cache = new ClientSymbolCache();

        // Empty cache should return NO_ENTRY for any key
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(0));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(1));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(100));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(Integer.MAX_VALUE));
    }

    @Test
    public void testPutAndGet_returnsValue() {
        ClientSymbolCache cache = new ClientSymbolCache();

        // Put some entries
        cache.put(0, 10);
        cache.put(1, 20);
        cache.put(5, 50);

        // Verify retrieval
        assertEquals(10, cache.get(0));
        assertEquals(20, cache.get(1));
        assertEquals(50, cache.get(5));

        // Other keys still return NO_ENTRY
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(2));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(3));
    }

    @Test
    public void testPutOverwrite_updatesValue() {
        ClientSymbolCache cache = new ClientSymbolCache();

        cache.put(0, 10);
        assertEquals(10, cache.get(0));

        // Overwrite with new value
        cache.put(0, 100);
        assertEquals(100, cache.get(0));

        // Overwrite again
        cache.put(0, 999);
        assertEquals(999, cache.get(0));
    }

    @Test
    public void testClear_removesAllEntries() {
        ClientSymbolCache cache = new ClientSymbolCache();

        // Add several entries
        cache.put(0, 10);
        cache.put(1, 20);
        cache.put(2, 30);

        assertEquals(3, cache.size());
        assertEquals(10, cache.get(0));

        // Clear the cache
        cache.clear();

        assertEquals(0, cache.size());
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(0));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(1));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(2));
    }

    @Test
    public void testSize_tracksEntryCount() {
        ClientSymbolCache cache = new ClientSymbolCache();

        assertEquals(0, cache.size());

        cache.put(0, 10);
        assertEquals(1, cache.size());

        cache.put(1, 20);
        assertEquals(2, cache.size());

        cache.put(2, 30);
        assertEquals(3, cache.size());

        // Overwrite doesn't change size
        cache.put(1, 200);
        assertEquals(3, cache.size());
    }

    @Test
    public void testHighVolume_manyEntries() {
        ClientSymbolCache cache = new ClientSymbolCache(16); // Small initial capacity

        // Add many entries to trigger resizing
        for (int i = 0; i < 10000; i++) {
            cache.put(i, i * 2);
        }

        assertEquals(10000, cache.size());

        // Verify all entries are retrievable
        for (int i = 0; i < 10000; i++) {
            assertEquals(i * 2, cache.get(i));
        }
    }

    @Test
    public void testCheckAndInvalidate_firstCall_recordsWatermark() {
        ClientSymbolCache cache = new ClientSymbolCache();

        // First call should just record the watermark, not invalidate
        assertFalse(cache.checkAndInvalidate(100));
        assertEquals(100, cache.getLastKnownWatermark());
    }

    @Test
    public void testCheckAndInvalidate_sameWatermark_noInvalidation() {
        ClientSymbolCache cache = new ClientSymbolCache();

        cache.put(0, 10);
        cache.put(1, 150);  // Above watermark 100

        // Initialize watermark
        cache.checkAndInvalidate(100);

        // Same watermark should not invalidate
        assertFalse(cache.checkAndInvalidate(100));

        // All entries should still be present
        assertEquals(10, cache.get(0));
        assertEquals(150, cache.get(1));
    }

    @Test
    public void testCheckAndInvalidate_watermarkChanged_clearsCache() {
        ClientSymbolCache cache = new ClientSymbolCache();

        // Add some entries
        cache.put(0, 10);
        cache.put(1, 50);
        cache.put(2, 100);
        cache.put(3, 150);

        // Initialize watermark at 100
        cache.checkAndInvalidate(100);
        assertEquals(4, cache.size());

        // Watermark increases to 200 (e.g., after WAL apply)
        assertTrue(cache.checkAndInvalidate(200));

        // All entries should be cleared (conservative approach)
        assertEquals(0, cache.size());
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(0));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(1));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(2));
        assertEquals(ClientSymbolCache.NO_ENTRY, cache.get(3));
    }

    @Test
    public void testCheckAndInvalidate_watermarkDecreased_clearsCache() {
        ClientSymbolCache cache = new ClientSymbolCache();

        cache.put(0, 10);
        cache.put(1, 20);

        // Initialize watermark at 100
        cache.checkAndInvalidate(100);

        // Watermark decreases (unusual but possible)
        assertTrue(cache.checkAndInvalidate(50));

        // Cache should be cleared
        assertEquals(0, cache.size());
    }

    @Test
    public void testClear_resetsWatermark() {
        ClientSymbolCache cache = new ClientSymbolCache();

        cache.checkAndInvalidate(100);
        assertEquals(100, cache.getLastKnownWatermark());

        cache.clear();

        // Watermark should be reset to -1
        assertEquals(-1, cache.getLastKnownWatermark());
    }

    @Test
    public void testLargeClientIds_supported() {
        ClientSymbolCache cache = new ClientSymbolCache();

        // Large client IDs should work
        cache.put(Integer.MAX_VALUE, 10);
        cache.put(Integer.MAX_VALUE - 1, 20);
        cache.put(1000000, 30);

        assertEquals(10, cache.get(Integer.MAX_VALUE));
        assertEquals(20, cache.get(Integer.MAX_VALUE - 1));
        assertEquals(30, cache.get(1000000));
    }

    @Test
    public void testZeroTableId_cached() {
        ClientSymbolCache cache = new ClientSymbolCache();

        // Zero is a valid table symbol ID
        cache.put(0, 0);
        assertEquals(0, cache.get(0));

        // Verify it's distinguishable from NO_ENTRY
        assertNotEquals(ClientSymbolCache.NO_ENTRY, cache.get(0));
    }
}
