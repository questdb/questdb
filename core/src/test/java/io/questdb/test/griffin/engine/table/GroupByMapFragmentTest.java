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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.griffin.engine.table.GroupByMapFragment;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * White-box coverage for {@link GroupByMapFragment} shard allocation under a per-query
 * memory limit. The first time a fragment shards, {@code reopenShards()} opens 256 lazy
 * shard maps; each shard's {@code reopen()} allocates the hash-table backing (memStart)
 * and the key sink under {@code NATIVE_UNORDERED_MAP}, then its key-arena chunk index
 * under {@code NATIVE_GROUP_BY_FUNCTION}. When the per-query limit trips on the chunk
 * index, the shard is already partially allocated; it must still be released at
 * {@link GroupByMapFragment#close()}.
 * <p>
 * The matching end-to-end probe is
 * {@code ParallelGroupByMemoryTrackerTest#testParallelKeyedGroupByFailsOnLargeKeySet},
 * but the breach lands on the leaking allocation only for some thread interleavings, so
 * it catches this leak intermittently. Driving the fragment directly makes the breach
 * point deterministic.
 */
public class GroupByMapFragmentTest extends AbstractCairoTest {

    @Test
    public void testShardOpenBreachReleasesPartialShardMap() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.VARCHAR);
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes().add(ColumnType.LONG);

            // Measure one shard map's memStart with no tracker bound. reopenShards() opens each
            // shard's memStart first, so a limit of exactly this size lets shard 0's memStart
            // through (used == limit) and trips its very next tracker-charged allocation, the
            // key-arena chunk index, after memStart and the key sink are already allocated.
            final long shardMemStart;
            try (Map probe = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, true, false)) {
                probe.reopen();
                shardMemStart = probe.getHeapSize();
            }

            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(shardMemStart)) {
                final GroupByMapFragment fragment = new GroupByMapFragment(configuration, keyTypes, valueTypes, 4, 0);
                fragment.setMemoryTracker(tracker);
                try {
                    try {
                        fragment.shard();
                        Assert.fail("expected a per-query memory breach during shard open");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    }
                } finally {
                    fragment.close();
                }
                // close() must release the partially-opened shard 0 (memStart + key sink). Before
                // the fix the shard was registered only after reopen() returned, so the breach
                // orphaned it: assertMemoryLeak saw a NATIVE_UNORDERED_MAP leak and the per-query
                // counter never returned to zero.
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testSuccessfulShardReleasesAllShards() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.VARCHAR);
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes().add(ColumnType.LONG);

            // A generous limit lets every shard open. close() must return every tracker-charged byte.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024L)) {
                final GroupByMapFragment fragment = new GroupByMapFragment(configuration, keyTypes, valueTypes, 4, 0);
                fragment.setMemoryTracker(tracker);
                fragment.shard();
                Assert.assertFalse("fragment should be sharded", fragment.isNotSharded());
                fragment.close();
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    /**
     * Minimal {@link MemoryTracker} with a fixed limit, backed by its own native
     * {@code {used, limit}} block. The production {@code Unsafe} allocation path reads and
     * updates the block through {@link #nativeAddress()} exactly as it does for a pooled
     * per-query tracker.
     */
    private static final class LimitedMemoryTracker extends MemoryTracker {
        private long nativeAddress;

        LimitedMemoryTracker(long limitBytes) {
            nativeAddress = Unsafe.malloc(Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
            Unsafe.putLong(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET, 0L);
            Unsafe.putLong(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET, limitBytes);
        }

        @Override
        public void close() {
            if (nativeAddress != 0) {
                freeNativeAllocators();
                nativeAddress = Unsafe.free(nativeAddress, Unsafe.MEMORY_TRACKER_BLOCK_SIZE, MemoryTag.NATIVE_MEMORY_TRACKER);
            }
        }

        @Override
        public long getLimit() {
            return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_LIMIT_OFFSET);
        }

        @Override
        public long getQueryId() {
            return 1;
        }

        @Override
        public long getUsed() {
            return Unsafe.getLongVolatile(nativeAddress + Unsafe.MEMORY_TRACKER_USED_OFFSET);
        }

        @Override
        public MemoryTrackerWorkload getWorkload() {
            return MemoryTrackerWorkload.QUERY;
        }

        @Override
        public long nativeAddress() {
            return nativeAddress;
        }
    }
}
