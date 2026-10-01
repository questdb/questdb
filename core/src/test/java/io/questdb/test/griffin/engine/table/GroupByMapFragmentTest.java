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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LimitedMemoryTracker;
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
            // key-arena chunk index. The key sink allocated in between is global-only (it never
            // touches the tracker), so it does not move used and does not trip the limit.
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
                // close() must release the partially-opened shard 0. Before the fix the shard was
                // registered only after reopen() returned, so the breach orphaned it: assertMemoryLeak
                // saw a NATIVE_UNORDERED_MAP leak (memStart plus the global-only key sink, 3104 B) and
                // the per-query counter never returned to zero (memStart alone, 3072 B).
                Assert.assertEquals(0, tracker.getUsed());
            }
        });
    }

    @Test
    public void testShardReuseAfterBreachReopensAllShards() throws Exception {
        assertMemoryLeak(() -> {
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes().add(ColumnType.VARCHAR);
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes().add(ColumnType.LONG);

            // See testShardOpenBreachReleasesPartialShardMap: a limit of exactly one shard's
            // memStart trips shard 0's next tracker-charged allocation, the key-arena chunk index.
            final long shardMemStart;
            try (Map probe = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes, true, false)) {
                probe.reopen();
                shardMemStart = probe.getHeapSize();
            }

            final GroupByMapFragment fragment = new GroupByMapFragment(configuration, keyTypes, valueTypes, 4, 0);

            // First shard() breaches mid-open. reopenShards() registers all 256 shards before
            // opening any, so the breach leaves the shards list full, not partially filled. A
            // partially filled list is what makes the reuse below index past the end.
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(shardMemStart)) {
                fragment.setMemoryTracker(tracker);
                try {
                    try {
                        fragment.shard();
                        Assert.fail("expected a per-query memory breach during shard open");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                    }
                    Assert.assertTrue("a breached shard() must not mark the fragment sharded", fragment.isNotSharded());
                    Assert.assertEquals(GroupByMapFragment.NUM_SHARDS, fragment.getShards().size());
                } finally {
                    fragment.close();
                }
                Assert.assertEquals(0, tracker.getUsed());
            }

            // Reuse the same fragment under a generous limit. This takes the reuse branch of
            // reopenShards(), which reopens every shard via shards.getQuick(i). Before the fix the
            // breach left a partial list, so this read past the end and threw AssertionError (or
            // NPE without -ea) on the second shard().
            try (LimitedMemoryTracker tracker = new LimitedMemoryTracker(64 * 1024 * 1024L)) {
                fragment.setMemoryTracker(tracker);
                try {
                    fragment.shard();
                    Assert.assertFalse("fragment should be sharded after reuse", fragment.isNotSharded());
                    Assert.assertEquals(GroupByMapFragment.NUM_SHARDS, fragment.getShards().size());
                } finally {
                    fragment.close();
                }
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
}
