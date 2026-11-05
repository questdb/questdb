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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.RowExpirySkipVerdictCache;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.LongLongHashMap;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class RowExpirySkipVerdictCacheTest extends AbstractCairoTest {
    private static final long HOUR_MICROS = 3_600_000_000L;
    private static final int OTHER_CACHE_SIZE = 4;
    private static final String PREDICATE = "v < 0";

    @Test
    public void testPruneAllAbsentViews() throws Exception {
        assertPruneViews(1024, 0, 1024);
    }

    @Test
    public void testPruneDroppedAndRecreatedViewDoesNotInheritCache() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken dropped = token("recreated", 1);
            final TableToken recreated = token("recreated", 2);
            final TableToken kept = token("kept", 3);
            final RowExpirySkipVerdictCache cache = newCache();
            final Discovery discovery = new Discovery();
            createCache(cache, dropped, 3);
            final LongLongHashMap keptGenerations = createCache(cache, kept, 2);
            final Object keptCache = cache(cache, kept);
            discovery.add(recreated, PREDICATE);
            // A rename keeps the directory identity, unlike dropping and recreating the view.
            discovery.add(kept.renamed("renamed"), PREDICATE);

            discovery.prune(cache);

            Assert.assertNull(cache(cache, dropped));
            Assert.assertNull("discovery must not create a cache for the replacement view", cache(cache, recreated));
            Assert.assertSame(keptCache, cache(cache, kept));
            Assert.assertSame(keptGenerations, cache.getGenerations(kept));
            assertEntries(keptGenerations, 2, 0, 0);
            Assert.assertEquals(1, cache.getViewVerdicts().size());
            Assert.assertEquals(2, cache.getCachedPartitionCount());
        });
    }

    @Test
    public void testPruneDroppedPolicyFreesCapacityOnDiscovery() throws Exception {
        assertPruneRemovedViewFreesCapacity("ALTER MATERIALIZED VIEW mv_removed DROP EXPIRE");
    }

    @Test
    public void testPruneDroppedViewFreesCapacityOnDiscovery() throws Exception {
        assertPruneRemovedViewFreesCapacity("DROP MATERIALIZED VIEW mv_removed");
    }

    @Test
    public void testPruneEmptyCacheDoesNotCreateEntries() throws Exception {
        assertMemoryLeak(() -> {
            final RowExpirySkipVerdictCache cache = newCache();
            final Discovery discovery = new Discovery();
            final CharSequenceObjHashMap<?> original = cache.getViewVerdicts();
            discovery.add(token("uncached", 1), PREDICATE);

            discovery.prune(cache);

            Assert.assertSame(original, cache.getViewVerdicts());
            Assert.assertEquals(0, original.size());
            Assert.assertEquals(0, cache.getCachedPartitionCount());
        });
    }

    @Test
    public void testPruneFailedRebuildKeepsOriginalMapAndAccounting() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken dropped = token("dropped", 1);
            final TableToken changed = token("changed", 2);
            final TableToken kept = token("kept", 3);
            final CacheOperationCountingMap original = new CacheOperationCountingMap();
            final RowExpirySkipVerdictCache cache = newCache(original);
            final Discovery discovery = new Discovery();
            createCache(cache, dropped, 4);
            final LongLongHashMap changedGenerations = createCache(cache, changed, 3);
            final LongLongHashMap keptGenerations = createCache(cache, kept, 2);
            discovery.add(changed, "v > 100");
            discovery.add(kept, PREDICATE);
            discovery.add(token("uncached", 4), PREDICATE);
            original.getCount = 0;
            // Fail after the first pass and one insertion into the replacement map.
            original.failOnGet = 5;

            final RuntimeException error = Assert.assertThrows(RuntimeException.class, () -> discovery.prune(cache));

            Assert.assertSame(original.lookupFailure, error);
            Assert.assertSame(original, cache.getViewVerdicts());
            Assert.assertEquals(3, original.size());
            Assert.assertEquals("predicate invalidation is accounted for, but eviction is not yet published",
                    6, cache.getCachedPartitionCount());
            Assert.assertEquals(0, changedGenerations.size());
            Assert.assertEquals("v > 100", cache.getPredicate(changed));
            assertEntries(keptGenerations, 2, 0, 0);
            assertEntries(cache.getGenerations(dropped), 4, 0, 0);

            original.failOnGet = 0;
            discovery.prune(cache);

            Assert.assertNotSame(original, cache.getViewVerdicts());
            Assert.assertEquals(2, cache.getViewVerdicts().size());
            Assert.assertEquals(2, cache.getCachedPartitionCount());
            Assert.assertNull(cache(cache, dropped));
            Assert.assertSame(changedGenerations, cache.getGenerations(changed));
            Assert.assertSame(keptGenerations, cache.getGenerations(kept));
            Assert.assertEquals(0, original.removalCount);
        });
    }

    @Test
    public void testPruneManyAbsentViewsPreservesOtherViews() throws Exception {
        assertPruneViews(1024, 0, 768);
    }

    @Test
    public void testPruneMissingFirstMiddleAndLastView() throws Exception {
        assertPruneViews(8, 0, 1);
        assertPruneViews(8, 4, 5);
        assertPruneViews(8, 7, 8);
    }

    @Test
    public void testPrunePredicateChangeDoesNotRebuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken changed = token("changed", 1);
            final TableToken kept = token("kept", 2);
            final TableToken empty = token("empty", 3);
            final RowExpirySkipVerdictCache cache = newCache();
            final Discovery discovery = new Discovery();
            final CharSequenceObjHashMap<?> original = cache.getViewVerdicts();
            final LongLongHashMap changedGenerations = createCache(cache, changed, 3);
            final LongLongHashMap keptGenerations = createCache(cache, kept, 2);
            final LongLongHashMap emptyGenerations = createCache(cache, empty, 0);
            discovery.add(changed, "v > 100");
            discovery.add(kept, new String(PREDICATE));
            discovery.add(empty, PREDICATE);

            for (int i = 0; i < 2; i++) {
                discovery.prune(cache);

                Assert.assertSame(original, cache.getViewVerdicts());
                Assert.assertEquals(3, original.size());
                Assert.assertEquals(2, cache.getCachedPartitionCount());
                Assert.assertSame(changedGenerations, cache.getGenerations(changed));
                Assert.assertEquals(0, changedGenerations.size());
                Assert.assertEquals("v > 100", cache.getPredicate(changed));
                Assert.assertSame(keptGenerations, cache.getGenerations(kept));
                assertEntries(keptGenerations, 2, 0, 0);
                Assert.assertSame(emptyGenerations, cache.getGenerations(empty));
            }
        });
    }

    @Test
    public void testPruneUnchangedDiscoveryUsesLinearLookups() throws Exception {
        assertPruneViews(64, 0, 0);
        assertPruneViews(1024, 0, 0);
    }

    @Test
    public void testReleaseAllAbsentFloorsFromNonEmptySnapshot() throws Exception {
        assertReleaseAbsentFloors(16_380, 0, 16_380);
    }

    @Test
    public void testReleaseEmptySnapshotClearsOnlyAffectedCache() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = token("expired", 1);
            final TableToken otherToken = token("kept", 2);
            final RowExpirySkipVerdictCache cache = newCache();
            final LongList floors = new LongList();
            final RemovalCountingMap original = createCache(cache, token, 16_380);
            final RemovalCountingMap other = createCache(cache, otherToken, OTHER_CACHE_SIZE);

            cache.releaseAbsent(token, floors);

            Assert.assertEquals("bulk release must not remove keys individually", 0, original.removalCount);
            Assert.assertSame("an empty snapshot clears the existing map", original, cache.getGenerations(token));
            Assert.assertEquals(0, original.size());
            Assert.assertEquals(OTHER_CACHE_SIZE, cache.getCachedPartitionCount());
            Assert.assertSame(other, cache.getGenerations(otherToken));
            assertEntries(other, OTHER_CACHE_SIZE, 0, 0);

            cache.releaseAbsent(token, floors);
            Assert.assertEquals("releasing an empty cache must not debit the count twice",
                    OTHER_CACHE_SIZE, cache.getCachedPartitionCount());
        });
    }

    @Test
    public void testReleaseManyAbsentFloorsPreservesOtherViews() throws Exception {
        assertReleaseAbsentFloors(16_380, 0, 12_285);
    }

    @Test
    public void testReleaseMissingFirstMiddleAndLastFloor() throws Exception {
        assertReleaseAbsentFloors(8, 0, 1);
        assertReleaseAbsentFloors(8, 4, 5);
        assertReleaseAbsentFloors(8, 7, 8);
    }

    @Test
    public void testReleaseUnchangedSnapshotDoesNotRebuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = token("kept", 1);
            final RowExpirySkipVerdictCache cache = newCache();
            final RemovalCountingMap original = createCache(cache, token, 8);
            final LongList floors = new LongList();
            for (int i = 0; i < 9; i++) {
                floors.add(floor(i));
            }

            cache.releaseAbsent(token, floors);

            Assert.assertSame("an unchanged cache must not allocate a replacement map",
                    original, cache.getGenerations(token));
            Assert.assertEquals(0, original.removalCount);
            Assert.assertEquals(8, cache.getCachedPartitionCount());
            assertEntries(original, 8, 0, 0);
            Assert.assertEquals("release must not invent a verdict for an uncached floor",
                    Long.MIN_VALUE, original.get(floor(8)));
        });
    }

    private static void assertEntries(LongLongHashMap generations, int size, int removeFrom, int removeTo) {
        Assert.assertEquals(size - (removeTo - removeFrom), generations.size());
        for (int i = 0; i < size; i++) {
            Assert.assertEquals(
                    "generation at floor " + i,
                    i >= removeFrom && i < removeTo ? Long.MIN_VALUE : generation(i),
                    generations.get(floor(i))
            );
        }
    }

    private static void assertPruneViews(int size, int removeFrom, int removeTo) throws Exception {
        assertMemoryLeak(() -> {
            final CacheOperationCountingMap original = new CacheOperationCountingMap();
            final RowExpirySkipVerdictCache cache = newCache(original);
            final DiscoveryCountingList discovered = new DiscoveryCountingList();
            final Discovery discovery = new Discovery(discovered);
            final ObjList<TableToken> tokens = new ObjList<>();
            final ObjList<Object> caches = new ObjList<>();
            final ObjList<LongLongHashMap> generations = new ObjList<>();
            for (int i = 0; i < size; i++) {
                final TableToken token = token("view" + i, i + 1);
                tokens.add(token);
                generations.add(createCache(cache, token, 2));
                caches.add(cache(cache, token));
            }
            // Deliberately differ from cache insertion order and include an uncached view.
            for (int i = size - 1; i >= 0; i--) {
                if (i < removeFrom || i >= removeTo) {
                    discovery.add(tokens.getQuick(i), PREDICATE);
                }
            }
            final TableToken uncached = token("uncached", size + 1);
            discovery.add(uncached, PREDICATE);
            original.getCount = 0;

            discovery.prune(cache);

            Assert.assertEquals("bulk pruning must not remove keys individually", 0, original.removalCount);
            Assert.assertTrue("discovery reads must be linear, actual=" + discovered.readCount,
                    discovered.readCount <= 2 * discovered.size());
            Assert.assertTrue("cache lookups must be linear, actual=" + original.getCount,
                    original.getCount <= 2 * discovered.size());
            final CharSequenceObjHashMap<?> retained = cache.getViewVerdicts();
            if (removeFrom == removeTo) {
                Assert.assertSame("unchanged discovery must not replace the map", original, retained);
            } else {
                Assert.assertNotSame(original, retained);
            }
            final int retainedSize = size - (removeTo - removeFrom);
            Assert.assertEquals(retainedSize, retained.size());
            Assert.assertEquals(2 * retainedSize, cache.getCachedPartitionCount());
            Assert.assertNull(cache(cache, uncached));
            for (int i = 0; i < size; i++) {
                final TableToken token = tokens.getQuick(i);
                if (i >= removeFrom && i < removeTo) {
                    Assert.assertNull(cache(cache, token));
                } else {
                    Assert.assertSame(caches.getQuick(i), cache(cache, token));
                    Assert.assertSame(generations.getQuick(i), cache.getGenerations(token));
                    assertEntries(cache.getGenerations(token), 2, 0, 0);
                }
            }

            discovered.readCount = 0;
            discovery.prune(cache);

            Assert.assertSame("the next unchanged discovery must reuse the map", retained, cache.getViewVerdicts());
            Assert.assertEquals(2 * retainedSize, cache.getCachedPartitionCount());
            Assert.assertTrue(discovered.readCount <= 2 * discovered.size());
        });
    }

    private static void assertReleaseAbsentFloors(int size, int removeFrom, int removeTo) throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = token("expired", 1);
            final TableToken otherToken = token("kept", 2);
            final RowExpirySkipVerdictCache cache = newCache();
            final RemovalCountingMap original = createCache(cache, token, size);
            final RemovalCountingMap other = createCache(cache, otherToken, OTHER_CACHE_SIZE);
            final LongList floors = new LongList();
            for (int i = 0; i < size; i++) {
                if (i < removeFrom || i >= removeTo) {
                    floors.add(floor(i));
                }
            }
            // A live, uncached partition must not acquire a verdict during release.
            floors.add(floor(size));

            cache.releaseAbsent(token, floors);

            Assert.assertEquals("bulk release must not remove keys individually", 0, original.removalCount);
            final LongLongHashMap retained = cache.getGenerations(token);
            Assert.assertNotSame(original, retained);
            assertEntries(retained, size, removeFrom, removeTo);
            Assert.assertEquals(Long.MIN_VALUE, retained.get(floor(size)));
            Assert.assertEquals(size - (removeTo - removeFrom) + OTHER_CACHE_SIZE, cache.getCachedPartitionCount());
            Assert.assertSame(other, cache.getGenerations(otherToken));
            Assert.assertEquals(0, other.removalCount);
            assertEntries(other, OTHER_CACHE_SIZE, 0, 0);
            Assert.assertEquals(PREDICATE, cache.getPredicate(token));

            cache.releaseAbsent(token, floors);
            Assert.assertSame("the next unchanged sweep must reuse the rebuilt map",
                    retained, cache.getGenerations(token));
            Assert.assertEquals(size - (removeTo - removeFrom) + OTHER_CACHE_SIZE, cache.getCachedPartitionCount());
        });
    }

    private static Object cache(RowExpirySkipVerdictCache cache, TableToken token) {
        return cache.getViewVerdicts().get(token.getDirName());
    }

    private static RemovalCountingMap createCache(RowExpirySkipVerdictCache cache, TableToken token, int size) {
        final RemovalCountingMap generations = new RemovalCountingMap(size);
        for (int i = 0; i < size; i++) {
            generations.put(floor(i), generation(i));
        }
        cache.putGenerations(token, PREDICATE, generations);
        return generations;
    }

    private static String expiryPredicate(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getMetadata().getExpiryPredicate();
        }
    }

    private static long floor(int index) {
        // Exercise negative floors and zero as well as positive timestamps.
        return (index - 4L) * HOUR_MICROS;
    }

    private static long generation(int index) {
        return 31L * index + 7;
    }

    // Sizes the cache the way RowExpiryCleanupJob does, from the configured bound.
    private static RowExpirySkipVerdictCache newCache() {
        return new RowExpirySkipVerdictCache(engine.getConfiguration().getMatViewRowExpiryCleanupMaxCachedPartitions());
    }

    private static RowExpirySkipVerdictCache newCache(CharSequenceObjHashMap<?> viewVerdicts) {
        return new RowExpirySkipVerdictCache(engine.getConfiguration().getMatViewRowExpiryCleanupMaxCachedPartitions(), viewVerdicts);
    }

    private static TableToken token(String name, int id) {
        return new TableToken(name, name + "~" + id, null, id, TableToken.Type.MAT_VIEW, true, false, false, false);
    }

    private void assertPruneRemovedViewFreesCapacity(String ddl) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                        (1.0, '2024-01-01T00:00:00.000000Z'),
                        (2.0, '2024-01-02T00:00:00.000000Z'),
                        (3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv_removed AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 0");
            execute("CREATE MATERIALIZED VIEW mv_kept AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 0");
            drainWalAndMatViewQueues();

            final TableToken removed = engine.verifyTableName("mv_removed");
            final TableToken kept = engine.verifyTableName("mv_kept");
            final String removedPredicate = expiryPredicate(removed);
            final String keptPredicate = expiryPredicate(kept);
            setProperty(PropertyKey.CAIRO_MAT_VIEW_ROW_EXPIRY_CLEANUP_MAX_CACHED_PARTITIONS, "3");
            final RowExpirySkipVerdictCache cache = newCache();
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine, cache)) {
                Assert.assertFalse(job.cleanupTable(removed, removedPredicate));
                Assert.assertFalse(job.cleanupTable(kept, keptPredicate));
                Assert.assertEquals(4, job.getScalarPartitionScanCount());
                final Object keptCache = cache(cache, kept);
                final LongLongHashMap keptGenerations = cache.getGenerations(kept);
                Assert.assertEquals(1, keptGenerations.size());
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(5, job.getScalarPartitionScanCount());

                execute(ddl);
                drainWalAndMatViewQueues();
                Assert.assertFalse(job.runNow());
                Assert.assertEquals("discovery prunes even though the retained view is not due for cleanup",
                        5, job.getScalarPartitionScanCount());
                Assert.assertNull(cache(cache, removed));
                Assert.assertSame(keptCache, cache(cache, kept));
                Assert.assertSame(keptGenerations, cache.getGenerations(kept));
                Assert.assertEquals(1, cache.getCachedPartitionCount());

                Assert.assertFalse(job.cleanupTable(kept, keptPredicate));
                Assert.assertEquals("the retained view keeps its verdict and fills a freed slot",
                        6, job.getScalarPartitionScanCount());
                Assert.assertEquals(2, cache.getCachedPartitionCount());
                Assert.assertFalse(job.cleanupTable(kept, keptPredicate));
                Assert.assertEquals(6, job.getScalarPartitionScanCount());
            }
            assertQuery("SELECT count() FROM mv_kept").noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");
        });
    }

    private static final class CacheOperationCountingMap extends CharSequenceObjHashMap<Object> {
        private final RuntimeException lookupFailure = new RuntimeException("injected cache lookup failure");
        private int failOnGet;
        private int getCount;
        private int removalCount;

        @Override
        public Object get(CharSequence key) {
            if (++getCount == failOnGet) {
                throw lookupFailure;
            }
            return super.get(key);
        }

        @Override
        public void removeAt(int index) {
            removalCount++;
            super.removeAt(index);
        }
    }

    // A policied-object snapshot as RowExpiryCleanupJob discovers it: tokens paired by index with predicates.
    private static final class Discovery {
        private final ObjList<String> predicates = new ObjList<>();
        private final ObjList<TableToken> tokens;

        private Discovery() {
            this(new ObjList<>());
        }

        private Discovery(ObjList<TableToken> tokens) {
            this.tokens = tokens;
        }

        private void add(TableToken token, String predicate) {
            tokens.add(token);
            predicates.add(predicate);
        }

        private void prune(RowExpirySkipVerdictCache cache) {
            cache.prune(tokens, predicates);
        }
    }

    private static final class DiscoveryCountingList extends ObjList<TableToken> {
        private int readCount;

        @Override
        public TableToken getQuick(int index) {
            readCount++;
            return super.getQuick(index);
        }
    }

    // Counts single-key removals. Bulk release rebuilds the map instead of removing keys one by one.
    private static final class RemovalCountingMap extends LongLongHashMap {
        private int removalCount;

        private RemovalCountingMap(int capacity) {
            super(capacity, 0.5, Long.MIN_VALUE, Long.MIN_VALUE);
        }

        @Override
        public void removeAt(int index) {
            removalCount++;
            super.removeAt(index);
        }
    }
}
