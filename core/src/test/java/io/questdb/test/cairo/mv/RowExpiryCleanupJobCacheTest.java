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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RowExpiryCleanupJobCacheTest extends AbstractCairoTest {
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
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                createCache(job, dropped, 3);
                final CharSequenceLongHashMap keptGenerations = createCache(job, kept, 2);
                final Object keptCache = cache(job, kept);
                discover(job, recreated, PREDICATE);
                // A rename keeps the directory identity, unlike dropping and recreating the view.
                discover(job, kept.renamed("renamed"), PREDICATE);

                prune(job);

                Assert.assertNull(cache(job, dropped));
                Assert.assertNull("discovery must not create a cache for the replacement view", cache(job, recreated));
                Assert.assertSame(keptCache, cache(job, kept));
                Assert.assertSame(keptGenerations, generations(job, kept));
                assertEntries(keptGenerations, 2, 0, 0);
                Assert.assertEquals(1, cacheMap(job).size());
                Assert.assertEquals(2, cachedPartitionCount(job));
            }
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
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final CharSequenceObjHashMap<?> original = cacheMap(job);
                discover(job, token("uncached", 1), PREDICATE);

                prune(job);

                Assert.assertSame(original, cacheMap(job));
                Assert.assertEquals(0, original.size());
                Assert.assertEquals(0, cachedPartitionCount(job));
            }
        });
    }

    @Test
    public void testPruneFailedRebuildKeepsOriginalMapAndAccounting() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken dropped = token("dropped", 1);
            final TableToken changed = token("changed", 2);
            final TableToken kept = token("kept", 3);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final CacheOperationCountingMap original = new CacheOperationCountingMap();
                field(RowExpiryCleanupJob.class, "scalarPartitionCaches").set(job, original);
                createCache(job, dropped, 4);
                final CharSequenceLongHashMap changedGenerations = createCache(job, changed, 3);
                final CharSequenceLongHashMap keptGenerations = createCache(job, kept, 2);
                discover(job, changed, "v > 100");
                discover(job, kept, PREDICATE);
                discover(job, token("uncached", 4), PREDICATE);
                original.getCount = 0;
                // Fail after the first pass and one insertion into the replacement map.
                original.failOnGet = 5;

                final InvocationTargetException error = Assert.assertThrows(InvocationTargetException.class, () -> prune(job));

                Assert.assertSame(original.lookupFailure, error.getCause());
                Assert.assertSame(original, cacheMap(job));
                Assert.assertEquals(3, original.size());
                Assert.assertEquals("predicate invalidation is accounted for, but eviction is not yet published",
                        6, cachedPartitionCount(job));
                Assert.assertEquals(0, changedGenerations.size());
                Assert.assertEquals("v > 100", field(cache(job, changed).getClass(), "predicate").get(cache(job, changed)));
                assertEntries(keptGenerations, 2, 0, 0);
                assertEntries(generations(job, dropped), 4, 0, 0);

                original.failOnGet = 0;
                prune(job);

                Assert.assertNotSame(original, cacheMap(job));
                Assert.assertEquals(2, cacheMap(job).size());
                Assert.assertEquals(2, cachedPartitionCount(job));
                Assert.assertNull(cache(job, dropped));
                Assert.assertSame(changedGenerations, generations(job, changed));
                Assert.assertSame(keptGenerations, generations(job, kept));
                Assert.assertEquals(0, original.removalCount);
            }
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
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final CharSequenceObjHashMap<?> original = cacheMap(job);
                final CharSequenceLongHashMap changedGenerations = createCache(job, changed, 3);
                final CharSequenceLongHashMap keptGenerations = createCache(job, kept, 2);
                final CharSequenceLongHashMap emptyGenerations = createCache(job, empty, 0);
                discover(job, changed, "v > 100");
                discover(job, kept, new String(PREDICATE));
                discover(job, empty, PREDICATE);

                for (int i = 0; i < 2; i++) {
                    prune(job);

                    Assert.assertSame(original, cacheMap(job));
                    Assert.assertEquals(3, original.size());
                    Assert.assertEquals(2, cachedPartitionCount(job));
                    Assert.assertSame(changedGenerations, generations(job, changed));
                    Assert.assertEquals(0, changedGenerations.size());
                    Assert.assertEquals("v > 100", field(cache(job, changed).getClass(), "predicate").get(cache(job, changed)));
                    Assert.assertSame(keptGenerations, generations(job, kept));
                    assertEntries(keptGenerations, 2, 0, 0);
                    Assert.assertSame(emptyGenerations, generations(job, empty));
                }
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
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final RemovalCountingMap original = createCache(job, token, 16_380);
                final RemovalCountingMap other = createCache(job, otherToken, OTHER_CACHE_SIZE);

                release(job, token);

                Assert.assertEquals("bulk release must not remove keys individually", 0, original.removalCount);
                Assert.assertSame("an empty snapshot clears the existing map", original, generations(job, token));
                Assert.assertEquals(0, original.size());
                Assert.assertEquals(OTHER_CACHE_SIZE, cachedPartitionCount(job));
                Assert.assertSame(other, generations(job, otherToken));
                assertEntries(other, OTHER_CACHE_SIZE, 0, 0);

                release(job, token);
                Assert.assertEquals("releasing an empty cache must not debit the count twice",
                        OTHER_CACHE_SIZE, cachedPartitionCount(job));
            }
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
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final RemovalCountingMap original = createCache(job, token, 8);
                final LongList floors = partitionFloors(job);
                for (int i = 0; i < 9; i++) {
                    floors.add(floor(i));
                }

                release(job, token);

                Assert.assertSame("an unchanged cache must not allocate a replacement map",
                        original, generations(job, token));
                Assert.assertEquals(0, original.removalCount);
                Assert.assertEquals(8, cachedPartitionCount(job));
                assertEntries(original, 8, 0, 0);
                Assert.assertEquals("release must not invent a verdict for an uncached floor",
                        Long.MIN_VALUE, original.get(Long.toString(floor(8))));
            }
        });
    }

    private static void assertEntries(CharSequenceLongHashMap generations, int size, int removeFrom, int removeTo) {
        Assert.assertEquals(size - (removeTo - removeFrom), generations.size());
        for (int i = 0; i < size; i++) {
            Assert.assertEquals(
                    "generation at floor " + i,
                    i >= removeFrom && i < removeTo ? Long.MIN_VALUE : generation(i),
                    generations.get(Long.toString(floor(i)))
            );
        }
    }

    private static void assertPruneViews(int size, int removeFrom, int removeTo) throws Exception {
        assertMemoryLeak(() -> {
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final CacheOperationCountingMap original = new CacheOperationCountingMap();
                field(RowExpiryCleanupJob.class, "scalarPartitionCaches").set(job, original);
                final DiscoveryCountingList discovered = new DiscoveryCountingList();
                field(RowExpiryCleanupJob.class, "discoveredTokens").set(job, discovered);
                final ObjList<TableToken> tokens = new ObjList<>();
                final ObjList<Object> caches = new ObjList<>();
                final ObjList<CharSequenceLongHashMap> generations = new ObjList<>();
                for (int i = 0; i < size; i++) {
                    final TableToken token = token("view" + i, i + 1);
                    tokens.add(token);
                    generations.add(createCache(job, token, 2));
                    caches.add(cache(job, token));
                }
                // Deliberately differ from cache insertion order and include an uncached view.
                for (int i = size - 1; i >= 0; i--) {
                    if (i < removeFrom || i >= removeTo) {
                        discover(job, tokens.getQuick(i), PREDICATE);
                    }
                }
                final TableToken uncached = token("uncached", size + 1);
                discover(job, uncached, PREDICATE);
                original.getCount = 0;

                prune(job);

                Assert.assertEquals("bulk pruning must not remove keys individually", 0, original.removalCount);
                Assert.assertTrue("discovery reads must be linear, actual=" + discovered.readCount,
                        discovered.readCount <= 2 * discovered.size());
                Assert.assertTrue("cache lookups must be linear, actual=" + original.getCount,
                        original.getCount <= 2 * discovered.size());
                final CharSequenceObjHashMap<?> retained = cacheMap(job);
                if (removeFrom == removeTo) {
                    Assert.assertSame("unchanged discovery must not replace the map", original, retained);
                } else {
                    Assert.assertNotSame(original, retained);
                }
                final int retainedSize = size - (removeTo - removeFrom);
                Assert.assertEquals(retainedSize, retained.size());
                Assert.assertEquals(2 * retainedSize, cachedPartitionCount(job));
                Assert.assertNull(cache(job, uncached));
                for (int i = 0; i < size; i++) {
                    final TableToken token = tokens.getQuick(i);
                    if (i >= removeFrom && i < removeTo) {
                        Assert.assertNull(cache(job, token));
                    } else {
                        Assert.assertSame(caches.getQuick(i), cache(job, token));
                        Assert.assertSame(generations.getQuick(i), generations(job, token));
                        assertEntries(generations(job, token), 2, 0, 0);
                    }
                }

                discovered.readCount = 0;
                prune(job);

                Assert.assertSame("the next unchanged discovery must reuse the map", retained, cacheMap(job));
                Assert.assertEquals(2 * retainedSize, cachedPartitionCount(job));
                Assert.assertTrue(discovered.readCount <= 2 * discovered.size());
            }
        });
    }

    private static void assertReleaseAbsentFloors(int size, int removeFrom, int removeTo) throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = token("expired", 1);
            final TableToken otherToken = token("kept", 2);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final RemovalCountingMap original = createCache(job, token, size);
                final RemovalCountingMap other = createCache(job, otherToken, OTHER_CACHE_SIZE);
                final LongList floors = partitionFloors(job);
                for (int i = 0; i < size; i++) {
                    if (i < removeFrom || i >= removeTo) {
                        floors.add(floor(i));
                    }
                }
                // A live, uncached partition must not acquire a verdict during release.
                floors.add(floor(size));

                release(job, token);

                Assert.assertEquals("bulk release must not remove keys individually", 0, original.removalCount);
                final CharSequenceLongHashMap retained = generations(job, token);
                Assert.assertNotSame(original, retained);
                assertEntries(retained, size, removeFrom, removeTo);
                Assert.assertEquals(Long.MIN_VALUE, retained.get(Long.toString(floor(size))));
                Assert.assertEquals(size - (removeTo - removeFrom) + OTHER_CACHE_SIZE, cachedPartitionCount(job));
                Assert.assertSame(other, generations(job, otherToken));
                Assert.assertEquals(0, other.removalCount);
                assertEntries(other, OTHER_CACHE_SIZE, 0, 0);
                Assert.assertEquals(PREDICATE, field(cache(job, token).getClass(), "predicate").get(cache(job, token)));

                release(job, token);
                Assert.assertSame("the next unchanged sweep must reuse the rebuilt map",
                        retained, generations(job, token));
                Assert.assertEquals(size - (removeTo - removeFrom) + OTHER_CACHE_SIZE, cachedPartitionCount(job));
            }
        });
    }

    private static Object cache(RowExpiryCleanupJob job, TableToken token) throws Exception {
        return cacheMap(job).get(token.getDirName());
    }

    private static CharSequenceObjHashMap<?> cacheMap(RowExpiryCleanupJob job) throws Exception {
        return (CharSequenceObjHashMap<?>) field(RowExpiryCleanupJob.class, "scalarPartitionCaches").get(job);
    }

    private static int cachedPartitionCount(RowExpiryCleanupJob job) throws Exception {
        return field(RowExpiryCleanupJob.class, "cachedPartitionCount").getInt(job);
    }

    private static RemovalCountingMap createCache(RowExpiryCleanupJob job, TableToken token, int size) throws Exception {
        final Method method = RowExpiryCleanupJob.class.getDeclaredMethod("scalarPartitionCache", TableToken.class, String.class);
        method.setAccessible(true);
        final Object cache = method.invoke(job, token, PREDICATE);
        final RemovalCountingMap generations = new RemovalCountingMap(size);
        for (int i = 0; i < size; i++) {
            generations.put(Long.toString(floor(i)), generation(i));
        }
        field(cache.getClass(), "generations").set(cache, generations);
        field(RowExpiryCleanupJob.class, "cachedPartitionCount").setInt(job, cachedPartitionCount(job) + size);
        return generations;
    }

    @SuppressWarnings("unchecked")
    private static void discover(RowExpiryCleanupJob job, TableToken token, String predicate) throws Exception {
        ((ObjList<TableToken>) field(RowExpiryCleanupJob.class, "discoveredTokens").get(job)).add(token);
        ((ObjList<String>) field(RowExpiryCleanupJob.class, "discoveredPredicates").get(job)).add(predicate);
    }

    private static String expiryPredicate(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            return reader.getMetadata().getExpiryPredicate();
        }
    }

    private static Field field(Class<?> type, String name) throws Exception {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static long floor(int index) {
        // Exercise negative floors and zero as well as positive timestamps.
        return (index - 4L) * HOUR_MICROS;
    }

    private static long generation(int index) {
        return 31L * index + 7;
    }

    private static CharSequenceLongHashMap generations(RowExpiryCleanupJob job, TableToken token) throws Exception {
        final Object cache = cache(job, token);
        return (CharSequenceLongHashMap) field(cache.getClass(), "generations").get(cache);
    }

    private static LongList partitionFloors(RowExpiryCleanupJob job) throws Exception {
        return (LongList) field(RowExpiryCleanupJob.class, "partitionFloors").get(job);
    }

    private static void prune(RowExpiryCleanupJob job) throws Exception {
        final Method method = RowExpiryCleanupJob.class.getDeclaredMethod("pruneScalarPartitionCaches");
        method.setAccessible(true);
        method.invoke(job);
    }

    private static void release(RowExpiryCleanupJob job, TableToken token) throws Exception {
        final Method method = RowExpiryCleanupJob.class.getDeclaredMethod("releaseScalarPartitionsAbsentFromSnapshot", TableToken.class);
        method.setAccessible(true);
        method.invoke(job, token);
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
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.setMaxCachedPartitions(3);
                Assert.assertFalse(job.cleanupTable(removed, removedPredicate));
                Assert.assertFalse(job.cleanupTable(kept, keptPredicate));
                Assert.assertEquals(4, job.getScalarPartitionScanCount());
                final Object keptCache = cache(job, kept);
                final CharSequenceLongHashMap keptGenerations = generations(job, kept);
                Assert.assertEquals(1, keptGenerations.size());
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(5, job.getScalarPartitionScanCount());

                execute(ddl);
                drainWalAndMatViewQueues();
                Assert.assertFalse(job.runNow());
                Assert.assertEquals("discovery prunes even though the retained view is not due for cleanup",
                        5, job.getScalarPartitionScanCount());
                Assert.assertNull(cache(job, removed));
                Assert.assertSame(keptCache, cache(job, kept));
                Assert.assertSame(keptGenerations, generations(job, kept));
                Assert.assertEquals(1, cachedPartitionCount(job));

                Assert.assertFalse(job.cleanupTable(kept, keptPredicate));
                Assert.assertEquals("the retained view keeps its verdict and fills a freed slot",
                        6, job.getScalarPartitionScanCount());
                Assert.assertEquals(2, cachedPartitionCount(job));
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

    private static final class DiscoveryCountingList extends ObjList<TableToken> {
        private int readCount;

        @Override
        public TableToken getQuick(int index) {
            readCount++;
            return super.getQuick(index);
        }
    }

    // Count the operation that scans the key list instead of asserting wall-clock timings.
    private static final class RemovalCountingMap extends CharSequenceLongHashMap {
        private int removalCount;

        private RemovalCountingMap(int capacity) {
            super(capacity, 0.5, Long.MIN_VALUE);
        }

        @Override
        public void removeAt(int index) {
            removalCount++;
            super.removeAt(index);
        }
    }
}
